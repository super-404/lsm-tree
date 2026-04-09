package engine

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// realisticWriteCmd 描述一次更贴近真实业务的写操作。
//
// 这里不只模拟纯 Put，还会混入少量 Delete，用来更接近线上“覆盖写 + 删除”的写入形态。
type realisticWriteCmd struct {
	delete bool
	key    []byte
	value  []byte
}

// realisticWriteScenario 描述一个可重复执行的写入场景。
//
// 一个 scenario 由若干“批次”组成：
//   - 对单写场景，每个 batch 里只有 1 条命令
//   - 对批量写场景，每个 batch 里会有多条命令，最终走 Engine.Write(batch)
//
// benchmark 时会循环复用这些预构建好的批次，避免把大量随机数生成和字节构造时间混进测量结果。
type realisticWriteScenario struct {
	name              string
	batches           [][]realisticWriteCmd
	batchSize         int
	avgLogicalBytes   float64
	avgLogicalOps     float64
	totalLogicalBytes int64
	totalLogicalOps   int64
}

type realisticWriteBenchmarkRow struct {
	Name             string
	Iterations       int
	Elapsed          time.Duration
	AvgPerIter       time.Duration
	AvgLogicalBytes  int64
	AvgLogicalOps    float64
	LogicalMiBPerSec float64
	LogicalOpsPerSec float64
	Level0Files      int
	LevelGE1Files    int
}

// BenchmarkLSMEngineRealisticWriteHeavy 模拟“真实磁盘写入”场景下的大量数据写入。
//
// 和现有的轻量 benchmark 相比，这里刻意加入了几类更贴近实际业务的特征：
//   - 使用磁盘模式打开 DB，而不是纯内存模式
//   - 让写入真实经过 WAL -> memtable -> flush -> compaction
//   - key 分布带明显热点，模拟业务里的热 key / 热分区
//   - 冷 key 会持续增长，避免只测热点覆盖写
//   - value 大小不是单一固定值，而是混合 256B / 1KiB / 4KiB
//   - 既覆盖单条写，也覆盖批量写
//
// 建议运行方式：
//
//	go test ./internal/engine -run '^$' -bench BenchmarkLSMEngineRealisticWriteHeavy -benchmem
//
// 可选环境变量：
//
//	LSM_REALISTIC_SINGLE_TARGET_MIB=16
//	LSM_REALISTIC_BATCH16_TARGET_MIB=128
//	LSM_REALISTIC_BATCH32_TARGET_MIB=160
//	LSM_REALISTIC_WRITE_REPORT_PATH=bench/reports/realistic-write-benchmark.md
func BenchmarkLSMEngineRealisticWriteHeavy(b *testing.B) {
	if b.N != 1 {
		b.Skip("run this profile benchmark with -benchtime=1x")
	}

	singleTargetMiB := parsePositiveIntEnv(b, "LSM_REALISTIC_SINGLE_TARGET_MIB", 16)
	batch16TargetMiB := parsePositiveIntEnv(b, "LSM_REALISTIC_BATCH16_TARGET_MIB", 128)
	batch32TargetMiB := parsePositiveIntEnv(b, "LSM_REALISTIC_BATCH32_TARGET_MIB", 160)

	scenarios := []realisticWriteScenario{
		buildRealisticWriteScenario("single-upsert-hotspot", 20260409, 1, int64(singleTargetMiB)<<20),
		buildRealisticWriteScenario("batch-16-hotspot", 20260409, 16, int64(batch16TargetMiB)<<20),
		buildRealisticWriteScenario("batch-32-hotspot", 20260409, 32, int64(batch32TargetMiB)<<20),
	}
	rows := make([]realisticWriteBenchmarkRow, 0, len(scenarios))

	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			dir := fixedWALTestDir(b, "benchmark_"+sanitizeBenchName(scenario.name))
			d, err := Open(dir, &Options{Dir: dir})
			if err != nil {
				b.Fatalf("Open(%s) error: %v", dir, err)
			}
			defer func() { _ = d.Close() }()

			b.ReportAllocs()
			if scenario.totalLogicalBytes > 0 {
				b.SetBytes(scenario.totalLogicalBytes)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, batch := range scenario.batches {
					if len(batch) == 1 {
						applyRealisticWriteCmd(b, d, batch[0])
						continue
					}
					wb := d.NewBatch()
					for _, cmd := range batch {
						if cmd.delete {
							wb.Delete(cmd.key)
						} else {
							wb.Put(cmd.key, cmd.value)
						}
					}
					if err := d.Write(wb); err != nil {
						b.Fatalf("Write(batch) error: %v", err)
					}
				}
			}
			elapsed := b.Elapsed()
			waitForBackgroundIdleTB(b, d, 30*time.Second)

			// 这些指标不是 Go benchmark 默认提供的维度，但对“真实写入”场景更直观：
			//   - 逻辑吞吐（MiB/s）
			//   - 每次 benchmark 迭代平均包含多少条逻辑写命令
			logicalMiBPerSec := 0.0
			logicalOpsPerSec := 0.0
			if seconds := b.Elapsed().Seconds(); seconds > 0 {
				logicalMiBPerSec = (float64(scenario.totalLogicalBytes) * float64(b.N) / (1 << 20)) / seconds
				logicalOpsPerSec = (float64(scenario.totalLogicalOps) * float64(b.N)) / seconds
				b.ReportMetric(logicalMiBPerSec, "MiB/s(logical)")
				b.ReportMetric(logicalOpsPerSec, "logical_ops/s")
			}

			d.mu.Lock()
			level0 := 0
			levelGE1 := 0
			for _, table := range d.ssts {
				if table.Meta().Level == 0 {
					level0++
				} else {
					levelGE1++
				}
			}
			d.mu.Unlock()
			b.ReportMetric(float64(level0), "L0_files")
			b.ReportMetric(float64(levelGE1), "L1+_files")

			avgPerIter := time.Duration(0)
			if b.N > 0 {
				avgPerIter = elapsed / time.Duration(b.N)
			}
			rows = append(rows, realisticWriteBenchmarkRow{
				Name:             scenario.name,
				Iterations:       b.N,
				Elapsed:          elapsed,
				AvgPerIter:       avgPerIter,
				AvgLogicalBytes:  scenario.totalLogicalBytes,
				AvgLogicalOps:    float64(scenario.totalLogicalOps),
				LogicalMiBPerSec: logicalMiBPerSec,
				LogicalOpsPerSec: logicalOpsPerSec,
				Level0Files:      level0,
				LevelGE1Files:    levelGE1,
			})
		})
	}

	reportPath := realisticWriteBenchmarkReportPath(b)
	if err := writeRealisticWriteBenchmarkMarkdownReport(reportPath, rows); err != nil {
		b.Fatalf("write realistic benchmark markdown report error: %v", err)
	}
	b.Logf("markdown report written to %s", reportPath)
}

func applyRealisticWriteCmd(b *testing.B, d *Engine, cmd realisticWriteCmd) {
	b.Helper()
	if cmd.delete {
		if err := d.Delete(cmd.key); err != nil {
			b.Fatalf("Delete(%q) error: %v", cmd.key, err)
		}
		return
	}
	if err := d.Put(cmd.key, cmd.value); err != nil {
		b.Fatalf("Put(%q) error: %v", cmd.key, err)
	}
}

func buildRealisticWriteScenario(name string, seed int64, batchSize int, targetLogicalBytes int64) realisticWriteScenario {
	rng := rand.New(rand.NewSource(seed + int64(len(name))*97 + int64(batchSize)*131))

	// 用 Zipf 生成热点 key，模拟真实业务里“少数 key 非常热、大量 key 长尾分布”的情况。
	//
	// 这里把热点 key 和冷 key 混合：
	//   - 约 70% 命中热点 key（频繁覆盖写）
	//   - 约 25% 产生新的冷 key（持续扩表）
	//   - 约 5% 做删除（引入 tombstone）
	const (
		hotKeySpace = 50_000
		coldKeyBase = 2_000_000
		hotHitPct   = 70
		coldPutPct  = 25
		deletePct   = 5
	)

	zipf := rand.NewZipf(rng, 1.15, 4, hotKeySpace-1)
	nextColdID := coldKeyBase

	makeHotKey := func() []byte {
		return []byte(fmt.Sprintf("user:%08d", zipf.Uint64()))
	}
	makeColdKey := func() []byte {
		key := []byte(fmt.Sprintf("order:%08d", nextColdID))
		nextColdID++
		return key
	}

	// 现实里 value 往往不是完全固定大小，所以这里混合三档：
	//   - 70%: 512B
	//   - 25%: 2KiB
	//   - 5% : 8KiB
	//
	// 相比之前稍微放大了一档，目的是让一次完整 scenario 更容易跨过 memtable flush 阈值，
	// 让 benchmark 真正覆盖到 flush / compaction 路径，而不是只停留在 active memtable。
	makeValue := func(tag int) []byte {
		sizeRoll := rng.Intn(100)
		size := 512
		switch {
		case sizeRoll < 70:
			size = 512
		case sizeRoll < 95:
			size = 2048
		default:
			size = 8192
		}
		v := make([]byte, size)
		for i := range v {
			v[i] = byte('a' + (i+tag)%26)
		}
		return v
	}

	if targetLogicalBytes <= 0 {
		targetLogicalBytes = 1 << 20
	}

	batches := make([][]realisticWriteCmd, 0, 1024)
	var (
		totalBytes int64
		totalOps   int64
	)
	for i := 0; totalBytes < targetLogicalBytes; i++ {
		ops := make([]realisticWriteCmd, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			roll := rng.Intn(100)
			switch {
			case roll < hotHitPct:
				key := makeHotKey()
				value := makeValue(i*batchSize + j)
				ops = append(ops, realisticWriteCmd{key: key, value: value})
				totalBytes += int64(len(key) + len(value))
			case roll < hotHitPct+coldPutPct:
				key := makeColdKey()
				value := makeValue(i*batchSize + j)
				ops = append(ops, realisticWriteCmd{key: key, value: value})
				totalBytes += int64(len(key) + len(value))
			default:
				// 删除既可能落在热点，也可能落在较新的冷 key 上。
				// 这有助于 benchmark 真实经过 tombstone 写入和后续 compaction 处理。
				key := makeHotKey()
				if rng.Intn(10) < deletePct && nextColdID > coldKeyBase {
					key = []byte(fmt.Sprintf("order:%08d", coldKeyBase+rng.Intn(nextColdID-coldKeyBase)))
				}
				ops = append(ops, realisticWriteCmd{delete: true, key: key})
				totalBytes += int64(len(key))
			}
			totalOps++
		}
		batches = append(batches, ops)
	}

	avgLogicalBytes := float64(totalBytes) / float64(len(batches))
	avgLogicalOps := float64(totalOps) / float64(len(batches))
	return realisticWriteScenario{
		name:              name,
		batches:           batches,
		batchSize:         batchSize,
		avgLogicalBytes:   avgLogicalBytes,
		avgLogicalOps:     avgLogicalOps,
		totalLogicalBytes: totalBytes,
		totalLogicalOps:   totalOps,
	}
}

func sanitizeBenchName(name string) string {
	replacer := strings.NewReplacer("/", "_", " ", "_", ":", "_")
	return replacer.Replace(name)
}

func waitForBackgroundIdleTB(tb testing.TB, d *Engine, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		immutables := len(d.immutables)
		flushInFlight := d.flushInFlight
		compactionInFlight := d.compactionInFlight
		flushErr := d.flushErr
		d.mu.Unlock()
		if flushErr != nil {
			tb.Fatalf("background task failed during benchmark: %v", flushErr)
		}
		if immutables == 0 && !flushInFlight && !compactionInFlight {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	tb.Fatalf(
		"waitForBackgroundIdle timeout: immutables=%d flushInFlight=%v compactionInFlight=%v flushErr=%v",
		len(d.immutables),
		d.flushInFlight,
		d.compactionInFlight,
		d.flushErr,
	)
}

func realisticWriteBenchmarkReportPath(tb testing.TB) string {
	tb.Helper()
	if raw := strings.TrimSpace(os.Getenv("LSM_REALISTIC_WRITE_REPORT_PATH")); raw != "" {
		return raw
	}
	root, err := findRepoRoot()
	if err != nil {
		tb.Fatalf("findRepoRoot error: %v", err)
	}
	return filepath.Join(root, "bench", "reports", "realistic-write-benchmark.md")
}

func writeRealisticWriteBenchmarkMarkdownReport(path string, rows []realisticWriteBenchmarkRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	var sb strings.Builder
	sb.WriteString("# Realistic Write Benchmark Report\n\n")
	sb.WriteString(fmt.Sprintf("- Generated at: `%s`\n", time.Now().Format(time.RFC3339)))
	sb.WriteString("- Benchmark: `BenchmarkLSMEngineRealisticWriteHeavy`\n")
	sb.WriteString("- Workload shape: hot keys + cold growth + mixed value sizes + delete tombstones + disk mode (`WAL -> memtable -> flush -> compaction`)\n")
	sb.WriteString("- Semantics: each benchmark iteration runs one full prebuilt scenario workload\n")
	sb.WriteString("\n")
	sb.WriteString("| Scenario | Iterations | Elapsed | Avg/Iter | Avg Logical Bytes | Avg Logical Ops | Logical MiB/s | Logical Ops/s | L0 Files | L1+ Files |\n")
	sb.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf(
			"| %s | %d | %s | %s | %d | %.2f | %.3f | %.0f | %d | %d |\n",
			row.Name,
			row.Iterations,
			row.Elapsed,
			row.AvgPerIter,
			row.AvgLogicalBytes,
			row.AvgLogicalOps,
			row.LogicalMiBPerSec,
			row.LogicalOpsPerSec,
			row.Level0Files,
			row.LevelGE1Files,
		))
	}
	sb.WriteString("\n## Notes\n\n")
	sb.WriteString("- `Avg Logical Bytes` 表示每次 benchmark 迭代平均写入的逻辑负载大小，不等于最终落盘字节数。\n")
	sb.WriteString("- `Logical MiB/s` 和 `Logical Ops/s` 反映的是业务层吞吐，不包含额外元数据开销的单独拆分展示。\n")
	sb.WriteString("- `L0 Files` / `L1+ Files` 反映 benchmark 结束时的 SST 层级状态，可用来观察 flush 与 compaction 是否开始介入。\n")

	return os.WriteFile(path, []byte(sb.String()), 0o644)
}
