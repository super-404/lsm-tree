package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// getLatencyStats 是一次点查采样的简单统计摘要。
type getLatencyStats struct {
	Avg time.Duration
	P50 time.Duration
	P95 time.Duration
}

// iteratorScanStats 是一次全表扫描采样的摘要。
type iteratorScanStats struct {
	Avg          time.Duration
	P50          time.Duration
	P95          time.Duration
	ItemsPerScan int
	AvgPerItem   time.Duration
}

// slowdownProfileRow 是 Markdown 报告里的一行观测结果。
type slowdownProfileRow struct {
	SSTCount    int
	OldestHit   getLatencyStats
	NewestHit   getLatencyStats
	Miss        getLatencyStats
	FullScan    iteratorScanStats
	OldestBaseX float64
}

// BenchmarkLSMEngineNoCompactionSlowdownProfile 用来在本机观察：
// “在没有 compaction 的情况下，随着 SST 数量增长，读路径什么时候开始明显变慢”。
//
// 这是一个“一次性 benchmark profile”，建议显式使用：
//
//	go test ./internal/engine -run '^$' -bench BenchmarkLSMEngineNoCompactionSlowdownProfile -benchtime=1x
//
// 它不会追求 testing.B 的自动校准，而是固定按自定义 rounds 做采样，
// 并把结果整理成 Markdown 文件写到 bench/reports/ 下，方便直接保存与对比。
//
// 每到一个 checkpoint，会同时采样：
//  1. oldest-hit：命中最老 SST 中的 key，最能体现“越老越慢”
//  2. newest-hit：命中最新 SST 中的 key，作为近端命中参考
//  3. miss：完全不存在的 key，观察 Bloom Filter + 多表遍历的成本
//  4. full-scan：全表 Iterator 扫描，观察 scan 路径的退化拐点
//
// 可选环境变量：
//
//	LSM_SLOWDOWN_SST_COUNTS=1,2,4,8,12,16
//	LSM_SLOWDOWN_GET_ROUNDS=300
//	LSM_SLOWDOWN_SCAN_ROUNDS=20
//	LSM_SLOWDOWN_REPORT_PATH=bench/reports/no-compaction-slowdown.md
func BenchmarkLSMEngineNoCompactionSlowdownProfile(b *testing.B) {
	if b.N != 1 {
		b.Skip("run this profile benchmark with -benchtime=1x")
	}

	checkpoints := parseSlowdownCheckpoints(b, "1,2,4,8,12,16")
	getRounds := parsePositiveIntEnv(b, "LSM_SLOWDOWN_GET_ROUNDS", 200)
	scanRounds := parsePositiveIntEnv(b, "LSM_SLOWDOWN_SCAN_ROUNDS", 20)

	dir := fixedWALTestDir(b, "no_compaction_slowdown_profile")
	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	// 用 1MiB value 可以更快把 memtable 推到 64MiB 阈值，从而在本机较快制造多个 SST。
	// 每张 SST 大概只需要几十个 key，就能把“无 compaction 多 SST”现象跑出来。
	value := make([]byte, 1<<20)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	var (
		oldestHitKey []byte
		newestHitKey []byte
		baseOldest   time.Duration
		baseScan     time.Duration
		slow2xAt     int
		slow4xAt     int
		scan2xAt     int
		scan4xAt     int
		rows         []slowdownProfileRow
	)

	b.Logf("observing no-compaction slowdown with checkpoints=%v getRounds=%d scanRounds=%d", checkpoints, getRounds, scanRounds)
	b.Logf("%-8s %-16s %-16s %-16s %-16s %-16s %-12s %s", "ssts", "oldest(avg/p95)", "newest(avg/p95)", "miss(avg/p95)", "scan(avg/p95)", "scan/item", "items/scan", "oldest/base")

	for _, targetSSTs := range checkpoints {
		for {
			d.mu.Lock()
			current := len(d.ssts)
			d.mu.Unlock()
			if current >= targetSSTs {
				break
			}
			probeKey := forceFlushOneSSTForProfile(b, d, current+1, value)
			if oldestHitKey == nil {
				oldestHitKey = append([]byte(nil), probeKey...)
			}
			newestHitKey = append([]byte(nil), probeKey...)
		}

		oldestStats := measureGetLatencyStats(b, d, oldestHitKey, getRounds)
		newestStats := measureGetLatencyStats(b, d, newestHitKey, getRounds)
		missStats := measureGetLatencyStats(b, d, []byte("m-miss-never-exists"), getRounds)
		scanStats := measureIteratorScanStats(b, d, scanRounds)

		if baseOldest == 0 {
			baseOldest = oldestStats.Avg
		}
		if baseScan == 0 {
			baseScan = scanStats.Avg
		}
		if slow2xAt == 0 && baseOldest > 0 && oldestStats.Avg >= 2*baseOldest {
			slow2xAt = targetSSTs
		}
		if slow4xAt == 0 && baseOldest > 0 && oldestStats.Avg >= 4*baseOldest {
			slow4xAt = targetSSTs
		}
		if scan2xAt == 0 && baseScan > 0 && scanStats.Avg >= 2*baseScan {
			scan2xAt = targetSSTs
		}
		if scan4xAt == 0 && baseScan > 0 && scanStats.Avg >= 4*baseScan {
			scan4xAt = targetSSTs
		}

		oldestRatio := 1.0
		if baseOldest > 0 {
			oldestRatio = float64(oldestStats.Avg) / float64(baseOldest)
		}

		rows = append(rows, slowdownProfileRow{
			SSTCount:    targetSSTs,
			OldestHit:   oldestStats,
			NewestHit:   newestStats,
			Miss:        missStats,
			FullScan:    scanStats,
			OldestBaseX: oldestRatio,
		})

		b.Logf(
			"%-8d %-16s %-16s %-16s %-16s %-16s %-12d x%.2f",
			targetSSTs,
			formatLatencyPair(oldestStats.Avg, oldestStats.P95),
			formatLatencyPair(newestStats.Avg, newestStats.P95),
			formatLatencyPair(missStats.Avg, missStats.P95),
			formatLatencyPair(scanStats.Avg, scanStats.P95),
			scanStats.AvgPerItem,
			scanStats.ItemsPerScan,
			oldestRatio,
		)
	}

	if baseOldest > 0 {
		if slow2xAt > 0 {
			b.Logf("oldest-hit avg reached about 2x baseline at %d SSTs", slow2xAt)
		} else {
			b.Logf("oldest-hit avg did not reach 2x baseline within checkpoints=%v", checkpoints)
		}
		if slow4xAt > 0 {
			b.Logf("oldest-hit avg reached about 4x baseline at %d SSTs", slow4xAt)
		} else {
			b.Logf("oldest-hit avg did not reach 4x baseline within checkpoints=%v", checkpoints)
		}
	}
	if baseScan > 0 {
		if scan2xAt > 0 {
			b.Logf("iterator full-scan avg reached about 2x baseline at %d SSTs", scan2xAt)
		} else {
			b.Logf("iterator full-scan avg did not reach 2x baseline within checkpoints=%v", checkpoints)
		}
		if scan4xAt > 0 {
			b.Logf("iterator full-scan avg reached about 4x baseline at %d SSTs", scan4xAt)
		} else {
			b.Logf("iterator full-scan avg did not reach 4x baseline within checkpoints=%v", checkpoints)
		}
	}

	reportPath := slowdownReportPath(b)
	if err := writeSlowdownMarkdownReport(reportPath, checkpoints, getRounds, scanRounds, rows, slow2xAt, slow4xAt, scan2xAt, scan4xAt); err != nil {
		b.Fatalf("write slowdown markdown report error: %v", err)
	}
	b.Logf("markdown report written to %s", reportPath)

	if len(rows) > 0 {
		last := rows[len(rows)-1]
		b.ReportMetric(float64(last.OldestHit.Avg.Nanoseconds()), "oldest_hit_ns")
		b.ReportMetric(float64(last.Miss.Avg.Nanoseconds()), "miss_ns")
		b.ReportMetric(float64(last.FullScan.Avg.Nanoseconds()), "scan_ns")
	}
}

// forceFlushOneSSTForProfile 生成一张新的 SST，并返回这张 SST 中一个“唯一 probe key”。
//
// 设计上每张表都会包含：
//   - 一个很小的 key（保证表的 minKey 很低）
//   - 一个很大的 key（保证表的 maxKey 很高）
//   - 一个唯一 probe key（供后续 oldest/newest hit 采样）
//
// 这样所有 SST 的 key range 都会大范围重叠，更接近“没有 compaction 时 L0 文件彼此重叠”的压力场景。
func forceFlushOneSSTForProfile(tb testing.TB, d *Engine, tableSeq int, value []byte) []byte {
	tb.Helper()

	before := currentPublishedSSTCount(d)
	probeKey := []byte(fmt.Sprintf("m-probe-%06d", tableSeq))

	seedKeys := [][]byte{
		[]byte(fmt.Sprintf("a-low-%06d", tableSeq)),
		probeKey,
		[]byte(fmt.Sprintf("z-high-%06d", tableSeq)),
	}
	for _, key := range seedKeys {
		if err := d.Put(key, value); err != nil {
			tb.Fatalf("Put(%q) error while building profile SST: %v", key, err)
		}
		if rotationStarted(d) {
			waitForFlushStateTB(tb, d, before+1)
			return probeKey
		}
	}

	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("m-fill-%06d-%06d", tableSeq, i))
		if err := d.Put(key, value); err != nil {
			tb.Fatalf("Put(%q) error while filling profile SST: %v", key, err)
		}
		if rotationStarted(d) {
			waitForFlushStateTB(tb, d, before+1)
			return probeKey
		}
	}
}

func waitForFlushStateTB(tb testing.TB, e *Engine, wantSSTs int) {
	tb.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		gotSSTs := len(e.ssts)
		gotImmutables := len(e.immutables)
		flushInFlight := e.flushInFlight
		flushErr := e.flushErr
		e.mu.Unlock()
		if flushErr != nil {
			tb.Fatalf("flush failed: %v", flushErr)
		}
		if gotSSTs >= wantSSTs && gotImmutables == 0 && !flushInFlight {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tb.Fatalf("waitForFlushState timeout: ssts=%d immutables=%d flushInFlight=%v flushErr=%v", len(e.ssts), len(e.immutables), e.flushInFlight, e.flushErr)
}

func rotationStarted(d *Engine) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.immutables) > 0 || d.flushInFlight
}

func currentPublishedSSTCount(d *Engine) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.ssts)
}

func measureGetLatencyStats(tb testing.TB, d *Engine, key []byte, rounds int) getLatencyStats {
	tb.Helper()
	if rounds <= 0 {
		rounds = 1
	}
	samples := make([]time.Duration, 0, rounds)
	var total time.Duration
	for i := 0; i < rounds; i++ {
		start := time.Now()
		_, _ = d.Get(key)
		cost := time.Since(start)
		samples = append(samples, cost)
		total += cost
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return getLatencyStats{
		Avg: total / time.Duration(len(samples)),
		P50: percentileDuration(samples, 50),
		P95: percentileDuration(samples, 95),
	}
}

func measureIteratorScanStats(tb testing.TB, d *Engine, rounds int) iteratorScanStats {
	tb.Helper()
	if rounds <= 0 {
		rounds = 1
	}
	samples := make([]time.Duration, 0, rounds)
	var (
		total      time.Duration
		itemsCount int
	)
	for i := 0; i < rounds; i++ {
		start := time.Now()
		it := d.NewIterator(nil)
		if it == nil {
			tb.Fatal("NewIterator(nil) returned nil during slowdown profile")
		}
		count := 0
		for it.Valid() {
			_ = it.Item()
			count++
			it.Next()
		}
		it.Close()
		cost := time.Since(start)
		samples = append(samples, cost)
		total += cost
		itemsCount = count
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	avg := total / time.Duration(len(samples))
	perItem := time.Duration(0)
	if itemsCount > 0 {
		perItem = avg / time.Duration(itemsCount)
	}
	return iteratorScanStats{
		Avg:          avg,
		P50:          percentileDuration(samples, 50),
		P95:          percentileDuration(samples, 95),
		ItemsPerScan: itemsCount,
		AvgPerItem:   perItem,
	}
}

func formatLatencyPair(avg, p95 time.Duration) string {
	return fmt.Sprintf("%s/%s", avg, p95)
}

func percentileDuration(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if percentile <= 0 {
		return samples[0]
	}
	if percentile >= 100 {
		return samples[len(samples)-1]
	}
	idx := (len(samples) - 1) * percentile / 100
	return samples[idx]
}

func parseSlowdownCheckpoints(tb testing.TB, fallback string) []int {
	tb.Helper()
	raw := os.Getenv("LSM_SLOWDOWN_SST_COUNTS")
	if raw == "" {
		raw = fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil || v <= 0 {
			tb.Fatalf("invalid LSM_SLOWDOWN_SST_COUNTS item %q", part)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		tb.Fatal("no valid slowdown checkpoints")
	}
	sort.Ints(out)
	dedup := out[:0]
	var prev int
	for i, v := range out {
		if i == 0 || v != prev {
			dedup = append(dedup, v)
			prev = v
		}
	}
	return dedup
}

func parsePositiveIntEnv(tb testing.TB, key string, fallback int) int {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		tb.Fatalf("invalid %s=%q", key, raw)
	}
	return v
}

func slowdownReportPath(tb testing.TB) string {
	tb.Helper()
	if raw := strings.TrimSpace(os.Getenv("LSM_SLOWDOWN_REPORT_PATH")); raw != "" {
		return raw
	}
	root, err := findRepoRoot()
	if err != nil {
		tb.Fatalf("findRepoRoot error: %v", err)
	}
	return filepath.Join(root, "bench", "reports", "no-compaction-slowdown.md")
}

func writeSlowdownMarkdownReport(
	path string,
	checkpoints []int,
	getRounds int,
	scanRounds int,
	rows []slowdownProfileRow,
	slow2xAt int,
	slow4xAt int,
	scan2xAt int,
	scan4xAt int,
) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	var sb strings.Builder
	sb.WriteString("# No Compaction Slowdown Report\n\n")
	sb.WriteString(fmt.Sprintf("- Generated at: `%s`\n", time.Now().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("- Checkpoints: `%v`\n", checkpoints))
	sb.WriteString(fmt.Sprintf("- Get rounds per checkpoint: `%d`\n", getRounds))
	sb.WriteString(fmt.Sprintf("- Full-scan rounds per checkpoint: `%d`\n", scanRounds))
	sb.WriteString("\n")
	sb.WriteString("| SSTs | Oldest Hit Avg | Oldest Hit P95 | Newest Hit Avg | Newest Hit P95 | Miss Avg | Miss P95 | Full Scan Avg | Full Scan P95 | Scan Avg/Item | Items/Scan | Oldest/Base |\n")
	sb.WriteString("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf(
			"| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | x%.2f |\n",
			row.SSTCount,
			row.OldestHit.Avg,
			row.OldestHit.P95,
			row.NewestHit.Avg,
			row.NewestHit.P95,
			row.Miss.Avg,
			row.Miss.P95,
			row.FullScan.Avg,
			row.FullScan.P95,
			row.FullScan.AvgPerItem,
			row.FullScan.ItemsPerScan,
			row.OldestBaseX,
		))
	}
	sb.WriteString("\n## Summary\n\n")
	if slow2xAt > 0 {
		sb.WriteString(fmt.Sprintf("- `oldest-hit` average reached about `2x` baseline at `%d` SSTs.\n", slow2xAt))
	} else {
		sb.WriteString("- `oldest-hit` average did not reach `2x` baseline within the configured checkpoints.\n")
	}
	if slow4xAt > 0 {
		sb.WriteString(fmt.Sprintf("- `oldest-hit` average reached about `4x` baseline at `%d` SSTs.\n", slow4xAt))
	} else {
		sb.WriteString("- `oldest-hit` average did not reach `4x` baseline within the configured checkpoints.\n")
	}
	if scan2xAt > 0 {
		sb.WriteString(fmt.Sprintf("- `full-scan` average reached about `2x` baseline at `%d` SSTs.\n", scan2xAt))
	} else {
		sb.WriteString("- `full-scan` average did not reach `2x` baseline within the configured checkpoints.\n")
	}
	if scan4xAt > 0 {
		sb.WriteString(fmt.Sprintf("- `full-scan` average reached about `4x` baseline at `%d` SSTs.\n", scan4xAt))
	} else {
		sb.WriteString("- `full-scan` average did not reach `4x` baseline within the configured checkpoints.\n")
	}

	return os.WriteFile(path, []byte(sb.String()), 0o644)
}
