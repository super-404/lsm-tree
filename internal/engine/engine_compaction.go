package engine

import (
	enginecompaction "lsm-tree/internal/compaction"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/sst"
	"os"
	"path"
	"path/filepath"
)

// compactionTask 描述一次后台 compaction 的完整输入。
//
// 和 flushTask 的区别在于：
//   - flushTask 对应的是一张 immutable memtable
//   - compactionTask 对应的是一组已经发布过的 SST
//
// 这里把“输入选择结果”和“预分配的新 SST ID”绑在一起，方便 engine
// 在发布阶段一次性完成新表写入、MANIFEST 替换和旧文件清理。
type compactionTask struct {
	plan *enginecompaction.Plan
}

const (
	maxCompactionLevels               = 7
	targetLevelBytesBase       uint64 = 512 << 20 // 512MB：控制 L1 目标总大小，避免单轮 L0->L1 后立刻继续下推
	targetLevelBytesMultiplier        = 10
	targetFileSizeBase         uint64 = 64 << 20 // 64MB：compaction 输出按目标文件大小切分
	targetFileSizeMultiplier          = 2
)

// maybeCompactL0ToL1 在后台 flush 空闲时尝试触发一轮最小版 L0 -> L1 compaction。
//
// 触发条件：
//   - 当前没有 compaction 在执行
//   - L0 文件数达到 level0CompactionTrigger
func (e *Engine) maybeCompactL0ToL1() error {
	task := e.prepareL0Compaction()
	if task == nil {
		return nil
	}
	return e.runL0Compaction(task)
}

// prepareL0Compaction 负责在持锁状态下完成“挑选输入 + 预留 SST ID”。
//
// 它不会真正执行 I/O，只做：
//   - 判断当前是否适合 compact
//   - 复制一份输入表集合
//   - 预分配新的 output SST ID
func (e *Engine) prepareL0Compaction() *compactionTask {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed || e.closing || e.compactionInFlight || e.flushErr != nil {
		return nil
	}

	plan := enginecompaction.Pick(e.ssts, enginecompaction.PickerOptions{
		Level0FileNumCompactionTrigger: level0CompactionTrigger,
		MaxLevels:                      maxCompactionLevels,
		TargetLevelBytesBase:           targetLevelBytesBase,
		TargetLevelBytesMultiplier:     targetLevelBytesMultiplier,
	})
	if plan == nil {
		return nil
	}

	e.compactionInFlight = true

	return &compactionTask{
		plan: plan,
	}
}

// runL0Compaction 执行一轮完整的 L0 -> L1 归并与发布。
//
// 发布顺序和 flush 一样保持保守：
//  1. 先写出新的 SST
//  2. 再持久化新的 MANIFEST
//  3. 最后切换内存视图
//  4. 旧 SST 文件仅做最佳努力删除
func (e *Engine) runL0Compaction(task *compactionTask) error {
	if task == nil || task.plan == nil {
		return nil
	}

	entries, err := enginecompaction.MergeTablesToEntries(task.plan.Inputs, task.plan.Bottommost)
	if err != nil {
		e.endCompaction()
		return err
	}

	metas, tables, err := e.writeCompactionOutputs(entries, task.plan.OutputLevel)
	if err != nil {
		e.endCompaction()
		return err
	}

	e.mu.Lock()
	nextManifest := cloneManifest(e.manifest)
	nextManifest.Tables = removeManifestTablesByID(nextManifest.Tables, task.plan.InputIDs)
	nextManifest.Tables = append(append([]sst.Meta(nil), metas...), nextManifest.Tables...)
	e.mu.Unlock()

	if err := saveManifest(e.dir, nextManifest); err != nil {
		e.endCompaction()
		return err
	}

	e.mu.Lock()
	e.manifest = nextManifest
	e.ssts = append(append([]*sst.Table(nil), tables...), removeTablesByID(e.ssts, task.plan.InputIDs)...)
	e.compactionInFlight = false
	e.flushCond.Broadcast()
	e.mu.Unlock()

	for _, relPath := range task.plan.InputPaths {
		_ = removePublishedSSTFile(e.dir, relPath)
	}
	return nil
}

func (e *Engine) writeCompactionOutputs(entries []lsmiter.Entry, level uint32) ([]sst.Meta, []*sst.Table, error) {
	if len(entries) == 0 {
		return nil, nil, nil
	}

	groups := splitCompactionEntries(entries, targetFileSizeForLevel(level))
	if len(groups) == 0 {
		return nil, nil, nil
	}

	e.mu.Lock()
	nextManifest := cloneManifest(e.manifest)
	ids := make([]uint64, len(groups))
	for i := range groups {
		ids[i] = nextManifest.NextSSTID
		nextManifest.NextSSTID++
	}
	e.manifest = nextManifest
	e.mu.Unlock()

	metas := make([]sst.Meta, 0, len(groups))
	tables := make([]*sst.Table, 0, len(groups))
	for i, group := range groups {
		relPath := path.Join("sst", formatSSTFilename(ids[i]))
		fullPath := filepath.Join(e.dir, filepath.FromSlash(relPath))
		meta, err := sst.WriteFile(fullPath, ids[i], level, enginecompaction.NewSliceEntryIterator(group))
		if err != nil {
			return nil, nil, err
		}
		meta.Path = relPath

		table, err := sst.Open(fullPath)
		if err != nil {
			return nil, nil, err
		}
		table.SetPublishedMeta(meta)
		metas = append(metas, meta)
		tables = append(tables, table)
	}
	return metas, tables, nil
}

func splitCompactionEntries(entries []lsmiter.Entry, targetBytes uint64) [][]lsmiter.Entry {
	if len(entries) == 0 {
		return nil
	}
	if targetBytes == 0 {
		return [][]lsmiter.Entry{entries}
	}

	var (
		out       [][]lsmiter.Entry
		start     int
		groupSize uint64
	)
	for i, entry := range entries {
		encodedLen := estimatedEntryEncodedBytes(entry)
		if i > start && groupSize+encodedLen > targetBytes {
			out = append(out, entries[start:i])
			start = i
			groupSize = 0
		}
		groupSize += encodedLen
	}
	out = append(out, entries[start:])
	return out
}

func estimatedEntryEncodedBytes(entry lsmiter.Entry) uint64 {
	// record 格式：
	//   op(1B) + keyLen(4B) + key + [valueLen(4B) + value]
	size := uint64(1 + 4 + len(entry.Key))
	if entry.Op == lsmiter.OpPut {
		size += uint64(4 + len(entry.Value))
	}
	return size
}

func targetFileSizeForLevel(level uint32) uint64 {
	size := targetFileSizeBase
	for lv := uint32(1); lv < level; lv++ {
		size *= targetFileSizeMultiplier
	}
	return size
}

func (e *Engine) endCompaction() {
	e.mu.Lock()
	e.compactionInFlight = false
	e.flushCond.Broadcast()
	e.mu.Unlock()
}

func removeManifestTablesByID(in []sst.Meta, remove map[uint64]struct{}) []sst.Meta {
	out := make([]sst.Meta, 0, len(in))
	for _, meta := range in {
		if _, shouldRemove := remove[meta.ID]; shouldRemove {
			continue
		}
		out = append(out, meta)
	}
	return out
}

func removeTablesByID(in []*sst.Table, remove map[uint64]struct{}) []*sst.Table {
	out := make([]*sst.Table, 0, len(in))
	for _, table := range in {
		if _, shouldRemove := remove[table.Meta().ID]; shouldRemove {
			continue
		}
		out = append(out, table)
	}
	return out
}

func removePublishedSSTFile(dir, relPath string) error {
	if dir == "" || relPath == "" {
		return nil
	}
	fullPath := filepath.Join(dir, filepath.FromSlash(relPath))
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
