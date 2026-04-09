package compaction

import (
	"bytes"
	"lsm-tree/internal/sst"
)

// PickerOptions 控制 leveled compaction picker 的基础参数。
type PickerOptions struct {
	Level0FileNumCompactionTrigger int
	MaxLevels                      int
	TargetLevelBytesBase           uint64
	TargetLevelBytesMultiplier     uint64
}

// Plan 描述一次 compaction 需要消费的输入表集合。
//
// 这里刻意不把“如何发布结果”也塞进 Plan：
//   - 输入表如何挑选，属于 compaction planner 的职责
//   - 新 SST 如何写入、如何更新 MANIFEST，仍属于 engine 的发布职责
//
// 这样拆开后，compaction 子目录里的代码只关心“该 compact 哪些表、如何归并它们”，
// engine 这一层继续掌握数据库的全局状态切换。
type Plan struct {
	SourceLevel uint32
	OutputLevel uint32
	Bottommost  bool
	MinKey      []byte
	MaxKey      []byte
	Inputs      []*sst.Table
	InputIDs    map[uint64]struct{}
	InputPaths  []string
}

// Pick 根据当前已发布 SST 视图挑选下一轮 leveled compaction。
//
// 选择顺序：
//  1. 先看 L0 是否达到文件数阈值
//  2. 再看 L1+ 哪一层的 score 最大且 > 1
//
// 当前 picker 仍然保持保守：
//   - L0 触发时，一次性吃掉全部 L0
//   - L1+ 每次只从 source level 选择一张表，再加上 next level 的所有重叠表
func Pick(tables []*sst.Table, opts PickerOptions) *Plan {
	if opts.Level0FileNumCompactionTrigger <= 0 {
		return nil
	}
	if opts.MaxLevels <= 1 {
		opts.MaxLevels = 2
	}
	if opts.TargetLevelBytesBase == 0 {
		opts.TargetLevelBytesBase = 1
	}
	if opts.TargetLevelBytesMultiplier == 0 {
		opts.TargetLevelBytesMultiplier = 10
	}

	if plan := pickL0ToL1(tables, opts.Level0FileNumCompactionTrigger); plan != nil {
		markBottommost(plan, tables)
		return plan
	}

	levelBytes := make(map[uint32]uint64)
	var maxLevel uint32
	for _, table := range tables {
		level := table.Meta().Level
		if level > maxLevel {
			maxLevel = level
		}
		if level == 0 {
			continue
		}
		levelBytes[level] += table.Meta().FileSize
	}

	var (
		bestLevel uint32
		bestScore float64
	)
	for level := uint32(1); level < uint32(opts.MaxLevels-1); level++ {
		if level > maxLevel {
			break
		}
		targetBytes := targetLevelBytes(level, opts)
		if targetBytes == 0 {
			continue
		}
		score := float64(levelBytes[level]) / float64(targetBytes)
		if score > 1.0 && score > bestScore {
			bestScore = score
			bestLevel = level
		}
	}
	if bestLevel == 0 {
		return nil
	}

	plan := pickLevelToNext(tables, bestLevel)
	if plan == nil {
		return nil
	}
	markBottommost(plan, tables)
	return plan
}

func pickL0ToL1(tables []*sst.Table, trigger int) *Plan {
	var level0 []*sst.Table
	for _, table := range tables {
		if table.Meta().Level == 0 {
			level0 = append(level0, table)
		}
	}
	if len(level0) < trigger {
		return nil
	}

	// L0 可能彼此重叠，所以先把所有 L0 的总范围求出来，再用这个总范围去挑选重叠的 L1。
	totalMin := cloneBytes(level0[0].Meta().MinKey)
	totalMax := cloneBytes(level0[0].Meta().MaxKey)
	for _, table := range level0[1:] {
		meta := table.Meta()
		if bytes.Compare(meta.MinKey, totalMin) < 0 {
			totalMin = cloneBytes(meta.MinKey)
		}
		if bytes.Compare(meta.MaxKey, totalMax) > 0 {
			totalMax = cloneBytes(meta.MaxKey)
		}
	}

	plan := &Plan{
		SourceLevel: 0,
		OutputLevel: 1,
		MinKey:      totalMin,
		MaxKey:      totalMax,
		InputIDs:    make(map[uint64]struct{}),
	}
	for _, table := range tables {
		meta := table.Meta()
		switch meta.Level {
		case 0:
			plan.addInput(table)
		case 1:
			if rangesOverlap(totalMin, totalMax, meta.MinKey, meta.MaxKey) {
				plan.addInput(table)
			}
		}
	}
	if len(plan.Inputs) == 0 {
		return nil
	}
	return plan
}

func pickLevelToNext(tables []*sst.Table, sourceLevel uint32) *Plan {
	outputLevel := sourceLevel + 1

	var source *sst.Table
	for i := len(tables) - 1; i >= 0; i-- {
		if tables[i].Meta().Level == sourceLevel {
			source = tables[i]
			break
		}
	}
	if source == nil {
		return nil
	}

	sourceMeta := source.Meta()
	plan := &Plan{
		SourceLevel: sourceLevel,
		OutputLevel: outputLevel,
		MinKey:      cloneBytes(sourceMeta.MinKey),
		MaxKey:      cloneBytes(sourceMeta.MaxKey),
		InputIDs:    make(map[uint64]struct{}),
	}
	plan.addInput(source)

	for _, table := range tables {
		meta := table.Meta()
		if meta.Level != outputLevel {
			continue
		}
		if rangesOverlap(plan.MinKey, plan.MaxKey, meta.MinKey, meta.MaxKey) {
			plan.addInput(table)
			if bytes.Compare(meta.MinKey, plan.MinKey) < 0 {
				plan.MinKey = cloneBytes(meta.MinKey)
			}
			if bytes.Compare(meta.MaxKey, plan.MaxKey) > 0 {
				plan.MaxKey = cloneBytes(meta.MaxKey)
			}
		}
	}
	return plan
}

func (p *Plan) addInput(table *sst.Table) {
	if p == nil || table == nil {
		return
	}
	meta := table.Meta()
	if _, exists := p.InputIDs[meta.ID]; exists {
		return
	}
	p.Inputs = append(p.Inputs, table)
	p.InputIDs[meta.ID] = struct{}{}
	p.InputPaths = append(p.InputPaths, meta.Path)
}

func markBottommost(plan *Plan, tables []*sst.Table) {
	if plan == nil {
		return
	}
	plan.Bottommost = true
	for _, table := range tables {
		meta := table.Meta()
		if meta.Level <= plan.OutputLevel {
			continue
		}
		if rangesOverlap(plan.MinKey, plan.MaxKey, meta.MinKey, meta.MaxKey) {
			plan.Bottommost = false
			return
		}
	}
}

func targetLevelBytes(level uint32, opts PickerOptions) uint64 {
	if level == 0 {
		return 0
	}
	bytes := opts.TargetLevelBytesBase
	for lv := uint32(1); lv < level; lv++ {
		bytes *= opts.TargetLevelBytesMultiplier
	}
	return bytes
}

func rangesOverlap(minA, maxA, minB, maxB []byte) bool {
	if len(minA) == 0 || len(maxA) == 0 || len(minB) == 0 || len(maxB) == 0 {
		return false
	}
	if bytes.Compare(maxA, minB) < 0 {
		return false
	}
	if bytes.Compare(maxB, minA) < 0 {
		return false
	}
	return true
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
