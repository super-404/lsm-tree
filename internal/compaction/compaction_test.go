package compaction

import (
	"fmt"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/sst"
	"path/filepath"
	"testing"
)

type sliceEntryIterator struct {
	entries []lsmiter.Entry
	idx     int
}

func (it *sliceEntryIterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

func (it *sliceEntryIterator) Item() lsmiter.Entry {
	if !it.Valid() {
		return lsmiter.Entry{}
	}
	return it.entries[it.idx]
}

func (it *sliceEntryIterator) Next() {
	if it.Valid() {
		it.idx++
	}
}

func (it *sliceEntryIterator) Close() {}

func makeTestTable(t *testing.T, dir string, id uint64, level uint32, entries []lsmiter.Entry) *sst.Table {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", id))
	meta, err := sst.WriteFile(path, id, level, &sliceEntryIterator{entries: entries})
	if err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
	meta.Path = fmt.Sprintf("sst/%06d.sst", id)
	table, err := sst.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", path, err)
	}
	table.SetPublishedMeta(meta)
	return table
}

func collectEntries(t *testing.T, table *sst.Table) []lsmiter.Entry {
	t.Helper()
	it := table.Entries()
	defer it.Close()
	var out []lsmiter.Entry
	for it.Valid() {
		out = append(out, it.Item())
		it.Next()
	}
	return out
}

func TestPickL0ToL1IncludesOverlappingL1(t *testing.T) {
	dir := t.TempDir()
	l0a := makeTestTable(t, dir, 1, 0, []lsmiter.Entry{{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut}})
	l0b := makeTestTable(t, dir, 2, 0, []lsmiter.Entry{{Key: []byte("m"), Value: []byte("2"), Op: lsmiter.OpPut}})
	l0c := makeTestTable(t, dir, 3, 0, []lsmiter.Entry{{Key: []byte("z"), Value: []byte("3"), Op: lsmiter.OpPut}})
	l0d := makeTestTable(t, dir, 4, 0, []lsmiter.Entry{{Key: []byte("t"), Value: []byte("4"), Op: lsmiter.OpPut}})
	l1Overlap := makeTestTable(t, dir, 5, 1, []lsmiter.Entry{{Key: []byte("p"), Value: []byte("5"), Op: lsmiter.OpPut}})
	l1Far := makeTestTable(t, dir, 6, 1, []lsmiter.Entry{{Key: []byte("zzzz"), Value: []byte("6"), Op: lsmiter.OpPut}})

	tables := []*sst.Table{l0d, l0c, l0b, l0a, l1Overlap, l1Far}
	plan := Pick(tables, PickerOptions{
		Level0FileNumCompactionTrigger: 4,
		MaxLevels:                      7,
		TargetLevelBytesBase:           1,
		TargetLevelBytesMultiplier:     10,
	})
	if plan == nil {
		t.Fatal("Pick() = nil, want L0->L1 compaction plan")
	}
	if plan.SourceLevel != 0 || plan.OutputLevel != 1 {
		t.Fatalf("plan levels = %d -> %d, want 0 -> 1", plan.SourceLevel, plan.OutputLevel)
	}
	if len(plan.Inputs) != 5 {
		t.Fatalf("plan input count = %d, want 5 (4 L0 + 1 overlapping L1)", len(plan.Inputs))
	}
	if _, ok := plan.InputIDs[l1Far.Meta().ID]; ok {
		t.Fatal("far L1 table should not be included in L0 compaction plan")
	}
	if _, ok := plan.InputIDs[l1Overlap.Meta().ID]; !ok {
		t.Fatal("overlapping L1 table should be included in L0 compaction plan")
	}
}

func TestPickUsesScoreForHigherLevels(t *testing.T) {
	dir := t.TempDir()
	l1 := makeTestTable(t, dir, 10, 1, []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("x"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Value: make([]byte, 128), Op: lsmiter.OpPut},
	})
	l2 := makeTestTable(t, dir, 11, 2, []lsmiter.Entry{{Key: []byte("a"), Value: []byte("older"), Op: lsmiter.OpPut}})
	l3 := makeTestTable(t, dir, 12, 3, []lsmiter.Entry{{Key: []byte("a1"), Value: []byte("deep"), Op: lsmiter.OpPut}})

	plan := Pick([]*sst.Table{l1, l2, l3}, PickerOptions{
		Level0FileNumCompactionTrigger: 4,
		MaxLevels:                      7,
		TargetLevelBytesBase:           1,
		TargetLevelBytesMultiplier:     2,
	})
	if plan == nil {
		t.Fatal("Pick() = nil, want score-based L1->L2 plan")
	}
	if plan.SourceLevel != 1 || plan.OutputLevel != 2 {
		t.Fatalf("plan levels = %d -> %d, want 1 -> 2", plan.SourceLevel, plan.OutputLevel)
	}
	if len(plan.Inputs) != 2 {
		t.Fatalf("plan input count = %d, want 2 (source L1 + overlapping L2)", len(plan.Inputs))
	}
	if plan.Bottommost {
		t.Fatal("plan.Bottommost = true, want false because overlapping deeper L3 exists")
	}
}

func TestMergeTablesToEntriesKeepsLatestAndDropsBottommostDeletes(t *testing.T) {
	dir := t.TempDir()
	older := makeTestTable(t, dir, 20, 1, []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("v1"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Value: []byte("v2"), Op: lsmiter.OpPut},
	})
	newer := makeTestTable(t, dir, 21, 0, []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("v3"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Op: lsmiter.OpDelete},
	})

	entries, err := MergeTablesToEntries([]*sst.Table{newer, older}, false)
	if err != nil {
		t.Fatalf("MergeTablesToEntries() error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries len = %d, want 2", len(entries))
	}
	if string(entries[0].Key) != "a" || string(entries[0].Value) != "v3" || entries[0].Op != lsmiter.OpPut {
		t.Fatalf("entries[0] = %+v, want a=v3 put", entries[0])
	}
	if string(entries[1].Key) != "b" || entries[1].Op != lsmiter.OpDelete {
		t.Fatalf("entries[1] = %+v, want b=delete", entries[1])
	}

	bottommostEntries, err := MergeTablesToEntries([]*sst.Table{newer, older}, true)
	if err != nil {
		t.Fatalf("MergeTablesToEntries(bottommost) error: %v", err)
	}
	if len(bottommostEntries) != 1 {
		t.Fatalf("bottommost entries len = %d, want 1", len(bottommostEntries))
	}
	if string(bottommostEntries[0].Key) != "a" || string(bottommostEntries[0].Value) != "v3" {
		t.Fatalf("bottommostEntries[0] = %+v, want only a=v3", bottommostEntries[0])
	}

	// 额外验证这批 entry 再写回 SST 后，确实不会把 b 写进去。
	table := makeTestTable(t, dir, 22, 2, bottommostEntries)
	got := collectEntries(t, table)
	if len(got) != 1 || string(got[0].Key) != "a" {
		t.Fatalf("written bottommost table entries = %+v, want only key a", got)
	}
}
