package compaction

import "lsm-tree/internal/iter"

// SliceEntryIterator 把已经排好序的 Entry 切片包装成统一的 EntryIterator。
//
// compaction 当前先用“先归并到内存切片，再整体写 SST”的做法，
// 因此这里需要一个轻量包装器把 []Entry 重新喂给 sst.WriteFile。
type SliceEntryIterator struct {
	entries []iter.Entry
	idx     int
}

// NewSliceEntryIterator 返回一个从切片头部开始顺序输出的 EntryIterator。
func NewSliceEntryIterator(entries []iter.Entry) *SliceEntryIterator {
	return &SliceEntryIterator{entries: entries}
}

func (it *SliceEntryIterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

func (it *SliceEntryIterator) Item() iter.Entry {
	if !it.Valid() {
		return iter.Entry{}
	}
	return it.entries[it.idx]
}

func (it *SliceEntryIterator) Next() {
	if !it.Valid() {
		return
	}
	it.idx++
}

func (it *SliceEntryIterator) Close() {}
