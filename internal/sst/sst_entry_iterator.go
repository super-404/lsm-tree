package sst

import (
	"bytes"
	lsmiter "lsm-tree/internal/iter"
	"os"
)

// Entries 返回整张 SST 的最新逻辑记录视图。
//
// 注意这里的“最新”仅表示单张 SST 内的记录语义：
//   - 单表内 key 不重复
//   - Put/Delete 都会保留
//   - 不做跨表新旧裁决
func (t *Table) Entries() lsmiter.EntryIterator {
	return t.EntriesRange(nil, nil)
}

// EntriesRange 返回范围 [start, end) 上的最新逻辑记录视图。
func (t *Table) EntriesRange(start, end []byte) lsmiter.EntryIterator {
	if t == nil {
		return &entryIterator{}
	}
	f, err := os.Open(t.path)
	if err != nil {
		return &entryIterator{}
	}
	startOffset, remaining := t.dataScanWindowForRangeStart(start)
	if _, err := f.Seek(int64(startOffset), 0); err != nil {
		_ = f.Close()
		return &entryIterator{}
	}
	it := &entryIterator{
		f:         f,
		remaining: remaining,
		start:     cloneBytes(start),
		end:       cloneBytes(end),
	}
	it.advance()
	return it
}

func (it *entryIterator) Valid() bool { return it != nil && it.valid }

func (it *entryIterator) Item() lsmiter.Entry {
	if !it.Valid() {
		return lsmiter.Entry{}
	}
	return it.entry
}

func (it *entryIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *entryIterator) Close() {
	if it == nil {
		return
	}
	if it.f != nil {
		_ = it.f.Close()
	}
	it.valid = false
	it.entry = lsmiter.Entry{}
}

func (it *entryIterator) advance() {
	it.valid = false
	it.entry = lsmiter.Entry{}
	for it.f != nil && it.remaining > 0 {
		entry, n, err := readEntry(it.f)
		if err != nil {
			it.Close()
			return
		}
		it.remaining -= uint64(n)
		if it.start != nil && bytes.Compare(entry.Key, it.start) < 0 {
			continue
		}
		if it.end != nil && bytes.Compare(entry.Key, it.end) >= 0 {
			it.Close()
			return
		}
		it.entry = entry
		it.valid = true
		return
	}
}
