package sst

import (
	"bytes"
	lsmiter "lsm-tree/internal/iter"
)

func (it *valueIterator) Valid() bool { return it != nil && it.valid }

// Item 返回当前 SST 迭代位置对应的可见键值对。
//
// 这里返回结构体而不是分散的 Key/Value getter，是为了让 memtable 与 sst
// 这两类数据源在引擎层说同一种“值迭代协议”，避免 engine 再额外维护一套私有接口。
func (it *valueIterator) Item() lsmiter.Value {
	var zero lsmiter.Value
	if !it.Valid() {
		return zero
	}
	return it.item
}

func (it *valueIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *valueIterator) Close() {
	if it == nil {
		return
	}
	if it.f != nil {
		_ = it.f.Close()
	}
	it.valid = false
	it.item = lsmiter.Value{}
}

// advance 顺序读取 SST 的下一条可见 Put。
//
// 这层逻辑很像 memtable.ValueIterator，只是输入源从内存跳表换成了磁盘文件：
//   - SST 内没有重复 key，因此不需要再做同 key 去重
//   - 如果读到 Delete tombstone，则直接跳过
//   - 区间边界仍保持 [start, end) 语义
func (it *valueIterator) advance() {
	it.valid = false
	it.item = lsmiter.Value{}
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
		if entry.Op != lsmiter.OpPut {
			continue
		}
		it.item.Key = entry.Key
		it.item.Value = entry.Value
		it.valid = true
		return
	}
}
