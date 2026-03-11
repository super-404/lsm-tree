package memtable

import (
	"bytes"
	"lsm-tree/internal/skl"
	"math"
)

// Op 命令类型：Put 写入，Delete 删除标记
type Op int

const (
	OpPut    Op = 1 // 1 表示写入，便于识别
	OpDelete Op = 0 // 0 表示删除，便于识别
)

// record 跳表存储的一条命令：(key, seq, op)，Put 时带 value
type record struct {
	Key   []byte
	Seq   uint64
	Op    Op
	Value []byte
}

// 比较：先按 Key 升序，再按 Seq 降序（同 key 下新命令排前面）
func compareRecord(a, b record) int {
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if a.Seq > b.Seq {
		return -1
	}
	if a.Seq < b.Seq {
		return 1
	}
	return 0
}

type memtableImpl struct {
	skl          *skl.SkiplistCmp[record]
	nextSeq      uint64
	payloadBytes int // 实时维护：所有 Key/Value 底层数组的字节数（跳表只计结点与 slice 头，不含此处）
}

type memtableIterator struct {
	base    *skl.Iterator[record]
	end     []byte
	copyOut bool
	hasPrev bool
	prevKey []byte
	valid   bool
	key     []byte
	value   []byte
}

// 编译期检查：*memtableImpl 实现 FlushIterable
var _ FlushIterable = (*memtableImpl)(nil)

// NewMemtable 构造一个新的 Memtable 实例。
func NewMemtable() Memtable {
	return &memtableImpl{skl: skl.NewSkiplistCmp(compareRecord)}
}

func (m *memtableImpl) Put(key, value []byte) {
	m.nextSeq++
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.skl.Add(record{Key: k, Seq: m.nextSeq, Op: OpPut, Value: v})
	m.payloadBytes += len(k) + len(v)
}

func (m *memtableImpl) Get(key []byte) ([]byte, bool) {
	// 同 key 下 seq 大的排前面；用 Seq=MaxUint64 做哨兵，LowerBound 返回该 key 下第一条（即最新）
	r, ok := m.skl.LowerBound(record{Key: key, Seq: math.MaxUint64})
	if !ok || !bytes.Equal(r.Key, key) {
		return nil, false
	}
	if r.Op == OpDelete {
		return nil, false
	}
	// 拷贝后返回，避免调用方改写内部数据
	out := make([]byte, len(r.Value))
	copy(out, r.Value)
	return out, true
}

// GetLatest 返回本层该 key 的最新命令（Put/Delete），便于上层遇 tombstone 时跨层返回 NotFound。
func (m *memtableImpl) GetLatest(key []byte) ([]byte, Op, bool) {
	r, ok := m.skl.LowerBound(record{Key: key, Seq: math.MaxUint64})
	if !ok || !bytes.Equal(r.Key, key) {
		return nil, 0, false
	}
	if r.Op == OpDelete {
		return nil, OpDelete, true
	}
	out := make([]byte, len(r.Value))
	copy(out, r.Value)
	return out, OpPut, true
}

func (m *memtableImpl) Delete(key []byte) {
	m.nextSeq++
	k := make([]byte, len(key))
	copy(k, key)
	m.skl.Add(record{Key: k, Seq: m.nextSeq, Op: OpDelete, Value: nil})
	m.payloadBytes += len(k)
}

func (m *memtableImpl) Len() int {
	return m.skl.Len()
}

// SizeBytes 返回估算的占用字节数：跳表结构 + 所有 Key/Value 的 payload
func (m *memtableImpl) SizeBytes() int {
	return m.skl.SizeBytes() + m.payloadBytes
}

func (m *memtableImpl) Iter() Iterator {
	return m.IterRange(nil, nil)
}

func (m *memtableImpl) IterRange(start, end []byte) Iterator {
	return m.newRangeIterator(start, end, true)
}

func (m *memtableImpl) RawIter() RawIterator {
	return m.RawIterRange(nil, nil)
}

func (m *memtableImpl) RawIterRange(start, end []byte) RawIterator {
	return m.newRangeIterator(start, end, false)
}

func (m *memtableImpl) newRangeIterator(start, end []byte, copyOut bool) *memtableIterator {
	var endCopy []byte
	if end != nil {
		endCopy = make([]byte, len(end))
		copy(endCopy, end)
	}
	var base *skl.Iterator[record]
	if start == nil {
		base = m.skl.NewIterator()
	} else {
		startCopy := make([]byte, len(start))
		copy(startCopy, start)
		base = m.skl.NewIteratorFrom(record{Key: startCopy, Seq: math.MaxUint64})
	}
	it := &memtableIterator{base: base, end: endCopy, copyOut: copyOut}
	it.advance()
	return it
}

func (it *memtableIterator) Valid() bool {
	return it != nil && it.valid
}

func (it *memtableIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.key
}

func (it *memtableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.value
}

func (it *memtableIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *memtableIterator) Close() {
	if it == nil {
		return
	}
	if it.base != nil {
		it.base.Close()
	}
	it.valid = false
	it.key = nil
	it.value = nil
}

func (it *memtableIterator) advance() {
	it.valid = false
	it.key = nil
	it.value = nil
	for it.base != nil && it.base.Valid() {
		r := it.base.Value()
		it.base.Next()
		if it.hasPrev && bytes.Equal(r.Key, it.prevKey) {
			continue
		}
		it.hasPrev = true
		it.prevKey = r.Key
		if it.end != nil && bytes.Compare(r.Key, it.end) >= 0 {
			if it.base != nil {
				it.base.Close()
			}
			return
		}
		if r.Op != OpPut {
			continue
		}
		if it.copyOut {
			it.key = make([]byte, len(r.Key))
			copy(it.key, r.Key)
			it.value = make([]byte, len(r.Value))
			copy(it.value, r.Value)
		} else {
			it.key = r.Key
			it.value = r.Value
		}
		it.valid = true
		return
	}
}
