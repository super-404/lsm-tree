package engine

import (
	"bytes"
	"container/heap"
	lsmiter "lsm-tree/internal/iter"
)

// Iterator 合并多张 memtable 的迭代器，按 key 升序产出，同 key 取最新；
// getValue 用于解析当前 key 的有效值（含 tombstone 屏蔽），为 nil 时直接采用子迭代器 value。
type Iterator struct {
	iters    []lsmiter.ValueIterator
	pq       iteratorMinHeap
	end      []byte
	getValue func(key []byte) ([]byte, bool)
	valid    bool
	key      []byte
	value    []byte
}

func (m *Iterator) Valid() bool   { return m.valid }
func (m *Iterator) Key() []byte   { return m.key }
func (m *Iterator) Value() []byte { return m.value }

// Item 让 Engine 自身的合并迭代器也满足公共 ValueIterator 协议。
//
// 这样上层若只关心“这是一个值视图迭代器”，就不必再区分它来自 memtable、
// sst，还是来自 engine 的多路归并结果。
func (m *Iterator) Item() lsmiter.Value {
	if !m.valid {
		return lsmiter.Value{}
	}
	return lsmiter.Value{Key: m.key, Value: m.value}
}

func (m *Iterator) Next() {
	if !m.valid {
		return
	}
	m.advance()
}

func (m *Iterator) Close() {
	for _, it := range m.iters {
		if it != nil {
			it.Close()
		}
	}
	m.iters = nil
	m.pq = nil
	m.valid = false
	m.key = nil
	m.value = nil
}

// advance 使用小根堆按 key 升序归并：
// 1) 每次取最小 key 的一组来源；
// 2) 组内按 source 优先级（active/newer immutable/older immutable）选 winner；
// 3) 组内所有迭代器一并前进，保证同 key 只产出一次；
// 4) 若 getValue 判定该 key 被 tombstone 屏蔽，则跳过该 key 并继续下一轮。
func (m *Iterator) advance() {
	m.valid = false
	m.key = nil
	m.value = nil
	if m.pq == nil {
		m.initHeap()
	}
	for len(m.pq) > 0 {
		group, key := m.popSameKeyGroup()
		// 堆已按 (key, source) 排序，先弹出的一定是同 key 中 source 最小的，即 winner
		winner := group[0]

		var (
			value []byte
			ok    bool
		)
		if m.getValue != nil {
			value, ok = m.getValue(key)
		} else {
			value = winner.it.Item().Value
			ok = true
		}

		// 无论是否产出该 key，都要先推进并回填本组所有迭代器，保证归并进度单调前进。
		for _, item := range group {
			item.it.Next()
			m.pushCandidate(item.source, item.it)
		}

		if !ok {
			continue
		}
		m.valid = true
		m.key = append(m.key[:0], key...)
		m.value = append(m.value[:0], value...)
		return
	}
}

func (m *Iterator) initHeap() {
	m.pq = make(iteratorMinHeap, 0, len(m.iters))
	heap.Init(&m.pq)
	for source, it := range m.iters {
		m.pushCandidate(source, it)
	}
}

func (m *Iterator) pushCandidate(source int, it lsmiter.ValueIterator) {
	if it == nil || !it.Valid() {
		return
	}
	key := it.Item().Key
	if m.end != nil && bytes.Compare(key, m.end) >= 0 {
		return
	}
	heap.Push(&m.pq, iteratorItem{source: source, it: it})
}

// popSameKeyGroup 弹出当前最小 key 的所有来源，并返回该组及 key 的副本。
func (m *Iterator) popSameKeyGroup() ([]iteratorItem, []byte) {
	first := heap.Pop(&m.pq).(iteratorItem)
	keyCopy := append([]byte(nil), first.it.Item().Key...)
	group := []iteratorItem{first}
	for len(m.pq) > 0 && bytes.Equal(m.pq[0].it.Item().Key, keyCopy) {
		group = append(group, heap.Pop(&m.pq).(iteratorItem))
	}
	return group, keyCopy
}
