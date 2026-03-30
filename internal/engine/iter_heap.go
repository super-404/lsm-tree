package engine

import (
	"bytes"
	lsmiter "lsm-tree/internal/iter"
)

// iteratorItem 堆中一项：来源编号 + 子迭代器。
type iteratorItem struct {
	source int
	it     lsmiter.ValueIterator
}

// iteratorMinHeap 按 key 升序；同 key 下 source 升序（source 小表示更新：active/newer immutable）。
// 实现 container/heap.Interface。
type iteratorMinHeap []iteratorItem

func (h iteratorMinHeap) Len() int { return len(h) }

func (h iteratorMinHeap) Less(i, j int) bool {
	ki := h[i].it.Item().Key
	kj := h[j].it.Item().Key
	if c := bytes.Compare(ki, kj); c != 0 {
		return c < 0
	}
	return h[i].source < h[j].source
}

func (h iteratorMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorMinHeap) Push(x any) { *h = append(*h, x.(iteratorItem)) }

func (h *iteratorMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
