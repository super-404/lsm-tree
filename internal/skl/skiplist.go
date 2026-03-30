package skl

import (
	"cmp"
	"math/rand"
	"unsafe"
)

const maxLevel = 32
const pFactor = 0.25

// CompareFunc 比较函数：a < b 返回负，a == b 返回 0，a > b 返回正。
type CompareFunc[T any] func(a, b T) int

// ---- Skiplist[T cmp.Ordered]：内置有序类型的薄封装，内部委托给 SkiplistCmp ----

type Skiplist[T cmp.Ordered] struct {
	*SkiplistCmp[T]
}

func NewSkiplist[T cmp.Ordered]() *Skiplist[T] {
	return &Skiplist[T]{SkiplistCmp: NewSkiplistCmp(cmp.Compare[T])}
}

func Constructor() *Skiplist[int] {
	return NewSkiplist[int]()
}

// ---- SkiplistCmp[T any]：唯一实现 ----

type skiplistCmpNode[T any] struct {
	val     T
	forward []*skiplistCmpNode[T]
}

type SkiplistCmp[T any] struct {
	head      *skiplistCmpNode[T]
	level     int
	cmp       CompareFunc[T]
	length    int
	sizeBytes int
}

// Iterator 是 SkiplistCmp 的顺序迭代器（从 level 0 链表读取）。
type Iterator[T any] struct {
	curr *skiplistCmpNode[T]
}

func nodeSizeBytes[T any](forwardCap int) int {
	const ptrSize = unsafe.Sizeof((*skiplistCmpNode[T])(nil))
	return int(unsafe.Sizeof(skiplistCmpNode[T]{})) + int(ptrSize)*forwardCap
}

func NewSkiplistCmp[T any](cmp CompareFunc[T]) *SkiplistCmp[T] {
	var zero T
	head := &skiplistCmpNode[T]{
		val:     zero,
		forward: make([]*skiplistCmpNode[T], maxLevel),
	}
	s := &SkiplistCmp[T]{head: head, level: 0, cmp: cmp}
	s.sizeBytes = int(unsafe.Sizeof(*s)) + nodeSizeBytes[T](maxLevel)
	return s
}

func (s *SkiplistCmp[T]) less(a, b T) bool  { return s.cmp(a, b) < 0 }
func (s *SkiplistCmp[T]) equal(a, b T) bool { return s.cmp(a, b) == 0 }

func (s *SkiplistCmp[T]) randomLevel() int {
	lv := 1
	for lv < maxLevel && rand.Float64() < pFactor {
		lv++
	}
	return lv
}

func (s *SkiplistCmp[T]) Search(target T) bool {
	_, ok := s.Get(target)
	return ok
}

func (s *SkiplistCmp[T]) Get(target T) (T, bool) {
	var zero T
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && s.less(curr.forward[i].val, target) {
			curr = curr.forward[i]
		}
	}
	curr = curr.forward[0]
	if curr == nil || !s.equal(curr.val, target) {
		return zero, false
	}
	return curr.val, true
}

// LowerBound 返回第一个 >= target 的元素（按比较函数定义的序），不存在则返回零值与 false
func (s *SkiplistCmp[T]) LowerBound(target T) (T, bool) {
	var zero T
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && s.less(curr.forward[i].val, target) {
			curr = curr.forward[i]
		}
	}
	curr = curr.forward[0]
	if curr == nil {
		return zero, false
	}
	return curr.val, true
}

// NewIterator 返回从最小元素开始的迭代器。
func (s *SkiplistCmp[T]) NewIterator() *Iterator[T] {
	return &Iterator[T]{curr: s.head.forward[0]}
}

// NewIteratorFrom 返回从第一个 >= lower 的元素开始的迭代器。
func (s *SkiplistCmp[T]) NewIteratorFrom(lower T) *Iterator[T] {
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && s.less(curr.forward[i].val, lower) {
			curr = curr.forward[i]
		}
	}
	return &Iterator[T]{curr: curr.forward[0]}
}

func (it *Iterator[T]) Valid() bool {
	return it != nil && it.curr != nil
}

func (it *Iterator[T]) Value() T {
	var zero T
	if !it.Valid() {
		return zero
	}
	return it.curr.val
}

func (it *Iterator[T]) Next() {
	if !it.Valid() {
		return
	}
	it.curr = it.curr.forward[0]
}

func (it *Iterator[T]) Close() {
	if it == nil {
		return
	}
	it.curr = nil
}

// Iter 按序遍历跳表（level 0 顺序），对每个元素调用 f；若 f 返回 false 则停止。不修改表结构。
func (s *SkiplistCmp[T]) Iter(f func(val T) bool) {
	it := s.NewIterator()
	for it.Valid() {
		if !f(it.Value()) {
			return
		}
		it.Next()
	}
}

// IterFrom 从第一个 >= lower 的元素开始按序遍历，对每个元素调用 f；若 f 返回 false 则停止。
func (s *SkiplistCmp[T]) IterFrom(lower T, f func(val T) bool) {
	it := s.NewIteratorFrom(lower)
	for it.Valid() {
		if !f(it.Value()) {
			return
		}
		it.Next()
	}
}

func (s *SkiplistCmp[T]) Len() int       { return s.length }
func (s *SkiplistCmp[T]) SizeBytes() int { return s.sizeBytes }

func (s *SkiplistCmp[T]) Add(num T) {
	update := make([]*skiplistCmpNode[T], maxLevel)
	for i := range update {
		update[i] = s.head
	}
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && s.less(curr.forward[i].val, num) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	lv := s.randomLevel()
	s.level = max(s.level, lv)
	newNode := &skiplistCmpNode[T]{val: num, forward: make([]*skiplistCmpNode[T], lv)}
	for i, node := range update[:lv] {
		newNode.forward[i] = node.forward[i]
		node.forward[i] = newNode
	}
	s.length++
	s.sizeBytes += nodeSizeBytes[T](lv)
}

func (s *SkiplistCmp[T]) Erase(num T) bool {
	update := make([]*skiplistCmpNode[T], maxLevel)
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && s.less(curr.forward[i].val, num) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	curr = curr.forward[0]
	if curr == nil || !s.equal(curr.val, num) {
		return false
	}
	for i := len(curr.forward) - 1; i >= 0; i-- {
		update[i].forward[i] = curr.forward[i]
	}
	for s.level > 1 && s.head.forward[s.level-1] == nil {
		s.level--
	}
	s.length--
	s.sizeBytes -= nodeSizeBytes[T](cap(curr.forward))
	return true
}

func max(a, b int) int {
	if b > a {
		return b
	}
	return a
}
