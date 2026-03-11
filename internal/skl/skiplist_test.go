package skl

import (
	"cmp"
	"testing"
	"unsafe"
)

// TestSkiplistGenericInt 验证有序整型跳表的基础增删查行为以及长度、大小统计。
func TestSkiplistGenericInt(t *testing.T) {
	sl := NewSkiplist[int]()
	sl.Add(1)
	sl.Add(2)
	sl.Add(3)
	sl.Add(4)
	sl.Add(5)
	sl.Add(6)
	sl.Add(7)
	sl.Add(8)
	sl.Add(9)
	sl.Add(10)
	sl.Add(11)
	sl.Add(12)
	sl.Add(13)
	sl.Add(14)
	sl.Add(15)
	sl.Add(16)
	sl.Add(17)
	sl.Add(18)
	sl.Add(19)
	sl.Add(20)
	sl.Add(21)
	sl.Add(22)
	sl.Add(23)
	sl.Add(24)
	sl.Add(25)
	sl.Add(26)
	sl.Add(27)
	sl.Add(28)
	sl.Add(29)
	sl.Add(30)
	sl.Add(31)
	sl.Add(32)
	sl.Add(33)
	sl.Add(34)
	sl.Add(35)
	sl.Add(36)
	sl.Add(37)
	sl.Add(38)
	sl.Add(39)
	sl.Add(40)
	sl.Add(41)
	if !sl.Search(1) {
		t.Fatal("Search(1)")
	}
	if !sl.Erase(1) {
		t.Fatal("Erase(1)")
	}
	if sl.Search(1) {
		t.Fatal("Search(1) after erase")
	}
	if sl.Len() != 40 {
		t.Fatalf("Len() after erase = %d, want 40", sl.Len())
	}
	// SizeBytes 随元素增多而增大，且 ≥ 结构体与头结点
	if sz := sl.SizeBytes(); sz <= 0 {
		t.Fatalf("SizeBytes() = %d, want > 0", sz)
	}
}

// TestSkiplistGenericString 验证字符串跳表的基础增删查行为。
func TestSkiplistGenericString(t *testing.T) {
	sl := NewSkiplist[string]()
	sl.Add("a")
	sl.Add("b")
	if !sl.Search("a") {
		t.Fatal("Search(\"a\")")
	}
	if !sl.Erase("a") {
		t.Fatal("Erase(\"a\")")
	}
	if sl.Search("a") {
		t.Fatal("Search(\"a\") after erase")
	}
	if sl.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", sl.Len())
	}
}

// TestSkiplistInterface 验证 NewSkiplist 返回值满足对外 SkiplistInterface 接口约束。
func TestSkiplistInterface(t *testing.T) {
	var _ SkiplistInterface[int] = NewSkiplist[int]()
	var _ SkiplistInterface[string] = NewSkiplist[string]()
}

// Item 复杂结构体：用于测试 SkiplistCmp 对自定义类型的支持
type Item struct {
	ID    int64
	Type  string
	Score float64
}

// CompareItem 定义 Item 的排序：先按 Type，再按 ID，再按 Score
func CompareItem(a, b Item) int {
	if c := cmp.Compare(a.Type, b.Type); c != 0 {
		return c
	}
	if c := cmp.Compare(a.ID, b.ID); c != 0 {
		return c
	}
	return cmp.Compare(a.Score, b.Score)
}

func CompareStringReverse(a, b string) int {
	return cmp.Compare(b, a)
}

// TestSkiplistCmpStringReverse 验证自定义倒序比较函数下的查找、删除与长度语义。
func TestSkiplistCmpStringReverse(t *testing.T) {
	// 使用倒序比较：字典序大的排前面，Search/Erase 仍按同一规则
	sl := NewSkiplistCmp(CompareStringReverse)
	sl.Add("a")
	sl.Add("m")
	sl.Add("z")
	if !sl.Search("a") {
		t.Fatal("Search(\"a\")")
	}
	if !sl.Search("z") {
		t.Fatal("Search(\"z\")")
	}
	if !sl.Erase("m") {
		t.Fatal("Erase(\"m\")")
	}
	if sl.Search("m") {
		t.Fatal("Search(\"m\") after erase should be false")
	}
	if !sl.Search("a") || !sl.Search("z") {
		t.Fatal("Search(\"a\") or Search(\"z\") after erase")
	}
	if sl.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", sl.Len())
	}
}

// TestSkiplistCmpComplexStruct 验证复杂结构体比较函数下的查找、删除和剩余元素可见性。
func TestSkiplistCmpComplexStruct(t *testing.T) {
	sl := NewSkiplistCmp(CompareItem)

	items := []Item{
		{ID: 3, Type: "A", Score: 1.0},
		{ID: 1, Type: "A", Score: 2.0},
		{ID: 2, Type: "B", Score: 0.5},
		{ID: 1, Type: "B", Score: 1.5},
	}
	for _, it := range items {
		sl.Add(it)
	}

	// 存在的 key 能查到
	if !sl.Search(Item{ID: 1, Type: "A", Score: 2.0}) {
		t.Fatal("Search(Item{A,1,2.0})")
	}
	if !sl.Search(Item{ID: 2, Type: "B", Score: 0.5}) {
		t.Fatal("Search(Item{B,2,0.5})")
	}

	// 不存在的 key 查不到
	if sl.Search(Item{ID: 99, Type: "Z", Score: 0}) {
		t.Fatal("Search(Item{Z,99,0}) should be false")
	}

	// 删除后查不到
	if !sl.Erase(Item{ID: 1, Type: "A", Score: 2.0}) {
		t.Fatal("Erase(Item{A,1,2.0})")
	}
	if sl.Search(Item{ID: 1, Type: "A", Score: 2.0}) {
		t.Fatal("Search after Erase should be false")
	}

	// 同 Type 不同 ID 的仍存在
	if !sl.Search(Item{ID: 3, Type: "A", Score: 1.0}) {
		t.Fatal("Search(Item{A,3,1.0}) after erase another")
	}
	if sl.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", sl.Len())
	}
}

// TestNodeSizeBytes 验证节点字节估算公式：基础节点大小 + forward 指针容量线性增长。
func TestNodeSizeBytes(t *testing.T) {
	type small struct {
		A int32
	}
	type large struct {
		A int64
		B string
		C [16]byte
	}

	cases := []struct {
		name       string
		sizeFn     func(int) int
		baseSize   int
		ptrSize    int
		forwardCap int
	}{
		{
			name: "small-cap-0",
			sizeFn: func(cap int) int {
				return nodeSizeBytes[small](cap)
			},
			baseSize:   int(unsafe.Sizeof(skiplistCmpNode[small]{})),
			ptrSize:    int(unsafe.Sizeof((*skiplistCmpNode[small])(nil))),
			forwardCap: 0,
		},
		{
			name: "small-cap-7",
			sizeFn: func(cap int) int {
				return nodeSizeBytes[small](cap)
			},
			baseSize:   int(unsafe.Sizeof(skiplistCmpNode[small]{})),
			ptrSize:    int(unsafe.Sizeof((*skiplistCmpNode[small])(nil))),
			forwardCap: 7,
		},
		{
			name: "large-cap-13",
			sizeFn: func(cap int) int {
				return nodeSizeBytes[large](cap)
			},
			baseSize:   int(unsafe.Sizeof(skiplistCmpNode[large]{})),
			ptrSize:    int(unsafe.Sizeof((*skiplistCmpNode[large])(nil))),
			forwardCap: 13,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.sizeFn(tc.forwardCap)
			want := tc.baseSize + tc.ptrSize*tc.forwardCap
			if got != want {
				t.Fatalf("nodeSizeBytes(%d) = %d, want %d", tc.forwardCap, got, want)
			}

			next := tc.sizeFn(tc.forwardCap + 1)
			if next-got != tc.ptrSize {
				t.Fatalf("nodeSizeBytes delta = %d, want ptrSize %d", next-got, tc.ptrSize)
			}
		})
	}
}

func recalcSizeBytes[T any](s *SkiplistCmp[T]) int {
	total := int(unsafe.Sizeof(*s)) + nodeSizeBytes[T](maxLevel)
	for curr := s.head.forward[0]; curr != nil; curr = curr.forward[0] {
		total += nodeSizeBytes[T](cap(curr.forward))
	}
	return total
}

// TestSkiplistSizeBytes 验证 SizeBytes 在初始化、插入、删除成功/失败场景下与重算结果一致。
func TestSkiplistSizeBytes(t *testing.T) {
	sl := NewSkiplistCmp(CompareItem)

	// 初始大小：仅包含 SkiplistCmp 结构体和头结点
	if got, want := sl.SizeBytes(), recalcSizeBytes(sl); got != want {
		t.Fatalf("initial SizeBytes() = %d, want %d", got, want)
	}

	// 插入若干元素后，实时统计值应与重算结果一致
	items := []Item{
		{ID: 1001, Type: "A", Score: 9.5},
		{ID: 1002, Type: "A", Score: 7.2},
		{ID: 2001, Type: "B", Score: 8.8},
		{ID: 2002, Type: "B", Score: 8.8},
		{ID: 3001, Type: "C", Score: 6.1},
		{ID: 1002, Type: "A", Score: 7.2}, // duplicate
	}
	for _, it := range items {
		sl.Add(it)
	}
	if got, want := sl.SizeBytes(), recalcSizeBytes(sl); got != want {
		t.Fatalf("SizeBytes() after add = %d, want %d", got, want)
	}

	// 删除存在元素后，大小应同步减少并保持一致
	beforeErase := sl.SizeBytes()
	if !sl.Erase(Item{ID: 1002, Type: "A", Score: 7.2}) {
		t.Fatal("Erase(Item{A,1002,7.2}) should succeed")
	}
	if got, want := sl.SizeBytes(), recalcSizeBytes(sl); got != want {
		t.Fatalf("SizeBytes() after erase existing = %d, want %d", got, want)
	}
	if sl.SizeBytes() >= beforeErase {
		t.Fatalf("SizeBytes() after erase existing = %d, want < %d", sl.SizeBytes(), beforeErase)
	}

	// 删除不存在元素应不改变大小
	beforeMissingErase := sl.SizeBytes()
	if sl.Erase(Item{ID: 9999, Type: "Z", Score: 0}) {
		t.Fatal("Erase(Item{Z,9999,0}) should fail")
	}
	if sl.SizeBytes() != beforeMissingErase {
		t.Fatalf("SizeBytes() after erase missing = %d, want %d", sl.SizeBytes(), beforeMissingErase)
	}
}

// TestSkiplistIterator 验证对象化迭代器全表遍历和从 lower bound 起迭代的顺序正确性。
func TestSkiplistIterator(t *testing.T) {
	sl := NewSkiplistCmp(cmp.Compare[int])
	for _, n := range []int{5, 1, 3, 2, 4} {
		sl.Add(n)
	}

	it := sl.NewIterator()
	defer it.Close()
	var got []int
	for it.Valid() {
		got = append(got, it.Value())
		it.Next()
	}
	want := []int{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("NewIterator got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NewIterator[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	from := sl.NewIteratorFrom(3)
	defer from.Close()
	got = nil
	for from.Valid() {
		got = append(got, from.Value())
		from.Next()
	}
	want = []int{3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("NewIteratorFrom got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NewIteratorFrom[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}
