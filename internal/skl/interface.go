package skl

import "cmp"

// SkiplistInterface 跳表抽象：支持查找、插入、删除。
// 泛型 T 需满足 cmp.Ordered（可比较且有序，用于定位）。
type SkiplistInterface[T cmp.Ordered] interface {
	Search(target T) bool
	Add(num T)
	Erase(num T) bool
}

// CompareFunc 比较函数：a < b 返回负，a == b 返回 0，a > b 返回正（同 cmp.Compare / strings.Compare）
type CompareFunc[T any] func(a, b T) int
