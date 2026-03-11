// 可执行示例：演示 internal/skl 跳表用法
package main

import (
	"fmt"
	"lsm-tree/internal/skl"
)

func main() {
	sl := skl.NewSkiplist[int]()
	sl.Add(3)
	sl.Add(1)
	sl.Add(2)
	fmt.Println("Search(2):", sl.Search(2))
	fmt.Println("Erase(1):", sl.Erase(1))
	fmt.Println("Search(1):", sl.Search(1))

	var list skl.SkiplistInterface[string] = skl.NewSkiplist[string]()
	list.Add("world")
	list.Add("hello")
	fmt.Println("Search(\"hello\"):", list.Search("hello"))
}
