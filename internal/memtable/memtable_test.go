package memtable

import (
	"bytes"
	"fmt"
	lsmiter "lsm-tree/internal/iter"
	"math"
	"math/rand"
	"testing"
	"unsafe"
)

func collectValuePairs(it lsmiter.ValueIterator) [][2]string {
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		item := it.Item()
		got = append(got, [2]string{string(item.Key), string(item.Value)})
		it.Next()
	}
	return got
}

func collectRawValuePairs(it lsmiter.ValueIterator) [][2]string {
	return collectValuePairs(it)
}

func collectEntries(it lsmiter.EntryIterator) []lsmiter.Entry {
	defer it.Close()
	var got []lsmiter.Entry
	for it.Valid() {
		got = append(got, it.Item())
		it.Next()
	}
	return got
}

// TestMemtablePutGetDelete 验证 memtable 的基础 Put/Get/Delete 语义，以及 Len/SizeBytes 的基本变化。
func TestMemtablePutGetDelete(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2"))
	if v, ok := mt.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("Get(a) = %q, %v, want \"1\", true", v, ok)
	}
	// Len 为命令条数：2 条 Put
	if mt.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", mt.Len())
	}
	mt.Delete([]byte("a")) // 新增一条 delete 命令，不删记录条数
	if _, ok := mt.Get([]byte("a")); ok {
		t.Fatal("Get(a) after delete want false")
	}
	// 共 3 条命令：Put(a), Put(b), Delete(a)
	if mt.Len() != 3 {
		t.Fatalf("Len() after delete = %d, want 3", mt.Len())
	}
	if mt.SizeBytes() <= 0 {
		t.Fatal("SizeBytes() want > 0")
	}
}

// TestMemtableVersionSemantics 验证同 key 多版本覆盖、Delete 墓碑、Delete 后再次 Put 的可见性规则。
func TestMemtableVersionSemantics(t *testing.T) {
	mt := NewMemtable()

	mt.Put([]byte("user:1"), []byte("v1"))
	mt.Put([]byte("user:1"), []byte("v2")) // 同 key 新版本
	if v, ok := mt.Get([]byte("user:1")); !ok || string(v) != "v2" {
		t.Fatalf("Get(user:1) = %q, %v, want \"v2\", true", v, ok)
	}

	mt.Delete([]byte("user:1")) // tombstone
	if _, ok := mt.Get([]byte("user:1")); ok {
		t.Fatal("Get(user:1) after delete want false")
	}

	mt.Put([]byte("user:1"), []byte("v3")) // delete 后再次 put
	if v, ok := mt.Get([]byte("user:1")); !ok || string(v) != "v3" {
		t.Fatalf("Get(user:1) = %q, %v, want \"v3\", true", v, ok)
	}

	// 命令条数：put(v1), put(v2), delete, put(v3)
	if mt.Len() != 4 {
		t.Fatalf("Len() = %d, want 4", mt.Len())
	}
}

// TestMemtableCopiesInputBuffers 验证 Put 会复制输入 key/value，调用方后续修改不会影响内部数据。
func TestMemtableCopiesInputBuffers(t *testing.T) {
	mt := NewMemtable()

	key := []byte("k1")
	val := []byte("value-1")
	mt.Put(key, val)

	// 修改调用方缓冲区，不应污染 memtable 内数据
	key[0] = 'x'
	val[0] = 'X'

	got, ok := mt.Get([]byte("k1"))
	if !ok || string(got) != "value-1" {
		t.Fatalf("Get(k1) = %q, %v, want \"value-1\", true", got, ok)
	}
}

// TestMemtableDeleteMissingKeyAndSizeGrowth 验证删除不存在 key 也会追加墓碑命令，且 SizeBytes 随命令增长。
func TestMemtableDeleteMissingKeyAndSizeGrowth(t *testing.T) {
	mt := NewMemtable()
	initialSize := mt.SizeBytes()

	// 删除不存在 key：也会追加一条 tombstone 命令
	mt.Delete([]byte("ghost"))
	if _, ok := mt.Get([]byte("ghost")); ok {
		t.Fatal("Get(ghost) after delete-missing want false")
	}
	if mt.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", mt.Len())
	}
	if mt.SizeBytes() <= initialSize {
		t.Fatalf("SizeBytes() = %d, want > %d", mt.SizeBytes(), initialSize)
	}

	beforePut := mt.SizeBytes()
	mt.Put([]byte("k"), []byte("v"))
	if mt.Len() != 2 {
		t.Fatalf("Len() after put = %d, want 2", mt.Len())
	}
	if mt.SizeBytes() <= beforePut {
		t.Fatalf("SizeBytes() after put = %d, want > %d", mt.SizeBytes(), beforePut)
	}
}

// TestMemtableIter 验证全表迭代按 key 升序输出，且会跳过最新状态为删除的 key。
func TestMemtableIter(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("b"), []byte("2"))
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("c"), []byte("3"))
	mt.Delete([]byte("b")) // b 已删除，迭代时不应出现

	got := collectValuePairs(mt.Values())
	want := [][2]string{{"a", "1"}, {"c", "3"}}
	if len(got) != len(want) {
		t.Fatalf("Iter got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("Iter[%d] = (%q, %q), want (%q, %q)", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}

}

// TestMemtableIterRange 验证范围迭代 [start,end) 的边界行为：仅 start、仅 end、空区间等场景。
func TestMemtableIterRange(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2"))
	mt.Put([]byte("c"), []byte("3"))
	mt.Put([]byte("d"), []byte("4"))
	mt.Put([]byte("e"), []byte("5"))

	// [b, d) 应得到 b, c
	got := collectValuePairs(mt.ValuesRange([]byte("b"), []byte("d")))
	want := [][2]string{{"b", "2"}, {"c", "3"}}
	if len(got) != len(want) {
		t.Fatalf("IterRange(b,d) got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("IterRange[%d] = (%q, %q), want (%q, %q)", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}

	// 仅 start：从 "c" 到结尾
	got = collectValuePairs(mt.ValuesRange([]byte("c"), nil))
	want = [][2]string{{"c", "3"}, {"d", "4"}, {"e", "5"}}
	if len(got) != 3 || got[0][0] != "c" || got[2][0] != "e" {
		t.Fatalf("IterRange(c,nil) got %v", got)
	}

	// 仅 end：从开头到 "c"（不含 c）
	got = collectValuePairs(mt.ValuesRange(nil, []byte("c")))
	want = [][2]string{{"a", "1"}, {"b", "2"}}
	if len(got) != 2 || got[0][0] != "a" || got[1][0] != "b" {
		t.Fatalf("IterRange(nil,c) got %v", got)
	}

	// 空范围 [d, d)
	got = collectValuePairs(mt.ValuesRange([]byte("d"), []byte("d")))
	if len(got) != 0 {
		t.Fatalf("IterRange(d,d) got %d pairs, want 0", len(got))
	}
}

// TestMemtableRawIterators 验证 raw 迭代结果与安全迭代一致，且范围语义保持一致。
func TestMemtableRawIterators(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2"))
	mt.Put([]byte("c"), []byte("3"))
	mt.Delete([]byte("b"))

	// NewRawIterator 与 Iter 结果一致
	var raw [][2]string
	raw = collectRawValuePairs(mt.RawValues())
	copied := collectValuePairs(mt.Values())
	if len(raw) != len(copied) {
		t.Fatalf("IterRaw got %d, Iter got %d", len(raw), len(copied))
	}
	for i := range raw {
		if raw[i][0] != copied[i][0] || raw[i][1] != copied[i][1] {
			t.Fatalf("IterRaw[%d] = (%q,%q), Iter = (%q,%q)", i, raw[i][0], raw[i][1], copied[i][0], copied[i][1])
		}
	}

	// NewRawRangeIterator [a, c) 与 IterRange 一致
	raw = collectRawValuePairs(mt.RawValuesRange([]byte("a"), []byte("c")))
	copied = collectValuePairs(mt.ValuesRange([]byte("a"), []byte("c")))
	if len(raw) != len(copied) {
		t.Fatalf("IterRawRange got %d, IterRange got %d", len(raw), len(copied))
	}
	for i := range raw {
		if raw[i][0] != copied[i][0] || raw[i][1] != copied[i][1] {
			t.Fatalf("RawRangeIterator[%d] = (%q,%q), IterRange = (%q,%q)", i, raw[i][0], raw[i][1], copied[i][0], copied[i][1])
		}
	}
}

// TestMemtableIterLatestVersionAndDelete 验证迭代时同 key 只看最新命令，最新为 Delete 时不产出该 key。
func TestMemtableIterLatestVersionAndDelete(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("v1"))
	mt.Put([]byte("a"), []byte("v2")) // 最新 put
	mt.Put([]byte("b"), []byte("v1"))
	mt.Delete([]byte("b")) // 最新 delete，应跳过
	mt.Put([]byte("c"), []byte("v1"))
	mt.Delete([]byte("c"))
	mt.Put([]byte("c"), []byte("v2")) // delete 后新 put，应产出 v2

	got := collectValuePairs(mt.Values())

	want := [][2]string{{"a", "v2"}, {"c", "v2"}}
	if len(got) != len(want) {
		t.Fatalf("Iter got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("Iter[%d] = (%q,%q), want (%q,%q)", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}
}

// TestMemtableIterRangeStartLowerBoundAndEndExclusive 验证 start 不命中时按 LowerBound 起跳，end 为开区间不包含边界。
func TestMemtableIterRangeStartLowerBoundAndEndExclusive(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("c"), []byte("3"))
	mt.Put([]byte("e"), []byte("5"))
	mt.Put([]byte("f"), []byte("6"))

	// start="b" 不存在，应从下一个 key "c" 开始；end="f" 为开区间，不包含 f
	got := collectValuePairs(mt.ValuesRange([]byte("b"), []byte("f")))

	want := [][2]string{{"c", "3"}, {"e", "5"}}
	if len(got) != len(want) {
		t.Fatalf("IterRange(b,f) got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("IterRange[%d] = (%q,%q), want (%q,%q)", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}
}

// TestMemtableIterReturnsCopiedBuffers 验证安全迭代器返回的是拷贝，回调修改 key/value 不影响内部存储。
func TestMemtableIterReturnsCopiedBuffers(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("k"), []byte("value"))

	it := mt.Values()
	if !it.Valid() {
		t.Fatal("iterator should be valid")
	}
	item := it.Item()
	key := item.Key
	value := item.Value
	key[0] = 'x'
	value[0] = 'X'
	it.Close()
	if v, ok := mt.Get([]byte("k")); !ok || string(v) != "value" {
		t.Fatalf("Get(k) after Iter mutation = %q, %v, want \"value\", true", v, ok)
	}

	it = mt.ValuesRange([]byte("k"), nil)
	if !it.Valid() {
		t.Fatal("range iterator should be valid")
	}
	item = it.Item()
	key = item.Key
	value = item.Value
	key[0] = 'y'
	value[0] = 'Y'
	it.Close()
	if v, ok := mt.Get([]byte("k")); !ok || string(v) != "value" {
		t.Fatalf("Get(k) after IterRange mutation = %q, %v, want \"value\", true", v, ok)
	}
}

// TestMemtableIterRangeSupportsEmptyKey 验证空 key 能被正确迭代，且不会被去重逻辑误跳过。
func TestMemtableIterRangeSupportsEmptyKey(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte(""), []byte("root"))
	mt.Put([]byte("a"), []byte("1"))

	got := collectValuePairs(mt.Values())
	if len(got) != 2 || got[0][0] != "" || got[0][1] != "root" || got[1][0] != "a" {
		t.Fatalf("Iter with empty key got %v, want [[\"\",\"root\"],[\"a\",\"1\"]]", got)
	}
}

// TestMemtableObjectIteratorBasic 验证对象化迭代器的基础行为：升序输出、去重并保留最新值。
func TestMemtableObjectIteratorBasic(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("b"), []byte("2"))
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2-new"))
	mt.Delete([]byte("c"))
	mt.Put([]byte("d"), []byte("4"))

	it := mt.Values()
	got := collectValuePairs(it)
	want := [][2]string{{"a", "1"}, {"b", "2-new"}, {"d", "4"}}
	if len(got) != len(want) {
		t.Fatalf("Iter got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Iter[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestMemtableObjectRangeIterator 验证对象化范围迭代器在 [start,end) 下的输出正确性。
func TestMemtableObjectRangeIterator(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("c"), []byte("3"))
	mt.Put([]byte("e"), []byte("5"))
	mt.Put([]byte("f"), []byte("6"))

	it := mt.ValuesRange([]byte("b"), []byte("f"))
	got := collectValuePairs(it)
	want := [][2]string{{"c", "3"}, {"e", "5"}}
	if len(got) != len(want) {
		t.Fatalf("IterRange got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IterRange[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestMemtableObjectIteratorCloseAndCopyIsolation 验证迭代器 Close 后状态、以及返回切片与内部数据的隔离性。
func TestMemtableObjectIteratorCloseAndCopyIsolation(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("k"), []byte("value"))

	it := mt.Values()
	if !it.Valid() {
		t.Fatal("iterator should be valid")
	}
	item := it.Item()
	k := item.Key
	v := item.Value
	k[0] = 'x'
	v[0] = 'X'
	if got, ok := mt.Get([]byte("k")); !ok || string(got) != "value" {
		t.Fatalf("Get(k) after iterator mutation = %q, %v, want \"value\", true", got, ok)
	}

	it.Close()
	if it.Valid() {
		t.Fatal("iterator should be invalid after Close")
	}
	if got := it.Item(); got.Key != nil || got.Value != nil {
		t.Fatal("Item should be empty after Close")
	}
}

// TestEntryIteratorPreservesDelete 验证 EntryIterator 会保留 Delete，供 flush/compaction 使用。
func TestEntryIteratorPreservesDelete(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1-old"))
	mt.Put([]byte("a"), []byte("1-new"))
	mt.Put([]byte("b"), []byte("2"))
	mt.Delete([]byte("b"))
	mt.Delete([]byte("c"))

	got := collectEntries(mt.Entries())
	if len(got) != 3 {
		t.Fatalf("EntryIter got %d entries, want 3", len(got))
	}

	if string(got[0].Key) != "a" || got[0].Op != OpPut || string(got[0].Value) != "1-new" {
		t.Fatalf("EntryIter[0] = %+v, want key=a op=Put value=1-new", got[0])
	}
	if string(got[1].Key) != "b" || got[1].Op != OpDelete || got[1].Value != nil {
		t.Fatalf("EntryIter[1] = %+v, want key=b op=Delete value=nil", got[1])
	}
	if string(got[2].Key) != "c" || got[2].Op != OpDelete || got[2].Value != nil {
		t.Fatalf("EntryIter[2] = %+v, want key=c op=Delete value=nil", got[2])
	}
}

// TestRawIteratorAliasesInternalBuffers 验证 raw 迭代器零拷贝语义：可观察到底层别名且结果与逻辑视图一致。
func TestRawIteratorAliasesInternalBuffers(t *testing.T) {
	mt := NewMemtable()
	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2"))
	mt.Delete([]byte("b"))
	mt.Put([]byte("c"), []byte("3"))

	it := mt.RawValuesRange([]byte("a"), []byte("d"))
	got := collectRawValuePairs(it)
	want := [][2]string{{"a", "1"}, {"c", "3"}}
	if len(got) != len(want) {
		t.Fatalf("raw iterator got %d pairs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("raw iterator[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// raw iterator 不拷贝：可观察到底层切片别名（仅测试，不代表允许业务代码修改）
	it2 := mt.RawValues()
	defer it2.Close()
	if !it2.Valid() {
		t.Fatal("raw iterator should be valid")
	}
	r, ok := mt.skl.LowerBound(record{Key: []byte("a"), Seq: math.MaxUint64})
	if !ok {
		t.Fatal("lower bound for key a should exist")
	}
	k := it2.Item().Key
	if len(k) == 0 || len(r.Key) == 0 {
		t.Fatal("unexpected empty key")
	}
	if &k[0] != &r.Key[0] {
		t.Fatal("raw iterator key should alias internal buffer")
	}
}

// TestSkiplistOnlySizeUnderestimatesPayload 验证仅统计 skiplist 结构会低估总占用，payload 需单独计入。
func TestSkiplistOnlySizeUnderestimatesPayload(t *testing.T) {
	rand.Seed(20260310)
	mt := NewMemtable()

	payload := 0
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := bytes.Repeat([]byte{'v'}, 4096)
		payload += len(key) + len(value)
		mt.Put(key, value)
	}

	skiplistOnly := mt.skl.SizeBytes()
	total := mt.SizeBytes()
	diff := total - skiplistOnly
	fmt.Printf(
		"[size-debug] sizeof(record)=%d skiplistOnly=%d total=%d payload=%d diff=%d\n",
		unsafe.Sizeof(record{}),
		skiplistOnly,
		total,
		payload,
		diff,
	)

	// 当前实现中，diff 由 payloadBytes 累加得到，应等于所有 key/value 的字节总和。
	if diff != payload {
		t.Fatalf("payload diff = %d, want %d (total=%d skiplistOnly=%d)", diff, payload, total, skiplistOnly)
	}
	if skiplistOnly >= total {
		t.Fatalf("skiplistOnly = %d, total = %d, want skiplistOnly < total", skiplistOnly, total)
	}
}
