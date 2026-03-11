package engine

import (
	"errors"
	"fmt"
	"lsm-tree/internal/db"
	"lsm-tree/internal/memtable"
	"math/rand"
	"sort"
	"testing"
)

type fakeBatch struct{}

func (f *fakeBatch) Put(key, value []byte) {}
func (f *fakeBatch) Delete(key []byte)     {}
func (f *fakeBatch) Len() int              { return 0 }
func (f *fakeBatch) Reset()                {}

type fakeIter struct{}

func (i *fakeIter) Valid() bool   { return false }
func (i *fakeIter) Key() []byte   { return nil }
func (i *fakeIter) Value() []byte { return nil }
func (i *fakeIter) Next()         {}
func (i *fakeIter) Close()        {}

type fakeMemtable struct {
	size int
}

func (m *fakeMemtable) Put(key, value []byte)                {}
func (m *fakeMemtable) Get(key []byte) ([]byte, bool)        { return nil, false }
func (m *fakeMemtable) GetLatest(key []byte) ([]byte, memtable.Op, bool) { return nil, 0, false }
func (m *fakeMemtable) Delete(key []byte)                    {}
func (m *fakeMemtable) Len() int                      { return 0 }
func (m *fakeMemtable) SizeBytes() int                { return m.size }
func (m *fakeMemtable) Iter() memtable.Iterator       { return &fakeIter{} }
func (m *fakeMemtable) IterRange(start, end []byte) memtable.Iterator {
	return &fakeIter{}
}

func collectPairs(it db.Iterator) [][2]string {
	if it == nil {
		return nil
	}
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		got = append(got, [2]string{string(it.Key()), string(it.Value())})
		it.Next()
	}
	return got
}

// TestLSMEngine_PutGetDelete 验证基础读写删语义：Put 后可读、Delete 后返回 ErrNotFound。
func TestLSMEngine_PutGetDelete(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	val, err := d.Get([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Fatalf("Get(k1) = %q, want v1", val)
	}

	if err := d.Delete([]byte("k1")); err != nil {
		t.Fatal(err)
	}
	_, err = d.Get([]byte("k1"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("Get after Delete: want ErrNotFound, got %v", err)
	}
}

// TestLSMEngine_Write 验证批量写顺序生效：同批次内 Delete 可覆盖此前 Put。
func TestLSMEngine_Write(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	batch := d.NewBatch()
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	batch.Delete([]byte("a"))
	if batch.Len() != 3 {
		t.Fatalf("batch.Len() = %d, want 3", batch.Len())
	}
	if err := d.Write(batch); err != nil {
		t.Fatal(err)
	}

	_, err = d.Get([]byte("a"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("Get(a) after batch delete: want ErrNotFound, got %v", err)
	}
	val, err := d.Get([]byte("b"))
	if err != nil || string(val) != "2" {
		t.Fatalf("Get(b) = %q, %v, want \"2\", nil", val, err)
	}
}

// TestLSMEngine_NewIterator 验证迭代器按 key 升序输出，且范围 [start,end) 为左闭右开并过滤已删除 key。
func TestLSMEngine_NewIterator(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// 构造更复杂状态：
	// 1) 多 key 无序写入
	// 2) 覆盖写（同 key 新值）
	// 3) 删除后再写恢复
	// 4) 纯删除 key 不应出现在迭代结果
	ops := []struct {
		put   bool
		key   string
		value string
	}{
		{put: true, key: "m", value: "1"},
		{put: true, key: "a", value: "1"},
		{put: true, key: "z", value: "1"},
		{put: true, key: "k", value: "1"},
		{put: true, key: "c", value: "1"},
		{put: true, key: "m", value: "2"}, // 覆盖
		{put: true, key: "a", value: "2"}, // 覆盖
		{put: false, key: "k"},            // 删除
		{put: true, key: "k", value: "2"}, // 删除后恢复
		{put: false, key: "z"},            // 删除后不恢复
		{put: false, key: "ghost"},        // 删除不存在 key
	}
	for _, op := range ops {
		if op.put {
			if err := d.Put([]byte(op.key), []byte(op.value)); err != nil {
				t.Fatalf("Put(%q) error: %v", op.key, err)
			}
		} else {
			if err := d.Delete([]byte(op.key)); err != nil {
				t.Fatalf("Delete(%q) error: %v", op.key, err)
			}
		}
	}

	// 全表：按 key 升序，只保留有效 key 且取最新值
	got := collectPairs(d.NewIterator(nil))
	want := [][2]string{
		{"a", "2"},
		{"c", "1"},
		{"k", "2"},
		{"m", "2"},
	}
	if len(got) != len(want) {
		t.Fatalf("iterator len = %d, want %d, got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterator[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// [b, n)：应命中 c/k/m
	got = collectPairs(d.NewIterator(&db.IterOptions{Start: []byte("b"), End: []byte("n")}))
	want = [][2]string{{"c", "1"}, {"k", "2"}, {"m", "2"}}
	if len(got) != len(want) {
		t.Fatalf("range[b,n) len = %d, want %d, got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("range[b,n)[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// 起点不存在时应从下一个 key 开始；终点开区间不包含自身
	got = collectPairs(d.NewIterator(&db.IterOptions{Start: []byte("l"), End: []byte("m")}))
	if len(got) != 0 {
		t.Fatalf("range[l,m) should be empty, got=%v", got)
	}

	// 空范围 [m,m)
	got = collectPairs(d.NewIterator(&db.IterOptions{Start: []byte("m"), End: []byte("m")}))
	if len(got) != 0 {
		t.Fatalf("range[m,m) should be empty, got=%v", got)
	}
}

// TestLSMEngine_Close 验证关闭后所有读写接口统一返回关闭错误，且 Close 可幂等调用。
func TestLSMEngine_Close(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() first call error: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() second call error: %v", err)
	}

	if _, err := d.Get([]byte("x")); !errors.Is(err, errClosed) {
		t.Fatalf("Get after Close: want errClosed, got %v", err)
	}
	if err := d.Put([]byte("k"), []byte("v")); !errors.Is(err, errClosed) {
		t.Fatalf("Put after Close: want errClosed, got %v", err)
	}
	if err := d.Delete([]byte("k")); !errors.Is(err, errClosed) {
		t.Fatalf("Delete after Close: want errClosed, got %v", err)
	}
	if err := d.Write(d.NewBatch()); !errors.Is(err, errClosed) {
		t.Fatalf("Write after Close: want errClosed, got %v", err)
	}
	if it := d.NewIterator(nil); it != nil {
		t.Fatalf("NewIterator after Close: want nil, got %v", it)
	}
}

// TestLSMEngine_WriteInvalidBatch 验证 Write 只接受本引擎创建的 batch，非法 batch 返回 errInvalidBatch。
func TestLSMEngine_WriteInvalidBatch(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Write(&fakeBatch{}); !errors.Is(err, errInvalidBatch) {
		t.Fatalf("Write(fakeBatch): want errInvalidBatch, got %v", err)
	}
}

// TestLSMEngine_BufferIsolation 验证输入切片与输出切片隔离：外部修改不会污染引擎内部数据。
func TestLSMEngine_BufferIsolation(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	key := []byte("k1")
	val := []byte("value-1")
	if err := d.Put(key, val); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	val[0] = 'X'

	got, err := d.Get([]byte("k1"))
	if err != nil || string(got) != "value-1" {
		t.Fatalf("Get(k1) = %q, %v, want \"value-1\", nil", got, err)
	}

	// Get 返回值也应可安全修改，不影响内部存储
	got[0] = 'V'
	got2, err := d.Get([]byte("k1"))
	if err != nil || string(got2) != "value-1" {
		t.Fatalf("Get(k1) second = %q, %v, want \"value-1\", nil", got2, err)
	}
}

// TestLSMEngine_BatchBufferIsolationAndReset 验证 batch 会拷贝输入缓冲区且 Reset 后可复用为空批。
func TestLSMEngine_BatchBufferIsolationAndReset(t *testing.T) {
	d, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	b := d.NewBatch()
	key := []byte("a")
	val := []byte("1")
	b.Put(key, val)
	key[0] = 'x'
	val[0] = '9'
	if err := d.Write(b); err != nil {
		t.Fatal(err)
	}
	got, err := d.Get([]byte("a"))
	if err != nil || string(got) != "1" {
		t.Fatalf("Get(a) = %q, %v, want \"1\", nil", got, err)
	}

	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("batch.Len() after Reset = %d, want 0", b.Len())
	}
}

// TestLSMEngine_GetPrefersNewestAcrossImmutables 验证跨多层 immutable 同 key 读取时优先返回最新版本。
func TestLSMEngine_GetPrefersNewestAcrossImmutables(t *testing.T) {
	e := &engineDB{active: memtable.NewMemtable()}

	e.active.Put([]byte("k"), []byte("v1"))
	e.immutables = append([]memtable.Memtable{e.active}, e.immutables...) // older
	e.active = memtable.NewMemtable()

	e.active.Put([]byte("k"), []byte("v2"))
	e.immutables = append([]memtable.Memtable{e.active}, e.immutables...) // newer
	e.active = memtable.NewMemtable()

	got, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get(k) error = %v, want nil", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(k) = %q, want \"v2\"", got)
	}
}

// TestLSMEngine_TombstoneMasksOldPut 验证新层 tombstone 屏蔽更老层里的旧 Put，Get 返回 NotFound。
func TestLSMEngine_TombstoneMasksOldPut(t *testing.T) {
	older := memtable.NewMemtable()
	older.Put([]byte("k"), []byte("old"))

	active := memtable.NewMemtable()
	active.Delete([]byte("k")) // 墓碑

	e := &engineDB{active: active, immutables: []memtable.Memtable{older}}
	got, err := e.Get([]byte("k"))
	if err == nil {
		t.Fatalf("Get(k) = %q, nil: want ErrNotFound (tombstone in active should mask old Put)", got)
	}
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("Get(k) err = %v, want ErrNotFound", err)
	}
}

// TestLSMEngine_IteratorSkipsKeyMaskedByTombstone 验证合并迭代时新层 tombstone 屏蔽的 key 不会产出。
func TestLSMEngine_IteratorSkipsKeyMaskedByTombstone(t *testing.T) {
	older := memtable.NewMemtable()
	older.Put([]byte("a"), []byte("old"))

	active := memtable.NewMemtable()
	active.Delete([]byte("a")) // 墓碑，迭代不应产出 "a"
	active.Put([]byte("b"), []byte("b"))

	e := &engineDB{active: active, immutables: []memtable.Memtable{older}}
	it := e.NewIterator(nil)
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		got = append(got, [2]string{string(it.Key()), string(it.Value())})
		it.Next()
	}
	// 只应有 "b"，不应有 "a"
	if len(got) != 1 || got[0][0] != "b" || got[0][1] != "b" {
		t.Fatalf("iterator got %v, want [[\"b\" \"b\"]] (key \"a\" must be skipped by tombstone)", got)
	}
}

// TestLSMEngine_IteratorDedupAndNewestAcrossImmutables 验证合并迭代时同 key 仅产出一次且取最新版本。
func TestLSMEngine_IteratorDedupAndNewestAcrossImmutables(t *testing.T) {
	e := &engineDB{active: memtable.NewMemtable()}

	older := memtable.NewMemtable()
	older.Put([]byte("a"), []byte("old"))

	newer := memtable.NewMemtable()
	newer.Put([]byte("a"), []byte("new"))

	e.immutables = []memtable.Memtable{newer, older}

	it := e.NewIterator(nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatal("iterator should be valid")
	}
	if string(it.Key()) != "a" || string(it.Value()) != "new" {
		t.Fatalf("first pair = (%q, %q), want (\"a\", \"new\")", it.Key(), it.Value())
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator should be exhausted, got (%q, %q)", it.Key(), it.Value())
	}
}

// TestLSMEngine_MaybeRotateThresholdBoundary 验证 64MB 阈值边界：等于阈值不轮转，超过阈值才轮转。
func TestLSMEngine_MaybeRotateThresholdBoundary(t *testing.T) {
	e := &engineDB{active: &fakeMemtable{size: maxMemtableSizeBytes}}
	e.maybeRotate()

	if len(e.immutables) != 0 {
		t.Fatalf("len(immutables) = %d, want 0 when size == threshold", len(e.immutables))
	}
	if _, ok := e.active.(*fakeMemtable); !ok {
		t.Fatal("active should not rotate when size == threshold")
	}

	e.active = &fakeMemtable{size: maxMemtableSizeBytes + 1}
	e.maybeRotate()
	if len(e.immutables) != 1 {
		t.Fatalf("len(immutables) = %d, want 1 when size > threshold", len(e.immutables))
	}
	if _, ok := e.active.(*fakeMemtable); ok {
		t.Fatal("active should be replaced with new memtable after rotate")
	}
}

// TestLSMEngine_MaybeRotatePrependsNewestImmutable 验证轮转后新冻结 memtable 头插到 immutables[0] 以保证“新优先”。
func TestLSMEngine_MaybeRotatePrependsNewestImmutable(t *testing.T) {
	oldest := &fakeMemtable{size: 1}
	older := &fakeMemtable{size: 2}
	newlyFrozen := &fakeMemtable{size: maxMemtableSizeBytes + 1}

	e := &engineDB{
		active:     newlyFrozen,
		immutables: []memtable.Memtable{older, oldest},
	}
	e.maybeRotate()

	if len(e.immutables) != 3 {
		t.Fatalf("len(immutables) = %d, want 3", len(e.immutables))
	}
	if e.immutables[0] != newlyFrozen {
		t.Fatal("newly frozen memtable should be prepended at immutables[0]")
	}
	if e.immutables[1] != older || e.immutables[2] != oldest {
		t.Fatalf("immutable order changed unexpectedly: %#v", e.immutables)
	}
}

// TestLSMEngine_RotateByRealPutsUntilThresholdExceeded 通过真实 Put 持续写入数据，验证超过 64MB 阈值后会触发 memtable 轮转。
func TestLSMEngine_RotateByRealPutsUntilThresholdExceeded(t *testing.T) {
	if testing.Short() {
		t.Skip("skip threshold stress test in short mode")
	}

	raw, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	e, ok := raw.(*engineDB)
	if !ok {
		t.Fatal("Open should return *engineDB")
	}

	value := make([]byte, 256<<10) // 256KB，减少循环次数
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}

	firstKey := []byte("k-000000")
	lastKey := firstKey
	rotated := false
	maxPuts := 4000

	for i := 0; i < maxPuts; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		if i == 0 {
			firstKey = key
		}
		lastKey = key
		if err := raw.Put(key, value); err != nil {
			t.Fatalf("Put(%q) error: %v", key, err)
		}
		if len(e.immutables) > 0 {
			rotated = true
			break
		}
	}

	if !rotated {
		t.Fatalf("did not rotate after %d puts; threshold=%d", maxPuts, maxMemtableSizeBytes)
	}
	if len(e.immutables) < 1 {
		t.Fatalf("immutables len = %d, want >= 1 after rotate", len(e.immutables))
	}
	if e.active.Len() != 0 {
		t.Fatalf("new active memtable should be empty right after first rotate, got Len=%d", e.active.Len())
	}

	// 轮转后旧数据仍应可读（来自 immutable）
	gotFirst, err := raw.Get(firstKey)
	if err != nil {
		t.Fatalf("Get(firstKey) error: %v", err)
	}
	if len(gotFirst) != len(value) {
		t.Fatalf("Get(firstKey) value size = %d, want %d", len(gotFirst), len(value))
	}

	// 最后一条写入触发轮转前已落在旧 active，也应可读
	gotLast, err := raw.Get(lastKey)
	if err != nil {
		t.Fatalf("Get(lastKey) error: %v", err)
	}
	if len(gotLast) != len(value) {
		t.Fatalf("Get(lastKey) value size = %d, want %d", len(gotLast), len(value))
	}
}

// TestLSMEngine_HeavyPutDeleteSameKeysUntilTenImmutables 是常驻压力回归：
// 在同一 key 集合上执行大量 Put/Delete（含多次轮转），校验 tombstone 不会被旧层 Put“复活”。
func TestLSMEngine_HeavyPutDeleteSameKeysUntilTenImmutables(t *testing.T) {
	if testing.Short() {
		t.Skip("skip heavy put/delete rotate test in short mode")
	}

	raw, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	e, ok := raw.(*engineDB)
	if !ok {
		t.Fatal("Open should return *engineDB")
	}

	rng := rand.New(rand.NewSource(20260311))
	const (
		keySpace         = 3000
		targetImmutables = 10
		maxOps           = 50000
	)

	// 128KB payload，能较快触发阈值轮转，同时保留“大量操作”的测试特征。
	valuePayload := make([]byte, 128<<10)
	for i := range valuePayload {
		valuePayload[i] = byte('a' + i%26)
	}

	// 期望模型：跟踪 key 的最终可见值（nil 表示已删除/不存在）
	model := make(map[string][]byte, keySpace)
	deleteCount := 0

	for i := 0; i < maxOps && len(e.immutables) < targetImmutables; i++ {
		k := fmt.Sprintf("hot-key-%04d", rng.Intn(keySpace))
		key := []byte(k)

		// 真实场景里以写入为主，间歇删除；这里约 80% Put, 20% Delete。
		if rng.Intn(10) < 8 {
			v := append([]byte(nil), valuePayload...)
			// 让值带上版本尾巴，避免所有 value 完全相同
			v = append(v, byte(i%251))
			if err := raw.Put(key, v); err != nil {
				t.Fatalf("Put(%q) error: %v", k, err)
			}
			model[k] = v
		} else {
			if err := raw.Delete(key); err != nil {
				t.Fatalf("Delete(%q) error: %v", k, err)
			}
			delete(model, k)
			deleteCount++
		}
	}

	if len(e.immutables) < targetImmutables {
		t.Fatalf("immutables = %d, want >= %d (threshold=%d)", len(e.immutables), targetImmutables, maxMemtableSizeBytes)
	}
	if deleteCount == 0 {
		t.Fatal("expected at least one delete operation in stress run")
	}

	// 一致性校验：遍历全部 key 空间，核对 Get 结果与模型一致（重点覆盖 tombstone 跨层屏蔽语义）。
	for i := 0; i < keySpace; i++ {
		k := fmt.Sprintf("hot-key-%04d", i)
		got, err := raw.Get([]byte(k))
		want, exists := model[k]
		if !exists {
			if !errors.Is(err, db.ErrNotFound) {
				t.Fatalf("Get(%q) err=%v, want ErrNotFound", k, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Get(%q) unexpected err: %v", k, err)
		}
		if string(got) != string(want) {
			t.Fatalf("Get(%q) mismatch: got(len=%d), want(len=%d)", k, len(got), len(want))
		}
	}
}

// TestLSMEngine_RandomOpsStress 大量随机混合 Put/Delete/Write/Get 操作，使用内存模型做一致性校验，
// 并周期性检查迭代器视图，覆盖真实环境下复杂写入序列。
func TestLSMEngine_RandomOpsStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip random stress test in short mode")
	}

	raw, err := Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	rng := rand.New(rand.NewSource(20260311))
	const (
		keySpace = 5000
		ops      = 80000
	)

	valuePayload := make([]byte, 64<<10) // 64KB
	for i := range valuePayload {
		valuePayload[i] = byte('a' + i%26)
	}

	model := make(map[string][]byte, keySpace)
	mkValue := func(version int) []byte {
		v := append([]byte(nil), valuePayload...)
		v = append(v, byte(version%251))
		return v
	}

	applyToModel := func(put bool, k string, v []byte) {
		if put {
			model[k] = append([]byte(nil), v...)
			return
		}
		delete(model, k)
	}

	// 周期性做抽样 Get 校验 + 迭代器校验，避免只在结尾发现问题。
	checkSample := func(step int) {
		for i := 0; i < 60; i++ {
			idx := rng.Intn(keySpace)
			k := fmt.Sprintf("rk-%05d", idx)
			got, err := raw.Get([]byte(k))
			want, exists := model[k]
			if !exists {
				if !errors.Is(err, db.ErrNotFound) {
					t.Fatalf("step=%d Get(%q) err=%v, want ErrNotFound", step, k, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("step=%d Get(%q) unexpected err: %v", step, k, err)
			}
			if string(got) != string(want) {
				t.Fatalf("step=%d Get(%q) mismatch got(len=%d) want(len=%d)", step, k, len(got), len(want))
			}
		}

		gotPairs := collectPairs(raw.NewIterator(nil))
		keys := make([]string, 0, len(model))
		for k := range model {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(gotPairs) != len(keys) {
			t.Fatalf("step=%d iterator size=%d, want=%d", step, len(gotPairs), len(keys))
		}
		for i, k := range keys {
			if gotPairs[i][0] != k {
				t.Fatalf("step=%d iterator key[%d]=%q, want %q", step, i, gotPairs[i][0], k)
			}
			if gotPairs[i][1] != string(model[k]) {
				t.Fatalf("step=%d iterator val mismatch on key=%q", step, k)
			}
		}
	}

	for i := 0; i < ops; i++ {
		op := rng.Intn(100)
		switch {
		case op < 50:
			k := fmt.Sprintf("rk-%05d", rng.Intn(keySpace))
			v := mkValue(i)
			if err := raw.Put([]byte(k), v); err != nil {
				t.Fatalf("Put(%q) error: %v", k, err)
			}
			applyToModel(true, k, v)
		case op < 70:
			k := fmt.Sprintf("rk-%05d", rng.Intn(keySpace))
			if err := raw.Delete([]byte(k)); err != nil {
				t.Fatalf("Delete(%q) error: %v", k, err)
			}
			applyToModel(false, k, nil)
		case op < 90:
			b := raw.NewBatch()
			n := 1 + rng.Intn(8)
			for j := 0; j < n; j++ {
				k := fmt.Sprintf("rk-%05d", rng.Intn(keySpace))
				if rng.Intn(10) < 7 {
					v := mkValue(i*17 + j)
					b.Put([]byte(k), v)
					applyToModel(true, k, v)
				} else {
					b.Delete([]byte(k))
					applyToModel(false, k, nil)
				}
			}
			if err := raw.Write(b); err != nil {
				t.Fatalf("Write(batch) error: %v", err)
			}
		default:
			k := fmt.Sprintf("rk-%05d", rng.Intn(keySpace))
			got, err := raw.Get([]byte(k))
			want, exists := model[k]
			if !exists {
				if !errors.Is(err, db.ErrNotFound) {
					t.Fatalf("Get(%q) err=%v, want ErrNotFound", k, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Get(%q) unexpected err: %v", k, err)
				}
				if string(got) != string(want) {
					t.Fatalf("Get(%q) mismatch got(len=%d) want(len=%d)", k, len(got), len(want))
				}
			}
		}

		if i > 0 && i%5000 == 0 {
			checkSample(i)
		}
	}

	// 结尾做一次全量一致性校验
	for i := 0; i < keySpace; i++ {
		k := fmt.Sprintf("rk-%05d", i)
		got, err := raw.Get([]byte(k))
		want, exists := model[k]
		if !exists {
			if !errors.Is(err, db.ErrNotFound) {
				t.Fatalf("final Get(%q) err=%v, want ErrNotFound", k, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("final Get(%q) unexpected err: %v", k, err)
		}
		if string(got) != string(want) {
			t.Fatalf("final Get(%q) mismatch got(len=%d) want(len=%d)", k, len(got), len(want))
		}
	}
}
