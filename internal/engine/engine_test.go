package engine

import (
	"errors"
	"fmt"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/memtable"
	"lsm-tree/internal/sst"
	"lsm-tree/internal/wal"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func fixedWALTestDir(t testing.TB, name string) string {
	t.Helper()
	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot error: %v", err)
	}
	dir := filepath.Join(root, "test", "wal", name)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create fixed wal test dir error: %v", err)
	}
	return dir
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cur := wd
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", fmt.Errorf("go.mod not found from %s upward", wd)
		}
		cur = parent
	}
}

type fakeMemtable struct {
	size int
}

func (m *fakeMemtable) Put(key, value []byte)                            {}
func (m *fakeMemtable) Get(key []byte) ([]byte, bool)                    { return nil, false }
func (m *fakeMemtable) GetLatest(key []byte) ([]byte, memtable.Op, bool) { return nil, 0, false }
func (m *fakeMemtable) Delete(key []byte)                                {}
func (m *fakeMemtable) Len() int                                         { return 0 }
func (m *fakeMemtable) SizeBytes() int                                   { return m.size }
func (m *fakeMemtable) Entries() lsmiter.EntryIterator                   { return nil }
func (m *fakeMemtable) Values() lsmiter.ValueIterator                    { return nil }
func (m *fakeMemtable) ValuesRange(start, end []byte) lsmiter.ValueIterator {
	return nil
}

func collectPairs(it lsmiter.ValueIterator) [][2]string {
	if it == nil {
		return nil
	}
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		item := it.Item()
		got = append(got, [2]string{string(item.Key), string(item.Value)})
		it.Next()
	}
	return got
}

func largeValue() []byte {
	value := make([]byte, 256<<10) // 256KB，足够较快触发 64MB rotate
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	return value
}

func waitForFlushState(t *testing.T, e *Engine, wantSSTs int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		gotSSTs := len(e.ssts)
		gotImmutables := len(e.immutables)
		flushInFlight := e.flushInFlight
		flushErr := e.flushErr
		e.mu.Unlock()
		if flushErr != nil {
			t.Fatalf("flush failed: %v", flushErr)
		}
		// 测试里的“flush 完成”定义要和引擎里的发布时序保持一致：
		//   - SST 已发布
		//   - immutable 已摘掉
		//   - flush worker 已经走到本轮结束（包含最佳努力的 WAL 回收）
		// 否则这里会把“中间发布态”误判成最终完成态。
		if gotSSTs >= wantSSTs && gotImmutables == 0 && !flushInFlight {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t.Fatalf("waitForFlushState timeout: ssts=%d immutables=%d flushInFlight=%v flushErr=%v", len(e.ssts), len(e.immutables), e.flushInFlight, e.flushErr)
}

func waitForCompactionState(t *testing.T, e *Engine, wantLevel0, wantAtLeastLevel1 int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		level0 := 0
		level1 := 0
		for _, table := range e.ssts {
			switch table.Meta().Level {
			case 0:
				level0++
			case 1:
				level1++
			}
		}
		flushInFlight := e.flushInFlight
		compactionInFlight := e.compactionInFlight
		flushErr := e.flushErr
		e.mu.Unlock()
		if flushErr != nil {
			t.Fatalf("background task failed: %v", flushErr)
		}
		if level0 == wantLevel0 && level1 >= wantAtLeastLevel1 && !flushInFlight && !compactionInFlight {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	level0 := 0
	level1 := 0
	for _, table := range e.ssts {
		switch table.Meta().Level {
		case 0:
			level0++
		case 1:
			level1++
		}
	}
	t.Fatalf("waitForCompactionState timeout: level0=%d level1=%d flushInFlight=%v compactionInFlight=%v flushErr=%v", level0, level1, e.flushInFlight, e.compactionInFlight, e.flushErr)
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
	if !errors.Is(err, ErrNotFound) {
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
	if !errors.Is(err, ErrNotFound) {
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
	got = collectPairs(d.NewIterator(&IterOptions{Start: []byte("b"), End: []byte("n")}))
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
	got = collectPairs(d.NewIterator(&IterOptions{Start: []byte("l"), End: []byte("m")}))
	if len(got) != 0 {
		t.Fatalf("range[l,m) should be empty, got=%v", got)
	}

	// 空范围 [m,m)
	got = collectPairs(d.NewIterator(&IterOptions{Start: []byte("m"), End: []byte("m")}))
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

	if err := d.Write(&Batch{}); !errors.Is(err, errInvalidBatch) {
		t.Fatalf("Write(foreign batch): want errInvalidBatch, got %v", err)
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
	e := &Engine{active: memtable.NewMemtable()}

	e.active.Put([]byte("k"), []byte("v1"))
	e.immutables = append([]mutableTable{e.active}, e.immutables...) // older
	e.active = memtable.NewMemtable()

	e.active.Put([]byte("k"), []byte("v2"))
	e.immutables = append([]mutableTable{e.active}, e.immutables...) // newer
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

	e := &Engine{active: active, immutables: []mutableTable{older}}
	got, err := e.Get([]byte("k"))
	if err == nil {
		t.Fatalf("Get(k) = %q, nil: want ErrNotFound (tombstone in active should mask old Put)", got)
	}
	if !errors.Is(err, ErrNotFound) {
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

	e := &Engine{active: active, immutables: []mutableTable{older}}
	it := e.NewIterator(nil)
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		item := it.Item()
		got = append(got, [2]string{string(item.Key), string(item.Value)})
		it.Next()
	}
	// 只应有 "b"，不应有 "a"
	if len(got) != 1 || got[0][0] != "b" || got[0][1] != "b" {
		t.Fatalf("iterator got %v, want [[\"b\" \"b\"]] (key \"a\" must be skipped by tombstone)", got)
	}
}

// TestLSMEngine_IteratorDedupAndNewestAcrossImmutables 验证合并迭代时同 key 仅产出一次且取最新版本。
func TestLSMEngine_IteratorDedupAndNewestAcrossImmutables(t *testing.T) {
	e := &Engine{active: memtable.NewMemtable()}

	older := memtable.NewMemtable()
	older.Put([]byte("a"), []byte("old"))

	newer := memtable.NewMemtable()
	newer.Put([]byte("a"), []byte("new"))

	e.immutables = []mutableTable{newer, older}

	it := e.NewIterator(nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatal("iterator should be valid")
	}
	item := it.Item()
	if string(item.Key) != "a" || string(item.Value) != "new" {
		t.Fatalf("first pair = (%q, %q), want (\"a\", \"new\")", item.Key, item.Value)
	}
	it.Next()
	if it.Valid() {
		last := it.Item()
		t.Fatalf("iterator should be exhausted, got (%q, %q)", last.Key, last.Value)
	}
}

// TestLSMEngine_MaybeRotateThresholdBoundary 验证 64MB 阈值边界：等于阈值不轮转，超过阈值才轮转。
func TestLSMEngine_MaybeRotateThresholdBoundary(t *testing.T) {
	e := &Engine{active: &fakeMemtable{size: maxMemtableSizeBytes}}
	if err := e.maybeRotate(); err != nil {
		t.Fatalf("maybeRotate() error: %v", err)
	}

	if len(e.immutables) != 0 {
		t.Fatalf("len(immutables) = %d, want 0 when size == threshold", len(e.immutables))
	}
	if _, ok := e.active.(*fakeMemtable); !ok {
		t.Fatal("active should not rotate when size == threshold")
	}

	e.active = &fakeMemtable{size: maxMemtableSizeBytes + 1}
	if err := e.maybeRotate(); err != nil {
		t.Fatalf("maybeRotate() error: %v", err)
	}
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

	e := &Engine{
		active:     newlyFrozen,
		immutables: []mutableTable{older, oldest},
	}
	if err := e.maybeRotate(); err != nil {
		t.Fatalf("maybeRotate() error: %v", err)
	}

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

	e := raw

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

	e := raw

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
			if !errors.Is(err, ErrNotFound) {
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
				if !errors.Is(err, ErrNotFound) {
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
				if !errors.Is(err, ErrNotFound) {
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
			if !errors.Is(err, ErrNotFound) {
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

// TestLSMEngine_WALRecoveryBasic 验证 WAL 在重启后可恢复 Put/Delete/Write 的最终状态。
func TestLSMEngine_WALRecoveryBasic(t *testing.T) {
	dir := fixedWALTestDir(t, "wal_recovery_basic")
	walPath := filepath.Join(dir, "wal", "000001.wal")
	t.Logf("wal path: %s", walPath)

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	b := d.NewBatch()
	b.Put([]byte("c"), []byte("3"))
	b.Delete([]byte("b"))
	b.Put([]byte("a"), []byte("4"))
	if err := d.Write(b); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(walPath); err != nil {
		t.Fatalf("wal file not generated at %s: %v", walPath, err)
	} else if st.Size() == 0 {
		t.Fatalf("wal file %s is empty, want > 0 bytes", walPath)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if v, err := d2.Get([]byte("a")); err != nil || string(v) != "4" {
		t.Fatalf("Get(a) = %q, %v, want \"4\", nil", v, err)
	}
	if _, err := d2.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(b) err = %v, want ErrNotFound", err)
	}
	if v, err := d2.Get([]byte("c")); err != nil || string(v) != "3" {
		t.Fatalf("Get(c) = %q, %v, want \"3\", nil", v, err)
	}
}

// TestLSMEngine_WALRecoveryDropsIncompleteBatch 验证回放时只应用完整 batch；未闭合 batch 会被丢弃。
func TestLSMEngine_WALRecoveryDropsIncompleteBatch(t *testing.T) {
	dir := fixedWALTestDir(t, "wal_recovery_incomplete_batch")
	walPath := filepath.Join(dir, "wal", "000001.wal")
	t.Logf("wal path: %s", walPath)

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("wal file not generated at %s: %v", walPath, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃前写入：batch begin + put(k,new)，但没有 batch end。
	if err := w.Append(
		wal.Record{Type: wal.RecordBatchBegin},
		wal.Record{Type: wal.RecordPut, Key: []byte("k"), Value: []byte("new")},
	); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	v, err := d2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get(k) err = %v, want nil", err)
	}
	if string(v) != "old" {
		t.Fatalf("Get(k) = %q, want \"old\" (incomplete batch should be ignored)", v)
	}
}

// TestLSMEngine_WALSegmentRotateWithMemtableRotate 验证 memtable 轮转时 WAL 也会切换到下一个 segment。
func TestLSMEngine_WALSegmentRotateWithMemtableRotate(t *testing.T) {
	dir := fixedWALTestDir(t, "engine_wal_segment_rotate")
	seg1 := filepath.Join(dir, "wal", "000001.wal")
	seg2 := filepath.Join(dir, "wal", "000002.wal")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	value := make([]byte, 256<<10)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	// 写到触发第一次 memtable rotate（64MB 阈值）。
	rotated := false
	for i := 0; i < 4000; i++ {
		k := []byte(fmt.Sprintf("seg-k-%06d", i))
		if err := d.Put(k, value); err != nil {
			t.Fatalf("Put(%q) error: %v", k, err)
		}
		if _, err := os.Stat(seg2); err == nil {
			rotated = true
			break
		}
	}
	if !rotated {
		t.Fatalf("wal segment did not rotate, %s not found", seg2)
	}
	// 旋转后再写一条，确保新 segment 落盘有内容。
	if err := d.Put([]byte("seg-tail"), value); err != nil {
		t.Fatalf("Put(seg-tail) error: %v", err)
	}
	if st, err := os.Stat(seg1); err != nil {
		t.Fatalf("segment1 missing: %v", err)
	} else if st.Size() == 0 {
		t.Fatalf("segment1 is empty, want > 0")
	}
	if st, err := os.Stat(seg2); err != nil {
		t.Fatalf("segment2 missing: %v", err)
	} else if st.Size() == 0 {
		t.Fatalf("segment2 is empty, want > 0")
	}
}

// TestLSMEngine_FlushPublishesSST 验证 active 超阈值后会异步 flush 成 SST，并在发布后移除对应 immutable。
func TestLSMEngine_FlushPublishesSST(t *testing.T) {
	dir := fixedWALTestDir(t, "flush_publishes_sst")
	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	value := largeValue()
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("flush-k-%06d", i))
		if err := d.Put(key, value); err != nil {
			t.Fatalf("Put(%q) error: %v", key, err)
		}
		d.mu.Lock()
		sstCount := len(d.ssts)
		d.mu.Unlock()
		if sstCount >= 1 {
			break
		}
	}

	waitForFlushState(t, d, 1)

	sstPath := filepath.Join(dir, "sst", "000001.sst")
	if st, err := os.Stat(sstPath); err != nil {
		t.Fatalf("sst file not found at %s: %v", sstPath, err)
	} else if st.Size() == 0 {
		t.Fatalf("sst file %s is empty", sstPath)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestFilename)); err != nil {
		t.Fatalf("manifest not found: %v", err)
	}
	// flush 发布成功后，对应的 sealed WAL segment 应该被最佳努力清理掉；
	// 与此同时，新 active 对应的活跃 segment 仍必须保留，供后续写入和崩溃恢复使用。
	if _, err := os.Stat(filepath.Join(dir, "wal", "000001.wal")); !os.IsNotExist(err) {
		t.Fatalf("sealed wal segment 000001.wal should be deleted after flush, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal", "000002.wal")); err != nil {
		t.Fatalf("active wal segment 000002.wal should remain after flush: %v", err)
	}

	v, err := d.Get([]byte("flush-k-000000"))
	if err != nil {
		t.Fatalf("Get(flush-k-000000) error: %v", err)
	}
	if len(v) != len(value) {
		t.Fatalf("Get(flush-k-000000) len=%d, want %d", len(v), len(value))
	}
}

// TestLSMEngine_FlushRecoveryWithDeleteAcrossSSTs 验证 tombstone flush 到更新的 SST 后，不会让更老 SST 中的旧值复活。
func TestLSMEngine_FlushRecoveryWithDeleteAcrossSSTs(t *testing.T) {
	dir := fixedWALTestDir(t, "flush_recovery_delete")
	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	value := largeValue()
	// 第一轮 flush：把 a=old 和一批填充 key 刷进第一个 SST。
	if err := d.Put([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("fill-old-%06d", i))
		if err := d.Put(key, value); err != nil {
			t.Fatalf("fill-old Put(%q) error: %v", key, err)
		}
		d.mu.Lock()
		sstCount := len(d.ssts)
		d.mu.Unlock()
		if sstCount >= 1 {
			break
		}
	}
	waitForFlushState(t, d, 1)

	// 第二轮 flush：写入删除 a 的 tombstone，并再刷出第二个 SST。
	if err := d.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("fill-del-%06d", i))
		if err := d.Put(key, value); err != nil {
			t.Fatalf("fill-del Put(%q) error: %v", key, err)
		}
		d.mu.Lock()
		sstCount := len(d.ssts)
		d.mu.Unlock()
		if sstCount >= 2 {
			break
		}
	}
	waitForFlushState(t, d, 2)

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if _, err := d2.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(a) after reopen err=%v, want ErrNotFound", err)
	}

	got := collectPairs(d2.NewIterator(nil))
	for _, pair := range got {
		if pair[0] == "a" {
			t.Fatalf("iterator should not resurrect deleted key a, got=%v", got)
		}
	}

	d2.mu.Lock()
	defer d2.mu.Unlock()
	if len(d2.ssts) < 2 {
		t.Fatalf("reopen sst count = %d, want >= 2", len(d2.ssts))
	}
	if len(d2.immutables) != 0 {
		t.Fatalf("reopen immutables = %d, want 0 when replay skips flushed segments", len(d2.immutables))
	}
}

// TestLSMEngine_CompactionL0ToL1MergesVersions 验证最小可用 compaction 会把多个 L0 整理成 L1，
// 并在归并时正确保留“最新逻辑记录”，不会让被 tombstone 覆盖的旧值复活。
func TestLSMEngine_CompactionL0ToL1MergesVersions(t *testing.T) {
	dir := fixedWALTestDir(t, "compaction_l0_to_l1_merges_versions")
	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	value := largeValue()
	rounds := []func() error{
		func() error { return d.Put([]byte("victim"), []byte("v1")) },
		func() error { return d.Put([]byte("victim"), []byte("v2")) },
		func() error { return d.Delete([]byte("victim")) },
		func() error { return d.Put([]byte("keeper"), []byte("keep")) },
	}

	for roundIdx, prime := range rounds {
		if err := prime(); err != nil {
			t.Fatalf("prime round %d error: %v", roundIdx, err)
		}
		for i := 0; i < 4000; i++ {
			key := []byte(fmt.Sprintf("compact-fill-%d-%06d", roundIdx, i))
			if err := d.Put(key, value); err != nil {
				t.Fatalf("fill Put(%q) error: %v", key, err)
			}
			d.mu.Lock()
			sstCount := len(d.ssts)
			d.mu.Unlock()
			if sstCount >= roundIdx+1 {
				break
			}
		}
		waitForFlushState(t, d, roundIdx+1)
	}

	waitForCompactionState(t, d, 0, 1)

	if _, err := d.Get([]byte("victim")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(victim) after compaction err=%v, want ErrNotFound", err)
	}
	if got, err := d.Get([]byte("keeper")); err != nil {
		t.Fatalf("Get(keeper) error: %v", err)
	} else if string(got) != "keep" {
		t.Fatalf("Get(keeper) = %q, want %q", got, "keep")
	}

	d.mu.Lock()
	level0 := 0
	level1 := 0
	victimPresent := false
	for _, table := range d.ssts {
		switch table.Meta().Level {
		case 0:
			level0++
		case 1:
			level1++
		}
		it := table.Entries()
		for it.Valid() {
			if string(it.Item().Key) == "victim" {
				victimPresent = true
				break
			}
			it.Next()
		}
		it.Close()
	}
	d.mu.Unlock()
	if level0 != 0 {
		t.Fatalf("level0 table count = %d, want 0 after compaction", level0)
	}
	if level1 < 1 {
		t.Fatalf("level1 table count = %d, want >= 1 after compaction", level1)
	}
	if victimPresent {
		t.Fatal("bottommost compaction should drop victim tombstone entirely, but victim key still exists in output entries")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if _, err := d2.Get([]byte("victim")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(victim) after reopen err=%v, want ErrNotFound", err)
	}
	if got, err := d2.Get([]byte("keeper")); err != nil {
		t.Fatalf("Get(keeper) after reopen error: %v", err)
	} else if string(got) != "keep" {
		t.Fatalf("Get(keeper) after reopen = %q, want %q", got, "keep")
	}
}

// TestLSMEngine_RecoveryIgnoresOrphanSSTBeforeManifest 模拟“sst 已生成，但 MANIFEST 尚未发布就崩溃”。
//
// 这个场景的稳健性要求是：
//   - 重启时只能信任 MANIFEST 中正式登记过的 SST
//   - 任何未登记的孤儿 SST 都必须被忽略
//   - 对应数据仍然要从 WAL 完整回放回来，不能丢失
//
// 为了让测试更有辨识度，这里故意让“孤儿 SST”里的值和 WAL 里的值不同。
// 如果恢复后拿到的是 WAL 里的值，就说明重启确实没有错误地采用那份未发布 SST。
func TestLSMEngine_RecoveryIgnoresOrphanSSTBeforeManifest(t *testing.T) {
	dir := fixedWALTestDir(t, "recovery_ignores_orphan_sst")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("a"), []byte("wal-a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("b"), []byte("wal-b")); err != nil {
		t.Fatal(err)
	}
	// 关闭只是在测试里释放句柄，方便手工构造“崩溃时遗留”的孤儿 SST。
	// 这里没有发生 flush，因此 MANIFEST 不会发布任何 SST，恢复仍应完全依赖 WAL。
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// 人工构造一份“已经落到磁盘，但还没写入 MANIFEST”的孤儿 SST。
	// 其中 a 的值故意写成与 WAL 不同，额外再放一个只存在于孤儿 SST 的 z。
	// 若恢复后仍读取到 wal-a 且 z 不存在，就能证明这份孤儿文件被正确忽略。
	mt := memtable.NewMemtable()
	mt.Put([]byte("a"), []byte("orphan-a"))
	mt.Put([]byte("z"), []byte("orphan-z"))
	orphanPath := filepath.Join(dir, "sst", "000001.sst")
	if _, err := sst.WriteFile(orphanPath, 1, 0, mt.Entries()); err != nil {
		t.Fatalf("WriteFile(orphan sst) error: %v", err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if got, err := d2.Get([]byte("a")); err != nil {
		t.Fatalf("Get(a) after reopen error: %v", err)
	} else if string(got) != "wal-a" {
		t.Fatalf("Get(a) after reopen = %q, want %q from WAL replay (orphan SST must be ignored)", got, "wal-a")
	}
	if got, err := d2.Get([]byte("b")); err != nil {
		t.Fatalf("Get(b) after reopen error: %v", err)
	} else if string(got) != "wal-b" {
		t.Fatalf("Get(b) after reopen = %q, want %q", got, "wal-b")
	}
	if _, err := d2.Get([]byte("z")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(z) after reopen err=%v, want ErrNotFound because orphan SST must not be loaded", err)
	}

	d2.mu.Lock()
	defer d2.mu.Unlock()
	if len(d2.ssts) != 0 {
		t.Fatalf("reopen sst count = %d, want 0 because orphan SST is not referenced by manifest", len(d2.ssts))
	}
	if len(d2.immutables) != 0 {
		t.Fatalf("reopen immutables = %d, want 0 after WAL replay finishes", len(d2.immutables))
	}
}
