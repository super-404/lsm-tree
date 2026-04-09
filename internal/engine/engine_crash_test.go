package engine

import (
	"errors"
	"fmt"
	"lsm-tree/internal/memtable"
	"lsm-tree/internal/sst"
	"lsm-tree/internal/wal"
	"os"
	"path/filepath"
	"testing"
)

// simulateCrash 用“非优雅退出”的方式释放当前 Engine 持有的进程内资源。
//
// 这里刻意不走 Engine.Close()，因为 Close 会等待 flush 并做正常收尾，
// 那测试到的就是“平滑关闭”而不是“进程崩溃/进程被杀死”。
//
// 这个 helper 只做两件事：
//  1. 关闭当前打开的 WAL 文件句柄，模拟进程死亡后 OS 自动回收 fd；
//  2. 关闭 flush worker 使用的 channel，避免测试中遗留后台 goroutine。
//
// 它不会刷新任何额外状态，也不会补做 manifest/sst 发布，因此更接近 crash 语义。
func simulateCrash(t *testing.T, e *Engine) {
	t.Helper()
	if e == nil {
		return
	}

	e.mu.Lock()
	if e.flushInFlight {
		e.mu.Unlock()
		t.Fatalf("simulateCrash requires no in-flight flush; wait for flush to finish before simulating crash")
	}
	w := e.wal
	e.wal = nil
	flushCh := e.flushCh
	e.flushCh = nil
	e.closed = true
	e.closing = true
	e.mu.Unlock()

	if w != nil {
		if err := w.Close(); err != nil {
			t.Fatalf("simulateCrash close wal error: %v", err)
		}
	}
	if flushCh != nil {
		close(flushCh)
	}
}

// TestLSMEngine_CrashRecoveryAfterPut 验证用户一次 Put 成功返回后，即使进程立刻崩溃，
// 重启也能从 WAL 把这次写入恢复回来。
func TestLSMEngine_CrashRecoveryAfterPut(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_after_put")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	
	if err := d.Put([]byte("put-k"), []byte("put-v")); err != nil {
		t.Fatal(err)
	}

	simulateCrash(t, d)

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	got, err := d2.Get([]byte("put-k"))
	if err != nil {
		t.Fatalf("Get(put-k) after reopen error: %v", err)
	}
	if string(got) != "put-v" {
		t.Fatalf("Get(put-k) after reopen = %q, want %q", got, "put-v")
	}
}

// TestLSMEngine_CrashRecoveryAfterDelete 验证 Delete 成功返回后即使随即崩溃，
// 重启仍能恢复出“该 key 已删除”的最终状态。
func TestLSMEngine_CrashRecoveryAfterDelete(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_after_delete")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("del-k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("del-k")); err != nil {
		t.Fatal(err)
	}

	simulateCrash(t, d)

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if _, err := d2.Get([]byte("del-k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(del-k) after reopen err=%v, want ErrNotFound", err)
	}
}

// TestLSMEngine_CrashRecoveryAfterCommittedBatch 验证 batch 成功返回后即使立刻崩溃，
// 整个 batch 的最终状态仍然会作为一个完整单元恢复出来。
func TestLSMEngine_CrashRecoveryAfterCommittedBatch(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_after_committed_batch")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	b := d.NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("a"))
	b.Put([]byte("c"), []byte("3"))
	if err := d.Write(b); err != nil {
		t.Fatal(err)
	}

	simulateCrash(t, d)

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if _, err := d2.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(a) after reopen err=%v, want ErrNotFound", err)
	}
	if got, err := d2.Get([]byte("b")); err != nil || string(got) != "2" {
		t.Fatalf("Get(b) after reopen = %q, %v, want %q, nil", got, err, "2")
	}
	if got, err := d2.Get([]byte("c")); err != nil || string(got) != "3" {
		t.Fatalf("Get(c) after reopen = %q, %v, want %q, nil", got, err, "3")
	}
}

// TestLSMEngine_CrashRecoveryDropsIncompleteBatch 验证崩溃发生在 batch 只写了一半时，
// 恢复阶段只会应用完整 batch，未闭合的 batch 不得污染最终状态。
func TestLSMEngine_CrashRecoveryDropsIncompleteBatch(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_incomplete_batch")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃前只把 batch begin 和第一条 put 刷进了 WAL，但没有 batch end。
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

	got, err := d2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get(k) after reopen error: %v", err)
	}
	if string(got) != "old" {
		t.Fatalf("Get(k) after reopen = %q, want %q because incomplete batch must be ignored", got, "old")
	}
}

// TestLSMEngine_CrashRecoveryAfterPublishedFlush 验证崩溃发生在：
//   - 一部分数据已经 flush 成 SST 并发布
//   - 另一部分更新还留在新的活跃 WAL segment
//
// 这种跨“SST + 新 WAL”的状态下，重启后两边数据都不能丢。
func TestLSMEngine_CrashRecoveryAfterPublishedFlush(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_after_published_flush")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

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

	if err := d.Put([]byte("tail-k"), []byte("tail-v")); err != nil {
		t.Fatal(err)
	}

	simulateCrash(t, d)

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if got, err := d2.Get([]byte("flush-k-000000")); err != nil {
		t.Fatalf("Get(flush-k-000000) after reopen error: %v", err)
	} else if len(got) != len(value) {
		t.Fatalf("Get(flush-k-000000) after reopen len=%d, want %d", len(got), len(value))
	}
	if got, err := d2.Get([]byte("tail-k")); err != nil {
		t.Fatalf("Get(tail-k) after reopen error: %v", err)
	} else if string(got) != "tail-v" {
		t.Fatalf("Get(tail-k) after reopen = %q, want %q", got, "tail-v")
	}
}

// TestLSMEngine_CrashRecoveryIgnoresOrphanSSTBeforeManifest 模拟“sst 已生成，但 MANIFEST 尚未发布就崩溃”。
//
// 这个场景的稳健性要求是：
//   - 重启时只能信任 MANIFEST 中正式登记过的 SST
//   - 任何未登记的孤儿 SST 都必须被忽略
//   - 对应数据仍然要从 WAL 完整回放回来，不能丢失
//
// 为了让测试更有辨识度，这里故意让“孤儿 SST”里的值和 WAL 里的值不同。
// 如果恢复后拿到的是 WAL 里的值，就说明重启确实没有错误地采用那份未发布 SST。
func TestLSMEngine_CrashRecoveryIgnoresOrphanSSTBeforeManifest(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_ignores_orphan_sst")

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

	simulateCrash(t, d)

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
}

// TestLSMEngine_CrashRecoveryIgnoresHalfWrittenSSTTemp 模拟“sst.tmp 只写了一半就崩溃”。
//
// 这类文件既没有 rename 成正式 SST，也不会进入 MANIFEST；恢复时必须完全忽略，
// 并继续依赖 WAL 把数据找回来。
func TestLSMEngine_CrashRecoveryIgnoresHalfWrittenSSTTemp(t *testing.T) {
	dir := fixedWALTestDir(t, "crash_ignores_half_written_tmp")

	d, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("tmp-k"), []byte("tmp-v")); err != nil {
		t.Fatal(err)
	}

	simulateCrash(t, d)

	if err := os.MkdirAll(filepath.Join(dir, "sst"), 0o755); err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(dir, "sst", "000001.sst.tmp")
	// 这里故意只写一小段垃圾/半截内容，模拟写 tmp 过程中的崩溃残留。
	if err := os.WriteFile(tmpPath, []byte("partial-sst-tmp"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", tmpPath, err)
	}

	d2, err := Open(dir, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	got, err := d2.Get([]byte("tmp-k"))
	if err != nil {
		t.Fatalf("Get(tmp-k) after reopen error: %v", err)
	}
	if string(got) != "tmp-v" {
		t.Fatalf("Get(tmp-k) after reopen = %q, want %q", got, "tmp-v")
	}

	d2.mu.Lock()
	defer d2.mu.Unlock()
	if len(d2.ssts) != 0 {
		t.Fatalf("reopen sst count = %d, want 0 because half-written tmp file must be ignored", len(d2.ssts))
	}
}
