package engine

import (
	"bytes"
	"errors"
	"fmt"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/memtable"
	"lsm-tree/internal/sst"
	"lsm-tree/internal/wal"
	"path"
	"path/filepath"
	"sync"
)

const (
	maxMemtableSizeBytes    = 64 << 20 // 64MB，超过则转为不可变并新建 active；后续可由 Options 配置
	level0CompactionTrigger = 4        // 第一版 compaction 触发阈值：L0 文件数达到 4 开始整理到 L1
)

var (
	ErrNotFound     = errors.New("engine: key not found")
	errClosed       = errors.New("engine: database is closed")
	errInvalidBatch = errors.New("engine: batch was not created by this DB")
)

// Options 打开数据库时的可选配置。
type Options struct {
	Dir string
}

// IterOptions 迭代器选项，范围 [Start, End)。
type IterOptions struct {
	Start []byte
	End   []byte
}

type mutableTable interface {
	Put(key, value []byte)
	GetLatest(key []byte) (value []byte, op memtable.Op, found bool)
	Delete(key []byte)
	Len() int
	SizeBytes() int
	Entries() lsmiter.EntryIterator
	Values() lsmiter.ValueIterator
	ValuesRange(start, end []byte) lsmiter.ValueIterator
}

// Engine 基于 Memtable 实现：仅有一个 active Memtable，超过 64MB 时转入不可变列表并新建 active。
type Engine struct {
	mu                 sync.Mutex
	flushCond          *sync.Cond
	active             mutableTable   // 当前可写
	immutables         []mutableTable // 不可变列表，immutables[0] 为最新冻结的
	ssts               []*sst.Table   // 已发布 SST，按新 -> 旧排列
	dir                string
	wal                *wal.WAL
	manifest           *manifestState
	flushCh            chan flushTask
	flushInFlight      bool
	compactionInFlight bool
	flushErr           error
	closing            bool
	closed             bool
}

// flushTask 描述一次待执行的 immutable -> SST 发布任务。
//
// hard limit = 2 的前提下，这个任务与一个 sealed WAL segment 一一对应：
//   - active 超阈值时先冻结为 immutable
//   - 同时 rotate WAL，把旧 segment 封口
//   - flush 成功后，manifest 里的恢复边界推进到这个 sealed segment
type flushTask struct {
	mt         mutableTable
	sstID      uint64
	walSegment int
}

// Open 打开（创建）数据库，dir 为数据目录，当前仅用于占位，数据仅在内存。
func Open(dir string, opts *Options) (*Engine, error) {
	if opts != nil && opts.Dir != "" {
		dir = opts.Dir
	}
	e := &Engine{
		active: memtable.NewMemtable(),
		dir:    dir,
	}
	e.flushCond = sync.NewCond(&e.mu)
	if dir == "" {
		return e, nil
	}
	manifest, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	e.manifest = manifest
	for _, meta := range manifest.Tables {
		tablePath := filepath.Join(dir, filepath.FromSlash(meta.Path))
		table, err := sst.Open(tablePath)
		if err != nil {
			return nil, err
		}
		fileMeta := table.Meta()
		if fileMeta.FileSize != meta.FileSize ||
			fileMeta.DataLength != meta.DataLength ||
			fileMeta.BlockIndexLength != meta.BlockIndexLength ||
			fileMeta.BloomFilterLength != meta.BloomFilterLength ||
			!bytes.Equal(fileMeta.MinKey, meta.MinKey) ||
			!bytes.Equal(fileMeta.MaxKey, meta.MaxKey) {
			return nil, errors.New("engine: manifest and sst meta mismatch")
		}
		table.SetPublishedMeta(meta)
		e.ssts = append(e.ssts, table)
	}
	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}
	e.wal = w
	e.flushCh = make(chan flushTask, 1)
	go e.runFlushWorker()
	if err := e.replayFromWAL(manifest.FlushedWALSegment + 1); err != nil {
		close(e.flushCh)
		_ = e.wal.Close()
		return nil, err
	}
	return e, nil
}

// maybeRotate 若 active 超过 64MB 则将其加入 immutables 并新建 active。
// 新冻结的表插到 immutables 头部，保证 immutables[0]=最新、immutables[len-1]=最老，与 Get/合并迭代的“新优先”一致。
// 正常写入路径中，同时轮转 WAL segment，做到“一个 active 对应一个活跃 segment”。
func (e *Engine) maybeRotate() error {
	if e.active.SizeBytes() <= maxMemtableSizeBytes {
		return nil
	}
	// 磁盘模式下采用 hard limit = 2：
	//  - 只允许 1 个 active + 1 个 immutable
	//  - 如果已经有 immutable 在等 flush，则不再生成第二个 immutable
	//  - 当前写仍然允许把 active 写“超”阈值，真正的背压发生在下一次写入入口
	if e.flushCh != nil && len(e.immutables) > 0 {
		return nil
	}

	sealedSegment := 0
	if e.wal != nil {
		sealedSegment = e.wal.CurrentSegmentID()
	}
	e.immutables = append([]mutableTable{e.active}, e.immutables...)
	e.active = memtable.NewMemtable()
	if e.wal != nil {
		if err := e.wal.Rotate(); err != nil {
			return err
		}
	}
	if e.flushCh != nil {
		return e.enqueueFlush(e.immutables[0], sealedSegment)
	}
	return nil
}

// maybeRotateReplay 仅在恢复回放阶段使用：保持内存旋转语义，但不修改 WAL 文件（避免 recovery 期间创建新 segment）。
func (e *Engine) maybeRotateReplay() {
	if e.active.SizeBytes() <= maxMemtableSizeBytes {
		return
	}
	e.immutables = append([]mutableTable{e.active}, e.immutables...)
	e.active = memtable.NewMemtable()
}

func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureWritable(); err != nil {
		return err
	}
	if err := e.waitForWriteRoom(); err != nil {
		return err
	}
	if e.wal != nil {
		if err := e.wal.Append(wal.Record{
			Type:  wal.RecordPut,
			Key:   key,
			Value: value,
		}); err != nil {
			return err
		}
	}
	e.active.Put(key, value)
	if err := e.maybeRotate(); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.Lock()
	memtables := append([]mutableTable{e.active}, e.immutables...)
	sstTables := append([]*sst.Table(nil), e.ssts...)
	e.mu.Unlock()
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	// 按层查 GetLatest：遇 tombstone 立即返回 NotFound，屏蔽更老层里的旧值
	for _, mt := range memtables {
		v, op, found := mt.GetLatest(key)
		if !found {
			continue
		}
		if op == memtable.OpDelete {
			return nil, ErrNotFound
		}
		return v, nil
	}
	for _, table := range sstTables {
		v, op, found := table.GetLatest(key)
		if !found {
			continue
		}
		if op == memtable.OpDelete {
			return nil, ErrNotFound
		}
		return v, nil
	}
	return nil, ErrNotFound
}

// tablesByNewest 返回 active 再 immutables（新→旧），用于 Get 与迭代顺序一致。
func (e *Engine) tablesByNewest() []mutableTable {
	out := make([]mutableTable, 0, 1+len(e.immutables))
	out = append(out, e.active)
	out = append(out, e.immutables...)
	return out
}

func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureWritable(); err != nil {
		return err
	}
	if err := e.waitForWriteRoom(); err != nil {
		return err
	}
	if e.wal != nil {
		if err := e.wal.Append(wal.Record{
			Type: wal.RecordDelete,
			Key:  key,
		}); err != nil {
			return err
		}
	}
	e.active.Delete(key)
	if err := e.maybeRotate(); err != nil {
		return err
	}
	return nil
}

func (e *Engine) NewBatch() *Batch {
	return &Batch{owner: e}
}

func (e *Engine) Write(batch *Batch) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureWritable(); err != nil {
		return err
	}
	if batch == nil || batch.owner != e {
		return errInvalidBatch
	}
	if err := e.waitForWriteRoom(); err != nil {
		return err
	}
	if e.wal != nil {
		recs := make([]wal.Record, 0, len(batch.commands)+2)
		recs = append(recs, wal.Record{Type: wal.RecordBatchBegin})
		for _, cmd := range batch.commands {
			switch c := cmd.(type) {
			case putCommand:
				recs = append(recs, wal.Record{
					Type:  wal.RecordPut,
					Key:   c.key,
					Value: c.value,
				})
			case deleteCommand:
				recs = append(recs, wal.Record{
					Type: wal.RecordDelete,
					Key:  c.key,
				})
			default:
				return errors.New("engine: unknown batch command")
			}
		}
		recs = append(recs, wal.Record{Type: wal.RecordBatchEnd})
		if err := e.wal.Append(recs...); err != nil {
			return err
		}
	}
	for _, cmd := range batch.commands {
		cmd.apply(e.active)
	}
	if err := e.maybeRotate(); err != nil {
		return err
	}
	return nil
}

func (e *Engine) NewIterator(opts *IterOptions) *Iterator {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	var start, end []byte
	if opts != nil {
		start, end = opts.Start, opts.End
	}
	memtables := append([]mutableTable{e.active}, e.immutables...)
	sstTables := append([]*sst.Table(nil), e.ssts...)
	e.mu.Unlock()
	// 合并迭代时用 Get 解析当前 key 的“有效值”，避免新层 tombstone 时仍产出老层旧值
	getValue := func(key []byte) ([]byte, bool) {
		v, err := e.Get(key)
		return v, err == nil
	}
	it := &Iterator{end: end, getValue: getValue}
	if start == nil && end == nil {
		for _, mt := range memtables {
			it.iters = append(it.iters, mt.Values())
		}
		for _, table := range sstTables {
			it.iters = append(it.iters, table.Values())
		}
	} else {
		for _, mt := range memtables {
			it.iters = append(it.iters, mt.ValuesRange(start, end))
		}
		for _, table := range sstTables {
			it.iters = append(it.iters, table.ValuesRange(start, end))
		}
	}
	it.advance()
	return it
}

func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closing = true
	for e.flushInFlight || e.compactionInFlight {
		e.flushCond.Wait()
	}
	flushCh := e.flushCh
	e.flushCh = nil
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	e.closed = true
	e.mu.Unlock()
	if flushCh != nil {
		close(flushCh)
	}
	return nil
}

func (e *Engine) ensureOpen() error {
	if e.closed || e.closing {
		return errClosed
	}
	return nil
}

func (e *Engine) ensureWritable() error {
	if e.closed || e.closing {
		return errClosed
	}
	if e.flushErr != nil {
		return e.flushErr
	}
	return nil
}

// waitForWriteRoom 在 flush 模式下执行 hard limit = 2 的背压控制。
//
// 只有满足下面两个条件时才真正阻塞：
//  1. 已经有 1 个 immutable 正在等待/执行 flush
//  2. 当前 active 又已经超过阈值，若继续写就会失去“1 active + 1 immutable”的上限
func (e *Engine) waitForWriteRoom() error {
	if e.flushCh == nil {
		return nil
	}
	for len(e.immutables) > 0 && e.active.SizeBytes() > maxMemtableSizeBytes {
		if err := e.ensureWritable(); err != nil {
			return err
		}
		e.flushCond.Wait()
	}
	return e.ensureWritable()
}

func (e *Engine) enqueueFlush(mt mutableTable, walSegment int) error {
	if e.flushCh == nil {
		return nil
	}
	meta := cloneManifest(e.manifest)
	sstID := meta.NextSSTID
	meta.NextSSTID++
	e.manifest = meta
	e.flushInFlight = true
	e.flushCh <- flushTask{mt: mt, sstID: sstID, walSegment: walSegment}
	return nil
}

// runFlushWorker 串行消费 flushTask，并按“先 SST、再 MANIFEST、最后摘内存表”的顺序发布结果。
func (e *Engine) runFlushWorker() {
	for task := range e.flushCh {
		e.processFlushTask(task)
		// flush 优先级高于 compaction：如果队列中已经有新的 flush task，
		// 先继续处理 flush，避免 active 因为后台在整理旧 SST 而更快触发背压。
		if len(e.flushCh) > 0 {
			continue
		}
		if err := e.maybeCompactL0ToL1(); err != nil {
			e.finishBackgroundError(err)
		}
	}
}

func (e *Engine) processFlushTask(task flushTask) {
	// flush 统一消费“最新逻辑记录”视图：
	//   - 同 key 只保留一条
	//   - 但 tombstone 仍会保留
	// 这样 SST 才能正确表达删除语义，而不是只保存可见值。
	mtEntryIter := task.mt.Entries()
	relPath := path.Join("sst", formatSSTFilename(task.sstID))
	fullPath := filepath.Join(e.dir, filepath.FromSlash(relPath))
	meta, err := sst.WriteFile(fullPath, task.sstID, 0, mtEntryIter)
	if err != nil {
		e.finishBackgroundError(err)
		return
	}
	meta.Path = relPath

	table, err := sst.Open(fullPath)
	if err != nil {
		e.finishBackgroundError(err)
		return
	}
	table.SetPublishedMeta(meta)

	e.mu.Lock()
	nextManifest := cloneManifest(e.manifest)
	for i := range nextManifest.Tables {
		if nextManifest.Tables[i].ID == task.sstID {
			// 该 ID 已被预分配过，仅在此覆盖元信息。
			nextManifest.Tables = append(nextManifest.Tables[:i], nextManifest.Tables[i+1:]...)
			break
		}
	}
	nextManifest.Tables = append([]sst.Meta{meta}, nextManifest.Tables...)
	if task.walSegment > nextManifest.FlushedWALSegment {
		nextManifest.FlushedWALSegment = task.walSegment
	}
	e.mu.Unlock()

	if err := saveManifest(e.dir, nextManifest); err != nil {
		e.finishBackgroundError(err)
		return
	}

	e.mu.Lock()
	e.manifest = nextManifest
	e.ssts = append([]*sst.Table{table}, e.ssts...)
	e.removeImmutable(task.mt)
	cleanupWAL := e.wal
	cleanupSegment := nextManifest.FlushedWALSegment
	e.mu.Unlock()

	// 旧 WAL segment 的删除只是空间回收动作，不应影响 flush 的发布语义：
	//   - 若在这里崩溃，恢复仍然会以 manifest 的 flushed 边界为准跳过旧段；
	//   - 若删除失败，也只是多残留一些旧日志文件，不会让本次 SST 发布失效。
	// 因此这里采用“先发布，再锁外最佳努力清理”的策略。
	if cleanupWAL != nil && cleanupSegment > 0 {
		_ = cleanupWAL.DeleteSegmentsUpTo(cleanupSegment)
	}

	// flush 对外“真正完成”的时点放在这里：
	//   - SST 已发布
	//   - immutable 已摘除
	//   - 旧 WAL 的最佳努力清理也已执行过
	// 这样等待 flush 完成的调用方看到的状态会更收敛，测试和恢复语义也更直观。
	e.mu.Lock()
	e.flushInFlight = false
	e.flushCond.Broadcast()
	e.mu.Unlock()
}

func (e *Engine) finishBackgroundError(err error) {
	e.mu.Lock()
	e.flushErr = err
	e.flushInFlight = false
	e.compactionInFlight = false
	e.flushCond.Broadcast()
	e.mu.Unlock()
}

func (e *Engine) removeImmutable(target mutableTable) {
	for i, mt := range e.immutables {
		if mt == target {
			e.immutables = append(e.immutables[:i], e.immutables[i+1:]...)
			return
		}
	}
}

func formatSSTFilename(id uint64) string {
	return fmt.Sprintf("%06d.sst", id)
}

func (e *Engine) replayFromWAL(minSegID int) error {
	if e.wal == nil {
		return nil
	}
	inBatch := false
	pending := make([]writeCommand, 0, 16)
	applyPending := func() {
		for _, cmd := range pending {
			cmd.apply(e.active)
		}
		pending = pending[:0]
	}
	return e.wal.ReplayFrom(minSegID, func(rec wal.DecodedRecord) error {
		switch rec.Type {
		case wal.RecordBatchBegin:
			inBatch = true
			pending = pending[:0]
		case wal.RecordBatchEnd:
			if inBatch {
				applyPending()
				e.maybeRotateReplay()
			}
			inBatch = false
		case wal.RecordPut:
			cmd := putCommand{key: rec.Key, value: rec.Value}
			if inBatch {
				pending = append(pending, cmd)
			} else {
				cmd.apply(e.active)
				e.maybeRotateReplay()
			}
		case wal.RecordDelete:
			cmd := deleteCommand{key: rec.Key}
			if inBatch {
				pending = append(pending, cmd)
			} else {
				cmd.apply(e.active)
				e.maybeRotateReplay()
			}
		}
		return nil
	})
}
