package engine

import (
	"errors"
	"lsm-tree/internal/db"
	"lsm-tree/internal/memtable"
)

const maxMemtableSizeBytes = 64 << 20 // 64MB，超过则转为不可变并新建 active；后续可由 Options 配置

var (
	errClosed       = errors.New("engine: database is closed")
	errInvalidBatch = errors.New("engine: batch was not created by this DB")
)

// engineDB 基于 Memtable 的 DB 实现：仅有一个 active Memtable，超过 64MB 时转入不可变列表并新建 active。
type engineDB struct {
	active     memtable.Memtable   // 当前可写
	immutables []memtable.Memtable // 不可变列表，immutables[0] 为最新冻结的
	dir        string
	closed     bool
}

// Open 打开（创建）数据库，dir 为数据目录，当前仅用于占位，数据仅在内存。
func Open(dir string, opts *db.Options) (db.DB, error) {
	if opts != nil && opts.Dir != "" {
		dir = opts.Dir
	}
	return &engineDB{
		active: memtable.NewMemtable(),
		dir:    dir,
	}, nil
}

// 编译期检查：*engineDB 实现 db.DB
var _ db.DB = (*engineDB)(nil)

// maybeRotate 若 active 超过 64MB 则将其加入 immutables 并新建 active。
// 新冻结的表插到 immutables 头部，保证 immutables[0]=最新、immutables[len-1]=最老，与 Get/合并迭代的“新优先”一致。
func (e *engineDB) maybeRotate() {
	if e.active.SizeBytes() <= maxMemtableSizeBytes {
		return
	}
	e.immutables = append([]memtable.Memtable{e.active}, e.immutables...)
	e.active = memtable.NewMemtable()
}

func (e *engineDB) Put(key, value []byte) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	e.active.Put(key, value)
	e.maybeRotate()
	return nil
}

func (e *engineDB) Get(key []byte) ([]byte, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	// 按层查 GetLatest：遇 tombstone 立即返回 NotFound，屏蔽更老层里的旧值
	for _, mt := range e.tablesByNewest() {
		v, op, found := mt.GetLatest(key)
		if !found {
			continue
		}
		if op == memtable.OpDelete {
			return nil, db.ErrNotFound
		}
		return v, nil
	}
	return nil, db.ErrNotFound
}

// tablesByNewest 返回 active 再 immutables（新→旧），用于 Get 与迭代顺序一致。
func (e *engineDB) tablesByNewest() []memtable.Memtable {
	out := make([]memtable.Memtable, 0, 1+len(e.immutables))
	out = append(out, e.active)
	out = append(out, e.immutables...)
	return out
}

func (e *engineDB) Delete(key []byte) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	e.active.Delete(key)
	e.maybeRotate()
	return nil
}

func (e *engineDB) NewBatch() db.Batch {
	return &engineBatch{owner: e}
}

func (e *engineDB) Write(batch db.Batch) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	b, ok := batch.(*engineBatch)
	if !ok || b.owner != e {
		return errInvalidBatch
	}
	for _, cmd := range b.commands {
		cmd.apply(e.active)
	}
	e.maybeRotate()
	return nil
}

func (e *engineDB) NewIterator(opts *db.IterOptions) db.Iterator {
	if e.closed {
		return nil
	}
	var start, end []byte
	if opts != nil {
		start, end = opts.Start, opts.End
	}
	// 合并迭代时用 Get 解析当前 key 的“有效值”，避免新层 tombstone 时仍产出老层旧值
	getValue := func(key []byte) ([]byte, bool) {
		v, err := e.Get(key)
		return v, err == nil
	}
	it := &mergeIterator{end: end, getValue: getValue}
	if start == nil && end == nil {
		it.iters = append(it.iters, e.active.Iter())
		for _, mt := range e.immutables {
			it.iters = append(it.iters, mt.Iter())
		}
	} else {
		it.iters = append(it.iters, e.active.IterRange(start, end))
		for _, mt := range e.immutables {
			it.iters = append(it.iters, mt.IterRange(start, end))
		}
	}
	it.advance()
	return it
}

func (e *engineDB) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	return nil
}

func (e *engineDB) ensureOpen() error {
	if e.closed {
		return errClosed
	}
	return nil
}
