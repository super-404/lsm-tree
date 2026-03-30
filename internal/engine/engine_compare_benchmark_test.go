package engine

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

type compareBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type compareIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close()
}

type compareDB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	NewBatch() compareBatch
	Write(batch compareBatch) error
	NewIterator(opts *IterOptions) compareIterator
	Close() error
}

type compareOpKind uint8

const (
	opPut compareOpKind = iota
	opDelete
	opGet
	opBatch
)

type compareBatchOp struct {
	put   bool
	key   []byte
	value []byte
}

type compareOp struct {
	kind  compareOpKind
	key   []byte
	value []byte
	batch []compareBatchOp
}

type compareWorkload struct {
	name      string
	ops       []compareOp
	valueSize int
}

type engineAdapter struct {
	*Engine
}

func (e *engineAdapter) NewBatch() compareBatch {
	return e.Engine.NewBatch()
}

func (e *engineAdapter) Write(batch compareBatch) error {
	b, ok := batch.(*Batch)
	if !ok {
		return errInvalidBatch
	}
	return e.Engine.Write(b)
}

func (e *engineAdapter) NewIterator(opts *IterOptions) compareIterator {
	return e.Engine.NewIterator(opts)
}

// externalCompareFactories 允许在其它 *_test.go 中注入“外部 LSM 引擎”适配器。
// 例子：
//
//	func init() {
//	  externalCompareFactories["other_lsm"] = func(b *testing.B) compareDB { ... }
//	}
var externalCompareFactories = map[string]func(b *testing.B) compareDB{}

func BenchmarkLSMCompare(b *testing.B) {
	workloads := []compareWorkload{
		buildWorkload("balanced-1k", 20260311, 20000, 10000, 1024, 35, 15, 30, 20),
		buildWorkload("write-heavy-4k", 20260311, 20000, 10000, 4096, 55, 20, 10, 15),
		buildWorkload("read-heavy-256b", 20260311, 20000, 10000, 256, 10, 10, 70, 10),
	}

	factories := map[string]func(b *testing.B) compareDB{
		"my_lsm": func(b *testing.B) compareDB {
			d, err := Open("", nil)
			if err != nil {
				b.Fatalf("Open(my_lsm) error: %v", err)
			}
			return &engineAdapter{Engine: d}
		},
		"map_ref": func(b *testing.B) compareDB {
			return newMapRefDB()
		},
	}
	for name, f := range externalCompareFactories {
		factories[name] = f
	}

	engineNames := make([]string, 0, len(factories))
	for name := range factories {
		engineNames = append(engineNames, name)
	}
	sort.Strings(engineNames)

	for _, engineName := range engineNames {
		newDB := factories[engineName]
		b.Run("engine="+engineName, func(b *testing.B) {
			for _, wl := range workloads {
				wl := wl
				b.Run("workload="+wl.name, func(b *testing.B) {
					d := newDB(b)
					defer func() { _ = d.Close() }()

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						applyCompareOp(b, d, wl.ops[i%len(wl.ops)])
					}
				})
			}
		})
	}
}

func buildWorkload(name string, seed int64, opCount, keySpace, valueSize int, putPct, delPct, getPct, batchPct int) compareWorkload {
	rng := rand.New(rand.NewSource(seed))
	ops := make([]compareOp, 0, opCount)

	mkKey := func() []byte {
		return []byte(fmt.Sprintf("bench-key-%05d", rng.Intn(keySpace)))
	}
	mkValue := func(tag int) []byte {
		v := make([]byte, valueSize+1)
		for i := 0; i < valueSize; i++ {
			v[i] = byte('a' + (i % 26))
		}
		v[valueSize] = byte(tag % 251)
		return v
	}

	for i := 0; i < opCount; i++ {
		x := rng.Intn(100)
		switch {
		case x < putPct:
			ops = append(ops, compareOp{
				kind:  opPut,
				key:   mkKey(),
				value: mkValue(i),
			})
		case x < putPct+delPct:
			ops = append(ops, compareOp{
				kind: opDelete,
				key:  mkKey(),
			})
		case x < putPct+delPct+getPct:
			ops = append(ops, compareOp{
				kind: opGet,
				key:  mkKey(),
			})
		default:
			n := 1 + rng.Intn(6)
			batch := make([]compareBatchOp, 0, n)
			for j := 0; j < n; j++ {
				k := mkKey()
				if rng.Intn(10) < 7 {
					batch = append(batch, compareBatchOp{
						put:   true,
						key:   k,
						value: mkValue(i*17 + j),
					})
				} else {
					batch = append(batch, compareBatchOp{
						put: false,
						key: k,
					})
				}
			}
			ops = append(ops, compareOp{
				kind:  opBatch,
				batch: batch,
			})
		}
	}
	return compareWorkload{name: name, ops: ops, valueSize: valueSize}
}

func applyCompareOp(b *testing.B, d compareDB, op compareOp) {
	switch op.kind {
	case opPut:
		if err := d.Put(op.key, op.value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	case opDelete:
		if err := d.Delete(op.key); err != nil {
			b.Fatalf("Delete error: %v", err)
		}
	case opGet:
		_, _ = d.Get(op.key)
	case opBatch:
		batch := d.NewBatch()
		for _, bo := range op.batch {
			if bo.put {
				batch.Put(bo.key, bo.value)
			} else {
				batch.Delete(bo.key)
			}
		}
		if err := d.Write(batch); err != nil {
			b.Fatalf("Write(batch) error: %v", err)
		}
	default:
		b.Fatalf("unknown op kind: %d", op.kind)
	}
}

// -----------------------------
// map_ref: 作为可复现对照基线
// -----------------------------

type mapRefDB struct {
	data map[string][]byte
}

func newMapRefDB() *mapRefDB {
	return &mapRefDB{data: make(map[string][]byte)}
}

func (m *mapRefDB) Put(key, value []byte) error {
	m.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *mapRefDB) Get(key []byte) ([]byte, error) {
	v, ok := m.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *mapRefDB) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mapRefDB) NewBatch() compareBatch { return &mapRefBatch{} }

func (m *mapRefDB) Write(batch compareBatch) error {
	b, ok := batch.(*mapRefBatch)
	if !ok {
		return errInvalidBatch
	}
	for _, op := range b.ops {
		if op.put {
			m.data[string(op.key)] = append([]byte(nil), op.value...)
		} else {
			delete(m.data, string(op.key))
		}
	}
	return nil
}

func (m *mapRefDB) NewIterator(opts *IterOptions) compareIterator {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	start := ""
	end := ""
	hasStart := opts != nil && opts.Start != nil
	hasEnd := opts != nil && opts.End != nil
	if hasStart {
		start = string(opts.Start)
	}
	if hasEnd {
		end = string(opts.End)
	}

	filtered := make([]string, 0, len(keys))
	for _, k := range keys {
		if hasStart && k < start {
			continue
		}
		if hasEnd && k >= end {
			continue
		}
		filtered = append(filtered, k)
	}
	return &mapRefIterator{keys: filtered, db: m}
}

func (m *mapRefDB) Close() error { return nil }

type mapRefBatchOp struct {
	put   bool
	key   []byte
	value []byte
}

type mapRefBatch struct {
	ops []mapRefBatchOp
}

func (b *mapRefBatch) Put(key, value []byte) {
	b.ops = append(b.ops, mapRefBatchOp{
		put:   true,
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
}

func (b *mapRefBatch) Delete(key []byte) {
	b.ops = append(b.ops, mapRefBatchOp{
		put: false,
		key: append([]byte(nil), key...),
	})
}

type mapRefIterator struct {
	keys []string
	idx  int
	db   *mapRefDB
}

func (it *mapRefIterator) Valid() bool {
	return it != nil && it.idx < len(it.keys)
}

func (it *mapRefIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return []byte(it.keys[it.idx])
}

func (it *mapRefIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return append([]byte(nil), it.db.data[it.keys[it.idx]]...)
}

func (it *mapRefIterator) Next() {
	if it.Valid() {
		it.idx++
	}
}

func (it *mapRefIterator) Close() {
	it.keys = nil
	it.db = nil
	it.idx = 0
}
