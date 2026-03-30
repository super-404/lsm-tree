package memtable

import (
	"bytes"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/skl"
	"math"
)

// Op 命令类型：Put 写入，Delete 删除标记
type Op = lsmiter.Op

const (
	OpPut    = lsmiter.OpPut    // 1 表示写入，便于识别
	OpDelete = lsmiter.OpDelete // 0 表示删除，便于识别
)

// record 跳表存储的一条命令：(key, seq, op)，Put 时带 value
type record struct {
	Key   []byte
	Seq   uint64
	Op    Op
	Value []byte
}

// 比较：先按 Key 升序，再按 Seq 降序（同 key 下新命令排前面）
func compareRecord(a, b record) int {
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if a.Seq > b.Seq {
		return -1
	}
	if a.Seq < b.Seq {
		return 1
	}
	return 0
}

// Memtable 保存按命令追加的 key 版本，读路径始终解析同 key 的最新命令。
type Memtable struct {
	skl          *skl.SkiplistCmp[record]
	nextSeq      uint64
	payloadBytes int // 实时维护：所有 Key/Value 底层数组的字节数（跳表只计结点与 slice 头，不含此处）
}

// recordIterator 是最底层的内部迭代器，只负责顺序吐出 skiplist 中的真实 record。
//
// 它不做任何语义折叠：
//   - 不去重
//   - 不过滤 Delete
//   - 只负责范围裁剪与顺序前进
//
// 之所以单独保留这一层，是为了让更高层 iterator 能明确地在其上叠加：
//   - latest-entry 语义：同 key 仅保留最新一条，保留 Delete
//   - visible-value 语义：在 latest-entry 基础上继续过滤 Delete
type recordIterator struct {
	base  *skl.Iterator[record]
	end   []byte
	valid bool
	rec   record
}

// entryIterator 迭代“每个 key 的最新逻辑记录”。
//
// 它建立在 recordIterator 之上，负责：
//   - 同 key 去重，只保留最新一条
//   - 保留 Delete，供 flush/compaction 使用
//   - 按配置决定返回拷贝还是直接暴露底层切片
//
// 注意：这里的“raw”只表示零拷贝，不表示“原始 record 流”。
type entryIterator struct {
	records *recordIterator
	copyOut bool
	hasPrev bool
	prevKey []byte
	valid   bool
	entry   lsmiter.Entry
}

// valueIterator 迭代“最终可见值”。
//
// 它建立在 entryIterator 之上，负责：
//   - 继续跳过 Delete
//   - 最终只向读路径暴露 Put
//
// 因此：
//   - SST flush 应该使用 iter.EntryIterator
//   - 普通读路径应使用 iter.ValueIterator
type valueIterator struct {
	entries *entryIterator
	valid   bool
	item    lsmiter.Value
}

// NewMemtable 构造一个新的 Memtable 实例。
func NewMemtable() *Memtable {
	return &Memtable{skl: skl.NewSkiplistCmp(compareRecord)}
}

func (m *Memtable) Put(key, value []byte) {
	m.nextSeq++
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.skl.Add(record{Key: k, Seq: m.nextSeq, Op: OpPut, Value: v})
	m.payloadBytes += len(k) + len(v)
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	// 同 key 下 seq 大的排前面；用 Seq=MaxUint64 做哨兵，LowerBound 返回该 key 下第一条（即最新）
	r, ok := m.skl.LowerBound(record{Key: key, Seq: math.MaxUint64})
	if !ok || !bytes.Equal(r.Key, key) {
		return nil, false
	}
	if r.Op == OpDelete {
		return nil, false
	}
	// 拷贝后返回，避免调用方改写内部数据
	out := make([]byte, len(r.Value))
	copy(out, r.Value)
	return out, true
}

// GetLatest 返回本层该 key 的最新命令（Put/Delete），便于上层遇 tombstone 时跨层返回 NotFound。
func (m *Memtable) GetLatest(key []byte) ([]byte, Op, bool) {
	r, ok := m.skl.LowerBound(record{Key: key, Seq: math.MaxUint64})
	if !ok || !bytes.Equal(r.Key, key) {
		return nil, 0, false
	}
	if r.Op == OpDelete {
		return nil, OpDelete, true
	}
	out := make([]byte, len(r.Value))
	copy(out, r.Value)
	return out, OpPut, true
}

func (m *Memtable) Delete(key []byte) {
	m.nextSeq++
	k := make([]byte, len(key))
	copy(k, key)
	m.skl.Add(record{Key: k, Seq: m.nextSeq, Op: OpDelete, Value: nil})
	m.payloadBytes += len(k)
}

func (m *Memtable) Len() int {
	return m.skl.Len()
}

// SizeBytes 返回估算的占用字节数：跳表结构 + 所有 Key/Value 的 payload
func (m *Memtable) SizeBytes() int {
	return m.skl.SizeBytes() + m.payloadBytes
}

// Values 返回“最终可见值”迭代器。
//
// 该迭代器会：
//   - 同 key 去重，只保留最新逻辑记录
//   - 跳过最新记录为 Delete 的 key
//   - 返回 key/value 的拷贝，调用方可安全修改
func (m *Memtable) Values() lsmiter.ValueIterator {
	return m.ValuesRange(nil, nil)
}

// ValuesRange 返回范围 [start, end) 上的“最终可见值”迭代器。
func (m *Memtable) ValuesRange(start, end []byte) lsmiter.ValueIterator {
	return m.newValueIterator(start, end, true)
}

// RawValues 返回零拷贝的“最终可见值”迭代器。
//
// 它与 Values 的逻辑语义完全一致，唯一差别是：
//   - Key/Value 直接别名到底层内存
//   - 仅适合后台顺序扫描，不应由业务代码持久保存或修改返回切片
func (m *Memtable) RawValues() lsmiter.ValueIterator {
	return m.RawValuesRange(nil, nil)
}

// RawValuesRange 返回范围 [start, end) 上零拷贝的“最终可见值”迭代器。
func (m *Memtable) RawValuesRange(start, end []byte) lsmiter.ValueIterator {
	return m.newValueIterator(start, end, false)
}

// Entries 返回“每个 key 的最新逻辑记录”迭代器。
//
// 与 Values 不同，Entries 不会过滤 Delete，因此可直接用于 SST flush。
func (m *Memtable) Entries() lsmiter.EntryIterator {
	return m.EntriesRange(nil, nil)
}

// EntriesRange 返回范围 [start, end) 上的最新逻辑记录迭代器。
func (m *Memtable) EntriesRange(start, end []byte) lsmiter.EntryIterator {
	return m.newEntryIterator(start, end, true)
}

// RawEntries 返回零拷贝的最新逻辑记录迭代器。
func (m *Memtable) RawEntries() lsmiter.EntryIterator {
	return m.RawEntriesRange(nil, nil)
}

// RawEntriesRange 返回范围 [start, end) 上零拷贝的最新逻辑记录迭代器。
func (m *Memtable) RawEntriesRange(start, end []byte) lsmiter.EntryIterator {
	return m.newEntryIterator(start, end, false)
}

func (m *Memtable) newRecordIterator(start, end []byte) *recordIterator {
	var endCopy []byte
	if end != nil {
		endCopy = make([]byte, len(end))
		copy(endCopy, end)
	}
	var base *skl.Iterator[record]
	if start == nil {
		base = m.skl.NewIterator()
	} else {
		startCopy := make([]byte, len(start))
		copy(startCopy, start)
		base = m.skl.NewIteratorFrom(record{Key: startCopy, Seq: math.MaxUint64})
	}
	it := &recordIterator{base: base, end: endCopy}
	it.advance()
	return it
}

func (m *Memtable) newEntryIterator(start, end []byte, copyOut bool) *entryIterator {
	it := &entryIterator{
		records: m.newRecordIterator(start, end),
		copyOut: copyOut,
	}
	it.advance()
	return it
}

func (m *Memtable) newValueIterator(start, end []byte, copyOut bool) *valueIterator {
	it := &valueIterator{
		entries: m.newEntryIterator(start, end, copyOut),
	}
	it.advance()
	return it
}

func (it *recordIterator) Valid() bool {
	return it != nil && it.valid
}

// Record 返回当前原始记录；仅供更高层 iterator 组合使用。
func (it *recordIterator) Record() record {
	var zero record
	if !it.Valid() {
		return zero
	}
	return it.rec
}

func (it *recordIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *recordIterator) Close() {
	if it == nil {
		return
	}
	if it.base != nil {
		it.base.Close()
	}
	it.valid = false
	it.rec = record{}
}

// advance 只负责顺序拉取下一条原始 record，并应用 end 边界。
//
// 注意：
//   - 这里故意不做同 key 去重
//   - 也不关心 Put/Delete
//   - 它只是为更上层语义提供干净、单调前进的输入流
func (it *recordIterator) advance() {
	it.valid = false
	it.rec = record{}
	for it.base != nil && it.base.Valid() {
		r := it.base.Value()
		it.base.Next()
		if it.end != nil && bytes.Compare(r.Key, it.end) >= 0 {
			it.base.Close()
			return
		}
		it.rec = r
		it.valid = true
		return
	}
}

func (it *entryIterator) Valid() bool {
	return it != nil && it.valid
}

// Item 返回当前 key 的最新逻辑记录。
//
// 返回值中的切片是否与底层内存共享，取决于该迭代器是否为 Raw 版本。
func (it *entryIterator) Item() lsmiter.Entry {
	var zero lsmiter.Entry
	if !it.Valid() {
		return zero
	}
	return it.entry
}

func (it *entryIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *entryIterator) Close() {
	if it == nil {
		return
	}
	if it.records != nil {
		it.records.Close()
	}
	it.valid = false
	it.entry = lsmiter.Entry{}
	it.prevKey = nil
}

// advance 在 record 流上做“同 key 取最新一条”的折叠。
//
// 底层 skiplist 已保证：
//   - 先按 key 升序
//   - 同 key 按 seq 降序
//
// 因此对于同一个 key，第一条遇到的 record 就一定是最新记录；
// 后续同 key 的旧版本全部跳过即可。
func (it *entryIterator) advance() {
	it.valid = false
	it.entry = lsmiter.Entry{}
	for it.records != nil && it.records.Valid() {
		r := it.records.Record()
		it.records.Next()
		if it.hasPrev && bytes.Equal(r.Key, it.prevKey) {
			continue
		}
		it.hasPrev = true
		it.prevKey = r.Key
		if it.copyOut {
			it.entry.Key = make([]byte, len(r.Key))
			copy(it.entry.Key, r.Key)
			if r.Op == OpPut {
				it.entry.Value = make([]byte, len(r.Value))
				copy(it.entry.Value, r.Value)
			}
		} else {
			it.entry.Key = r.Key
			if r.Op == OpPut {
				it.entry.Value = r.Value
			}
		}
		it.entry.Op = r.Op
		it.valid = true
		return
	}
}

func (it *valueIterator) Valid() bool {
	return it != nil && it.valid
}

func (it *valueIterator) Item() lsmiter.Value {
	var zero lsmiter.Value
	if !it.Valid() {
		return zero
	}
	return it.item
}

func (it *valueIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *valueIterator) Close() {
	if it == nil {
		return
	}
	if it.entries != nil {
		it.entries.Close()
	}
	it.valid = false
	it.item = lsmiter.Value{}
}

// advance 在 latest-entry 语义之上再过滤 Delete，最终只产出读路径可见的 Put。
func (it *valueIterator) advance() {
	it.valid = false
	it.item = lsmiter.Value{}
	for it.entries != nil && it.entries.Valid() {
		entry := it.entries.Item()
		it.entries.Next()
		if entry.Op != OpPut {
			continue
		}
		it.item.Key = entry.Key
		it.item.Value = entry.Value
		it.valid = true
		return
	}
}
