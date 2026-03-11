package memtable

// Memtable 内存表接口：按“命令”存储，每条为 (key, seq, op)，底层用跳表实现
type Memtable interface {
	Put(key, value []byte)
	Get(key []byte) (value []byte, ok bool)
	// GetLatest 返回该 key 在本层的最新命令：found 时 op 为 Put 或 Delete（墓碑）；遇 Delete 时 value 为 nil，供上层做跨层阻断。
	GetLatest(key []byte) (value []byte, op Op, found bool)
	Delete(key []byte)
	Len() int
	SizeBytes() int
	// Iter 返回全表顺序迭代器（每 key 仅最新版本，已删除键不产出）。
	Iter() Iterator
	// IterRange 返回范围 [start, end) 顺序迭代器；start/end 为 nil 表示无界。
	IterRange(start, end []byte) Iterator
}

// Iterator 是 memtable 的对象化顺序迭代器。
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close()
}

// RawIterator 是供 flush/compaction 使用的零拷贝迭代器。
// Key/Value 指向内部缓冲区：调用方不得修改，且不得在 Next/Close 后保留引用。
type RawIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close()
}

// FlushIterable 供 flush/compaction 使用的零拷贝迭代能力（对象化）。
type FlushIterable interface {
	Memtable
	// RawIter 返回全表零拷贝迭代器，语义同 Iter（每 key 仅最新、已删除不产出）。
	RawIter() RawIterator
	// RawIterRange 返回范围 [start, end) 零拷贝迭代器，语义同 IterRange。
	RawIterRange(start, end []byte) RawIterator
}
