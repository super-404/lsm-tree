package iter

// Op 表示一条逻辑记录的操作类型。
//
// 这个定义被放到公共 iterator 协议层，而不是继续留在 memtable 包里，
// 是为了让 memtable / sst / engine 在“记录类型”上也使用同一套语言。
type Op int

const (
	OpPut    Op = 1
	OpDelete Op = 0
)

// Entry 表示“每个 key 的最新逻辑记录”。
//
// 这是 flush / compaction / 调试工具最适合消费的中间语义：
//   - 每个 key 只出现一次
//   - 会保留 Delete tombstone
//   - Delete 时 Value 为 nil
type Entry struct {
	Key   []byte
	Value []byte
	Op    Op
}

// Value 表示“最终可见值”。
//
// 它已经过滤掉 Delete，只保留读路径最终应该看到的 Put。
type Value struct {
	Key   []byte
	Value []byte
}

// EntryIterator 统一描述“按 key 升序输出最新逻辑记录”的迭代器协议。
type EntryIterator interface {
	Valid() bool
	Item() Entry
	Next()
	Close()
}

// ValueIterator 统一描述“按 key 升序输出最终可见值”的迭代器协议。
type ValueIterator interface {
	Valid() bool
	Item() Value
	Next()
	Close()
}
