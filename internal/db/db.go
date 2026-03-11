package db

import "errors"

// 常见错误，实现方 Get 在 key 不存在时可返回此错误。
var ErrNotFound = errors.New("db: key not found")

// Options 打开数据库时的可选配置（由具体实现扩展）。
type Options struct {
	// Dir 数据目录，由实现解析
	Dir string
}

// DB 存储引擎接口。
// Open 由具体实现包提供，例如：engine.Open(path, opts) (DB, error)
type DB interface {
	// Put 写入单条 key-value。
	Put(key, value []byte) error
	// Get 读取 key 对应的 value；若 key 不存在返回 ErrNotFound。
	Get(key []byte) ([]byte, error)
	// Delete 删除 key（写入删除标记，读时视为不存在）。
	Delete(key []byte) error
	// NewBatch 创建新的批量写对象，用于 Write。
	NewBatch() Batch
	// Write 原子执行一批写操作。
	Write(batch Batch) error
	// NewIterator 创建迭代器，opts 为 nil 表示全表顺序迭代。
	NewIterator(opts *IterOptions) Iterator
	// Close 关闭数据库，释放资源。
	Close() error
}

// IterOptions 迭代器选项，范围 [Start, End)，左闭右开。
// Start、End 为 nil 表示无下界/无上界。
type IterOptions struct {
	Start []byte
	End   []byte
}

// Iterator 顺序迭代器，按 key 升序产出当前有效键值对。
// 与 internal/memtable.Iterator 方法集兼容，便于实现层复用。
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close()
}

// Batch 批量写：先 Put/Delete 往批内追加，再通过 DB.Write 原子提交。
type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	// Len 返回批内操作条数（Put + Delete）。
	Len() int
	// Reset 清空批内操作，便于复用同一 Batch 实例。
	Reset()
}
