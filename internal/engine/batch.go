package engine

import "lsm-tree/internal/memtable"

// writeCommand 抽象单条写命令，新增命令类型时无需修改 Write 主流程（开闭原则）。
type writeCommand interface {
	apply(memtable.Memtable)
}

type putCommand struct {
	key   []byte
	value []byte
}

func (c putCommand) apply(store memtable.Memtable) {
	store.Put(c.key, c.value)
}

type deleteCommand struct {
	key []byte
}

func (c deleteCommand) apply(store memtable.Memtable) {
	store.Delete(c.key)
}

type engineBatch struct {
	owner    *engineDB
	commands []writeCommand
}

func (b *engineBatch) Put(key, value []byte) {
	b.commands = append(b.commands, putCommand{
		key:   copyBytes(key),
		value: copyBytes(value),
	})
}

func (b *engineBatch) Delete(key []byte) {
	b.commands = append(b.commands, deleteCommand{
		key: copyBytes(key),
	})
}

func (b *engineBatch) Len() int {
	return len(b.commands)
}

func (b *engineBatch) Reset() {
	b.commands = b.commands[:0]
}

func copyBytes(x []byte) []byte {
	if x == nil {
		return nil
	}
	out := make([]byte, len(x))
	copy(out, x)
	return out
}
