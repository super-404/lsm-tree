package engine

// writeCommand 抽象单条写命令，新增命令类型时无需修改 Write 主流程（开闭原则）。
type writeCommand interface {
	apply(mutableTable)
}

type putCommand struct {
	key   []byte
	value []byte
}

func (c putCommand) apply(store mutableTable) {
	store.Put(c.key, c.value)
}

type deleteCommand struct {
	key []byte
}

func (c deleteCommand) apply(store mutableTable) {
	store.Delete(c.key)
}

type Batch struct {
	owner    *Engine
	commands []writeCommand
}

func (b *Batch) Put(key, value []byte) {
	b.commands = append(b.commands, putCommand{
		key:   copyBytes(key),
		value: copyBytes(value),
	})
}

func (b *Batch) Delete(key []byte) {
	b.commands = append(b.commands, deleteCommand{
		key: copyBytes(key),
	})
}

func (b *Batch) Len() int {
	return len(b.commands)
}

func (b *Batch) Reset() {
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
