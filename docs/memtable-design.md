# Memtable 设计方案：按命令存储

## 1. 设计思路

内存表（Memtable）**不直接存“key → value”的当前值**，而是存**写操作命令**。每条记录表示一次对某个 key 的写入或删除，例如：

- `("name", seq=9, delete)`：对 key `"name"` 的一条删除命令，序号 9
- `("name", seq=5, put, "alice")`：对 key `"name"` 的一条写入命令，序号 5，值为 `"alice"`

**Get(key)** 时，根据该 key 下**最新一条命令**（序号最大）决定返回值或“已删除”。

这样做的意义：

- 与 LSM 的 WAL / SST 语义一致：日志和 SST 里也是“按命令/记录”而不是“按当前值”组织。
- 支持多版本：同一 key 可有多条命令，通过 seq 区分新旧。
- Delete 不覆盖旧记录，只追加一条删除命令，便于后续 compaction 与恢复。

---

## 2. 数据结构

### 2.1 命令类型 Op

| 常量     | 含义     |
|----------|----------|
| `OpPut`  | 写入     |
| `OpDelete` | 删除标记（墓碑） |

### 2.2 单条记录 record

跳表里存的是结构体 `record`，表示**一条命令**：

| 字段   | 类型   | 含义 |
|--------|--------|------|
| `Key`  | `[]byte` | 键 |
| `Seq`  | `uint64` | 单调递增序号，同一 Memtable 内每条命令唯一 |
| `Op`   | `Op`     | 命令类型：Put 或 Delete |
| `Value`| `[]byte` | 仅当 Op 为 Put 时有效；Delete 时为 nil |

示例：

- Put：`record{Key: "name", Seq: 9, Op: OpPut, Value: "alice"}`
- Delete：`record{Key: "name", Seq: 12, Op: OpDelete, Value: nil}`

---

## 3. 排序规则

底层使用 `skl.SkiplistCmp[record]`，比较函数 `compareRecord` 定义顺序为：

1. **先按 Key 升序**（`bytes.Compare(a.Key, b.Key)`）
2. **再按 Seq 降序**（同 key 下，seq 大的排前面）

因此同一 key 的多条命令在跳表中**相邻**，且**最新命令（seq 最大）排在最前**。Get 时只需取该 key 下的**第一条**记录即为当前逻辑状态。

---

## 4. 接口语义

### 4.1 Put(key, value []byte)

- 分配 `nextSeq++`
- 向跳表**追加**一条记录：`record{Key: key, Seq: nextSeq, Op: OpPut, Value: value}`
- 不覆盖、不删除该 key 的旧命令

### 4.2 Get(key []byte) (value []byte, ok bool)

- 用 **LowerBound(record{Key: key, Seq: math.MaxUint64})** 在跳表中找**第一条 Key ≥ key 的记录**（因 seq 降序，等价于该 key 下 seq 最大的那条）
- 若不存在或返回的 `r.Key != key`，则 `ok = false`
- 若 `r.Op == OpDelete`，视为该 key 已删除，返回 `(nil, false)`
- 否则返回 `(r.Value, true)`

### 4.3 Delete(key []byte)

- 分配 `nextSeq++`
- 向跳表**追加**一条记录：`record{Key: key, Seq: nextSeq, Op: OpDelete, Value: nil}`
- **不删除**该 key 的旧 Put/Delete 记录；后续 Get 根据这条最新命令返回“已删除”

### 4.4 Len() int

- 返回当前**命令条数**（所有 Put + Delete 的条数），不是“不同 key 的个数”

### 4.5 SizeBytes() int

- 返回底层跳表估算占用的字节数（由 skl 实时维护）

---

## 5. 依赖与实现要点

- **底层存储**：`internal/skl.SkiplistCmp[record]`，比较函数为 `compareRecord`。
- **序号**：Memtable 内部维护 `nextSeq`，每次 Put/Delete 自增，保证同一 Memtable 内命令有序、可比较。
- **LowerBound**：依赖 skl 的 `LowerBound(target T) (T, bool)`，返回第一个 ≥ target 的元素，用于 Get 时按 key 取“最新命令”。

---

## 6. 小结

| 项目       | 说明 |
|------------|------|
| 存什么     | 命令：(key, seq, op)，Put 时带 value |
| 排序       | Key 升序，同 key 下 Seq 降序（新命令在前） |
| Get        | 取该 key 下第一条（最新）命令，若为 Delete 则返回不存在 |
| Put/Delete | 只追加命令，不覆盖、不删旧记录 |
| Len        | 命令条数 |

这样 Memtable 的形态与“按命令/日志”的 LSM 设计一致，便于后续接 WAL、Flush 成 SST 以及 Compaction。
