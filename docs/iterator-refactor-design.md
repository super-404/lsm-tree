# Iterator 重构设计说明（草稿）

本文档对应当前代码的第二轮收敛结果：

- 新增公共协议包 `internal/iter`
- 共享 `Entry`、`Value`、`EntryIterator`、`ValueIterator`
- `memtable`、`sst`、`engine` 围绕同一套迭代协议协作
- 方法名统一为 `Entries / RawEntries / Values / RawValues`

## 1. 背景

当前 `internal/memtable` 的迭代器接口已经能支持：

- 普通读路径顺序遍历
- 范围遍历
- 零拷贝顺序扫描

但随着 SST flush 设计逐步明确，现有 iterator 语义暴露出几个问题：

- 一个 `Iterator` 同时承担了“遍历语义”和“返回值所有权语义”
- `RawValues()` 名称比旧的 `RawIter()` 更清楚，但“语义层”和“返回值形态”仍需要正式拆分
- 当前迭代器内部已经写死“同 key 去重 + 跳过 Delete”，不适合直接用于 SST flush
- flush、读路径、调试工具对“同一批内存数据”的遍历需求并不相同，但当前接口没有显式区分

本文给出一版 iterator 重构方案，目标是直接理顺语义层次，并同步修改相关调用点，为后续 flush / SST / compaction 留出清晰接口。

## 2. 设计目标

### 2.1 目标

- 区分“遍历什么”和“返回数据是否拷贝”这两个维度
- 为 flush 提供能保留 tombstone 的迭代语义
- 保持普通读路径接口尽量平滑迁移
- 减少后续 SST / compaction 实现时对 iterator 语义的猜测

### 2.2 非目标

本文档暂不涉及：

- 引擎层 merge iterator 的重构
- SST reader 的 iterator 设计
- 多版本读（MVCC）
- 跳表底层结构优化

## 3. 当前问题

当前 `memtable.Iterator` 同时耦合了两类职责：

### 3.1 语义职责

当前 `advance()` 内部固定做了这些工作：

- 同 key 去重，只保留最新逻辑记录
- 删除记录（`OpDelete`）直接跳过
- 范围裁剪

因此它本质上返回的是：

- “最终可见值”

而不是：

- 原始记录流
- 或“每个 key 的最新逻辑记录”

### 3.2 所有权职责

当前还用 `copyOut` 区分：

- 返回拷贝
- 返回底层切片别名

这导致：

- “遍历语义”与“内存所有权”被混在一个类型里
- `RawIter()` 的“raw”只表示零拷贝，但名字却容易被理解为“原始记录”

### 3.3 对 flush 的影响

flush 到 SST 时，不能沿用当前“跳过 Delete”的语义。

原因是：

- SST 需要保留每个 key 的最新逻辑记录
- 若最新记录是 Delete，则必须写 tombstone
- 否则更老 SST 中的旧值可能在读路径中复活

因此 flush 需要的并不是“可见值视图”，而是另一种语义明确、保留 tombstone 的 iterator。

## 4. 重构原则

本次重构遵循两条原则：

### 4.1 语义与所有权分离

“返回什么”与“返回值是否拷贝”是两个独立维度，不应由同一个布尔参数隐式决定。

换句话说：

- `raw` 只表示零拷贝
- `entry/value/record` 才表示遍历语义

### 4.2 从底向上分层

建议将 memtable 的遍历能力拆为三个语义层次：

1. 原始记录层
2. 最新逻辑记录层
3. 可见值层

不同调用方使用不同层次，而不是试图用一个 iterator 覆盖所有场景。

## 5. 建议的数据模型

建议新增一个统一的逻辑项类型：

```go
type Entry struct {
    Key   []byte
    Value []byte
    Op    Op
}
```

约定：

- `OpPut` 时，`Value` 为最新值
- `OpDelete` 时，`Value` 为 `nil`

这个类型的作用是：

- 作为 flush / compaction / 调试工具的通用逻辑输出
- 显式表达 tombstone，而不是用“跳过该 key”隐式表示删除

## 6. 建议的迭代层次

### 6.1 `recordIter`

最底层 iterator，仅在 `memtable` 包内部使用。

语义：

- 遍历跳表里的真实 `record`
- 不去重
- 不过滤 Delete
- 顺序为：按 key 升序、同 key 按 seq 降序

输出内容：

- `record{Key, Seq, Op, Value}`

用途：

- 作为更高层 iterator 的基础
- 便于后续调试或实现更复杂聚合逻辑

### 6.2 `latestEntryIter`

建立在 `recordIter` 之上。

语义：

- 对每个 key 只输出一条“最新逻辑记录”
- 保留 Delete
- 输出按 key 升序

输出内容：

- `Entry{Key, Value, Op}`

用途：

- SST flush
- compaction 输入
- 后台工具导出逻辑状态

这是后续 flush 应直接消费的核心语义。

### 6.3 `visibleValueIter`

建立在 `latestEntryIter` 之上。

语义：

- 对每个 key 只保留最新逻辑记录
- 若该记录是 Delete，则跳过该 key
- 最终只输出用户可见的 Put

输出内容：

- `Key`
- `Value`

用途：

- 普通读路径迭代
- 当前 `Values()` / `RawValues()` 的语义

## 7. 公开接口建议

建议通过公共包 `internal/iter` 暴露两类共享 iterator 接口，`memtable` 和 `sst` 都返回它们。

### 7.1 Entry 级 iterator

```go
type EntryIterator interface {
    Valid() bool
    Item() Entry
    Next()
    Close()
}
```

对应方法：

```go
func (m *Memtable) Entries() EntryIterator
func (m *Memtable) EntriesRange(start, end []byte) EntryIterator
func (m *Memtable) RawEntries() EntryIterator
func (m *Memtable) RawEntriesRange(start, end []byte) EntryIterator
```

语义：

- 每个 key 一条最新逻辑记录
- 保留 Delete
- `Raw*` 版本零拷贝

### 7.2 Value 级 iterator

```go
type KVIterator interface {
    Valid() bool
    Item() Value
    Next()
    Close()
}
```

对应方法：

```go
func (m *Memtable) Values() ValueIterator
func (m *Memtable) ValuesRange(start, end []byte) ValueIterator
func (m *Memtable) RawValues() ValueIterator
func (m *Memtable) RawValuesRange(start, end []byte) ValueIterator
```

语义：

- 每个 key 一条最终可见值
- Delete 被过滤
- `Raw*` 版本零拷贝

## 8. 命名替换策略

本次重构不保留旧 iterator 命名兼容层，相关调用点一并修改。

新的公开命名统一为：

- `Entries()` / `EntriesRange()`
- `RawEntries()` / `RawEntriesRange()`
- `Values()` / `ValuesRange()`
- `RawValues()` / `RawValuesRange()`

旧命名直接移除：

- `Iter()` / `IterRange()`
- `RawIter()` / `RawIterRange()`

这样可以避免继续保留语义含混的名字，减少后续实现和文档里的双重表述。

## 9. 为什么不继续沿用 `RawIter()`

一个直觉上的替代方案是：

- 保留 `RawIter()` 这个名字，只修改它的行为

但这会带来两个问题：

### 9.1 名称继续模糊

即便调整行为，`RawIter()` 这个名字依然只强调“raw”，并没有说明：

- 是原始记录
- 最新逻辑记录
- 还是最终可见值

### 9.2 不利于长期维护

如果继续沿用旧命名：

- 文档需要反复解释“`RawIter` 其实不是原始记录”
- 新人阅读代码时仍然会误解它的语义
- flush / compaction / 读路径会长期共享一组含混接口

因此更合适的方向是直接改名：

- 用 `EntryIter()` / `RawEntryIter()` 表达“最新逻辑记录”
- 用 `ValueIter()` / `RawValueIter()` 表达“最终可见值”

## 10. 与 SST flush 的关系

SST flush 需要的是：

- key 升序
- 同 key 只保留最新一条逻辑记录
- 保留 Delete 作为 tombstone

因此后续 flush 应依赖：

- `EntryIter()`
- 或 `RawEntryIter()`

这也是本次重构最直接的驱动力。

## 11. 迁移步骤建议

建议按以下顺序重构：

1. 在 `memtable` 内部引入 `Entry` 类型
2. 新增底层 `recordIter`
3. 新增 `latestEntryIter`
4. 基于 `latestEntryIter` 实现 `visibleValueIter`
5. 导出 `EntryIter/RawEntryIter` 与 `ValueIter/RawValueIter`
6. 修改所有相关调用点，替换旧 `Iter/RawIter`
7. flush 设计改为依赖 Entry 级 iterator
8. 删除旧命名与相关测试/文档残留

## 12. 风险与收益

### 12.1 收益

- iterator 语义更清晰
- flush 可以正确保留 tombstone
- 后续 compaction 更容易复用同一套 Entry 语义
- “是否拷贝”不再和“逻辑含义”耦合

### 12.2 风险

- 需要调整 memtable iterator 相关测试
- 若命名迁移过快，可能影响现有调用方理解
- 如果内部抽象拆得过细，初期实现会比当前稍复杂

## 13. 待讨论问题

### 13.1 是否公开 `Entry` 类型

待定：

- 仅在 `memtable` 包内使用
- 或作为公开类型供 engine / sst / compaction 共享

### 13.2 `EntryIterator` 是否返回对象还是拆分方法

两种可选风格：

```go
Entry() Entry
```

或：

```go
Key() []byte
Value() []byte
Op() Op
```

前者更简洁，后者更贴近当前 iterator 风格。

## 14. 结论

当前 iterator 的核心问题不在于“实现不工作”，而在于：

- 一个类型同时承担了过多语义
- `raw` 与“原始记录”之间存在命名误导
- flush 所需的 tombstone 语义无法自然表达

建议的重构方向是：

- 以 `Entry` 为中间语义层
- 按“record -> latest entry -> visible value”分层
- 将“语义”和“所有权”拆开

这样可以在统一命名和语义的前提下，为 SST flush 和后续 compaction 提供更稳定的基础。
