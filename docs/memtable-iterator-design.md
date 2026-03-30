# Memtable 迭代器设计说明

本文说明当前 `internal/memtable` 中迭代器体系的设计目标、语义边界和性能权衡。

## 1. 设计目标

迭代器设计主要解决两类需求：

1. 读路径（安全优先）
- 调用方希望拿到可安全使用的 `key/value`，不担心内部缓冲区被篡改或悬挂引用。
- 对应方法：`Memtable.Iter()` / `Memtable.IterRange(...)`，返回 `*Iterator`（拷贝输出）。

2. Flush/Compaction 路径（性能优先）
- 后台流程希望尽量减少分配和拷贝，用顺序流方式写 SST 或做 merge。
- 对应方法：`Memtable.RawIter()` / `Memtable.RawIterRange(...)`，返回 `*Iterator`（零拷贝输出）。

核心思路：同一套迭代核心逻辑，提供两种输出模式（拷贝 / 零拷贝）。

## 2. 方法分层

### 2.1 Memtable

`Memtable` 负责通用读写与安全迭代：

- `Put/Get/Delete/Len/SizeBytes`
- `Iter() *Iterator`
- `IterRange(start, end []byte) *Iterator`

语义：
- 遍历按 `key` 升序。
- 每个 `key` 只产出一次（只看最新命令）。
- 若最新命令是 `Delete`，该 key 不产出。
- `IterRange` 范围是 `[start, end)`，`nil` 表示无界。

### 2.2 Iterator（安全）

`Iterator` 方法：
- `Valid() bool`
- `Key() []byte`
- `Value() []byte`
- `Next()`
- `Close()`

语义：
- `Key/Value` 是拷贝，调用方可修改。
- `Close()` 后应视为不可继续使用。

### 2.3 Raw Iterator（零拷贝）

`Memtable` 直接暴露：
- `RawIter() *Iterator`
- `RawIterRange(start, end []byte) *Iterator`

raw 迭代器与普通 `Iterator` 方法签名相同，但语义不同：
- `Key/Value` 指向内部缓冲区（零拷贝）。
- 调用方不得修改。
- 不得在 `Next/Close` 后继续持有引用。

不再保留 `FlushIterable` 这层额外抽象，raw 能力直接挂在 `Memtable` 上，由调用约定区分使用场景。

## 3. 底层数据语义如何映射到迭代输出

Memtable 底层存储的是命令日志记录 `record{Key, Seq, Op, Value}`，排序规则：

1. `Key` 升序
2. 同 key 下 `Seq` 降序（最新在前）

因此，遍历时同 key 的第一条就是“当前逻辑状态”。

迭代器在扫描底层序列时执行两步过滤：

1. 去重：同一个 key 仅处理第一条记录（最新版本）
2. 过滤墓碑：若第一条 `OpDelete`，跳过该 key

最终产出“当前有效键值视图”。

## 4. 范围扫描语义

`IterRange(start, end)` / `RawIterRange(start, end)` 语义一致：

- `start == nil`：从最小 key 开始
- `end == nil`：无上界
- 上界是开区间：`key >= end` 即停止

实现上起点通过 skiplist 的 lower-bound 进入（第一个 `>= start` 的位置），保证范围扫描是顺序流式。

## 5. 生命周期与状态机

迭代器的使用协议：

1. 创建：`it := mt.Iter()` 或 `it := mt.RawIterRange(...)`
2. 循环：
- `for it.Valid() { use(it.Key(), it.Value()); it.Next() }`
3. 结束：`it.Close()`

约束：
- `Valid()==false` 时，`Key()/Value()` 返回 `nil`（不应继续使用）。
- 推荐调用 `Close()` 释放内部引用（尤其 raw 迭代器）。

## 6. 性能与复杂度

### 6.1 时间复杂度

- `Iter()`：顺序扫描，整体 `O(n)`（n 为 memtable 命令数）
- `IterRange(start,end)`：定位起点 `O(log n)` + 区间扫描 `O(m)`（m 为区间内命令数）

### 6.2 空间与分配

1. `Iterator`（安全）：
- 每次产出会拷贝 `key/value`
- GC 压力更高，但安全边界清晰

2. raw `*Iterator`（零拷贝）：
- 基本无额外 payload 拷贝
- 更适合 flush/compaction 的高吞吐顺序处理
- 需要调用方严格遵守“只读 + 不持有引用”约束

## 7. 为什么不用单一接口

raw 能力现在已经直接并入 `Memtable`，因此更需要依赖命名和调用约定避免误用（改写内部数据或持有失效引用）。

当前拆分策略的优点：
- API 层面表达意图：默认安全，显式拿 raw
- 可维护性更好：性能优化和安全语义可以并行演进

## 8. 典型使用方式

### 8.1 业务读路径（安全）

```go
it := mt.IterRange([]byte("a"), []byte("m"))
defer it.Close()
for it.Valid() {
    k := it.Key()   // copy
    v := it.Value() // copy
    // 可安全保存或修改 k/v
    it.Next()
}
```

### 8.2 Flush 路径（零拷贝）

```go
it := mt.RawIter()
defer it.Close()
for it.Valid() {
    // 只读、同步消费，不能跨 Next/Close 持有
    writeKVToSST(it.Key(), it.Value())
    it.Next()
}
```

## 9. 已覆盖的关键测试点

当前测试覆盖了这些语义：

1. 顺序与范围
- 全表升序输出
- `[start, end)` 上界开区间
- `start` 不命中时从下一个 key 开始

2. 多版本与 tombstone
- 同 key 只取最新
- 最新为 delete 时跳过

3. 安全/零拷贝差异
- 安全迭代器输出可修改且不影响内部状态
- raw 迭代器可观察到底层别名（用于证明零拷贝语义）

4. 边界
- 空 key 场景
- `Close` 后状态

## 10. 后续可演进方向

1. 并发模型声明
- 当前迭代器默认单线程一致性语义，若未来支持并发写，需要引入 immutable snapshot 或外部同步约束。

2. 统一错误通道
- 若后续接入 I/O 或可失败的数据源，可给迭代器加 `Err()`。

3. 迭代器复用池
- 对安全迭代器的拷贝缓冲可考虑池化，降低大规模扫描时分配压力。

4. 基准测试
- 建议增加 `BenchmarkIter` / `BenchmarkRawIter` / `BenchmarkIterRange`，量化拷贝与零拷贝差异。
