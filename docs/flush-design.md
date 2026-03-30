# Flush 设计说明（v1）

## 1. 背景

当前引擎已经具备以下能力：

- 单 `active memtable`
- 多 `immutable memtable`
- WAL 先写后写内存
- `active` 超过阈值后 rotate
- `Memtable.Entries()` / `Memtable.RawEntries()` 可用于顺序扫描最新逻辑记录

当前仍缺失：

- `immutable -> SST` 的落盘流程
- SST 元数据管理
- flush 与 WAL 回收的协同
- flush 期间的写入背压与错误处理

本文定义第一版 flush 方案，目标是在不引入复杂 compaction/多路后台任务的前提下，补齐最小可用的 SST 落盘能力。

## 2. 设计目标

### 2.1 功能目标

- 当 `active memtable` 超过阈值时，将其冻结为 `immutable`
- 将 `immutable` 顺序写成一个 SST 文件
- flush 成功后，将该表从内存中移除
- 读路径后续可同时覆盖 `active + immutables + SST`

### 2.2 一致性目标

- 保持“先 WAL，后内存”的写入顺序
- flush 成功前，不移除对应 `immutable`
- 只有在 SST 与 MANIFEST 都持久化成功后，才允许把该 flush 视为完成
- flush 失败时，不丢数据，不错误回收 WAL

### 2.3 简化目标

第一版刻意采用保守实现：

- `hard limit = 2`
- 最多只允许 `1 active + 1 immutable`
- 只启用 `1` 个后台 flush worker
- SST 可先不实现块索引
- 暂不引入 compaction

## 3. 术语与约束

### 3.1 hard limit = 2

按“总 memtable 数量”计数：

- 1 张 `active`
- 最多 1 张 `immutable`

即：

- 正常运行时：只有 `active`
- rotate 后：`active + immutable`
- flush 未完成前，不允许再产生第 2 张 `immutable`

### 3.2 flush trigger

当满足以下条件时触发 flush：

- `active.SizeBytes() > maxMemtableSizeBytes`

触发动作：

1. 将 `active` 冻结为 `immutable`
2. 创建新的 `active`
3. rotate WAL segment
4. 立即将新 `immutable` 投递给后台 flush worker

### 3.3 stall 条件

若同时满足：

- 已存在 1 张 `immutable` 正在 flush
- 新 `active` 又超过 memtable 阈值

则新的写入必须阻塞，直到：

- 当前 flush 完成，或
- flush 失败并返回错误

## 4. 总体方案

总体流程如下：

```text
写请求
  -> WAL.Append
  -> active memtable apply
  -> 检查是否超过阈值
      -> 否：返回
      -> 是：
           若当前无 immutable：
             rotate active -> immutable
             WAL.Rotate
             投递后台 flush
             返回
           若当前已有 immutable：
             不再 rotate
             当前写完成
             后续写入在入口处阻塞
```

后台 flush worker：

```text
flush worker
  -> 取出 immutable
  -> 顺序迭代生成 SST
  -> 写 *.sst.tmp
  -> fsync
  -> rename 为正式 SST
  -> 更新 MANIFEST
  -> 发布 SST 元信息
  -> 从内存移除 immutable
  -> 唤醒阻塞写入
```

## 5. 组件设计

### 5.1 Engine 状态扩展

建议后续在 `Engine` 中引入以下状态：

- `mu`
  - 保护 flush 状态、immutable 生命周期、SST 元信息
- `flushCond`
  - 写入路径等待 flush 完成
- `flushCh`
  - flush task 队列，容量为 1
- `flushInFlight`
  - 当前是否有 flush 在执行
- `flushErr`
  - 最近一次 flush 错误；非空时后续写入失败
- `closing`
  - 标识关闭流程中，阻止新的写入
- `ssts`
  - 已发布 SST 的内存元信息集合

### 5.2 flush task

每次 flush 对应一个任务：

- 待刷盘的 immutable memtable
- 对应的 SST ID
- 与 WAL/manifest 协同所需的信息

### 5.3 flush worker

第一版仅启用一个后台 worker，串行执行 flush，避免：

- 多个 immutable 并发发布
- WAL 回收边界复杂化
- flush 顺序与 MANIFEST 顺序不一致

## 6. Memtable 迭代要求

### 6.1 现状问题

当前 `RawValues()` 返回的是逻辑可见值：

- key 升序
- 同 key 去重
- 只产出 Put
- Delete 会被跳过

这不适合 flush 到 SST。

### 6.2 原因

如果 flush 不输出 tombstone：

- 新 memtable 中的 Delete 无法进入 SST
- 更老 SST 中的旧值会在读路径中“复活”

因此 flush 必须保留删除语义。

### 6.3 方案要求

需要一个专用于 flush 的顺序迭代接口，满足：

- 每个 key 仅输出一条“最新逻辑记录”
- 记录类型包含 `Put/Delete`
- key 按字节序升序
- 可顺序扫描，尽量减少分配与拷贝

可选形式：

- 直接使用 `Entries()` / `RawEntries()`
- 或在此基础上再封装 flush 专用适配层

## 7. 写入路径设计

### 7.1 写入前检查

所有写入入口，包括：

- `Put`
- `Delete`
- `Write(batch)`

在执行前都需要检查是否可继续写。

规则：

- 若 `flushErr != nil`，直接返回错误
- 若数据库正在关闭，返回关闭错误
- 若已有 1 张 immutable 且当前 active 已超过阈值，则阻塞等待

### 7.2 正常写入顺序

对每次写入：

1. 先写 WAL
2. 再更新 active memtable
3. 再检查是否需要 rotate

这一顺序不变。

### 7.3 rotate 规则

写后若 `active` 超过阈值：

- 若当前没有 `immutable`
  - rotate 成 `immutable`
  - 创建新 `active`
  - `WAL.Rotate()`
  - 投递 flush task
- 若当前已有 `immutable`
  - 当前写调用直接返回
  - 下一个写请求在入口处被阻塞

## 8. Flush 执行流程

### 8.1 flush 步骤

后台 flush worker 执行以下步骤：

1. 读取待刷盘 immutable
2. 使用 flush 专用迭代器顺序扫描记录
3. 创建临时 SST 文件 `*.sst.tmp`
4. 写 File Header
5. 写 Data Section
6. 第一版可不写 Block Index
7. 写 Meta Footer
8. 写 Trailer
9. `fsync` SST 文件
10. `rename` 为正式 `NNNNNN.sst`
11. 更新并持久化 MANIFEST
12. 将 SST 加入已发布集合
13. 从内存移除对应 immutable
14. 清理 flush 状态并唤醒等待写入

### 8.2 发布顺序要求

必须遵守以下顺序：

1. SST 文件写完并 durable
2. MANIFEST 更新并 durable
3. 内存中 immutable 移除

否则可能出现：

- crash 后 SST 未被 MANIFEST 引用
- 或内存表已删除但磁盘状态尚未正式发布

## 9. WAL 协同

### 9.1 rotate 时机

当 `active -> immutable` 时，立即执行 `WAL.Rotate()`。

目的：

- 让旧 immutable 对应一个已封口的 WAL segment
- 新 active 使用新的活跃 WAL segment

### 9.2 WAL 回收条件

只有在以下条件都满足后，才允许回收旧 WAL segment：

- 对应 immutable 已成功 flush 成 SST
- SST 文件已 durable
- MANIFEST 已 durable 并包含该 SST

第一版也可以先不删除 WAL，只预留这个协同点。

## 10. 读路径影响

flush 期间：

- 读路径仍应从 `immutable` 读取该批数据
- 因为 SST 还未正式发布

flush 成功后：

- 先发布 SST 元信息
- 再从内存中移除 `immutable`

未来读路径顺序应为：

- `active`
- `immutables`
- `SSTs`

并保持“新数据覆盖旧数据”的优先级。

## 11. 错误处理

### 11.1 flush 失败

若 flush 失败：

- 保留该 immutable
- 不删除对应 WAL
- 设置 `flushErr`
- 唤醒所有等待写线程
- 后续写入直接返回错误

第一版采用 fail-fast 策略，不尝试自动重试。

### 11.2 SST 成功但 MANIFEST 失败

视同 flush 失败处理：

- 磁盘上可能残留孤儿 SST 文件
- 但逻辑上不能把该 SST 视为已发布
- 不能移除内存中的 immutable

### 11.3 关闭时失败

若 `Close()` 期间 final flush 失败：

- 返回错误
- 保持 WAL 和未发布状态不变
- 由下次启动恢复或人工处理

## 12. Close 语义

建议第一版 `Close()` 语义如下：

1. 标记进入 closing 状态
2. 阻止新的写入
3. 等待当前正在进行的 flush 完成
4. 如有未落盘数据，可选择将 active 强制 flush
5. 关闭 WAL
6. 返回

可选策略：

- 简化版：只等待已有 flush，不强制 flush active
- 完整版：关闭前尽量把 active 也落成 SST

建议优先讨论后再定。

## 13. 恢复语义

引入 flush 后，恢复流程需要从“仅 WAL 回放”演进为：

1. 加载 MANIFEST
2. 打开并登记已发布 SST
3. 回放仍保留的 WAL segment
4. 重建 active/必要的内存状态

第一版 flush 可以先只做写入与发布，不立即完成完整恢复闭环，但设计上必须保留兼容空间。

## 14. 非目标

本文档第一版不解决：

- Compaction
- 多 immutable 并发 flush
- 多后台 worker
- 分层调度
- flush 自动重试
- 复杂限速策略
- Block Index 优化

## 15. 实现建议顺序

建议按以下顺序落地：

1. 基于 `Entries()` / `RawEntries()` 接通 flush 路径，补 tombstone 输出能力
2. 实现最小版 `internal/sst.Writer`
3. 实现最小版 `MANIFEST` 写入
4. 在 `Engine` 中加入 flush worker 与 stall 控制
5. 接通 `active -> immutable -> SST`
6. 扩展读路径读取 SST
7. 最后补 WAL 回收与恢复协议

## 16. 待讨论问题

### 16.1 迭代接口形式

待定：

- 新增 `FlushIter()`
- 或扩展现有 `Iterator` 接口

### 16.2 Close 行为

待定：

- 关闭时是否必须 flush final active

### 16.3 flush 失败后的模式

待定：

- 是否直接进入“只读/拒绝写入”状态
- 或允许人工触发 retry

### 16.4 第一版是否立即实现 WAL 回收

待定：

- 先只做 SST 发布，不删 WAL
- 还是同时做 segment 回收
