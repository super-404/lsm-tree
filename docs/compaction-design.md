# Compaction 设计说明（完整版）

## 1. 背景

当前引擎已经具备以下能力：

- `active memtable -> immutable -> SST` 的 flush 闭环
- `MANIFEST` 持久化 SST 元信息
- WAL 恢复边界推进与旧 segment 回收
- SST 的 `Block Index` 与 `Bloom Filter`
- 基础 crash / recovery 测试

这意味着系统已经可以：

- 接收写入
- 落盘生成 SST
- 重启后恢复数据

但当前仍然**没有 compaction**。因此随着运行时间增长，会出现这些问题：

- SST 数量持续增加，读路径需要检查越来越多的文件
- 旧版本与 tombstone 长期堆积，空间无法回收
- `L0` 文件范围彼此重叠，scan 会先明显变慢
- manifest 中的表项越来越多，恢复与管理成本上升

前面的本机 benchmark 已经验证了一点：在没有 compaction 的情况下，`Iterator` 全表扫描会比点查更早出现明显退化。

本文定义完整的 **leveled compaction** 方案，目标是在保持实现可演进的前提下，明确最终系统应该具备的层级模型、调度规则、归并语义、tombstone 回收规则以及 crash 语义。

---

## 2. 设计目标

### 2.1 功能目标

- 支持完整的 `L0/L1/L2/.../Ln` 层级 compaction
- 在 compaction 中淘汰同 key 的旧版本
- 正确保留并最终回收删除语义，避免旧值复活
- 按层级目标大小控制文件数量与层大小
- compaction 成功后发布新的 SST，并删除旧输入文件

### 2.2 一致性目标

- compaction 的发布顺序必须与 flush 一样，遵循“先文件 durable，再元数据 durable，再切换内存视图”
- crash 时不能因为 compaction 导致数据丢失或旧值复活
- compaction 输入文件在新输出发布成功前不能提前删除
- 旧文件删除失败不能影响已发布的新结果

### 2.3 性能目标

- 控制 `L0` 文件数，避免读放大快速恶化
- 让 `L1+` 尽量保持层内范围不重叠
- 控制空间放大，逐步清理旧版本与 tombstone
- 为 scan/range query 提供更稳定的长期性能

---

## 3. 为什么选 Leveled Compaction

RocksDB 默认 compaction style 是 `Level Compaction`。它的核心操作可以概括为：

1. 从上一层挑一批 SST
2. 找出下一层与其 key range 重叠的 SST
3. 对这些输入做多路归并
4. 写出新的下一层 SST
5. 原子发布新文件并删除旧文件

这个策略的核心收益是：

- 控制每层文件数
- 尽量让 `L1+` 层内文件范围不重叠
- 降低读放大
- 在长期运行下更容易保持稳定

相较于 `Universal Compaction`：

- `Leveled` 更偏向降低读放大与空间放大
- `Universal` 更偏向降低写放大

结合当前项目现状，`Leveled` 更适合作为最终方案：

- 现有 `sst.Meta` 已经带 `Level`
- 读路径最终可以利用“层内不重叠”性质优化查询
- 更适合缓解当前最明显的 scan 退化问题

---

## 4. 层级模型

## 4.1 层定义

系统定义完整层级：

- `L0`
  - flush 直接生成的 SST
  - 允许 key range 重叠
  - 文件数多时读放大恶化最明显
- `L1/L2/.../Ln`
  - compaction 产出的层
  - 目标是层内文件 key range 尽量不重叠
  - 层号越大，通常数据越旧、总容量越大

### 4.2 L0 的特殊性

`L0` 与 `L1+` 的语义不同：

- `L0` 更像一组 flush 直接追加出来的 sorted runs
- `L1+` 才是经过整理后的正式 leveled 层

因此：

- `L0` 的 compaction 触发主要看文件数和积压压力
- `L1+` 的 compaction 触发主要看层大小和 score

### 4.3 层内不重叠约束

最终目标是：

- `L0` 可重叠
- `L1+` 层内 SST 尽量不重叠

这意味着在 `L1+` 做 compaction 时：

- 任何输入文件的 key range
- 都必须与下一层所有重叠文件一起 merge

否则无法维持层内不重叠性质。

---

## 5. 触发与调度

## 5.1 调度原则

完整方案里，compaction 不应只靠单一阈值，而应采用：

- `L0` 使用文件数/积压阈值
- `L1+` 使用 score-based 触发

### 5.2 L0 触发

推荐参数：

- `level0_file_num_compaction_trigger = 4`

即：

- 当 `L0` 文件数达到 4 时，开始触发 compaction 候选

后续还可以增加：

- `level0_slowdown_writes_trigger`
- `level0_stop_writes_trigger`

用于写入背压。

### 5.3 L1+ score

对于 `L1+`，定义：

```text
score(level) = actual_level_bytes / target_level_bytes
```

规则：

- `score > 1` 表示该层超出目标大小
- 优先 compact score 最大的层

### 5.4 调度优先级

推荐优先级：

1. `L0` 写入压力最高时优先处理 `L0`
2. 否则选择 score 最大的 `L1+`
3. 每次只执行一个 compaction job

---

## 6. 文件选择规则

## 6.1 L0 -> L1

完整方案里，`L0` 不一定每次都“全量 compact 全部 L0 文件”，但第一阶段可以从全量开始。

最终规则建议是：

- 从 `L0` 里选一批文件作为 source inputs
- 计算这些输入的总 key range
- 在 `L1` 中找到所有重叠文件
- 一起参与 compaction

### 6.2 L1+ -> next level

对于 `L1 -> L2`、`L2 -> L3` 等：

- 从 source level 选择一个或少量文件
- 计算其 key range
- 找出 next level 所有重叠文件
- 一起做 merge

这就是 leveled compaction 的核心 picking 规则。

### 6.3 扩大输入范围

必要时 picker 可以进一步扩展输入：

- 如果加入额外 source 文件不会扩大 next level overlap，允许一起合并
- 这样可以减少后续反复 compaction 同一小段范围

第一版实现可以先不做这类优化，但文档里应保留这一演进空间。

---

## 7. 多路归并语义

## 7.1 输入视图

compaction 不能只消费 `Values()`，必须消费 `Entry` 级逻辑记录：

- `Put`
- `Delete tombstone`

因此前置要求是：

- `memtable` 提供 `Entries()`
- `sst.Table` 也提供 `Entries()` / `EntriesRange()`

### 7.2 新旧优先级

归并时同 key 的版本优先级应按“新到旧”决定：

- 更新的层优先级高于更老层
- 同层中更新的文件优先级高于更老文件

对同一个 key：

- 只保留最新逻辑记录
- 更老版本全部淘汰

### 7.3 Delete 语义

如果某个 key 的最新逻辑记录是 `Delete`：

- 输出 tombstone
- 更老的 `Put` 不能再出现在输出中

这一步是保证“删除不复活”的关键。

---

## 8. Tombstone 与旧版本回收

## 8.1 第一原则

tombstone 的物理回收必须满足一个前提：

**已经能够证明更老层中不会再有这个 key 的旧值。**

否则：

- 如果 tombstone 提前丢弃
- 更老 SST 里的旧值就可能在未来再次暴露

### 8.2 分层回收规则

推荐规则：

- 当 compaction 输出还没有到最底层时：
  - tombstone 默认保留
- 当 compaction 输出到最底层，且已覆盖所有更老数据时：
  - tombstone 可以被丢弃

### 8.3 旧版本回收规则

同 key 的旧 `Put` 版本，在 compaction 中只要已经被更新版本覆盖，就可以直接淘汰。

因此：

- 旧版本 `Put` 可以较早清理
- tombstone 需要更保守

### 8.4 完整规则总结

对某个 key：

- 如果最新记录是 `Put`
  - 输出这个 `Put`
  - 丢弃所有更老版本
- 如果最新记录是 `Delete`
  - 若尚未到最底层，输出 tombstone
  - 若已到最底层且安全，可直接丢弃 tombstone

---

## 9. 输出文件切分规则

## 9.1 为什么不能总写一张大表

如果每次 compaction 都输出一张无限大的 SST：

- 单文件过大
- 后续 compaction 粒度太粗
- range overlap 会越来越重

因此完整方案必须支持按目标大小切分输出。

### 9.2 目标参数

建议定义：

- `target_file_size_base`
- `target_file_size_multiplier`
- `max_bytes_for_level_base`
- `max_bytes_for_level_multiplier`

例如：

- `L1` 有基础单文件大小与层总大小目标
- 更深层按 multiplier 放大

### 9.3 输出策略

compaction merge 输出时：

- 按 key 全局有序写出
- 累计到目标文件大小后切成一个新的 SST
- 继续写下一个输出 SST

这样有助于：

- 控制单文件大小
- 提高后续 compaction 的选择灵活性

---

## 10. Worker 与组件设计

## 10.1 组件拆分

完整方案建议拆成这些组件：

- `CompactionScheduler`
  - 定期检查是否需要 compact
- `CompactionPicker`
  - 负责选层、选文件、算 overlap
- `CompactionExecutor`
  - 负责多路归并并写新 SST
- `CompactionPublisher`
  - 负责更新 manifest、切换新旧表元数据
- `CompactionCleanup`
  - 负责删除旧 SST 与清理孤儿文件

### 10.2 并发模型

第一版推荐：

- 只允许一个后台 compaction worker
- 并且 compaction 与 flush 的 manifest 发布串行化

原因：

- 状态更少
- crash 语义更简单
- 不会出现多路后台任务争抢 manifest 发布顺序

### 10.3 与 flush 的关系

建议关系如下：

- flush 按既有规则执行
- flush 成功后检查 `L0` 压力
- 如果达到阈值，投递 compaction task
- compaction 发布时和 flush 一样，需要独占 manifest 发布路径

---

## 11. Manifest 变更

当前 `MANIFEST` 已记录：

- `NextSSTID`
- `FlushedWALSegment`
- `Tables`

完整 compaction 仍可基于现有结构演进，只需正确维护：

- flush 新表：`Level = 0`
- compaction 输出：`Level = outputLevel`

发布 compaction 时：

- 删除所有输入表的 `Meta`
- 添加新的输出表 `Meta`

如果未来调度更复杂，可再增加统计字段，但不是第一步必需。

---

## 12. 读路径影响

当前读路径已经支持：

- `active`
- `immutables`
- `ssts`

完整 compaction 接入后，`ssts` 将跨多个 level。

### 12.1 逻辑顺序

推荐逻辑顺序：

- 先查 `active`
- 再查 `immutables`
- 再查 `L0`（新到旧）
- 再查 `L1+`

### 12.2 后续优化空间

当前阶段可先维持已有合并逻辑；
但完整 leveled compaction 的最终收益在于：

- `L1+` 层内不重叠
- 对 point get 而言，每层通常最多只需要看一张 SST

因此未来读路径可以进一步优化为真正的“按层查找”，而不是简单扫所有 SST。

---

## 13. 发布与 crash 语义

## 13.1 发布顺序

compaction 发布必须遵守：

1. 新 SST 文件写完并 durable
2. 新 MANIFEST durable
3. 内存中的表集合切换到新版本
4. 旧 SST 文件删除

### 13.2 crash 场景

#### 新 SST durable、MANIFEST 未发布

- 新 SST 是孤儿文件
- 恢复时必须忽略

#### MANIFEST 已发布、旧 SST 未删

- 以 MANIFEST 为准
- 旧 SST 只是垃圾文件，不影响正确性

#### 删除旧 SST 中途 crash

- 恢复仍以 MANIFEST 为准
- 未删干净的旧文件只是清理问题

### 13.3 总结

**MANIFEST 是 compaction 发布语义的唯一真相。**

---

## 14. 错误处理

### 14.1 compaction 写新 SST 失败

- compaction 失败
- 不更新 MANIFEST
- 保留所有输入文件

### 14.2 MANIFEST 更新失败

- compaction 失败
- 输入文件继续保留
- 新输出 SST 可能成为孤儿文件

### 14.3 旧 SST 删除失败

- 不回滚 compaction 发布
- 记录错误或留待后续清理

---

## 15. 参数设计

完整版建议配置这些参数：

- `level0_file_num_compaction_trigger`
- `level0_slowdown_writes_trigger`
- `level0_stop_writes_trigger`
- `max_bytes_for_level_base`
- `max_bytes_for_level_multiplier`
- `target_file_size_base`
- `target_file_size_multiplier`
- `max_background_compactions`

第一阶段实现时，可以先只实际启用：

- `level0_file_num_compaction_trigger = 4`
- `target_file_size_base`

其它参数先保留结构。

---

## 16. 分阶段落地建议

虽然本文定义的是完整版，但实现建议仍然分阶段推进。

### 阶段 1

- 给 `sst.Table` 增加 `Entries()` / `EntriesRange()`
- 实现 compaction 专用的多路 `Entry` merge iterator
- 实现 `L0 -> L1`
- `L0` 文件数达到 4 时触发
- tombstone 先保守保留

### 阶段 2

- 支持 `L1 -> L2` 以及更深层级
- 引入按层大小 score 的 picker
- 输出文件按大小切分

### 阶段 3

- 最底层 tombstone 回收
- 更完整的读路径分层优化
- 启动时孤儿 SST 清理
- 更强的 crash / failpoint compaction 测试

---

## 17. 一句话总结

完整 leveled compaction 的核心是：

**按层级压力选择源文件，将其与下一层重叠文件一起做多路归并，输出到下一层，同时只在安全条件下回收 tombstone，并始终以 MANIFEST 作为发布与恢复的唯一真相。**
