# 项目后续需求讨论

本文档基于当前代码与设计文档，梳理已实现能力与可选的后续需求，便于排期与取舍。

---

## 1. 当前实现概览

| 模块 | 状态 | 说明 |
|------|------|------|
| **Memtable** | ✅ | 跳表、按命令存储（Put/Delete + Seq），Value/Entry 两层迭代语义与 Raw 零拷贝迭代 |
| **WAL** | ✅ | 分段文件、帧格式（长度+CRC32）、RecordPut/Delete/BatchBegin/BatchEnd，Open 时截断坏尾、按 batch 原子恢复 |
| **Engine** | ✅ | 单 active + 多 immutables、64MB 轮转、合并迭代（active 优先）、Batch 写、WAL 先写再写内存 |
| **SST / 落盘** | ❌ | 未实现；immutables 仅驻留内存，重启后仅靠 WAL 恢复 |
| **Compaction** | ❌ | 未实现；无多层级、无 SST 合并 |

当前引擎是「内存 + WAL」形态：数据在内存（active + immutables），持久化只靠 WAL；immutable 表不会被刷盘，也不会被合并压缩。

---

## 2. 后续需求方向（按优先级与依赖）

### 2.1 高优先级：SST 落盘与读路径（Flush）

**目标**：immutable Memtable 刷成磁盘上的 SST 文件，读路径能合并「active + immutables + 磁盘 SST」。

**依赖**：已有 `Memtable.Entries()` / `Memtable.RawEntries()`，可直接用于顺序扫 Memtable 写 SST，无需额外抽象层。

**主要工作**：

- **SST 文件格式**：定义块/页或简单「有序 key-value 序列 + 索引」格式；实现编码/解码与校验（如 CRC、魔数、版本）。
- **Flush 流程**：在 `maybeRotate()` 或独立后台任务中，将刚冻结的 immutable 用 `Entries()` / `RawEntries()` 顺序写出到 `dir/sst/`（或类似）下的单个 SST 文件；写完后可考虑从 immutables 中移除该表（或先保留，读时合并内存+磁盘）。
- **读路径扩展**：Get / NewIterator 除合并 active + immutables 外，还要合并磁盘上的 SST 层（按 key 范围或 LSM 层级顺序），得到全局视图。
- **元数据**：在内存或小文件中记录「当前有哪些 SST、其 key 范围/层级」，便于 Get 与迭代时定位要读的文件。

**产出**：数据可落盘、重启后可从 SST + WAL 恢复，不再完全依赖 WAL 重放整段历史（可选：WAL 在 flush 后截断或轮转以节省空间）。

---

### 2.2 高优先级：WAL 与 Flush 的协同（可选策略）

- **策略 A**：Flush 完成后，该 immutable 对应时间段内的 WAL 段可归档或删除，减少恢复时重放量。
- **策略 B**：保留 WAL 一段时间，用于崩溃恢复时「重放 WAL → 重建 active」，再与已有 SST 一起提供一致视图。
- 需明确：恢复时是「只从 SST + 新 WAL」恢复，还是「SST + 全量/部分 WAL 重放」。当前实现是「仅 WAL 重放」，引入 SST 后需要定稿恢复协议。

---

### 2.3 中优先级：Compaction（压缩/合并）

**目标**：控制 SST 数量与读放大，合并重叠 key 范围、丢弃过期/墓碑。

**依赖**：先有 SST 格式与读路径（2.1）。

**主要工作**：

- **层级或分层策略**：例如 L0（刚 flush 的 SST，可重叠）→ L1…Ln（每层内 key 不重叠、层间有序），或简化版「单层多文件 + 定期合并」。
- **Compaction 任务**：选择若干 SST（同层或跨层），多路归并（含 tombstone 删除逻辑），写出新 SST，替换旧文件并更新元数据，删除旧文件。
- **触发条件**：按 SST 数量、单层字节数、或读放大等触发；可先做「手动/按数量」简单策略。

**产出**：磁盘空间与读性能可控，避免无限增长 SST。

---

### 2.4 中低优先级：配置与可观测性

- **配置**：`engine.Options` 或引擎专属 Options：`MaxMemtableBytes`、WAL 目录、SST 目录、是否启用 flush/compaction、compaction 策略参数等。
- **可观测性**：关键指标（memtable 大小、immutable 数量、SST 数量/层级、flush/compaction 次数或延迟）通过回调或简单 metrics 暴露，便于监控与调优。

---

### 2.5 低优先级：测试、工具与文档

- **测试**：SST 编码解码单测、Flush 端到端测试（写 → 轮转 → flush → 重启 → 读）、compaction 正确性（key 覆盖、tombstone）测试。
- **工具**：CLI 或小工具：dump WAL/SST、检查文件完整性、简单 bench（与现有 bench 对齐）。
- **文档**：在现有 `wal-format.md`、`memtable-design.md` 基础上，补充 SST 格式说明（类似 `sst-format.md`）、恢复流程、compaction 策略简述。

---

## 3. 建议的迭代顺序

1. **SST 格式 + Flush**：先定 SST 布局与编解码，再实现「immutable → SST」与读路径合并 SST，保证功能闭环。
2. **恢复协议**：明确「SST + WAL」的恢复顺序与 WAL 裁剪策略，并实现。
3. **Compaction**：在单层多文件或简单多层级上实现合并与淘汰，再逐步细化策略。
4. **配置与可观测性**：按需在 1～3 中逐步加入 Options 与关键指标。

---

## 4. 与现有设计文档的对应关系

- **memtable-design.md / memtable-iterator-design.md**：已为「按命令存储」和「Flush 用 Entries」打好基础，后续只需在引擎侧调用 `Memtable.Entries()` / `Memtable.RawEntries()` 并实现 SST 写入与读路径合并。
- **wal-format.md / wal-review.md**：WAL 格式与正确性已文档化；后续若做 WAL 裁剪或与 Flush 协同，可在本文档或单独「恢复流程」文档中补充。
- **README 学习链接**：提到的 Leveled/Tiered Compaction、性能与 SSD 寿命等，可作为 2.3 的参考，不必一次做完。

---

## 5. 小结

| 需求 | 优先级 | 依赖 |
|------|--------|------|
| SST 格式 + Flush + 读路径合并 SST | 高 | 无（EntryIter 已有） |
| 恢复协议（SST + WAL） | 高 | Flush |
| Compaction | 中 | SST + 读路径 |
| 配置 / 可观测性 | 中低 | 无 |
| 测试 / 工具 / 文档 | 低 | 随功能补全 |

后续可针对「先做 Flush 还是先做 Compaction 设计」「单层还是多层级」等做进一步技术方案细化（例如单独写 `sst-format.md` 或 `compaction-design.md`）。
