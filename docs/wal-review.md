# WAL 实现检查报告

## 1. 整体结构

- **包**：`internal/wal`，与 engine 解耦，仅负责分段日志的追加、帧格式、恢复时扫描。
- **帧格式**：8 字节头（4B 体长 LittleEndian + 4B CRC32） + body；body 内为 type(1) + LSN(8) + 变长 key/value，可解析、可校验。
- **分段**：`000001.wal`、`000002.wal`，按 id 排序后顺序读；Memtable 轮转时 engine 调 `Rotate()` 切到下一段。

## 2. 正确性结论


| 项目                 | 结论                                                                                                       |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| 先写 WAL 再写 Memtable | engine 在 Put/Delete/Write 中均先 `wal.Append` 再写内存，符合先写日志语义。                                                |
| Open 时截断坏尾         | `Open` 对每个 segment 调 `scanValidPrefix`，按帧校验 CRC、解析 body，只保留到最后一帧完整位置并 `Truncate`，坏尾被丢弃。                  |
| nextLSN 恢复         | 扫描时维护 `lastLSN`，返回后 `nextLSN = lastLSN + 1`，重启后 LSN 连续。                                                  |
| Replay 与 Open 一致   | Replay 时再次 `listSegments` 并顺序读各段，读到的是 Open 时已截断后的“有效前缀”，与 Open 逻辑一致。                                     |
| Batch 不完整则丢弃       | `replayFromWAL` 用 `inBatch` + `pending`，只有遇到 `RecordBatchEnd` 才 `applyPending()`；未闭合的 batch 不会应用，符合测试预期。 |
| 编解码对称              | Put/Delete/BatchBegin/BatchEnd 的 `encodeBody` / `decodeBody` 长度与字段一致，边界检查完整（pos+长度与 `len(body)` 比较）。     |


## 3. 潜在问题与建议

### 3.1 安全：body 长度上限（建议加）

- **问题**：`scanValidPrefix` 中 `body := make([]byte, bodyLen)` 若 `bodyLen` 来自损坏或恶意文件（如 0xFFFFFFFF），会分配过大内存。
- **建议**：在读取 body 前限制 `bodyLen <= maxFrameBodySize`（例如 1<<24 或 1<<20），否则视为损坏并停止扫描、返回当前 `validOffset`。

### 3.2 bodyLen == 0 的语义

- **现状**：`scanValidPrefix` 中若 `bodyLen == 0` 则直接 `return validOffset, lastLSN, nil`，不再读 body。
- **说明**：当前从未写入 body 长度为 0 的帧，因此不会误截断；若将来用“空帧”表示段尾，需在文档中约定，避免与“损坏/截断”混淆。

### 3.3 Replay 后 SeekEnd

- **现状**：`Replay` 末尾 `l.f.Seek(0, io.SeekEnd)`，保证写入句柄指向当前段末尾。
- **说明**：Replay 时是用 `Open(seg.path)` 单独打开各段只读，未改 `l.f`；`l.f` 仍是 Open 时打开的最后一个 segment，SeekEnd 正确。

### 3.4 错误处理

- **Rotate**：若 `OpenFile(nextPath)` 成功但后续未正确更新 `l.f`，原有 `l.f` 已 Close，可能留下“无写入目标”的状态；当前逻辑是先 Close 再 Open 再赋值，顺序正确。
- **Append**：若 `writeFrame` 写了一部分后失败，当前帧不完整；下次 Open 会通过 CRC/长度检测截断到上一完整帧，语义正确。

## 4. 与 engine 的对接

- **dir 为空**：不打开 WAL，行为与“仅内存”一致。
- **dir 非空**：`wal.Open(filepath.Join(dir, "wal"))`，恢复时 `replayFromWAL()` 重放后再对外服务；Memtable 轮转时 `maybeRotate()` 内调 `wal.Rotate()`，与“一个 active 对应一个活跃 segment”的约定一致。
- **Batch**：Write 时写 `RecordBatchBegin` + 若干 Put/Delete + `RecordBatchEnd`，恢复时仅完整 batch 会 `applyPending()`，与测试 `TestLSMEngine_WALRecoveryDropsIncompleteBatch` 一致。

## 5. 小结

- 实现与设计一致：先写 WAL、分段、帧带长度与 CRC、Open 截断坏尾、LSN 恢复、Replay 只应用完整 batch。
- 建议增加 **body 长度上限** 防止异常/恶意文件导致大分配；其余为说明性建议，可按需在注释或文档中写明。

