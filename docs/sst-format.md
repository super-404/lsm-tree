# SST 文件格式说明（草案）

本文档定义 LSM-Tree 引擎拟采用的 **SST（Sorted String Table，有序字符串表）** 磁盘格式 **v1**，与 `[wal-format.md](wal-format.md)` 一致：**多字节整数一律 Little Endian（小端序）**。

> **状态**：尚未在代码中实现；实现时以本文档为契约，版本升级时递增 `version` / `meta_version` 并保留旧版解码说明。

---

## 1. 设计目标

- **顺序写友好**：Flush 时从 Memtable 顺序写出 Data；**Block Index** 在 Data 写完后一次性写出，无需回写 Data。
- **自描述**：文件头魔数 + 版本；文件尾 Meta + 定长 Trailer，支持从尾部解析并校验。
- **可选块索引**：Meta 中记录 **块索引区偏移与长度**；无索引时长度为 0，兼容「整段 Data 单块」的简单实现。
- **与 MANIFEST 对齐**：磁盘上 SST Meta 与 MANIFEST 中每条 SST 描述使用**相同的 key 范围与长度字段编码**，便于校验与工具解析。
- **与 WAL 对齐**：字节序、CRC 算法（IEEE CRC32）、Put/Delete 类型取值与 WAL 记录类型一致（仅使用子集）。

---

## 2. 文件命名与存放

- 建议目录：`{数据目录}/sst/`（与 `wal/` 并列）。
- SST 文件名：**6 位十进制序号 + `.sst`**，例如 `000042.sst`。序号与 **MANIFEST 中的 `sst_id`** 建议一致，便于对应。
- **Level** 不在文件名中编码，由 **MANIFEST** 的 `level` 字段描述。

---

## 3. 文件整体布局

从文件开头到结尾：

```
+------------------+
| File Header      |  固定 16 字节
+------------------+
| Data Section     |  变长：紧密排列的 Record
+------------------+
| Block Index      |  变长；可为空（长度 0）
+------------------+
| Meta Footer      |  变长，长度 = Trailer.meta_size
+------------------+
| Trailer          |  固定 8 字节（文件最末尾）
+------------------+
```

**偏移关系（须同时满足）：**

- `data_offset`：**v1 固定为 `16`**（紧接 File Header）。
- `block_index_offset`：**必须等于** `data_offset + data_length`（索引区紧跟 Data，中间无填充）。
- Meta 起始偏移：`block_index_offset + block_index_length`。
- 文件总大小：`block_index_offset + block_index_length + meta_size + 8`。

读取顺序建议：

1. 读 **File Header**，校验 `magic` / `version`。
2. `Seek` 到 `EOF-8`，读 **Trailer**，得 `meta_size`。
3. 读 **Meta Footer**，校验 `meta_version`、各段长度与 **CRC**、与文件大小是否自洽。
4. 若 `block_index_length > 0`，按 **Block Index** 做 `Get` 时的块定位；否则将整个 **Data Section** 视为单块。

---

## 4. File Header（16 字节，文件偏移 0）


| 偏移  | 长度  | 类型        | 字段        | 说明                                 |
| --- | --- | --------- | --------- | ---------------------------------- |
| 0   | 8   | `[8]byte` | `magic`   | 魔数，必须为 ASCII `**LSMTSST1`**（8 字节）。 |
| 8   | 4   | `uint32`  | `version` | 格式版本；**v1 固定为 `1`**。               |
| 12  | 4   | `uint32`  | `flags`   | **v1 保留为 `0`**。                    |


---

## 5. Data Section：单条 Record 布局

Data Section 从 `**data_offset**`（v1 为 16）开始，连续 `**data_length**` 字节。

### 5.1 约束

- 所有记录的 **key 按字节序严格递增**，**同一 SST 内 key 不得重复**。
- 单条 record 负载上限建议与 WAL 的 `maxFrameBodySize` 策略一致（如 **16 MiB**），防止恶意/损坏文件导致过大分配。

### 5.2 类型字节（`op`）


| 值   | 含义                   |
| --- | -------------------- |
| `1` | Put、 设计              |
| `2` | Delete（无 value 长度字段） |


### 5.3 Put（`op == 1`）


| 顺序  | 长度          | 类型       | 说明          |
| --- | ----------- | -------- | ----------- |
| 1   | 1           | `uint8`  | `op` = `1`  |
| 2   | 4           | `uint32` | `key_len`   |
| 3   | `key_len`   | bytes    | `key`       |
| 4   | 4           | `uint32` | `value_len` |
| 5   | `value_len` | bytes    | `value`     |


### 5.4 Delete（`op == 2`）


| 顺序  | 长度        | 类型       | 说明         |
| --- | --------- | -------- | ---------- |
| 1   | 1         | `uint8`  | `op` = `2` |
| 2   | 4         | `uint32` | `key_len`  |
| 3   | `key_len` | bytes    | `key`      |


### 5.5 Flush 与墓碑

同前：逻辑视图 Flush 可能仅有 Put；Compaction 路径可写 Delete，仍须 key 全局递增。

### 5.6 逻辑块（与 Block Index 对应）

- **无 Block Index**（`block_index_length == 0`）时：整段 Data 视为 **1 个逻辑块**。
- **有 Block Index** 时：每条索引项描述一个 **物理连续** 子区间；所有块 **首尾相接**、无重叠无间隙，并 **完全覆盖** `[data_offset, data_offset + data_length)`。块内仍是一条条 Record 紧排，**不在块边界额外写块头**；块边界由索引项的 `block_start` / `block_len` 推导。

---

## 6. Block Index Section（可选）

- **位置**：`[block_index_offset, block_index_offset + block_index_length)`。
- `**block_index_length == 0`**：本区不占字节；`block_index_crc32` 为对**空输入**的 IEEE CRC32。

### 6.1 二进制布局（Little Endian）


| 顺序  | 长度                 | 类型       | 字段             | 说明                                       |
| --- | ------------------ | -------- | -------------- | ---------------------------------------- |
| 1   | 4                  | `uint32` | `block_count`  | 块个数；与 `block_index_length > 0` 时至少为 `1`。 |
| 2   | 重复 `block_count` 次 |          | **BlockEntry** | 见下表。                                     |


**单个 BlockEntry：**


| 顺序  | 长度              | 类型       | 字段              | 说明                                                                        |
| --- | --------------- | -------- | --------------- | ------------------------------------------------------------------------- |
| 1   | 8               | `uint64` | `block_start`   | 该块在文件内的起始偏移，须满足 `data_offset <= block_start < data_offset + data_length`。 |
| 2   | 4               | `uint32` | `block_len`     | 该块字节数；所有块的 `block_len` 之和须等于 `data_length`。                               |
| 3   | 4               | `uint32` | `first_key_len` | 该块内**第一条** Record 的 key 长度。                                               |
| 4   | `first_key_len` | bytes    | `first_key`     | 用于 Seek：查找 key 所属块时可对 `first_key` 做二分（须与 Data 内实际第一条 key 一致）。             |


**一致性：**

- 第一块的 `block_start` 须等于 `data_offset`。
- 第 `i+1` 块的 `block_start` 须等于第 `i` 块的 `block_start + block_len`。
- 最后一块的 `block_start + block_len` 须等于 `data_offset + data_length`。

`**block_index_length`** 须等于整块 Block Index Section 编码后的实际字节数（含 `block_count` 与所有 BlockEntry）。

---

## 7. Meta Footer（变长，紧邻 Trailer 之前）

总字节数 = `**meta_size`**（由 Trailer 给出）。字段顺序（Little Endian）：


| 顺序  | 长度            | 类型       | 字段                   | 说明                                                                                  |
| --- | ------------- | -------- | -------------------- | ----------------------------------------------------------------------------------- |
| 1   | 4             | `uint32` | `meta_version`       | **v1 固定为 `1`**（与 File Header `version` 一致）。                                         |
| 2   | 8             | `uint64` | `record_count`       | Data Section 内逻辑记录条数。                                                               |
| 3   | 8             | `uint64` | `data_offset`        | **v1 固定为 `16`**。                                                                    |
| 4   | 8             | `uint64` | `data_length`        | Data Section 总长度。                                                                   |
| 5   | 4             | `uint32` | `data_crc32`         | 对 `[data_offset, data_offset + data_length)` 的 **IEEE CRC32**。                      |
| 6   | 8             | `uint64` | `block_index_offset` | **必须等于** `data_offset + data_length`。                                               |
| 7   | 8             | `uint64` | `block_index_length` | Block Index Section 长度；`0` 表示无索引。                                                   |
| 8   | 4             | `uint32` | `block_index_crc32`  | 对 `[block_index_offset, block_index_offset + block_index_length)` 的 **IEEE CRC32**。 |
| 9   | 4             | `uint32` | `min_key_len`        | 全表最小 key 长度；`record_count == 0` 时为 `0`。                                             |
| 10  | `min_key_len` | bytes    | `min_key`            |                                                                                     |
| 11  | 4             | `uint32` | `max_key_len`        | 全表最大 key 长度；`record_count == 0` 时为 `0`。                                             |
| 12  | `max_key_len` | bytes    | `max_key`            |                                                                                     |


空 SST：`record_count == 0`，`data_length == 0`，`block_index_length == 0`，`block_index_offset == data_offset`，两个 CRC 均为空 buffer 的 CRC32。

---

## 8. Trailer（固定 8 字节，文件最末尾）

从 **EOF−8** 起：


| 偏移（相对 EOF−8） | 长度  | 类型       | 字段          |
| ------------ | --- | -------- | ----------- |
| 0            | 4   | `uint32` | `meta_size` |
| 4            | 4   | `uint32` | `eof_magic` |


- `**eof_magic`（v1）**：固定 `**0x31545353`**，磁盘末 4 字节为 ASCII `**SST1**`。
- **写入顺序**：先写 `meta_size`，再写 `eof_magic`（低地址 → 高地址）。

---

## 9. 写入顺序（Flush 伪流程）

1. 打开临时文件（如 `*.sst.tmp`）。
2. 写 **File Header**。
3. 顺序写 **Data Section**，维护 `record_count`、`min_key`、`max_key`，计算 `data_crc32`。
4. 令 `block_index_offset = 当前文件大小`（应等于 `16 + data_length`）。
5. （可选）按目标块大小（如 64KiB）切分逻辑块，写 **Block Index Section**，计算 `block_index_length` 与 `block_index_crc32`；若不写索引，则 `block_index_length = 0`，`block_index_crc32` = 空 CRC。
6. 写 **Meta Footer**（含上述全部字段）。
7. 写 **Trailer**：`meta_size`、`eof_magic`。
8. `fsync` 后 **rename** 为 `NNNNNN.sst`，并在 **MANIFEST** 中追加/更新对应条目（见下节）。

---

## 10. MANIFEST 文件格式（v1）

MANIFEST 描述**当前数据库引用哪些 SST** 及其 **level**、**路径**、**与 SST Meta 对齐的键范围与长度信息**，便于启动时无需扫描全部 SST 即可构建层结构。

### 10.1 路径与命名

- 建议路径：`{数据目录}/MANIFEST`（单文件）或 `{数据目录}/MANIFEST-000001`（多版本轮换时可后续扩展）。
- 本文仅定义 **单文件 `MANIFEST`** 的 **v1 二进制布局**。

### 10.2 文件布局

```
+------------------+
| Manifest Header  |  固定
+------------------+
| Entry 1          |
| Entry 2          |  变长条目序列
| ...              |
+------------------+
| Manifest Footer  |  固定尾部（魔数 + 整体校验可选）
+------------------+
```

为简化实现，可采用 **Header + 重复 Entry + 定长 Footer**；或 **整文件 JSON** —— 若用 JSON，**字段名与下表语义一致**即可与 SST 对齐。下文描述 **二进制 v1**。

### 10.3 Manifest Header


| 偏移  | 长度  | 类型        | 字段            | 说明                                     |
| --- | --- | --------- | ------------- | -------------------------------------- |
| 0   | 8   | `[8]byte` | `magic`       | ASCII `**LSMTMAN1`**。                  |
| 8   | 4   | `uint32`  | `version`     | **v1 为 `1`**。                          |
| 12  | 4   | `uint32`  | `entry_count` | 当前包含的 SST 条目数。                         |
| 16  | 8   | `uint64`  | `next_sst_id` | 下一个分配 SST 序号（与文件名 `000042.sst` 中数字一致）。 |


Header 固定 **24 字节**。

### 10.4 MANIFEST Entry（单条 SST 描述）

每条 Entry 紧排，字段顺序（Little Endian）：


| 顺序  | 长度            | 类型       | 字段                   | 说明                                        |
| --- | ------------- | -------- | -------------------- | ----------------------------------------- |
| 1   | 8             | `uint64` | `sst_id`             | 与文件名 `NNNNNN.sst` 中数字一致。                  |
| 2   | 4             | `uint32` | `level`              | LSM 层号，**L0 = 0**。                        |
| 3   | 4             | `uint32` | `flags`              | **v1 保留 `0`**。                            |
| 4   | 4             | `uint32` | `path_len`           | 相对数据目录的路径 UTF-8 字节数，如 `sst/000042.sst`。   |
| 5   | `path_len`    | bytes    | `path`               | 相对路径，不含盘符。                                |
| 6   | 8             | `uint64` | `file_size`          | SST 文件总字节数（与 `stat` 一致）；用于快速校验。           |
| 7   | 8             | `uint64` | `data_length`        | 与 SST Meta 中 `**data_length**` 相同。        |
| 8   | 8             | `uint64` | `block_index_length` | 与 SST Meta 中 `**block_index_length**` 相同。 |
| 9   | 4             | `uint32` | `min_key_len`        | 与 SST Meta `**min_key_len**` 相同。          |
| 10  | `min_key_len` | bytes    | `min_key`            | 与 SST Meta `**min_key**` 相同。              |
| 11  | 4             | `uint32` | `max_key_len`        | 与 SST Meta `**max_key_len**` 相同。          |
| 12  | `max_key_len` | bytes    | `max_key`            | 与 SST Meta `**max_key**` 相同。              |


**说明：**

- `**data_offset` / `block_index_offset` 不写入 MANIFEST**：二者可由 v1 规则推导（`data_offset=16`，`block_index_offset=16+data_length`），避免冗余不一致。
- `**record_count` / CRC** 可选作为扩展字段放在 **meta_version 2**；v1 以打开 SST 读 Meta 为准做深度校验。

### 10.5 Manifest Footer（可选，v1 建议）

文件末尾 **8 字节**（与 SST Trailer 风格一致）：


| 偏移（相对 EOF−8） | 长度  | 类型       | 字段            |
| ------------ | --- | -------- | ------------- |
| 0            | 4   | `uint32` | `header_size` |
| 4            | 4   | `uint32` | `eof_magic`   |


若采用 Footer，则 **Entry 区**占 `file_size - 24 - 8`；解析时：`EOF-8` 读 Footer，确认 `eof_magic`，再用 `header_size` 校验 Header。  
**若不使用 Footer**，则 `entry_count` 与 Header 后**直到文件结束**均为 Entry（实现更简单，但缺少尾魔数）。

---

## 11. SST Meta 与 MANIFEST 字段对齐表

以下字段 **语义与字节级编码**（`uint32`/`uint64` + key 长度前缀）在 **SST Meta** 与 **MANIFEST Entry** 中**一致**，Flush 后写 MANIFEST 时应从刚写完的 SST Meta **拷贝**。


| 字段                                                  | SST Meta | MANIFEST Entry    |
| --------------------------------------------------- | -------- | ----------------- |
| `min_key_len` / `min_key`                           | ✅        | ✅ 同左              |
| `max_key_len` / `max_key`                           | ✅        | ✅ 同左              |
| `data_length`                                       | ✅        | ✅ 同左              |
| `block_index_length`                                | ✅        | ✅ 同左              |
| `sst_id` / `level` / `path`                         | —        | ✅ MANIFEST 独有     |
| `data_offset` / `block_index_offset`                | ✅        | 不存（推导）            |
| `record_count` / `data_crc32` / `block_index_crc32` | ✅        | v1 可不存（打开 SST 校验） |


启动流程建议：读 MANIFEST 构建「路径 + level + key 范围」索引；打开具体 SST 时再读 **Trailer → Meta** 校验 **CRC** 与 **file_size**，并与 MANIFEST 的 `file_size`、`data_length`、`min/max key` 比对，不一致则报错或进入恢复流程。

---

## 12. 校验与安全

- **文件大小**：`block_index_offset + block_index_length + meta_size + 8 == file_size`，且 `block_index_offset == data_offset + data_length`。
- **Block Index**：`block_count` 与各 `BlockEntry` 须满足第 6 节连续性约束；`first_key` 须与 Data 内该块首条 Record 的 key 一致（实现可 Flush 时记录）。
- **MANIFEST**：`file_size` 与磁盘 `stat` 一致；`path` 指向可读 SST。

---

## 13. 与 WAL 的对应关系


| 项目         | WAL                            | SST v1                        |
| ---------- | ------------------------------ | ----------------------------- |
| 字节序        | Little Endian                  | Little Endian                 |
| CRC        | Frame Body：IEEE CRC32          | Data / Block Index：IEEE CRC32 |
| Put/Delete | `RecordPut`=1，`RecordDelete`=2 | `op` 同左                       |


---

## 14. 参考实现位置（待补充）

- `internal/sst/`：`Writer` / `Reader`、块索引构建与 Seek。
- `internal/manifest/` 或 `internal/engine/`：MANIFEST 读写、与 Flush 原子更新（先写临时 MANIFEST 再 rename）。

