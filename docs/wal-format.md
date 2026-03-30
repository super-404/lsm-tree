# WAL 文件格式说明

本文档描述当前 LSM-Tree 引擎所用 **Write-Ahead Log（WAL，预写日志）** 的磁盘文件格式，便于实现解析器、调试与兼容性维护。

---

## 1. 总体结构

### 1.1 分段（Segment）与命名

- WAL 由**多个分段文件**组成，存放在同一目录下（如 `wal/`）。
- 分段文件命名：**6 位十进制数字 + `.wal`**，例如：
  - `000001.wal`：第一个分段
  - `000002.wal`：第二个分段
  - …
- 分段按数字 ID **升序** 使用；回放时按此顺序依次读取。
- **无全局“WAL 文件头”**：每个 `.wal` 文件从第一个 Frame 开始，无魔数或版本号。

### 1.2 单文件内布局

每个 `.wal` 文件由一连串 **Frame** 组成，无额外文件头或文件尾：

```
+----------+----------+----------+-------
| Frame 1  | Frame 2  | Frame 3  | ...
+----------+----------+----------+-------
```

每个 Frame 由 **8 字节定长 Header** 与 **变长 Body** 组成。

---

## 2. Frame 格式

### 2.1 Frame 概览

```
+------------------+------------------+
|   Frame Header   |   Frame Body     |
|   (8 bytes)      |   (变长)         |
+------------------+------------------+
```

### 2.2 Frame Header（8 字节，Little Endian）

| 偏移 | 长度 | 含义 |
|------|------|------|
| 0    | 4    | **Body 长度**（`uint32`），单位字节。 |
| 4    | 4    | **Body 的 CRC32 校验和**（IEEE 多项式），对 Body 全部字节计算。 |

- 字节序：**Little Endian**。
- 若读到的 `bodyLen == 0`，表示“无更多有效记录”，扫描在此停止（合法结束或预留）。
- 若 `bodyLen > maxFrameBodySize`（当前为 **16 MiB**），该 Frame 视为无效，其后内容视为坏尾，启动时会被截断。

### 2.3 Frame Body（变长）

Body 即**单条 WAL 记录**的编码。所有记录类型的**前 9 字节**布局一致：

| 偏移 | 长度 | 含义 |
|------|------|------|
| 0    | 1    | **记录类型**（`RecordType`，见下表）。 |
| 1    | 8    | **LSN**（Log Sequence Number，`uint64`），由 WAL 在追加时分配，单调递增。 |

**记录类型取值：**

| 值 | 常量名           | 含义           |
|----|------------------|----------------|
| 1  | `RecordPut`      | 写入键值对     |
| 2  | `RecordDelete`   | 删除键         |
| 3  | `RecordBatchBegin`| 批量开始标记   |
| 4  | `RecordBatchEnd` | 批量结束标记   |

从 Body 第 10 字节起，依记录类型不同而不同，见下。

---

## 3. 各记录类型 Body 布局

以下均假设 Body 从偏移 0 开始；**字节序均为 Little Endian**。

### 3.1 RecordPut（类型 = 1）

表示一次 `Put(key, value)`。

| 偏移 | 长度     | 含义        |
|------|----------|-------------|
| 0    | 1        | `0x01`（RecordPut） |
| 1    | 8        | LSN         |
| 9    | 4        | key 长度 `keyLen`（`uint32`） |
| 13   | keyLen   | key 的原始字节 |
| 13+keyLen | 4   | value 长度 `valueLen`（`uint32`） |
| 17+keyLen | valueLen | value 的原始字节 |

- **Body 总长**：`1 + 8 + 4 + len(key) + 4 + len(value)` 字节。

### 3.2 RecordDelete（类型 = 2）

表示一次 `Delete(key)`。

| 偏移 | 长度     | 含义        |
|------|----------|-------------|
| 0    | 1        | `0x02`（RecordDelete） |
| 1    | 8        | LSN         |
| 9    | 4        | key 长度 `keyLen`（`uint32`） |
| 13   | keyLen   | key 的原始字节 |

- **Body 总长**：`1 + 8 + 4 + len(key)` 字节。

### 3.3 RecordBatchBegin（类型 = 3）

表示一次批量写（`Write(batch)`）的开始，无 key/value。

| 偏移 | 长度 | 含义        |
|------|------|-------------|
| 0    | 1    | `0x03`（RecordBatchBegin） |
| 1    | 8    | LSN         |

- **Body 总长**：**9** 字节。

### 3.4 RecordBatchEnd（类型 = 4）

表示该批量写的结束，无 key/value。

| 偏移 | 长度 | 含义        |
|------|------|-------------|
| 0    | 1    | `0x04`（RecordBatchEnd） |
| 1    | 8    | LSN         |

- **Body 总长**：**9** 字节。

---

## 4. 批量（Batch）语义

- 一次 `Write(batch)` 在 WAL 中对应：
  - 一条 **RecordBatchBegin**
  - 若干条 **RecordPut** / **RecordDelete**（顺序与 batch 内操作一致）
  - 一条 **RecordBatchEnd**
- 恢复时，只有“从 RecordBatchBegin 到 RecordBatchEnd”**完整出现**的一段才会被应用到 memtable；若崩溃导致缺少 RecordBatchEnd，该不完整 batch 会被**整体丢弃**，以保证批量写的原子性。

---

## 5. 校验与坏尾处理

- **CRC**：对 **Frame Body** 全部字节计算 CRC32（IEEE），写入 Header 的 [4:8]；回放时先读 Body 再验 CRC，不匹配则视该 Frame 及之后为坏尾。
- **坏尾**：以下情况会令当前 Frame 被视为无效，其后的内容在 **Open** 时被截断（文件 `Truncate` 到上一帧结束位置）：
  - `bodyLen == 0` 或 `bodyLen > maxFrameBodySize`
  - 读 Body 时发生 EOF / UnexpectedEOF
  - Body 的 CRC 与 Header 中存储的不一致
  - Body 解码失败（如长度字段非法、类型未知等）

---

## 6. 常量与限制汇总

| 项目 | 值 | 说明 |
|------|-----|------|
| 单帧 Body 最大长度 | 16 MiB（`16 << 20`） | 防止损坏/恶意文件导致过大分配 |
| 分段文件名格式 | `%06d.wal` | 6 位数字，不足前导零 |
| 字节序 | Little Endian | 所有多字节整数 |
| CRC 算法 | CRC-32 IEEE | 仅对 Frame Body 计算 |

---

## 7. 示例：一段 WAL 的二进制布局

假设一次 `Write(batch)` 包含：`Put("k1", "v1")`、`Delete("k2")`。

可能的顺序（示意）：

1. **Frame A**：RecordBatchBegin，LSN=1 → Body 9 字节，Header 中 bodyLen=9，CRC 为这 9 字节的 CRC32。
2. **Frame B**：RecordPut，LSN=2，key="k1"，value="v1" → Body 长度 1+8+4+2+4+2 = 21 字节。
3. **Frame C**：RecordDelete，LSN=3，key="k2" → Body 长度 1+8+4+2 = 15 字节。
4. **Frame D**：RecordBatchEnd，LSN=4 → Body 9 字节。

每个 Frame 均为：**[4B bodyLen][4B crc][body...]**，无额外填充或对齐。

---

## 8. 参考代码位置

- 常量与类型定义：`internal/wal/wal.go`（Record 类型、DecodedRecord、maxFrameBodySize）
- Frame 写入：`writeFrame`、`encodeBody`
- Frame 读取与校验：`scanValidPrefix`、`decodeBody`
- 分段路径：`segmentPath`、`listSegments`
