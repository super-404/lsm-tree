package sst

import (
	lsmiter "lsm-tree/internal/iter"
	"os"
)

const (
	fileHeaderSize  = 16
	trailerSize     = 8
	targetBlockSize = 64 << 10 // 64KiB：按 record 边界切块时采用的目标块大小

	fileVersion = 2
	metaVersion = 2

	fileMagic = "LSMTSST1"
	eofMagic  = 0x31545353 // 小端写出后，磁盘末 4 字节为 ASCII "SST1"

	recordOpPut    = 1
	recordOpDelete = 2

	legacyFileVersion = 1
	legacyMetaVersion = 1

	bloomVersion     = 1
	bloomBitsPerKey  = 10
	bloomHashCount   = 6
	minBloomByteSize = 8
)

// Meta 是 SST 在引擎内与 MANIFEST 中共享的最小元数据集合。
//
// 这份结构刻意同时包含两类信息：
//  1. 文件自身的校验/布局信息（DataLength、RecordCount、MinKey、MaxKey）
//  2. 引擎层管理信息（ID、Path、Level）
//
// 这样做的目的是让 flush 发布阶段可以直接把刚写出的 SST 元数据
// 交给 MANIFEST，而不必再额外打开文件做二次解析。
type Meta struct {
	ID                uint64 `json:"id"`
	Level             uint32 `json:"level"`
	Path              string `json:"path"`
	FileSize          uint64 `json:"file_size"`
	RecordCount       uint64 `json:"record_count"`
	DataLength        uint64 `json:"data_length"`
	BlockIndexLength  uint64 `json:"block_index_length"`
	BloomFilterLength uint64 `json:"bloom_filter_length"`
	MinKey            []byte `json:"min_key"`
	MaxKey            []byte `json:"max_key"`
}

// Table 表示一个已经发布的 SST 文件。
//
// 当前这版实现已经具备：
//   - 块索引（Block Index）
//   - Bloom Filter
//   - 范围扫描与点查
//
// 但仍刻意把结构拆得比较直接，便于先把 flush / manifest / 恢复闭环打通。
type Table struct {
	path              string
	meta              Meta
	dataOffset        uint64
	dataLength        uint64
	blockIndexOffset  uint64
	blockIndexLength  uint64
	bloomFilterOffset uint64
	bloomFilterLength uint64
	blocks            []blockEntry
	bloom             bloomFilter
}

// blockEntry 是 Block Index 在内存中的展开结构。
//
// 它只描述一个连续的数据块，以及该块的 firstKey，便于 Open 后直接在内存中二分定位。
type blockEntry struct {
	blockStart uint64
	blockLen   uint32
	firstKey   []byte
}

// bloomFilter 是每张 SST 的“存在性预检查器”。
//
// 它只回答：
//   - 某个 key 一定不在这张表里
//   - 或者某个 key 可能在这张表里
//
// 因此它非常适合 Get 的 miss 场景：如果 Bloom Filter 已经判定“不可能存在”，
// 引擎就不需要再去读块索引或数据块。
type bloomFilter struct {
	hashCount uint32
	bits      []byte
}

// valueIterator 顺序扫描一个 SST 的 Data Section，并产出最终可见值。
//
// 由于 SST 内已经保证“同 key 不重复”，因此这里不需要再做去重；
// 只需在读取到 Delete tombstone 时跳过该 key 即可。
type valueIterator struct {
	f         *os.File
	remaining uint64
	start     []byte
	end       []byte
	valid     bool
	item      lsmiter.Value
}

// entryIterator 顺序扫描一个 SST 的 Data Section，并产出最新逻辑记录。
//
// 与 valueIterator 的区别在于：
//   - 它不会过滤 Delete
//   - 因此 compaction / 调试工具可以直接消费它
//
// 由于单个 SST 内 key 已严格递增且不重复，这一层只需要做范围裁剪，
// 不需要像 memtable 那样做“同 key 取最新”的去重。
type entryIterator struct {
	f         *os.File
	remaining uint64
	start     []byte
	end       []byte
	valid     bool
	entry     lsmiter.Entry
}

type trailerFields struct {
	metaSize uint32
}

type metaFooterFields struct {
	recordCount       uint64
	dataOffset        uint64
	dataLength        uint64
	dataCRC32         uint32
	blockIndexOffset  uint64
	blockIndexLength  uint64
	blockIndexCRC32   uint32
	bloomFilterOffset uint64
	bloomFilterLength uint64
	bloomFilterCRC32  uint32
	minKey            []byte
	maxKey            []byte
}
