package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	lsmiter "lsm-tree/internal/iter"
	"os"
	"path/filepath"
)

const (
	fileHeaderSize  = 16
	trailerSize     = 8
	targetBlockSize = 64 << 10 // 64KiB：按 record 边界切块时采用的目标块大小

	fileVersion = 1
	metaVersion = 1

	fileMagic = "LSMTSST1"
	eofMagic  = 0x31545353 // 小端写出后，磁盘末 4 字节为 ASCII "SST1"

	recordOpPut    = 1
	recordOpDelete = 2
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
	ID               uint64 `json:"id"`
	Level            uint32 `json:"level"`
	Path             string `json:"path"`
	FileSize         uint64 `json:"file_size"`
	RecordCount      uint64 `json:"record_count"`
	DataLength       uint64 `json:"data_length"`
	BlockIndexLength uint64 `json:"block_index_length"`
	MinKey           []byte `json:"min_key"`
	MaxKey           []byte `json:"max_key"`
}

// Table 表示一个已经发布的 SST 文件。
//
// 当前第一版实现不启用块索引，因此读路径仍然是顺序扫描：
//   - GetLatest 按 key 单向扫描，遇到目标或越界即结束
//   - ValueIterRange 从 Data Section 起顺序扫到区间末尾
//
// 虽然不是最终性能形态，但这能先把 flush / manifest / 恢复闭环打通。
type Table struct {
	path             string
	meta             Meta
	dataOffset       uint64
	dataLength       uint64
	blockIndexOffset uint64
	blockIndexLength uint64
	blocks           []blockEntry
}

// blockEntry 是 Block Index 在内存中的展开结构。
//
// 它只描述一个连续的数据块，以及该块的 firstKey，便于 Open 后直接在内存中二分定位。
type blockEntry struct {
	blockStart uint64
	blockLen   uint32
	firstKey   []byte
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

// WriteFile 将 EntryIterator 的逻辑记录顺序写入一个 SST 文件。
//
// 当前版本的实现约束：
//   - 必须传入 EntryIterator，而不是 ValueIterator
//   - 允许写 Put/Delete，两者都会进入 Data Section
//   - 默认按约 64KiB 目标块大小切分逻辑块，并在 Data 后写出 Block Index
//
// 写入顺序严格遵循 sst-format.md：
//  1. File Header
//  2. Data Section
//  3. Block Index
//  4. Meta Footer
//  5. Trailer
func WriteFile(path string, id uint64, level uint32, it lsmiter.EntryIterator) (Meta, error) {
	if it == nil {
		return Meta{}, fmt.Errorf("sst: nil entry iterator")
	}
	defer it.Close()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Meta{}, err
	}

	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return Meta{}, err
	}

	cleanup := func(closeFile bool) {
		if closeFile {
			_ = f.Close()
		}
		_ = os.Remove(tmpPath)
	}

	if err := writeFileHeader(f); err != nil {
		cleanup(true)
		return Meta{}, err
	}

	// 下面这些统计量会在顺序写 Data 时一边落盘一边累加。
	// 这样可以保持 flush 的写入模式完全顺序化，不需要回写前面的字节。
	var (
		recordCount uint64
		dataLength  uint64
		prevKey     []byte
		minKey      []byte
		maxKey      []byte
		dataCRC     = crc32.NewIEEE()
		blocks      []blockEntry

		currentBlockStart    = uint64(fileHeaderSize)
		currentBlockLen      uint32
		currentBlockFirstKey []byte
	)

	for it.Valid() {
		entry := it.Item()
		if len(prevKey) > 0 && bytes.Compare(entry.Key, prevKey) <= 0 {
			cleanup(true)
			return Meta{}, fmt.Errorf("sst: keys must be strictly increasing, prev=%q current=%q", prevKey, entry.Key)
		}
		recBytes, err := encodeRecord(entry)
		if err != nil {
			cleanup(true)
			return Meta{}, err
		}
		if _, err := f.Write(recBytes); err != nil {
			cleanup(true)
			return Meta{}, err
		}
		if _, err := dataCRC.Write(recBytes); err != nil {
			cleanup(true)
			return Meta{}, err
		}

		if recordCount == 0 {
			minKey = cloneBytes(entry.Key)
		}
		maxKey = cloneBytes(entry.Key)
		prevKey = cloneBytes(entry.Key)
		recordCount++
		dataLength += uint64(len(recBytes))

		// 块边界只写入 Block Index，不污染 Data Section。
		// 因此这里的策略很简单：按 record 顺序累计字节，达到阈值就把当前块封口。
		if currentBlockLen == 0 {
			currentBlockFirstKey = cloneBytes(entry.Key)
		}
		currentBlockLen += uint32(len(recBytes))
		if currentBlockLen >= targetBlockSize {
			blocks = append(blocks, blockEntry{
				blockStart: currentBlockStart,
				blockLen:   currentBlockLen,
				firstKey:   cloneBytes(currentBlockFirstKey),
			})
			currentBlockStart += uint64(currentBlockLen)
			currentBlockLen = 0
			currentBlockFirstKey = nil
		}
		it.Next()
	}

	if currentBlockLen > 0 {
		blocks = append(blocks, blockEntry{
			blockStart: currentBlockStart,
			blockLen:   currentBlockLen,
			firstKey:   cloneBytes(currentBlockFirstKey),
		})
	}

	blockIndexOffset := uint64(fileHeaderSize) + dataLength
	blockIndexBytes, err := encodeBlockIndex(blocks)
	if err != nil {
		cleanup(true)
		return Meta{}, err
	}
	if _, err := f.Write(blockIndexBytes); err != nil {
		cleanup(true)
		return Meta{}, err
	}
	blockIndexLength := uint64(len(blockIndexBytes))
	blockIndexCRC := crc32.ChecksumIEEE(blockIndexBytes)
	metaFooter := encodeMetaFooter(metaFooterFields{
		recordCount:      recordCount,
		dataOffset:       fileHeaderSize,
		dataLength:       dataLength,
		dataCRC32:        dataCRC.Sum32(),
		blockIndexOffset: blockIndexOffset,
		blockIndexLength: blockIndexLength,
		blockIndexCRC32:  blockIndexCRC,
		minKey:           minKey,
		maxKey:           maxKey,
	})
	if _, err := f.Write(metaFooter); err != nil {
		cleanup(true)
		return Meta{}, err
	}

	if err := writeTrailer(f, uint32(len(metaFooter))); err != nil {
		cleanup(true)
		return Meta{}, err
	}

	if err := f.Sync(); err != nil {
		cleanup(true)
		return Meta{}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return Meta{}, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return Meta{}, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return Meta{}, err
	}

	st, err := os.Stat(path)
	if err != nil {
		return Meta{}, err
	}
	return Meta{
		ID:               id,
		Level:            level,
		Path:             path,
		FileSize:         uint64(st.Size()),
		RecordCount:      recordCount,
		DataLength:       dataLength,
		BlockIndexLength: blockIndexLength,
		MinKey:           minKey,
		MaxKey:           maxKey,
	}, nil
}

// Open 打开一个已存在的 SST 文件，并从文件尾部解析其 Meta Footer。
//
// 这里的职责主要是“建立一个可信的 Table 视图”：
//   - 校验 Header / Trailer / Meta 的结构自洽性
//   - 提取 Data Section 的偏移与长度
//   - 提取最小/最大 key 等常用元信息
//
// 当前第一版为了保持打开成本较低，不在 Open 阶段重算整段 Data 的 CRC；
// 真正的读取仍然依赖文件结构自洽与写入端的 fsync + rename 保证。
func Open(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < fileHeaderSize+trailerSize {
		return nil, fmt.Errorf("sst: file too small: %s", path)
	}

	if err := readAndValidateHeader(f); err != nil {
		return nil, err
	}

	trailer, err := readTrailer(f, st.Size())
	if err != nil {
		return nil, err
	}
	metaOffset := st.Size() - int64(trailerSize) - int64(trailer.metaSize)
	if metaOffset < fileHeaderSize {
		return nil, fmt.Errorf("sst: invalid meta offset for %s", path)
	}
	meta, err := readMetaFooter(f, metaOffset, trailer.metaSize)
	if err != nil {
		return nil, err
	}
	if meta.dataOffset != fileHeaderSize {
		return nil, fmt.Errorf("sst: unexpected data offset %d", meta.dataOffset)
	}
	if meta.blockIndexOffset != meta.dataOffset+meta.dataLength {
		return nil, fmt.Errorf("sst: invalid block index offset %d", meta.blockIndexOffset)
	}
	if metaOffset != int64(meta.blockIndexOffset+meta.blockIndexLength) {
		return nil, fmt.Errorf("sst: meta offset mismatch, want=%d got=%d", meta.blockIndexOffset+meta.blockIndexLength, metaOffset)
	}
	blocks, err := readAndValidateBlockIndex(f, meta)
	if err != nil {
		return nil, err
	}

	return &Table{
		path: path,
		meta: Meta{
			Path:             path,
			FileSize:         uint64(st.Size()),
			RecordCount:      meta.recordCount,
			DataLength:       meta.dataLength,
			BlockIndexLength: meta.blockIndexLength,
			MinKey:           cloneBytes(meta.minKey),
			MaxKey:           cloneBytes(meta.maxKey),
		},
		dataOffset:       meta.dataOffset,
		dataLength:       meta.dataLength,
		blockIndexOffset: meta.blockIndexOffset,
		blockIndexLength: meta.blockIndexLength,
		blocks:           blocks,
	}, nil
}

// Meta 返回 Table 持有的元信息副本。
func (t *Table) Meta() Meta {
	if t == nil {
		return Meta{}
	}
	m := t.meta
	m.MinKey = cloneBytes(m.MinKey)
	m.MaxKey = cloneBytes(m.MaxKey)
	return m
}

// SetPublishedMeta 用 manifest 中的引擎层信息补齐打开后的 Table。
//
// SST 文件自身并不编码：
//   - sst id
//   - level
//   - 相对路径
//
// 这些信息属于 MANIFEST 管理范围，因此在恢复时需要由引擎层回填。
func (t *Table) SetPublishedMeta(meta Meta) {
	if t == nil {
		return
	}
	t.meta.ID = meta.ID
	t.meta.Level = meta.Level
	t.meta.Path = meta.Path
}

// GetLatest 在单个 SST 中查找某个 key 的最新逻辑记录。
//
// 由于单个 SST 内 key 不重复，这里的“最新”其实就是“该 key 是否存在，以及它是 Put 还是 Delete”。
// 当块索引存在时，会先按 firstKey 二分定位目标块，再只扫描块内记录。
func (t *Table) GetLatest(key []byte) ([]byte, lsmiter.Op, bool) {
	if t == nil {
		return nil, 0, false
	}
	if len(t.meta.MinKey) > 0 && bytes.Compare(key, t.meta.MinKey) < 0 {
		return nil, 0, false
	}
	if len(t.meta.MaxKey) > 0 && bytes.Compare(key, t.meta.MaxKey) > 0 {
		return nil, 0, false
	}

	f, err := os.Open(t.path)
	if err != nil {
		return nil, 0, false
	}
	defer f.Close()
	startOffset, remaining := t.dataScanWindowForKey(key)
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return nil, 0, false
	}
	for remaining > 0 {
		entry, n, err := readEntry(f)
		if err != nil {
			return nil, 0, false
		}
		remaining -= uint64(n)
		cmp := bytes.Compare(entry.Key, key)
		if cmp < 0 {
			continue
		}
		if cmp > 0 {
			return nil, 0, false
		}
		if entry.Op == lsmiter.OpDelete {
			return nil, lsmiter.OpDelete, true
		}
		return cloneBytes(entry.Value), lsmiter.OpPut, true
	}
	return nil, 0, false
}

func (t *Table) Values() lsmiter.ValueIterator {
	return t.ValuesRange(nil, nil)
}

func (t *Table) ValuesRange(start, end []byte) lsmiter.ValueIterator {
	if t == nil {
		return &valueIterator{}
	}
	f, err := os.Open(t.path)
	if err != nil {
		return &valueIterator{}
	}
	startOffset, remaining := t.dataScanWindowForRangeStart(start)
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		_ = f.Close()
		return &valueIterator{}
	}
	it := &valueIterator{
		f:         f,
		remaining: remaining,
		start:     cloneBytes(start),
		end:       cloneBytes(end),
	}
	it.advance()
	return it
}

func (it *valueIterator) Valid() bool { return it != nil && it.valid }

// Item 返回当前 SST 迭代位置对应的可见键值对。
//
// 这里返回结构体而不是分散的 Key/Value getter，是为了让 memtable 与 sst
// 这两类数据源在引擎层说同一种“值迭代协议”，避免 engine 再额外维护一套私有接口。
func (it *valueIterator) Item() lsmiter.Value {
	var zero lsmiter.Value
	if !it.Valid() {
		return zero
	}
	return it.item
}

func (it *valueIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *valueIterator) Close() {
	if it == nil {
		return
	}
	if it.f != nil {
		_ = it.f.Close()
	}
	it.valid = false
	it.item = lsmiter.Value{}
}

// advance 顺序读取 SST 的下一条可见 Put。
//
// 这层逻辑很像 memtable.ValueIterator，只是输入源从内存跳表换成了磁盘文件：
//   - SST 内没有重复 key，因此不需要再做同 key 去重
//   - 如果读到 Delete tombstone，则直接跳过
//   - 区间边界仍保持 [start, end) 语义
func (it *valueIterator) advance() {
	it.valid = false
	it.item = lsmiter.Value{}
	for it.f != nil && it.remaining > 0 {
		entry, n, err := readEntry(it.f)
		if err != nil {
			it.Close()
			return
		}
		it.remaining -= uint64(n)
		if it.start != nil && bytes.Compare(entry.Key, it.start) < 0 {
			continue
		}
		if it.end != nil && bytes.Compare(entry.Key, it.end) >= 0 {
			it.Close()
			return
		}
		if entry.Op != lsmiter.OpPut {
			continue
		}
		it.item.Key = entry.Key
		it.item.Value = entry.Value
		it.valid = true
		return
	}
}

type trailerFields struct {
	metaSize uint32
}

type metaFooterFields struct {
	recordCount      uint64
	dataOffset       uint64
	dataLength       uint64
	dataCRC32        uint32
	blockIndexOffset uint64
	blockIndexLength uint64
	blockIndexCRC32  uint32
	minKey           []byte
	maxKey           []byte
}

func (t *Table) dataScanWindowForKey(key []byte) (uint64, uint64) {
	if len(t.blocks) == 0 {
		return t.dataOffset, t.dataLength
	}
	idx := t.findBlockForKey(key)
	block := t.blocks[idx]
	return block.blockStart, uint64(block.blockLen)
}

func (t *Table) dataScanWindowForRangeStart(start []byte) (uint64, uint64) {
	if len(t.blocks) == 0 || start == nil {
		return t.dataOffset, t.dataLength
	}
	idx := t.findBlockForKey(start)
	block := t.blocks[idx]
	return block.blockStart, t.dataLength - (block.blockStart - t.dataOffset)
}

// findBlockForKey 返回“最后一个 firstKey <= key”的块。
//
// 在 key 全局递增的前提下，这个块就是目标 key 可能出现的唯一块。
func (t *Table) findBlockForKey(key []byte) int {
	if len(t.blocks) == 0 {
		return 0
	}
	lo, hi := 0, len(t.blocks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(t.blocks[mid].firstKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return 0
	}
	return lo - 1
}

func writeFileHeader(w io.Writer) error {
	var header [fileHeaderSize]byte
	copy(header[0:8], []byte(fileMagic))
	binary.LittleEndian.PutUint32(header[8:12], fileVersion)
	binary.LittleEndian.PutUint32(header[12:16], 0)
	_, err := w.Write(header[:])
	return err
}

func writeTrailer(w io.Writer, metaSize uint32) error {
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:4], metaSize)
	binary.LittleEndian.PutUint32(trailer[4:8], eofMagic)
	_, err := w.Write(trailer[:])
	return err
}

func readAndValidateHeader(f *os.File) error {
	var header [fileHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return err
	}
	if string(header[0:8]) != fileMagic {
		return fmt.Errorf("sst: invalid file magic")
	}
	if binary.LittleEndian.Uint32(header[8:12]) != fileVersion {
		return fmt.Errorf("sst: unsupported version")
	}
	return nil
}

func readTrailer(f *os.File, fileSize int64) (trailerFields, error) {
	if _, err := f.Seek(fileSize-trailerSize, io.SeekStart); err != nil {
		return trailerFields{}, err
	}
	var trailer [trailerSize]byte
	if _, err := io.ReadFull(f, trailer[:]); err != nil {
		return trailerFields{}, err
	}
	if binary.LittleEndian.Uint32(trailer[4:8]) != eofMagic {
		return trailerFields{}, fmt.Errorf("sst: invalid eof magic")
	}
	return trailerFields{
		metaSize: binary.LittleEndian.Uint32(trailer[0:4]),
	}, nil
}

func encodeMetaFooter(m metaFooterFields) []byte {
	buf := make([]byte, 0, 64+len(m.minKey)+len(m.maxKey))
	buf = binary.LittleEndian.AppendUint32(buf, metaVersion)
	buf = binary.LittleEndian.AppendUint64(buf, m.recordCount)
	buf = binary.LittleEndian.AppendUint64(buf, m.dataOffset)
	buf = binary.LittleEndian.AppendUint64(buf, m.dataLength)
	buf = binary.LittleEndian.AppendUint32(buf, m.dataCRC32)
	buf = binary.LittleEndian.AppendUint64(buf, m.blockIndexOffset)
	buf = binary.LittleEndian.AppendUint64(buf, m.blockIndexLength)
	buf = binary.LittleEndian.AppendUint32(buf, m.blockIndexCRC32)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.minKey)))
	buf = append(buf, m.minKey...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.maxKey)))
	buf = append(buf, m.maxKey...)
	return buf
}

func encodeBlockIndex(blocks []blockEntry) ([]byte, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	buf := make([]byte, 0, 4+len(blocks)*24)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(blocks)))
	for _, block := range blocks {
		if len(block.firstKey) == 0 {
			return nil, fmt.Errorf("sst: block entry missing first key")
		}
		buf = binary.LittleEndian.AppendUint64(buf, block.blockStart)
		buf = binary.LittleEndian.AppendUint32(buf, block.blockLen)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(block.firstKey)))
		buf = append(buf, block.firstKey...)
	}
	return buf, nil
}

func readMetaFooter(f *os.File, offset int64, size uint32) (metaFooterFields, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return metaFooterFields{}, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return metaFooterFields{}, err
	}

	var (
		pos int
		m   metaFooterFields
	)
	readU32 := func() (uint32, error) {
		if pos+4 > len(buf) {
			return 0, io.ErrUnexpectedEOF
		}
		v := binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
		return v, nil
	}
	readU64 := func() (uint64, error) {
		if pos+8 > len(buf) {
			return 0, io.ErrUnexpectedEOF
		}
		v := binary.LittleEndian.Uint64(buf[pos : pos+8])
		pos += 8
		return v, nil
	}
	readBytes := func(n uint32) ([]byte, error) {
		if pos+int(n) > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		v := cloneBytes(buf[pos : pos+int(n)])
		pos += int(n)
		return v, nil
	}

	version, err := readU32()
	if err != nil {
		return metaFooterFields{}, err
	}
	if version != metaVersion {
		return metaFooterFields{}, fmt.Errorf("sst: unsupported meta version")
	}
	if m.recordCount, err = readU64(); err != nil {
		return metaFooterFields{}, err
	}
	if m.dataOffset, err = readU64(); err != nil {
		return metaFooterFields{}, err
	}
	if m.dataLength, err = readU64(); err != nil {
		return metaFooterFields{}, err
	}
	if m.dataCRC32, err = readU32(); err != nil {
		return metaFooterFields{}, err
	}
	if m.blockIndexOffset, err = readU64(); err != nil {
		return metaFooterFields{}, err
	}
	if m.blockIndexLength, err = readU64(); err != nil {
		return metaFooterFields{}, err
	}
	if m.blockIndexCRC32, err = readU32(); err != nil {
		return metaFooterFields{}, err
	}
	minLen, err := readU32()
	if err != nil {
		return metaFooterFields{}, err
	}
	if m.minKey, err = readBytes(minLen); err != nil {
		return metaFooterFields{}, err
	}
	maxLen, err := readU32()
	if err != nil {
		return metaFooterFields{}, err
	}
	if m.maxKey, err = readBytes(maxLen); err != nil {
		return metaFooterFields{}, err
	}
	return m, nil
}

func readAndValidateBlockIndex(f *os.File, meta metaFooterFields) ([]blockEntry, error) {
	if meta.blockIndexLength == 0 {
		return nil, nil
	}
	if _, err := f.Seek(int64(meta.blockIndexOffset), io.SeekStart); err != nil {
		return nil, err
	}
	if meta.blockIndexLength > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("sst: block index too large")
	}
	buf := make([]byte, int(meta.blockIndexLength))
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(buf) != meta.blockIndexCRC32 {
		return nil, fmt.Errorf("sst: invalid block index crc")
	}
	blocks, err := decodeBlockIndex(buf)
	if err != nil {
		return nil, err
	}
	if err := validateBlockIndex(blocks, meta); err != nil {
		return nil, err
	}
	return blocks, nil
}

func decodeBlockIndex(buf []byte) ([]blockEntry, error) {
	if len(buf) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	blockCount := binary.LittleEndian.Uint32(buf[:4])
	pos := 4
	blocks := make([]blockEntry, 0, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		if pos+16 > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		blockStart := binary.LittleEndian.Uint64(buf[pos : pos+8])
		pos += 8
		blockLen := binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
		firstKeyLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		if pos+firstKeyLen > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		blocks = append(blocks, blockEntry{
			blockStart: blockStart,
			blockLen:   blockLen,
			firstKey:   cloneBytes(buf[pos : pos+firstKeyLen]),
		})
		pos += firstKeyLen
	}
	if pos != len(buf) {
		return nil, fmt.Errorf("sst: trailing bytes in block index")
	}
	return blocks, nil
}

func validateBlockIndex(blocks []blockEntry, meta metaFooterFields) error {
	if meta.blockIndexLength == 0 {
		if len(blocks) != 0 {
			return fmt.Errorf("sst: block index length is zero but entries exist")
		}
		return nil
	}
	if len(blocks) == 0 {
		return fmt.Errorf("sst: empty block index")
	}

	expectedStart := meta.dataOffset
	var covered uint64
	for i, block := range blocks {
		if len(block.firstKey) == 0 {
			return fmt.Errorf("sst: empty first key in block index")
		}
		if block.blockLen == 0 {
			return fmt.Errorf("sst: zero-length block")
		}
		if block.blockStart != expectedStart {
			return fmt.Errorf("sst: invalid block start %d", block.blockStart)
		}
		if i > 0 && bytes.Compare(blocks[i-1].firstKey, block.firstKey) >= 0 {
			return fmt.Errorf("sst: non-increasing block first key")
		}
		expectedStart += uint64(block.blockLen)
		covered += uint64(block.blockLen)
	}
	if covered != meta.dataLength {
		return fmt.Errorf("sst: block index data length mismatch")
	}
	if expectedStart != meta.dataOffset+meta.dataLength {
		return fmt.Errorf("sst: block index coverage mismatch")
	}
	return nil
}

func encodeRecord(entry lsmiter.Entry) ([]byte, error) {
	switch entry.Op {
	case lsmiter.OpPut:
		out := make([]byte, 0, 1+4+len(entry.Key)+4+len(entry.Value))
		out = append(out, recordOpPut)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Key)))
		out = append(out, entry.Key...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Value)))
		out = append(out, entry.Value...)
		return out, nil
	case lsmiter.OpDelete:
		out := make([]byte, 0, 1+4+len(entry.Key))
		out = append(out, recordOpDelete)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Key)))
		out = append(out, entry.Key...)
		return out, nil
	default:
		return nil, fmt.Errorf("sst: unknown entry op %d", entry.Op)
	}
}

func readEntry(r io.Reader) (lsmiter.Entry, int, error) {
	var (
		opBuf  [1]byte
		lenBuf [4]byte
		readN  int
	)
	if _, err := io.ReadFull(r, opBuf[:]); err != nil {
		return lsmiter.Entry{}, readN, err
	}
	readN++
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return lsmiter.Entry{}, readN, err
	}
	readN += 4
	keyLen := int(binary.LittleEndian.Uint32(lenBuf[:]))
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return lsmiter.Entry{}, readN, err
	}
	readN += keyLen

	switch opBuf[0] {
	case recordOpPut:
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return lsmiter.Entry{}, readN, err
		}
		readN += 4
		valLen := int(binary.LittleEndian.Uint32(lenBuf[:]))
		value := make([]byte, valLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return lsmiter.Entry{}, readN, err
		}
		readN += valLen
		return lsmiter.Entry{Key: key, Value: value, Op: lsmiter.OpPut}, readN, nil
	case recordOpDelete:
		return lsmiter.Entry{Key: key, Op: lsmiter.OpDelete}, readN, nil
	default:
		return lsmiter.Entry{}, readN, fmt.Errorf("sst: unknown record op %d", opBuf[0])
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func syncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}
