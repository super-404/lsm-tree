package sst

import (
	"bytes"
	"fmt"
	"io"
	lsmiter "lsm-tree/internal/iter"
	"os"
)

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
	if meta.bloomFilterOffset != meta.blockIndexOffset+meta.blockIndexLength {
		return nil, fmt.Errorf("sst: invalid bloom filter offset %d", meta.bloomFilterOffset)
	}
	if metaOffset != int64(meta.bloomFilterOffset+meta.bloomFilterLength) {
		return nil, fmt.Errorf("sst: meta offset mismatch, want=%d got=%d", meta.bloomFilterOffset+meta.bloomFilterLength, metaOffset)
	}
	blocks, err := readAndValidateBlockIndex(f, meta)
	if err != nil {
		return nil, err
	}
	filter, err := readAndValidateBloomFilter(f, meta)
	if err != nil {
		return nil, err
	}

	return &Table{
		path: path,
		meta: Meta{
			Path:              path,
			FileSize:          uint64(st.Size()),
			RecordCount:       meta.recordCount,
			DataLength:        meta.dataLength,
			BlockIndexLength:  meta.blockIndexLength,
			BloomFilterLength: meta.bloomFilterLength,
			MinKey:            cloneBytes(meta.minKey),
			MaxKey:            cloneBytes(meta.maxKey),
		},
		dataOffset:        meta.dataOffset,
		dataLength:        meta.dataLength,
		blockIndexOffset:  meta.blockIndexOffset,
		blockIndexLength:  meta.blockIndexLength,
		bloomFilterOffset: meta.bloomFilterOffset,
		bloomFilterLength: meta.bloomFilterLength,
		blocks:            blocks,
		bloom:             filter,
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
	if !t.bloom.mayContain(key) {
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
