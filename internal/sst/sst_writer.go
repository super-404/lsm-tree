package sst

import (
	"bytes"
	"fmt"
	"hash/crc32"
	lsmiter "lsm-tree/internal/iter"
	"os"
	"path/filepath"
)

// WriteFile 将 EntryIterator 的逻辑记录顺序写入一个 SST 文件。
//
// 当前版本的实现约束：
//   - 必须传入 EntryIterator，而不是 ValueIterator
//   - 允许写 Put/Delete，两者都会进入 Data Section
//   - 默认按约 64KiB 目标块大小切分逻辑块，并在 Data 后写出 Block Index
//   - 同时为整张表构建 Bloom Filter，供点查 miss 场景快速排除
//
// 写入顺序严格遵循 sst-format.md：
//  1. File Header
//  2. Data Section
//  3. Block Index
//  4. Bloom Filter
//  5. Meta Footer
//  6. Trailer
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
		filterKeys  [][]byte

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
		filterKeys = append(filterKeys, cloneBytes(entry.Key))

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
	bloomFilterOffset := blockIndexOffset + blockIndexLength
	filter := newBloomFilter(filterKeys)
	bloomFilterBytes, err := encodeBloomFilter(filter)
	if err != nil {
		cleanup(true)
		return Meta{}, err
	}
	if _, err := f.Write(bloomFilterBytes); err != nil {
		cleanup(true)
		return Meta{}, err
	}
	bloomFilterLength := uint64(len(bloomFilterBytes))
	bloomFilterCRC := crc32.ChecksumIEEE(bloomFilterBytes)
	metaFooter := encodeMetaFooter(metaFooterFields{
		recordCount:       recordCount,
		dataOffset:        fileHeaderSize,
		dataLength:        dataLength,
		dataCRC32:         dataCRC.Sum32(),
		blockIndexOffset:  blockIndexOffset,
		blockIndexLength:  blockIndexLength,
		blockIndexCRC32:   blockIndexCRC,
		bloomFilterOffset: bloomFilterOffset,
		bloomFilterLength: bloomFilterLength,
		bloomFilterCRC32:  bloomFilterCRC,
		minKey:            minKey,
		maxKey:            maxKey,
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
		ID:                id,
		Level:             level,
		Path:              path,
		FileSize:          uint64(st.Size()),
		RecordCount:       recordCount,
		DataLength:        dataLength,
		BlockIndexLength:  blockIndexLength,
		BloomFilterLength: bloomFilterLength,
		MinKey:            minKey,
		MaxKey:            maxKey,
	}, nil
}
