package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

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
