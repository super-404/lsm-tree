package sst

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
)

func newBloomFilter(keys [][]byte) bloomFilter {
	if len(keys) == 0 {
		return bloomFilter{}
	}
	byteSize := (len(keys)*bloomBitsPerKey + 7) / 8
	if byteSize < minBloomByteSize {
		byteSize = minBloomByteSize
	}
	filter := bloomFilter{
		hashCount: bloomHashCount,
		bits:      make([]byte, byteSize),
	}
	for _, key := range keys {
		filter.add(key)
	}
	return filter
}

func (f bloomFilter) mayContain(key []byte) bool {
	if len(f.bits) == 0 || f.hashCount == 0 {
		return true
	}
	bitCount := uint64(len(f.bits) * 8)
	h1, h2 := bloomHashPair(key)
	for i := uint32(0); i < f.hashCount; i++ {
		bit := (h1 + uint64(i)*h2 + uint64(i*i)) % bitCount
		if f.bits[bit/8]&(1<<uint(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (f *bloomFilter) add(key []byte) {
	if f == nil || len(f.bits) == 0 || f.hashCount == 0 {
		return
	}
	bitCount := uint64(len(f.bits) * 8)
	h1, h2 := bloomHashPair(key)
	for i := uint32(0); i < f.hashCount; i++ {
		bit := (h1 + uint64(i)*h2 + uint64(i*i)) % bitCount
		f.bits[bit/8] |= 1 << uint(bit%8)
	}
}

func bloomHashPair(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write(key)
	sum1 := h1.Sum64()
	sum2 := uint64(crc32.ChecksumIEEE(key))
	if sum2 == 0 {
		sum2 = 0x9e3779b97f4a7c15
	}
	return sum1, sum2
}

func encodeBloomFilter(filter bloomFilter) ([]byte, error) {
	if len(filter.bits) == 0 {
		return nil, nil
	}
	buf := make([]byte, 0, 12+len(filter.bits))
	buf = binary.LittleEndian.AppendUint32(buf, bloomVersion)
	buf = binary.LittleEndian.AppendUint32(buf, filter.hashCount)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(filter.bits)))
	buf = append(buf, filter.bits...)
	return buf, nil
}

func readAndValidateBloomFilter(f *os.File, meta metaFooterFields) (bloomFilter, error) {
	if meta.bloomFilterLength == 0 {
		return bloomFilter{}, nil
	}
	if _, err := f.Seek(int64(meta.bloomFilterOffset), io.SeekStart); err != nil {
		return bloomFilter{}, err
	}
	if meta.bloomFilterLength > uint64(^uint(0)>>1) {
		return bloomFilter{}, fmt.Errorf("sst: bloom filter too large")
	}
	buf := make([]byte, int(meta.bloomFilterLength))
	if _, err := io.ReadFull(f, buf); err != nil {
		return bloomFilter{}, err
	}
	if crc32.ChecksumIEEE(buf) != meta.bloomFilterCRC32 {
		return bloomFilter{}, fmt.Errorf("sst: invalid bloom filter crc")
	}
	filter, err := decodeBloomFilter(buf)
	if err != nil {
		return bloomFilter{}, err
	}
	return filter, nil
}

func decodeBloomFilter(buf []byte) (bloomFilter, error) {
	if len(buf) == 0 {
		return bloomFilter{}, nil
	}
	if len(buf) < 12 {
		return bloomFilter{}, io.ErrUnexpectedEOF
	}
	version := binary.LittleEndian.Uint32(buf[:4])
	if version != bloomVersion {
		return bloomFilter{}, fmt.Errorf("sst: unsupported bloom filter version")
	}
	hashCount := binary.LittleEndian.Uint32(buf[4:8])
	bitsetLen := int(binary.LittleEndian.Uint32(buf[8:12]))
	if 12+bitsetLen != len(buf) {
		return bloomFilter{}, fmt.Errorf("sst: invalid bloom filter length")
	}
	if hashCount == 0 && bitsetLen > 0 {
		return bloomFilter{}, fmt.Errorf("sst: invalid bloom hash count")
	}
	filter := bloomFilter{
		hashCount: hashCount,
		bits:      cloneBytes(buf[12:]),
	}
	return filter, nil
}
