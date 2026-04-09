package sst

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	lsmiter "lsm-tree/internal/iter"
	"os"
)

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
	version := binary.LittleEndian.Uint32(header[8:12])
	if version != legacyFileVersion && version != fileVersion {
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
	buf := make([]byte, 0, 84+len(m.minKey)+len(m.maxKey))
	buf = binary.LittleEndian.AppendUint32(buf, metaVersion)
	buf = binary.LittleEndian.AppendUint64(buf, m.recordCount)
	buf = binary.LittleEndian.AppendUint64(buf, m.dataOffset)
	buf = binary.LittleEndian.AppendUint64(buf, m.dataLength)
	buf = binary.LittleEndian.AppendUint32(buf, m.dataCRC32)
	buf = binary.LittleEndian.AppendUint64(buf, m.blockIndexOffset)
	buf = binary.LittleEndian.AppendUint64(buf, m.blockIndexLength)
	buf = binary.LittleEndian.AppendUint32(buf, m.blockIndexCRC32)
	buf = binary.LittleEndian.AppendUint64(buf, m.bloomFilterOffset)
	buf = binary.LittleEndian.AppendUint64(buf, m.bloomFilterLength)
	buf = binary.LittleEndian.AppendUint32(buf, m.bloomFilterCRC32)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.minKey)))
	buf = append(buf, m.minKey...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.maxKey)))
	buf = append(buf, m.maxKey...)
	return buf
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
	if version != legacyMetaVersion && version != metaVersion {
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
	if version >= metaVersion {
		if m.bloomFilterOffset, err = readU64(); err != nil {
			return metaFooterFields{}, err
		}
		if m.bloomFilterLength, err = readU64(); err != nil {
			return metaFooterFields{}, err
		}
		if m.bloomFilterCRC32, err = readU32(); err != nil {
			return metaFooterFields{}, err
		}
	} else {
		m.bloomFilterOffset = m.blockIndexOffset + m.blockIndexLength
		m.bloomFilterLength = 0
		m.bloomFilterCRC32 = crc32.ChecksumIEEE(nil)
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
