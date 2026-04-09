package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	RecordPut byte = iota + 1
	RecordDelete
	RecordBatchBegin
	RecordBatchEnd
)

var errUnknownRecordType = errors.New("wal: unknown record type")

// maxFrameBodySize 单帧 body 上限，避免损坏/恶意文件导致大分配（16MB）。
const maxFrameBodySize = 16 << 20

// Record 表示一条待写 WAL 记录；LSN 由 WAL 在追加时自动分配。
type Record struct {
	Type  byte
	Key   []byte
	Value []byte
}

// DecodedRecord 表示回放时解析出的 WAL 记录。
type DecodedRecord struct {
	Type  byte
	LSN   uint64
	Key   []byte
	Value []byte
}

// WAL 管理分段日志：000001.wal, 000002.wal, ...
type WAL struct {
	dir     string
	segID   int
	f       *os.File
	w       *bufio.Writer
	nextLSN uint64
}

type segmentInfo struct {
	id   int
	path string
}

// Open 打开（或创建）WAL，并在启动时截断坏尾，恢复 nextLSN。
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	segments, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		segments = []segmentInfo{{id: 1, path: segmentPath(dir, 1)}}
		f, err := os.OpenFile(segments[0].path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
	}

	var lastLSN uint64
	for _, seg := range segments {
		f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		validOffset, segLastLSN, err := scanValidPrefix(f, nil)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Truncate(validOffset); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		if segLastLSN > lastLSN {
			lastLSN = segLastLSN
		}
	}

	lastSeg := segments[len(segments)-1]
	f, err := os.OpenFile(lastSeg.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}

	return &WAL{
		dir:     dir,
		segID:   lastSeg.id,
		f:       f,
		w:       bufio.NewWriterSize(f, 64<<10),
		nextLSN: lastLSN + 1,
	}, nil
}

// Append 追加多条记录（按顺序分配 LSN），并在返回前 fsync，保证崩溃恢复可见。
func (l *WAL) Append(records ...Record) error {
	if len(records) == 0 {
		return nil
	}
	for _, rec := range records {
		body, err := encodeBody(l.nextLSN, rec)
		if err != nil {
			return err
		}
		if err := writeFrame(l.w, body); err != nil {
			return err
		}
		l.nextLSN++
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Rotate 封存当前 segment，并创建下一个 segment 作为新的活跃 WAL 文件。
func (l *WAL) Rotate() error {
	if l == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	if err := l.f.Close(); err != nil {
		return err
	}

	l.segID++
	nextPath := segmentPath(l.dir, l.segID)
	f, err := os.OpenFile(nextPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	l.f = f
	l.w = bufio.NewWriterSize(f, 64<<10)
	return nil
}

// Replay 顺序回放 WAL 完整记录；坏尾已在 Open 时截断。
func (l *WAL) Replay(fn func(rec DecodedRecord) error) error {
	return l.ReplayFrom(1, fn)
}

// ReplayFrom 顺序回放 id >= minSegID 的 WAL segment。
//
// 这个接口是 flush 恢复链路的关键一环：
//   - flush 成功发布到 MANIFEST 后，会记录“已经覆盖到哪个 sealed segment”
//   - 启动时只需回放更晚的 segment，避免把已经进入 SST 的历史再重放回内存
func (l *WAL) ReplayFrom(minSegID int, fn func(rec DecodedRecord) error) error {
	segments, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if seg.id < minSegID {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			return err
		}
		if _, _, err := scanValidPrefix(f, fn); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	_, err = l.f.Seek(0, io.SeekEnd)
	return err
}

// CurrentSegmentID 返回当前活跃 WAL 文件的 segment 编号。
//
// flush 方案里需要在 rotate 前记住“旧 active 对应的是哪个 sealed segment”，
// 这样 flush 成功后才能把恢复边界推进到这个 segment。
func (l *WAL) CurrentSegmentID() int {
	if l == nil {
		return 0
	}
	return l.segID
}

// DeleteSegmentsUpTo 最佳努力删除所有 id <= maxID 的 sealed WAL segment。
//
// 这里刻意不删除当前活跃 segment，即使调用方把 maxID 传得过大也会自动截断。
// 这样 flush 成功后的 WAL 回收只会影响“已经被 SST + MANIFEST 覆盖”的旧段，
// 不会误伤当前仍在接收写入的日志文件。
func (l *WAL) DeleteSegmentsUpTo(maxID int) error {
	if l == nil || maxID <= 0 {
		return nil
	}
	segments, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	activeID := l.segID
	for _, seg := range segments {
		if seg.id > maxID {
			continue
		}
		if seg.id >= activeID {
			continue
		}
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (l *WAL) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		_ = l.f.Close()
		return err
	}
	if err := l.f.Sync(); err != nil {
		_ = l.f.Close()
		return err
	}
	return l.f.Close()
}

func writeFrame(w io.Writer, body []byte) error {
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(body))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func scanValidPrefix(r io.ReadSeeker, fn func(rec DecodedRecord) error) (validOffset int64, lastLSN uint64, err error) {
	var (
		offset int64
		header [8]byte
	)
	for {
		n, readErr := io.ReadFull(r, header[:])
		if readErr == io.EOF {
			return offset, lastLSN, nil
		}
		if readErr == io.ErrUnexpectedEOF {
			return offset, lastLSN, nil
		}
		if readErr != nil {
			return offset, lastLSN, readErr
		}
		offset += int64(n)

		bodyLen := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])
		if bodyLen == 0 {
			return validOffset, lastLSN, nil
		}
		if bodyLen > maxFrameBodySize {
			return validOffset, lastLSN, nil
		}
		body := make([]byte, bodyLen)
		n, readErr = io.ReadFull(r, body)
		if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
			return validOffset, lastLSN, nil
		}
		if readErr != nil {
			return validOffset, lastLSN, readErr
		}
		offset += int64(n)
		if crc32.ChecksumIEEE(body) != wantCRC {
			return validOffset, lastLSN, nil
		}
		rec, decErr := decodeBody(body)
		if decErr != nil {
			return validOffset, lastLSN, nil
		}
		validOffset = offset
		lastLSN = rec.LSN
		if fn != nil {
			if err := fn(rec); err != nil {
				return validOffset, lastLSN, err
			}
		}
	}
}

func encodeBody(lsn uint64, rec Record) ([]byte, error) {
	switch rec.Type {
	case RecordPut:
		body := make([]byte, 1+8+4+len(rec.Key)+4+len(rec.Value))
		body[0] = rec.Type
		binary.LittleEndian.PutUint64(body[1:9], lsn)
		pos := 9
		binary.LittleEndian.PutUint32(body[pos:pos+4], uint32(len(rec.Key)))
		pos += 4
		copy(body[pos:pos+len(rec.Key)], rec.Key)
		pos += len(rec.Key)
		binary.LittleEndian.PutUint32(body[pos:pos+4], uint32(len(rec.Value)))
		pos += 4
		copy(body[pos:pos+len(rec.Value)], rec.Value)
		return body, nil
	case RecordDelete:
		body := make([]byte, 1+8+4+len(rec.Key))
		body[0] = rec.Type
		binary.LittleEndian.PutUint64(body[1:9], lsn)
		pos := 9
		binary.LittleEndian.PutUint32(body[pos:pos+4], uint32(len(rec.Key)))
		pos += 4
		copy(body[pos:pos+len(rec.Key)], rec.Key)
		return body, nil
	case RecordBatchBegin, RecordBatchEnd:
		body := make([]byte, 1+8)
		body[0] = rec.Type
		binary.LittleEndian.PutUint64(body[1:9], lsn)
		return body, nil
	default:
		return nil, errUnknownRecordType
	}
}

func decodeBody(body []byte) (DecodedRecord, error) {
	if len(body) < 9 {
		return DecodedRecord{}, io.ErrUnexpectedEOF
	}
	rec := DecodedRecord{
		Type: body[0],
		LSN:  binary.LittleEndian.Uint64(body[1:9]),
	}
	pos := 9
	switch rec.Type {
	case RecordPut:
		if len(body) < pos+4 {
			return DecodedRecord{}, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if len(body) < pos+keyLen+4 {
			return DecodedRecord{}, io.ErrUnexpectedEOF
		}
		rec.Key = append([]byte(nil), body[pos:pos+keyLen]...)
		pos += keyLen
		valLen := int(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if len(body) < pos+valLen {
			return DecodedRecord{}, io.ErrUnexpectedEOF
		}
		rec.Value = append([]byte(nil), body[pos:pos+valLen]...)
		return rec, nil
	case RecordDelete:
		if len(body) < pos+4 {
			return DecodedRecord{}, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if len(body) < pos+keyLen {
			return DecodedRecord{}, io.ErrUnexpectedEOF
		}
		rec.Key = append([]byte(nil), body[pos:pos+keyLen]...)
		return rec, nil
	case RecordBatchBegin, RecordBatchEnd:
		return rec, nil
	default:
		return DecodedRecord{}, errUnknownRecordType
	}
}

func listSegments(dir string) ([]segmentInfo, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segs := make([]segmentInfo, 0, len(ents))
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		idStr := strings.TrimSuffix(name, ".wal")
		id, err := strconv.Atoi(idStr)
		if err != nil || id <= 0 {
			continue
		}
		segs = append(segs, segmentInfo{
			id:   id,
			path: filepath.Join(dir, name),
		})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].id < segs[j].id })
	return segs, nil
}

func segmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", id))
}
