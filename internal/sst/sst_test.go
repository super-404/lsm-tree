package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	lsmiter "lsm-tree/internal/iter"
	"lsm-tree/internal/memtable"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sliceEntryIterator 是测试专用的最小 EntryIterator 实现。
//
// 它让单测可以直接精确构造一组逻辑记录，而不必每次都依赖 memtable 构造输入；
// 这样在测试“乱序写入会失败”这类边界场景时会更直接。
type sliceEntryIterator struct {
	entries []lsmiter.Entry
	idx     int
}

func (it *sliceEntryIterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

func (it *sliceEntryIterator) Item() lsmiter.Entry {
	if !it.Valid() {
		return lsmiter.Entry{}
	}
	return it.entries[it.idx]
}

func (it *sliceEntryIterator) Next() {
	if it.Valid() {
		it.idx++
	}
}

func (it *sliceEntryIterator) Close() {}

func collectValuePairs(it lsmiter.ValueIterator) [][2]string {
	if it == nil {
		return nil
	}
	defer it.Close()
	var got [][2]string
	for it.Valid() {
		item := it.Item()
		got = append(got, [2]string{string(item.Key), string(item.Value)})
		it.Next()
	}
	return got
}

// TestWriteFileOpenAndGetLatest 验证 SST 的主闭环：
//   - 用 memtable 的 Entries 视图写出 SST
//   - 重新 Open 后能恢复出正确的 Meta Footer
//   - GetLatest 对 Put/Delete/missing 三种情况都返回正确结果
func TestWriteFileOpenAndGetLatest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	mt := memtable.NewMemtable()
	mt.Put([]byte("a"), []byte("old"))
	mt.Put([]byte("a"), []byte("new"))
	mt.Put([]byte("b"), []byte("2"))
	mt.Delete([]byte("b"))
	mt.Put([]byte("c"), []byte("3"))

	meta, err := WriteFile(path, 1, 0, mt.RawEntries())
	if err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	if meta.RecordCount != 3 {
		t.Fatalf("RecordCount = %d, want 3", meta.RecordCount)
	}
	if meta.BlockIndexLength == 0 {
		t.Fatal("BlockIndexLength = 0, want block index to be written")
	}
	if !bytes.Equal(meta.MinKey, []byte("a")) || !bytes.Equal(meta.MaxKey, []byte("c")) {
		t.Fatalf("min/max = (%q,%q), want (\"a\",\"c\")", meta.MinKey, meta.MaxKey)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}

	fileMeta := table.Meta()
	if fileMeta.RecordCount != 3 {
		t.Fatalf("opened RecordCount = %d, want 3", fileMeta.RecordCount)
	}
	if !bytes.Equal(fileMeta.MinKey, []byte("a")) || !bytes.Equal(fileMeta.MaxKey, []byte("c")) {
		t.Fatalf("opened min/max = (%q,%q), want (\"a\",\"c\")", fileMeta.MinKey, fileMeta.MaxKey)
	}

	v, op, found := table.GetLatest([]byte("a"))
	if !found || op != lsmiter.OpPut || string(v) != "new" {
		t.Fatalf("GetLatest(a) = (%q,%v,%v), want (\"new\",OpPut,true)", v, op, found)
	}

	v, op, found = table.GetLatest([]byte("b"))
	if !found || op != lsmiter.OpDelete || v != nil {
		t.Fatalf("GetLatest(b) = (%q,%v,%v), want (nil,OpDelete,true)", v, op, found)
	}

	v, op, found = table.GetLatest([]byte("missing"))
	if found || op != 0 || v != nil {
		t.Fatalf("GetLatest(missing) = (%q,%v,%v), want (nil,0,false)", v, op, found)
	}
}

// TestValuesRangeSkipsDelete 验证 SST 值迭代器的可见值语义：
//   - tombstone 不应出现在 Values 输出里
//   - 范围仍保持 [start, end) 的左闭右开边界
func TestValuesRangeSkipsDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000002.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Op: lsmiter.OpDelete},
		{Key: []byte("c"), Value: []byte("3"), Op: lsmiter.OpPut},
		{Key: []byte("d"), Value: []byte("4"), Op: lsmiter.OpPut},
	}}
	if _, err := WriteFile(path, 2, 0, entries); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}

	got := collectValuePairs(table.Values())
	want := [][2]string{{"a", "1"}, {"c", "3"}, {"d", "4"}}
	if len(got) != len(want) {
		t.Fatalf("Values len = %d, want %d, got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Values[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	got = collectValuePairs(table.ValuesRange([]byte("b"), []byte("d")))
	want = [][2]string{{"c", "3"}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("ValuesRange[b,d) = %v, want %v", got, want)
	}
}

// TestWriteFileRejectsNonIncreasingKeys 验证写入端会拒绝非严格递增的 key，
// 这样才能保证单个 SST 的有序与“单 key 不重复”约束。
func TestWriteFileRejectsNonIncreasingKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("b"), Value: []byte("2"), Op: lsmiter.OpPut},
		{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut},
	}}

	if _, err := WriteFile(path, 3, 0, entries); err == nil {
		t.Fatal("WriteFile() error = nil, want non-increasing key error")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bad sst should not be published, stat err = %v", err)
	}
}

// TestOpenRejectsCorruptedTrailer 验证从文件尾回溯 Meta Footer 的入口足够严格：
// 一旦 Trailer 的 eof magic 被破坏，Open 应直接失败，而不是误解析错误元数据。
func TestOpenRejectsCorruptedTrailer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000004.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut},
	}}
	if _, err := WriteFile(path, 4, 0, entries); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error: %v", err)
	}
	defer f.Close()
	if _, err := f.Seek(-4, 2); err != nil {
		t.Fatalf("Seek() error: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 0}); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync() error: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("Open() error = nil, want invalid eof magic")
	}
}

// TestTrailerLocatesMetaFooterByBytes 直接按磁盘字节布局验证：
//   - Trailer 确实位于文件最后 8 字节
//   - 其中 meta_size 能反推出 Meta Footer 的起始位置
//   - 反推出的 Meta Footer 再解析后，与 WriteFile/Open 返回的统计信息一致
//
// 这条测试不依赖 Open 的高层流程，而是直接按文件字节布局做定位，
// 用来守住 “Trailer -> meta_size -> Meta Footer offset” 这条最核心的解析链路。
func TestTrailerLocatesMetaFooterByBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000005.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Op: lsmiter.OpDelete},
		{Key: []byte("c"), Value: []byte("3"), Op: lsmiter.OpPut},
	}}

	writtenMeta, err := WriteFile(path, 5, 0, entries)
	if err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error: %v", err)
	}
	if len(raw) < fileHeaderSize+trailerSize {
		t.Fatalf("file size = %d, want >= %d", len(raw), fileHeaderSize+trailerSize)
	}

	trailer := raw[len(raw)-trailerSize:]
	metaSize := binary.LittleEndian.Uint32(trailer[:4])
	gotEOFMagic := binary.LittleEndian.Uint32(trailer[4:8])
	if gotEOFMagic != eofMagic {
		t.Fatalf("eof magic = %#x, want %#x", gotEOFMagic, eofMagic)
	}

	metaOffset := len(raw) - trailerSize - int(metaSize)
	if metaOffset < fileHeaderSize {
		t.Fatalf("metaOffset = %d, want >= %d", metaOffset, fileHeaderSize)
	}

	// 加入 Block Index 和 Bloom Filter 后，Meta Footer 应该位于两者之后。
	wantMetaOffset := fileHeaderSize + int(writtenMeta.DataLength) + int(writtenMeta.BlockIndexLength) + int(writtenMeta.BloomFilterLength)
	if metaOffset != wantMetaOffset {
		t.Fatalf("metaOffset = %d, want %d (header + dataLength + blockIndexLength + bloomFilterLength)", metaOffset, wantMetaOffset)
	}

	metaBytes := raw[metaOffset : len(raw)-trailerSize]
	if len(metaBytes) != int(metaSize) {
		t.Fatalf("meta footer len = %d, want %d", len(metaBytes), metaSize)
	}
	if binary.LittleEndian.Uint32(metaBytes[:4]) != metaVersion {
		t.Fatalf("meta version = %d, want %d", binary.LittleEndian.Uint32(metaBytes[:4]), metaVersion)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("OpenFile() error: %v", err)
	}
	defer f.Close()

	parsedMeta, err := readMetaFooter(f, int64(metaOffset), metaSize)
	if err != nil {
		t.Fatalf("readMetaFooter() error: %v", err)
	}
	if parsedMeta.dataOffset != fileHeaderSize {
		t.Fatalf("parsed dataOffset = %d, want %d", parsedMeta.dataOffset, fileHeaderSize)
	}
	if parsedMeta.dataLength != writtenMeta.DataLength {
		t.Fatalf("parsed dataLength = %d, want %d", parsedMeta.dataLength, writtenMeta.DataLength)
	}
	if parsedMeta.bloomFilterLength != writtenMeta.BloomFilterLength {
		t.Fatalf("parsed bloomFilterLength = %d, want %d", parsedMeta.bloomFilterLength, writtenMeta.BloomFilterLength)
	}
	if parsedMeta.recordCount != writtenMeta.RecordCount {
		t.Fatalf("parsed recordCount = %d, want %d", parsedMeta.recordCount, writtenMeta.RecordCount)
	}
	if !bytes.Equal(parsedMeta.minKey, writtenMeta.MinKey) || !bytes.Equal(parsedMeta.maxKey, writtenMeta.MaxKey) {
		t.Fatalf("parsed min/max = (%q,%q), want (%q,%q)", parsedMeta.minKey, parsedMeta.maxKey, writtenMeta.MinKey, writtenMeta.MaxKey)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	opened := table.Meta()
	if opened.FileSize != uint64(len(raw)) {
		t.Fatalf("opened FileSize = %d, want %d", opened.FileSize, len(raw))
	}
}

// TestMetaFooterStoresCorrectDataCRC 验证写入时落到 Meta Footer 的 data_crc32
// 与 Data Section 真实字节计算出的 CRC32 完全一致。
func TestMetaFooterStoresCorrectDataCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000005-data-crc.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("a"), Value: []byte("value-a"), Op: lsmiter.OpPut},
		{Key: []byte("b"), Op: lsmiter.OpDelete},
		{Key: []byte("c"), Value: bytes.Repeat([]byte("c"), 128), Op: lsmiter.OpPut},
	}}
	meta, err := WriteFile(path, 50, 0, entries)
	if err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error: %v", err)
	}
	trailer := raw[len(raw)-trailerSize:]
	metaSize := binary.LittleEndian.Uint32(trailer[:4])
	metaOffset := len(raw) - trailerSize - int(metaSize)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer f.Close()

	parsedMeta, err := readMetaFooter(f, int64(metaOffset), metaSize)
	if err != nil {
		t.Fatalf("readMetaFooter() error: %v", err)
	}

	dataBytes := raw[fileHeaderSize : fileHeaderSize+int(meta.DataLength)]
	gotCRC := crc32.ChecksumIEEE(dataBytes)
	if gotCRC != parsedMeta.dataCRC32 {
		t.Fatalf("data crc = %#x, want %#x", gotCRC, parsedMeta.dataCRC32)
	}
}

// TestStoredDataCRCDetectsTamperedData 验证 data_crc32 确实能区分“原始数据”和“被篡改后的数据”。
//
// 当前 Open 还不会主动校验 data_crc32，这条测试验证的是 checksum 本身的有效性：
//   - 原始 Data Section 的 CRC 应与 Meta Footer 中存储的一致
//   - 篡改 Data Section 任一字节后，重新计算的 CRC 应失配
func TestStoredDataCRCDetectsTamperedData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000005-data-crc-tamper.sst")

	entries := &sliceEntryIterator{entries: []lsmiter.Entry{
		{Key: []byte("a"), Value: bytes.Repeat([]byte("x"), 256), Op: lsmiter.OpPut},
		{Key: []byte("b"), Value: bytes.Repeat([]byte("y"), 256), Op: lsmiter.OpPut},
	}}
	meta, err := WriteFile(path, 51, 0, entries)
	if err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error: %v", err)
	}
	trailer := raw[len(raw)-trailerSize:]
	metaSize := binary.LittleEndian.Uint32(trailer[:4])
	metaOffset := len(raw) - trailerSize - int(metaSize)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	parsedMeta, err := readMetaFooter(f, int64(metaOffset), metaSize)
	_ = f.Close()
	if err != nil {
		t.Fatalf("readMetaFooter() error: %v", err)
	}

	dataStart := fileHeaderSize
	dataEnd := fileHeaderSize + int(meta.DataLength)
	if dataEnd-dataStart < 8 {
		t.Fatalf("data section too small: %d", dataEnd-dataStart)
	}
	corruptOffset := dataStart + (dataEnd-dataStart)/2
	raw[corruptOffset] ^= 0x01
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile(corrupted) error: %v", err)
	}

	corrupted, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(corrupted) error: %v", err)
	}
	gotCRC := crc32.ChecksumIEEE(corrupted[dataStart:dataEnd])
	if gotCRC == parsedMeta.dataCRC32 {
		t.Fatalf("tampered data crc = %#x, want mismatch with stored %#x", gotCRC, parsedMeta.dataCRC32)
	}
}

// TestOpenLoadsBlockIndexAndSupportsCrossBlockQueries 验证较大的 SST 会生成多块索引，
// 并且点查、范围扫描都能从非首块开始正常工作。
func TestOpenLoadsBlockIndexAndSupportsCrossBlockQueries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000006.sst")

	entries := make([]lsmiter.Entry, 0, 80)
	for i := 0; i < 80; i++ {
		entries = append(entries, lsmiter.Entry{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: bytes.Repeat([]byte{byte('a' + i%26)}, 2048),
			Op:    lsmiter.OpPut,
		})
	}

	meta, err := WriteFile(path, 6, 0, &sliceEntryIterator{entries: entries})
	if err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	if meta.BlockIndexLength == 0 {
		t.Fatal("BlockIndexLength = 0, want block index to be written")
	}

	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if len(table.blocks) < 2 {
		t.Fatalf("len(blocks) = %d, want >= 2", len(table.blocks))
	}
	if len(table.bloom.bits) == 0 {
		t.Fatal("bloom filter should be loaded")
	}
	if !table.bloom.mayContain([]byte("k079")) {
		t.Fatal("bloom filter reported false for an existing key")
	}

	value, op, found := table.GetLatest([]byte("k079"))
	if !found || op != lsmiter.OpPut || len(value) != 2048 {
		t.Fatalf("GetLatest(k079) = (len=%d,%v,%v), want (2048,OpPut,true)", len(value), op, found)
	}

	// 对一批不存在 key 做探测，至少应能找到一个被 Bloom Filter 明确排除的 miss。
	foundBloomMiss := false
	for i := 0; i < 1024; i++ {
		key := []byte(fmt.Sprintf("missing-%03d", i))
		if !table.bloom.mayContain(key) {
			if v, op, ok := table.GetLatest(key); ok || op != 0 || v != nil {
				t.Fatalf("GetLatest(%q) = (%q,%v,%v), want (nil,0,false)", key, v, op, ok)
			}
			foundBloomMiss = true
			break
		}
	}
	if !foundBloomMiss {
		t.Fatal("expected bloom filter to reject at least one missing key")
	}

	got := collectValuePairs(table.ValuesRange([]byte("k055"), []byte("k060")))
	wantKeys := []string{"k055", "k056", "k057", "k058", "k059"}
	if len(got) != len(wantKeys) {
		t.Fatalf("ValuesRange(k055,k060) len = %d, want %d", len(got), len(wantKeys))
	}
	for i := range wantKeys {
		if got[i][0] != wantKeys[i] || len(got[i][1]) != 2048 {
			t.Fatalf("ValuesRange[%d] = (%q,len=%d), want (%q,len=2048)", i, got[i][0], len(got[i][1]), wantKeys[i])
		}
	}
}

// TestOpenRejectsMalformedFiles 人工构造几类坏文件，覆盖 Open 的更多失败分支。
//
// 这些样例分别模拟：
//   - 文件过短
//   - Header 魔数损坏
//   - Header 版本损坏
//   - Trailer 里的 meta_size 过大，导致 Meta Footer 起点越过 Header
//   - Trailer 里的 meta_size 过小，导致读取 Meta Footer 时提前 EOF
//   - Meta Footer 版本损坏
func TestOpenRejectsMalformedFiles(t *testing.T) {
	const (
		metaVersionFieldOffset     = 0
		metaRecordCountFieldOffset = 4
		metaDataOffsetFieldOffset  = metaRecordCountFieldOffset + 8
		metaDataLengthFieldOffset  = metaDataOffsetFieldOffset + 8
		metaDataCRC32FieldOffset   = metaDataLengthFieldOffset + 8
		metaBlockIndexOffsetField  = metaDataCRC32FieldOffset + 4
		metaBlockIndexLengthField  = metaBlockIndexOffsetField + 8
		metaBlockIndexCRC32Field   = metaBlockIndexLengthField + 8
		metaBloomFilterOffsetField = metaBlockIndexCRC32Field + 4
		metaBloomFilterCRC32Field  = metaBloomFilterOffsetField + 8 + 8
	)

	makeValidFile := func(t *testing.T, dir, name string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		entries := &sliceEntryIterator{entries: []lsmiter.Entry{
			{Key: []byte("a"), Value: []byte("1"), Op: lsmiter.OpPut},
			{Key: []byte("b"), Value: []byte("2"), Op: lsmiter.OpPut},
		}}
		if _, err := WriteFile(path, 6, 0, entries); err != nil {
			t.Fatalf("WriteFile(%s) error: %v", name, err)
		}
		return path
	}

	readRawAndMetaOffset := func(t *testing.T, path string) ([]byte, int) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile() error: %v", err)
		}
		metaSize := binary.LittleEndian.Uint32(raw[len(raw)-trailerSize : len(raw)-4])
		metaOffset := len(raw) - trailerSize - int(metaSize)
		return raw, metaOffset
	}

	writeBytes := func(t *testing.T, path string, data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error: %v", path, err)
		}
	}

	dir := t.TempDir()

	tests := []struct {
		name    string
		build   func(t *testing.T) string
		wantErr string
	}{
		{
			name: "too small",
			build: func(t *testing.T) string {
				path := filepath.Join(dir, "too-small.sst")
				writeBytes(t, path, []byte("short"))
				return path
			},
			wantErr: "file too small",
		},
		{
			name: "invalid file magic",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-magic.sst")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile() error: %v", err)
				}
				raw[0] = 'X'
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "invalid file magic",
		},
		{
			name: "unsupported version",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-version.sst")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile() error: %v", err)
				}
				binary.LittleEndian.PutUint32(raw[8:12], fileVersion+1)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "unsupported version",
		},
		{
			name: "meta size too large",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "meta-too-large.sst")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile() error: %v", err)
				}
				binary.LittleEndian.PutUint32(raw[len(raw)-trailerSize:len(raw)-4], uint32(len(raw)))
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "invalid meta offset",
		},
		{
			name: "meta size too small",
			build: func(t *testing.T) string {
				path := filepath.Join(dir, "meta-too-small.sst")
				raw := make([]byte, 0, fileHeaderSize+4+trailerSize)

				// 手工构造一个“Header 正确、Trailer 正确、但 Meta Footer 只有 4 字节 version”的文件。
				// Open 读取 Trailer 后会得到 meta_size=4，随后 readMetaFooter 在继续解析
				// recordCount 时会稳定命中 io.ErrUnexpectedEOF。
				var header [fileHeaderSize]byte
				copy(header[0:8], []byte(fileMagic))
				binary.LittleEndian.PutUint32(header[8:12], fileVersion)
				raw = append(raw, header[:]...)
				raw = binary.LittleEndian.AppendUint32(raw, metaVersion)

				var trailer [trailerSize]byte
				binary.LittleEndian.PutUint32(trailer[0:4], 4)
				binary.LittleEndian.PutUint32(trailer[4:8], eofMagic)
				raw = append(raw, trailer[:]...)

				writeBytes(t, path, raw)
				return path
			},
			wantErr: "unexpected EOF",
		},
		{
			name: "unsupported meta version",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-meta-version.sst")
				raw, metaOffset := readRawAndMetaOffset(t, path)
				binary.LittleEndian.PutUint32(raw[metaOffset+metaVersionFieldOffset:metaOffset+metaVersionFieldOffset+4], metaVersion+1)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "unsupported meta version",
		},
		{
			name: "unexpected data offset",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-data-offset.sst")
				raw, metaOffset := readRawAndMetaOffset(t, path)
				// 正常值应为 fileHeaderSize，这里故意改成 fileHeaderSize+1，
				// 用来验证 Open 会拒绝“Meta Footer 声称 Data 不从 Header 后开始”的文件。
				binary.LittleEndian.PutUint64(raw[metaOffset+metaDataOffsetFieldOffset:metaOffset+metaDataOffsetFieldOffset+8], fileHeaderSize+1)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "unexpected data offset",
		},
		{
			name: "invalid block index offset",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-block-index-offset.sst")
				raw, metaOffset := readRawAndMetaOffset(t, path)
				dataLength := binary.LittleEndian.Uint64(raw[metaOffset+metaDataLengthFieldOffset : metaOffset+metaDataLengthFieldOffset+8])
				// 第一版 block index 长度固定为 0，因此 blockIndexOffset 必须严格等于
				// dataOffset + dataLength。这里故意偏移一个字节，验证自洽性校验会拦截。
				binary.LittleEndian.PutUint64(raw[metaOffset+metaBlockIndexOffsetField:metaOffset+metaBlockIndexOffsetField+8], uint64(fileHeaderSize)+dataLength+1)
				// 显式把 blockIndexLength 保持为 0，避免误伤到其它校验分支。
				binary.LittleEndian.PutUint64(raw[metaOffset+metaBlockIndexLengthField:metaOffset+metaBlockIndexLengthField+8], 0)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "invalid block index offset",
		},
		{
			name: "invalid bloom filter offset",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-bloom-filter-offset.sst")
				raw, metaOffset := readRawAndMetaOffset(t, path)
				blockIndexOffset := binary.LittleEndian.Uint64(raw[metaOffset+metaBlockIndexOffsetField : metaOffset+metaBlockIndexOffsetField+8])
				blockIndexLength := binary.LittleEndian.Uint64(raw[metaOffset+metaBlockIndexLengthField : metaOffset+metaBlockIndexLengthField+8])
				binary.LittleEndian.PutUint64(raw[metaOffset+metaBloomFilterOffsetField:metaOffset+metaBloomFilterOffsetField+8], blockIndexOffset+blockIndexLength+1)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "invalid bloom filter offset",
		},
		{
			name: "invalid bloom filter crc",
			build: func(t *testing.T) string {
				path := makeValidFile(t, dir, "bad-bloom-filter-crc.sst")
				raw, metaOffset := readRawAndMetaOffset(t, path)
				storedCRC := binary.LittleEndian.Uint32(raw[metaOffset+metaBloomFilterCRC32Field : metaOffset+metaBloomFilterCRC32Field+4])
				binary.LittleEndian.PutUint32(raw[metaOffset+metaBloomFilterCRC32Field:metaOffset+metaBloomFilterCRC32Field+4], storedCRC^0xFFFFFFFF)
				writeBytes(t, path, raw)
				return path
			},
			wantErr: "invalid bloom filter crc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.build(t)
			_, err := Open(path)
			if err == nil {
				t.Fatalf("Open(%s) error = nil, want contains %q", path, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Open(%s) error = %q, want contains %q", path, err.Error(), tc.wantErr)
			}
		})
	}
}
