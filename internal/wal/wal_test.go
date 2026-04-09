package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func countDecodedRecordsInSegment(t *testing.T, path string) int {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", path, err)
	}
	defer f.Close()

	count := 0
	validOffset, _, err := scanValidPrefix(f, func(rec DecodedRecord) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("scanValidPrefix(%s) error: %v", path, err)
	}
	if validOffset == 0 {
		t.Fatalf("segment %s has no decodable WAL records", path)
	}
	return count
}

func TestWALReplayReadsBothRotatedSegments(t *testing.T) {
	dir := t.TempDir()
	seg1 := filepath.Join(dir, "000001.wal")
	seg2 := filepath.Join(dir, "000002.wal")

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", dir, err)
	}
	defer l.Close()

	if err := l.Append(Record{Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Append(seg1) error: %v", err)
	}
	if err := l.Rotate(); err != nil {
		t.Fatalf("Rotate(seg1->seg2) error: %v", err)
	}
	if err := l.Append(Record{Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")}); err != nil {
		t.Fatalf("Append(seg2) error: %v", err)
	}

	if _, err := os.Stat(seg1); err != nil {
		t.Fatalf("segment1 missing: %v", err)
	}
	if _, err := os.Stat(seg2); err != nil {
		t.Fatalf("segment2 missing: %v", err)
	}

	seg1Count := countDecodedRecordsInSegment(t, seg1)
	seg2Count := countDecodedRecordsInSegment(t, seg2)
	if seg1Count == 0 || seg2Count == 0 {
		t.Fatalf("expected both segments to contain records, got seg1=%d seg2=%d", seg1Count, seg2Count)
	}

	replayWAL, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", dir, err)
	}
	defer replayWAL.Close()

	replayed := 0
	if err := replayWAL.Replay(func(rec DecodedRecord) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("Replay() error: %v", err)
	}

	want := seg1Count + seg2Count
	if replayed != want {
		t.Fatalf("Replay() decoded %d records, want %d from %s and %s", replayed, want, filepath.Base(seg1), filepath.Base(seg2))
	}

	t.Logf("decoded %d records from %s and %d records from %s", seg1Count, filepath.Base(seg1), seg2Count, filepath.Base(seg2))
	t.Logf("Replay() successfully consumed both segments from %s", fmt.Sprintf("%s, %s", seg1, seg2))
}

func TestWALDeleteSegmentsUpToKeepsActiveSegment(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", dir, err)
	}
	defer l.Close()

	// 依次写出 3 个 segment，其中 000003.wal 保持为当前活跃 segment。
	if err := l.Append(Record{Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Append(seg1) error: %v", err)
	}
	if err := l.Rotate(); err != nil {
		t.Fatalf("Rotate(seg1->seg2) error: %v", err)
	}
	if err := l.Append(Record{Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")}); err != nil {
		t.Fatalf("Append(seg2) error: %v", err)
	}
	if err := l.Rotate(); err != nil {
		t.Fatalf("Rotate(seg2->seg3) error: %v", err)
	}
	if err := l.Append(Record{Type: RecordPut, Key: []byte("k3"), Value: []byte("v3")}); err != nil {
		t.Fatalf("Append(seg3) error: %v", err)
	}

	seg1 := filepath.Join(dir, "000001.wal")
	seg2 := filepath.Join(dir, "000002.wal")
	seg3 := filepath.Join(dir, "000003.wal")
	for _, seg := range []string{seg1, seg2, seg3} {
		if _, err := os.Stat(seg); err != nil {
			t.Fatalf("expected segment %s to exist before cleanup: %v", seg, err)
		}
	}

	if err := l.DeleteSegmentsUpTo(2); err != nil {
		t.Fatalf("DeleteSegmentsUpTo(2) error: %v", err)
	}

	if _, err := os.Stat(seg1); !os.IsNotExist(err) {
		t.Fatalf("segment %s should be deleted, stat err=%v", seg1, err)
	}
	if _, err := os.Stat(seg2); !os.IsNotExist(err) {
		t.Fatalf("segment %s should be deleted, stat err=%v", seg2, err)
	}
	if _, err := os.Stat(seg3); err != nil {
		t.Fatalf("active segment %s should remain after cleanup: %v", seg3, err)
	}

	// 即使上限传得比当前活跃段更大，也不能把活跃文件删掉。
	if err := l.DeleteSegmentsUpTo(99); err != nil {
		t.Fatalf("DeleteSegmentsUpTo(99) error: %v", err)
	}
	if _, err := os.Stat(seg3); err != nil {
		t.Fatalf("active segment %s should still remain after oversized cleanup: %v", seg3, err)
	}
}
