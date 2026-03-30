package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error: %v", err)
	}
	cur := wd
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			t.Fatalf("go.mod not found from %s upward", wd)
		}
		cur = parent
	}
}

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
	root := findRepoRoot(t)
	dir := filepath.Join(root, "test", "wal", "wal_segment_rotate", "wal")
	seg1 := filepath.Join(dir, "000001.wal")
	seg2 := filepath.Join(dir, "000002.wal")

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

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", dir, err)
	}
	defer l.Close()

	replayed := 0
	if err := l.Replay(func(rec DecodedRecord) error {
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
