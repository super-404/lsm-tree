package engine

import (
	"encoding/json"
	"lsm-tree/internal/sst"
	"os"
	"path/filepath"
)

const manifestFilename = "MANIFEST"

// manifestState 是第一版 flush 的最小持久化状态。
//
// 这里没有采用 sst-format.md 中那套二进制 MANIFEST，而是走文档里允许的 JSON 方案：
//   - 结构足够简单，便于先把 flush / 发布 / 恢复闭环做通
//   - 每次 flush 后整体重写，避免实现增量日志与回滚协议
//
// 额外保存的 flushedWalSegment 用来标记：
//   - 哪个 WAL segment 之前的数据已经被 SST 覆盖并正式发布
//   - 启动恢复时只需要回放更“新”的 segment
type manifestState struct {
	NextSSTID         uint64     `json:"next_sst_id"`
	FlushedWALSegment int        `json:"flushed_wal_segment"`
	Tables            []sst.Meta `json:"tables"`
}

func loadManifest(dir string) (*manifestState, error) {
	path := filepath.Join(dir, manifestFilename)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &manifestState{NextSSTID: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var st manifestState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.NextSSTID == 0 {
		st.NextSSTID = 1
	}
	return &st, nil
}

func saveManifest(dir string, st *manifestState) error {
	if st == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, manifestFilename)
	tmpPath := path + ".tmp"
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

func cloneManifest(st *manifestState) *manifestState {
	if st == nil {
		return &manifestState{NextSSTID: 1}
	}
	out := &manifestState{
		NextSSTID:         st.NextSSTID,
		FlushedWALSegment: st.FlushedWALSegment,
		Tables:            make([]sst.Meta, len(st.Tables)),
	}
	copy(out.Tables, st.Tables)
	return out
}
