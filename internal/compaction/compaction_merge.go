package compaction

import (
	"lsm-tree/internal/iter"
	"lsm-tree/internal/sst"
	"sort"
)

// MergeTablesToEntries 把多张输入表归并成“每个 key 一条最新逻辑记录”。
//
// 当前实现采用一个很朴素、但足够正确的做法：
//   - 输入 tables 按 engine 当前发布顺序传入：新 -> 旧
//   - 归并时反向遍历为：旧 -> 新
//   - 用 map[string]Entry 覆盖写入，让更新的记录自然覆盖更老记录
//   - 最后再按 key 排序，得到写新 SST 所需的升序 Entry 列表
//
// 这版实现的优势是：
//   - tombstone 语义天然保留
//   - 逻辑非常直白，适合第一版 compaction 落地
//
// 它的代价也很明确：
//   - 需要把本轮 compaction 的 key 暂存在内存 map 中
//   - 不是最终的流式 merge iterator 方案
func MergeTablesToEntries(tables []*sst.Table, dropBottommostTombstones bool) ([]iter.Entry, error) {
	latest := make(map[string]iter.Entry)
	for i := len(tables) - 1; i >= 0; i-- {
		it := tables[i].Entries()
		for it.Valid() {
			entry := it.Item()
			latest[string(entry.Key)] = cloneEntry(entry)
			it.Next()
		}
		it.Close()
	}

	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]iter.Entry, 0, len(keys))
	for _, key := range keys {
		entry := latest[key]
		if dropBottommostTombstones && entry.Op == iter.OpDelete {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

func cloneEntry(entry iter.Entry) iter.Entry {
	return iter.Entry{
		Key:   cloneBytes(entry.Key),
		Value: cloneBytes(entry.Value),
		Op:    entry.Op,
	}
}
