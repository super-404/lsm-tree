# LSM 对比基准模板（可复现）

本文给出“当前 `lsm-tree` 与其他实现”可复现对比模板。

## 1. 已提供内容

1. 对比基准代码  
[`/Users/a58/myProject/lsm-tree/internal/engine/engine_compare_benchmark_test.go`](/Users/a58/myProject/lsm-tree/internal/engine/engine_compare_benchmark_test.go)

2. 一键运行与汇总脚本  
[`/Users/a58/myProject/lsm-tree/scripts/bench_compare.sh`](/Users/a58/myProject/lsm-tree/scripts/bench_compare.sh)

3. 报告输出位置  
`bench/reports/latest.md`

## 2. 默认对比对象

默认包含两个“引擎工厂”：

1. `my_lsm`：当前项目的 `engine.Open`
2. `map_ref`：内置 map 基线（用于 sanity check，不代表 LSM 真实竞品）

## 3. 如何接入“另一个 lsm-tree”

在 `internal/engine` 下新增一个仅测试用文件，例如：

`internal/engine/engine_compare_other_test.go`

示例：

```go
package engine

import (
  "testing"
)

func init() {
  externalCompareFactories["other_lsm"] = func(b *testing.B) compareDB {
    // TODO: 在这里构造并返回另一个 lsm 实现的 benchmark 适配对象
    // 需要提供 Put/Get/Delete/NewBatch/Write/NewIterator/Close 这组最小能力
    b.Fatal("wire your other_lsm adapter here")
    return nil
  }
}
```

## 4. 运行方式

```bash
./scripts/bench_compare.sh
```

可选参数（环境变量）：

1. `COUNT`：重复次数（默认 `5`）
2. `BENCHTIME`：每轮时长（默认 `2s`）
3. `CPU`：基准 cpu 并发（默认 `1`）

示例：

```bash
COUNT=8 BENCHTIME=3s CPU=1 ./scripts/bench_compare.sh
```

## 5. 输出说明

脚本会生成 Markdown 表格：

| Engine | Workload | ns/op(avg) | B/op(avg) | allocs/op(avg) | runs |
|---|---|---:|---:|---:|---:|
| my_lsm | balanced-1k | ... | ... | ... | ... |
| other_lsm | balanced-1k | ... | ... | ... | ... |

含义：

1. `ns/op(avg)`：多轮平均单次操作耗时（越小越好）
2. `B/op(avg)`：平均每次操作分配字节（越小越好）
3. `allocs/op(avg)`：平均每次操作分配次数（越小越好）

## 6. 可复现建议

1. 固定机器、固定电源模式、关闭后台高负载任务
2. 使用 `CPU=1` 做单线程对比
3. `COUNT>=5`，避免偶然波动
4. 每次对比保持相同 `COUNT/BENCHTIME/CPU`
