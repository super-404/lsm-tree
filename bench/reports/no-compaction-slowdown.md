# No Compaction Slowdown Report

- Generated at: `2026-04-09T17:11:17+08:00`
- Checkpoints: `[1 2 4 8 12 16]`
- Get rounds per checkpoint: `200`
- Full-scan rounds per checkpoint: `20`


| SSTs | Oldest Hit Avg | Oldest Hit P95 | Newest Hit Avg | Newest Hit P95 | Miss Avg | Miss P95 | Full Scan Avg | Full Scan P95 | Scan Avg/Item | Items/Scan | Oldest/Base |
| ---- | -------------- | -------------- | -------------- | -------------- | -------- | -------- | ------------- | ------------- | ------------- | ---------- | ----------- |
| 1    | 159.844µs      | 295.708µs      | 121.558µs      | 156.5µs        | 90ns     | 125ns    | 21.658485ms   | 31.482542ms   | 338.413µs     | 64         | x1.00       |
| 2    | 150.517µs      | 209.459µs      | 121.712µs      | 167.291µs      | 118ns    | 166ns    | 35.677014ms   | 36.864042ms   | 278.726µs     | 128        | x0.94       |
| 4    | 239.954µs      | 432µs          | 414.431µs      | 767.25µs       | 423ns    | 334ns    | 77.425316ms   | 88.747167ms   | 302.442µs     | 256        | x1.50       |
| 8    | 117.938µs      | 163.291µs      | 120.539µs      | 157.375µs      | 234ns    | 250ns    | 127.71272ms   | 132.243541ms  | 285.073µs     | 448        | x0.74       |
| 12   | 161.288µs      | 216.417µs      | 124.147µs      | 172.084µs      | 303ns    | 292ns    | 198.545216ms  | 203.993708ms  | 282.024µs     | 704        | x1.01       |
| 16   | 163.722µs      | 257.75µs       | 211.232µs      | 165.834µs      | 225ns    | 292ns    | 257.352758ms  | 261.09525ms   | 287.224µs     | 896        | x1.02       |


## Summary

- `oldest-hit` average did not reach `2x` baseline within the configured checkpoints.
- `oldest-hit` average did not reach `4x` baseline within the configured checkpoints.
- `full-scan` average reached about `2x` baseline at `4` SSTs.
- `full-scan` average reached about `4x` baseline at `8` SSTs.

