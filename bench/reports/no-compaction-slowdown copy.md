# No Compaction Slowdown Report

- Generated at: `2026-03-31T14:38:56+08:00`
- Checkpoints: `[1 2 4 8 12 16]`
- Get rounds per checkpoint: `200`
- Full-scan rounds per checkpoint: `20`


| SSTs | Oldest Hit Avg | Oldest Hit P95 | Newest Hit Avg | Newest Hit P95 | Miss Avg | Miss P95 | Full Scan Avg | Full Scan P95 | Scan Avg/Item | Items/Scan | Oldest/Base |
| ---- | -------------- | -------------- | -------------- | -------------- | -------- | -------- | ------------- | ------------- | ------------- | ---------- | ----------- |
| 1    | 142.652µs      | 235.208µs      | 126.04µs       | 189.5µs        | 94ns     | 125ns    | 19.739802ms   | 21.750416ms   | 308.434µs     | 64         | x1.00       |
| 2    | 125.67µs       | 181.208µs      | 120.546µs      | 165.625µs      | 114ns    | 125ns    | 38.550431ms   | 44.561083ms   | 301.175µs     | 128        | x0.88       |
| 4    | 147.937µs      | 217.958µs      | 123.256µs      | 175.792µs      | 191ns    | 209ns    | 76.531508ms   | 79.635292ms   | 298.951µs     | 256        | x1.04       |
| 8    | 128.544µs      | 177.125µs      | 127.096µs      | 171.084µs      | 378ns    | 375ns    | 153.115783ms  | 155.087833ms  | 299.054µs     | 512        | x0.90       |
| 12   | 151.914µs      | 203.333µs      | 348.041µs      | 485.041µs      | 566ns    | 667ns    | 243.349622ms  | 316.303333ms  | 316.861µs     | 768        | x1.06       |
| 16   | 135.39µs       | 185.25µs       | 121.349µs      | 176.208µs      | 739ns    | 750ns    | 292.369016ms  | 295.456584ms  | 285.516µs     | 1024       | x0.95       |


## Summary

- `oldest-hit` average did not reach `2x` baseline within the configured checkpoints.
- `oldest-hit` average did not reach `4x` baseline within the configured checkpoints.
- `full-scan` average reached about `2x` baseline at `4` SSTs.
- `full-scan` average reached about `4x` baseline at `8` SSTs.

