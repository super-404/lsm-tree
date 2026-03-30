
| Engine  | Workload        | ns/op(avg) | B/op(avg) | allocs/op(avg) | runs |
| ------- | --------------- | ---------- | --------- | -------------- | ---- |
| my_lsm  | write-heavy-4k  | 5111       | 6696      | 6.00           | 1    |
| my_lsm  | read-heavy-256b | 6820       | 341       | 3.00           | 1    |
| map_ref | write-heavy-4k  | 1748       | 6622      | 3.00           | 1    |
| my_lsm  | balanced-1k     | 7708       | 1919      | 7.00           | 1    |
| map_ref | read-heavy-256b | 253        | 306       | 1.00           | 1    |
| map_ref | balanced-1k     | 838        | 1847      | 3.00           | 1    |


