# 📊 jsondb-high Benchmark Results

> Generated: 2026-01-30T22:26:29.714Z  
> Iterations per test: 10,000  
> Warmup iterations: 1,000

## System Information

- **Platform**: linux
- **Architecture**: x64
- **Node Version**: v24.3.0

## Summary

| Operation | In-Memory (ops/s) | WAL Mode (ops/s) | Avg Latency (ms) |
| --------- | ----------------- | ---------------- | ---------------- |
| set (simple) | 266,567 | 244,557 | 0.0038 |
| set (nested) | 229,894 | 241,600 | 0.0043 |
| get (existing) | 762,585 | 992,655 | 0.0013 |
| get (default) | 1,453,881 | 2,069,142 | 0.0007 |
| has (existing) | 1,842,944 | 1,830,801 | 0.0005 |
| has (missing) | 1,938,518 | 1,729,562 | 0.0005 |
| delete | 331,851 | 321,576 | 0.0030 |
| add | 333,251 | 334,420 | 0.0030 |
| subtract | 334,696 | 340,233 | 0.0030 |
| push | 3,436 | 3,227 | 0.2911 |
| pull | 2,127 | 2,162 | 0.4700 |
| query.where().exec() | 1,104 | 1,061 | 0.9054 |
| query.sort().limit() | 755 | 737 | 1.3244 |
| query.count() | 1,168 | 1,150 | 0.8563 |
| query.sum() | 1,146 | 1,118 | 0.8728 |
| find (predicate) | 1,169 | 1,119 | 0.8555 |
| find (object) | 1,179 | 1,142 | 0.8481 |
| paginate | 1,158 | 1,145 | 0.8638 |
| findByIndex | 511,932 | 451,284 | 0.0020 |
| batch (10 ops) | 187,280 | 151,731 | 0.0053 |

## Detailed Results

### In-Memory Mode

In-memory mode prioritizes speed. Data is kept in RAM and flushed to disk periodically.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
| set (simple) | 10,000 | 37.51 | 0.0038 | 266,567 |
| set (nested) | 10,000 | 43.50 | 0.0043 | 229,894 |
| get (existing) | 10,000 | 13.11 | 0.0013 | 762,585 |
| get (default) | 10,000 | 6.88 | 0.0007 | 1,453,881 |
| has (existing) | 10,000 | 5.43 | 0.0005 | 1,842,944 |
| has (missing) | 10,000 | 5.16 | 0.0005 | 1,938,518 |
| delete | 10,000 | 30.13 | 0.0030 | 331,851 |
| add | 10,000 | 30.01 | 0.0030 | 333,251 |
| subtract | 10,000 | 29.88 | 0.0030 | 334,696 |
| push | 1,000 | 291.05 | 0.2911 | 3,436 |
| pull | 100 | 47.00 | 0.4700 | 2,127 |
| query.where().exec() | 1,000 | 905.43 | 0.9054 | 1,104 |
| query.sort().limit() | 1,000 | 1324.37 | 1.3244 | 755 |
| query.count() | 10,000 | 8562.88 | 0.8563 | 1,168 |
| query.sum() | 10,000 | 8727.72 | 0.8728 | 1,146 |
| find (predicate) | 1,000 | 855.51 | 0.8555 | 1,169 |
| find (object) | 1,000 | 848.12 | 0.8481 | 1,179 |
| paginate | 1,000 | 863.80 | 0.8638 | 1,158 |
| findByIndex | 10,000 | 19.53 | 0.0020 | 511,932 |
| batch (10 ops) | 1,000 | 5.34 | 0.0053 | 187,280 |

### WAL Mode (Durable)

WAL mode provides crash safety by appending operations to a write-ahead log before applying.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
| set (simple) | 10,000 | 40.89 | 0.0041 | 244,557 |
| set (nested) | 10,000 | 41.39 | 0.0041 | 241,600 |
| get (existing) | 10,000 | 10.07 | 0.0010 | 992,655 |
| get (default) | 10,000 | 4.83 | 0.0005 | 2,069,142 |
| has (existing) | 10,000 | 5.46 | 0.0005 | 1,830,801 |
| has (missing) | 10,000 | 5.78 | 0.0006 | 1,729,562 |
| delete | 10,000 | 31.10 | 0.0031 | 321,576 |
| add | 10,000 | 29.90 | 0.0030 | 334,420 |
| subtract | 10,000 | 29.39 | 0.0029 | 340,233 |
| push | 1,000 | 309.89 | 0.3099 | 3,227 |
| pull | 100 | 46.25 | 0.4625 | 2,162 |
| query.where().exec() | 1,000 | 942.68 | 0.9427 | 1,061 |
| query.sort().limit() | 1,000 | 1355.94 | 1.3559 | 737 |
| query.count() | 10,000 | 8697.43 | 0.8697 | 1,150 |
| query.sum() | 10,000 | 8947.00 | 0.8947 | 1,118 |
| find (predicate) | 1,000 | 893.43 | 0.8934 | 1,119 |
| find (object) | 1,000 | 875.83 | 0.8758 | 1,142 |
| paginate | 1,000 | 873.13 | 0.8731 | 1,145 |
| findByIndex | 10,000 | 22.16 | 0.0022 | 451,284 |
| batch (10 ops) | 1,000 | 6.59 | 0.0066 | 151,731 |

## Interpretation

### Key Takeaways

1. **Read Operations** (`get`, `has`) are extremely fast in both modes since they only access in-memory data.
2. **Write Operations** (`set`, `delete`) are faster in In-Memory mode but still performant in WAL mode.
3. **Index Lookups** (`findByIndex`) provide O(1) performance regardless of dataset size.
4. **Query Operations** scale with dataset size but remain efficient for moderate collections.
5. **Batch Operations** are highly efficient for bulk writes.

### When to Use Each Mode

| Use Case | Recommended Mode |
| -------- | ---------------- |
| Caching / Sessions | In-Memory |
| Critical Data | WAL |
| High Write Volume | In-Memory |
| Financial / Audit | WAL |

---

*Benchmarks run using [jsondb-high](https://github.com/sethunthunder111/jsondb-high)*
