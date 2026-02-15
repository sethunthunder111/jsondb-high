# 📊 jsondb-high Benchmark Results

> Generated: 2026-02-15T16:22:51.968Z  
> Iterations per test: 10,000  
> Warmup iterations: 1,000

## System Information

- **Platform**: linux
- **Architecture**: x64
- **Node Version**: v24.3.0

## Summary

| Operation | In-Memory (ops/s) | WAL Mode (ops/s) | Avg Latency (ms) |
| --------- | ----------------- | ---------------- | ---------------- |
| set (simple) | 545,512 | 525,713 | 0.0018 |
| set (nested) | 484,991 | 408,078 | 0.0021 |
| get (existing) | 1,181,674 | 1,298,581 | 0.0008 |
| get (default) | 2,094,591 | 2,471,058 | 0.0005 |
| has (existing) | 2,301,343 | 2,380,147 | 0.0004 |
| has (missing) | 2,643,143 | 2,580,611 | 0.0004 |
| delete | 621,390 | 560,561 | 0.0016 |
| add | 614,138 | 571,588 | 0.0016 |
| subtract | 599,335 | 538,039 | 0.0017 |
| push | 3,910 | 3,737 | 0.2557 |
| pull | 2,493 | 1,554 | 0.4011 |
| query.where().exec() | 4,690 | 4,186 | 0.2132 |
| query.sort().limit() | 903 | 961 | 1.1074 |
| query.count() | 1,245 | 1,374 | 0.8032 |
| query.sum() | 1,228 | 1,352 | 0.8143 |
| find (predicate) | 1,249 | 1,377 | 0.8006 |
| find (object) | 1,242 | 1,366 | 0.8051 |
| paginate | 1,247 | 1,375 | 0.8020 |
| findByIndex | 696,810 | 639,626 | 0.0014 |
| batch (10 ops) | 264,029 | 178,946 | 0.0038 |

## Detailed Results

### In-Memory Mode

In-memory mode prioritizes speed. Data is kept in RAM and flushed to disk periodically.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
| set (simple) | 10,000 | 18.33 | 0.0018 | 545,512 |
| set (nested) | 10,000 | 20.62 | 0.0021 | 484,991 |
| get (existing) | 10,000 | 8.46 | 0.0008 | 1,181,674 |
| get (default) | 10,000 | 4.77 | 0.0005 | 2,094,591 |
| has (existing) | 10,000 | 4.35 | 0.0004 | 2,301,343 |
| has (missing) | 10,000 | 3.78 | 0.0004 | 2,643,143 |
| delete | 10,000 | 16.09 | 0.0016 | 621,390 |
| add | 10,000 | 16.28 | 0.0016 | 614,138 |
| subtract | 10,000 | 16.69 | 0.0017 | 599,335 |
| push | 1,000 | 255.74 | 0.2557 | 3,910 |
| pull | 100 | 40.11 | 0.4011 | 2,493 |
| query.where().exec() | 1,000 | 213.21 | 0.2132 | 4,690 |
| query.sort().limit() | 1,000 | 1107.37 | 1.1074 | 903 |
| query.count() | 10,000 | 8032.03 | 0.8032 | 1,245 |
| query.sum() | 10,000 | 8143.44 | 0.8143 | 1,228 |
| find (predicate) | 1,000 | 800.58 | 0.8006 | 1,249 |
| find (object) | 1,000 | 805.14 | 0.8051 | 1,242 |
| paginate | 1,000 | 802.03 | 0.8020 | 1,247 |
| findByIndex | 10,000 | 14.35 | 0.0014 | 696,810 |
| batch (10 ops) | 1,000 | 3.79 | 0.0038 | 264,029 |

### WAL Mode (Durable)

WAL mode provides crash safety by appending operations to a write-ahead log before applying.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
| set (simple) | 10,000 | 19.02 | 0.0019 | 525,713 |
| set (nested) | 10,000 | 24.51 | 0.0025 | 408,078 |
| get (existing) | 10,000 | 7.70 | 0.0008 | 1,298,581 |
| get (default) | 10,000 | 4.05 | 0.0004 | 2,471,058 |
| has (existing) | 10,000 | 4.20 | 0.0004 | 2,380,147 |
| has (missing) | 10,000 | 3.88 | 0.0004 | 2,580,611 |
| delete | 10,000 | 17.84 | 0.0018 | 560,561 |
| add | 10,000 | 17.50 | 0.0017 | 571,588 |
| subtract | 10,000 | 18.59 | 0.0019 | 538,039 |
| push | 1,000 | 267.63 | 0.2676 | 3,737 |
| pull | 100 | 64.34 | 0.6434 | 1,554 |
| query.where().exec() | 1,000 | 238.92 | 0.2389 | 4,186 |
| query.sort().limit() | 1,000 | 1040.17 | 1.0402 | 961 |
| query.count() | 10,000 | 7280.65 | 0.7281 | 1,374 |
| query.sum() | 10,000 | 7396.04 | 0.7396 | 1,352 |
| find (predicate) | 1,000 | 726.23 | 0.7262 | 1,377 |
| find (object) | 1,000 | 731.85 | 0.7319 | 1,366 |
| paginate | 1,000 | 727.12 | 0.7271 | 1,375 |
| findByIndex | 10,000 | 15.63 | 0.0016 | 639,626 |
| batch (10 ops) | 1,000 | 5.59 | 0.0056 | 178,946 |

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
