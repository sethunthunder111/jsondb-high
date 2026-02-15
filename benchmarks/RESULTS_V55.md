# 📊 jsondb-high v5.5 Enterprise Benchmark Results

> Generated: 2026-02-15T18:00:04.997Z  
> Node.js: v24.3.0  
> Platform: linux x64

## Performance Summary

### 🔥 Native Query Engine

The v5.5 native query engine executes the entire query pipeline (filter → sort → skip → limit → select) in a single Rust call, eliminating JS↔Rust boundary crossings.

| Test | Dataset | Ops/s | Avg Latency | Engine |
|------|---------|-------|-------------|--------|
| Native: where(age > 30) | 1,000 | 971 | 1.0302ms | 🟢 Rust |
| JS Fallback: filter(fn) | 1,000 | 519 | 1.9254ms | 🔴 JS |
| Native: multi-filter+sort+limit | 1,000 | 6,625 | 0.1509ms | 🟢 Rust |
| Native: filter+select(3 fields) | 1,000 | 10,594 | 0.0944ms | 🟢 Rust |
| Native: count(age > 30) | 1,000 | 516 | 1.9382ms | 🟢 Rust |
| Native: avg(salary) | 1,000 | 486 | 2.0560ms | 🟢 Rust |
| Native: where(age > 30) | 10,000 | 104 | 9.6163ms | 🟢 Rust |
| JS Fallback: filter(fn) | 10,000 | 39 | 25.3457ms | 🔴 JS |
| Native: multi-filter+sort+limit | 10,000 | 1,116 | 0.8962ms | 🟢 Rust |
| Native: filter+select(3 fields) | 10,000 | 3,656 | 0.2735ms | 🟢 Rust |
| Native: count(age > 30) | 10,000 | 40 | 24.9334ms | 🟢 Rust |
| Native: avg(salary) | 10,000 | 38 | 26.6264ms | 🟢 Rust |
| Native: where(age > 30) | 50,000 | 17 | 57.2014ms | 🟢 Rust |
| JS Fallback: filter(fn) | 50,000 | 5 | 182.3317ms | 🔴 JS |
| Native: multi-filter+sort+limit | 50,000 | 214 | 4.6825ms | 🟢 Rust |
| Native: filter+select(3 fields) | 50,000 | 1,426 | 0.7013ms | 🟢 Rust |
| Native: count(age > 30) | 50,000 | 5 | 187.1166ms | 🟢 Rust |
| Native: avg(salary) | 50,000 | 5 | 194.2858ms | 🟢 Rust |

### 📚 Array Operations

Optimized push/pull with HashSet-based dedup and single-pass removal.

| Test | Array Size | Ops/s | Avg Latency | Notes |
|------|-----------|-------|-------------|-------|
| push (single item) | 100 | 3,202 | 0.3123ms |  |
| push (batch 10 items) | 100 | 617 | 1.6210ms | Uses native pushBatch() |
| pull (single item) | 100 | 15,084 | 0.0663ms | Uses native pullItems() |
| push (single item) | 1,000 | 1,014 | 0.9861ms |  |
| push (batch 10 items) | 1,000 | 429 | 2.3317ms | Uses native pushBatch() |
| pull (single item) | 1,000 | 1,398 | 0.7153ms | Uses native pullItems() |
| push (single item) | 5,000 | 251 | 3.9809ms |  |
| push (batch 10 items) | 5,000 | 177 | 5.6559ms | Uses native pushBatch() |
| pull (single item) | 5,000 | 256 | 3.9054ms | Uses native pullItems() |

### 🧵 Concurrent Write Scaling

Striped lock manager allows concurrent writes to different top-level collections.

| Test | Ops/s | Avg Latency | Notes |
|------|-------|-------------|-------|
| Sequential writes (same collection) | 427,333 | 0.0023ms | All writes to "single_coll" |
| Sequential writes (different collections) | 407,636 | 0.0025ms | Writes spread across 8 collections |
| Concurrent 10 writes (same collection) | 42,467 | 0.0235ms | Promise.all with 10 ops to same collection |
| Concurrent 10 writes (different collections) | 44,174 | 0.0226ms | Promise.all with 10 ops across different collections |
| Batch mixed (100 ops, 10 collections) | 10,968 | 0.0912ms | 100 set ops across 10 collections per batch |

### 💾 Memory Management

Smart offloading with LRU-based eviction and transparent restore.

| Test | Dataset | Ops/s | Avg Latency | Notes |
|------|---------|-------|-------------|-------|
| offload (1K items) | 1,000 | 1,781 | 0.5616ms | Offload + restore cycle |
| restore (1K items) | 1,000 | 1,794 | 0.5575ms | Cold → hot restore |
| memoryStats() | 5,000 | 12,428 | 0.0805ms | Read memory utilization |

### ⚡ Core CRUD at Scale

| Test | Dataset | Ops/s | Avg Latency |
|------|---------|-------|-------------|
| get (random key) | 1,000 | 417,708 | 0.0024ms |
| set (update existing) | 1,000 | 510,301 | 0.0020ms |
| has (random key) | 1,000 | 1,500,286 | 0.0007ms |
| get (random key) | 10,000 | 363,845 | 0.0027ms |
| set (update existing) | 10,000 | 455,880 | 0.0022ms |
| has (random key) | 10,000 | 1,258,749 | 0.0008ms |


## Architecture Improvements in v5.5

### Native Query Engine
- Full query pipeline in Rust: filter → sort → skip → limit → select
- Rayon parallel processing for datasets > 100 items
- Pre-compiled filter enums (no string matching in hot loop)
- Regex cache (LRU 256) for compiled patterns
- 18 filter operations supported natively

### Striped Write Locks
- 64-stripe lock manager for collection-level concurrency
- Writes to different top-level collections proceed concurrently
- Deadlock-free batch locking via sorted stripe acquisition

### Optimized Array Operations
- `pushBatch`: Single lock + HashSet dedup for O(1) contains
- `pullItems`: Single-pass O(N) removal with HashSet lookup

### Smart Memory Management
- LRU-based eviction when memory limit approached
- Transparent cold storage restore on access
- Per-key access tracking and size estimation

---

*Benchmarks run using [jsondb-high](https://github.com/sethunthunder111/jsondb-high) v5.5*
