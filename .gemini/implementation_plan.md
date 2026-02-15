# 🚀 jsondb-high v5.5 "Enterprise" — Implementation Plan

> **Goal**: Transform jsondb-high from a great embedded DB into an enterprise-contender by addressing every weakness identified in benchmarks and feature analysis.

## Current Architecture Problems Identified

### 1. Query Engine is in JavaScript (CRITICAL BOTTLENECK)
The `QueryBuilder` class (`index.ts:449-830`) runs **entirely in JavaScript**. Even when `parallelQuery()` exists in Rust, the `exec()` method in `QueryBuilder` still:
- Materializes all data into JS via `ensureData()` → calls `getSync()` → crosses N-API boundary
- Runs `applyFilters()` in JS with closure-based filter functions
- Runs `applyPostProcessing()` (sort, skip, limit, select) all in JS
- Every `getFieldValue()` call does string splitting and object traversal in JS

**This is why queries are 1,000-5,000 ops/s instead of 100K+.**

### 2. Push/Pull Do Full Array Scans
- `push()` calls `arr.contains(&value)` for dedup — O(N) scan per push
- `pull()` in TypeScript does `arr.filter(deepEqual)` — O(N*M) where M = items to pull

### 3. Cold Storage is Manual Only
- `offload()` and `restore()` exist but must be called manually
- No automatic memory pressure detection
- No LRU-based eviction

### 4. Write Lock Contention
- All writes go through `self.data.write()` — a single `RwLock`
- No sharding, no concurrent write paths
- Batch operations hold the write lock for the entire batch duration

### 5. Single-Node, No Network
- No replication protocol
- No way for multiple services to access the same DB

---

## Implementation Phases

---

## Phase 1: Move Query Engine to Rust (HIGHEST IMPACT 🔥)

**Files**: `src/lib.rs`, `index.ts`

### What to do:
Create a new `src/query_engine.rs` module that handles the entire query pipeline natively:

```rust
// New Rust-native query engine
pub struct NativeQuery {
    path: String,
    filters: Vec<PreparedFilter>,
    sort: Option<Vec<(String, i8)>>,    // field, direction
    limit: Option<usize>,
    skip: Option<usize>,
    select: Option<Vec<String>>,
}

#[napi]
impl NativeDB {
    /// Full query execution in Rust — filter, sort, skip, limit, select
    #[napi]
    pub fn execute_query(
        &self,
        path: String,
        filters: Vec<QueryFilter>,
        sort_json: Option<String>,      // JSON string of sort options
        limit: Option<u32>,
        skip: Option<u32>,
        select_fields: Option<Vec<String>>,
    ) -> Result<Value> { ... }
    
    /// Native aggregation — count, sum, avg, min, max with filters
    #[napi]
    pub fn execute_aggregate(
        &self,
        path: String,
        filters: Vec<QueryFilter>,
        operation: String,
        field: Option<String>,
    ) -> Result<Value> { ... }
}
```

### Key optimizations in Rust:
1. **Parallel filter** using Rayon `par_iter()` — already exists but underused
2. **Parallel sort** using Rayon `par_sort_unstable_by()` — O(N log N / P) where P = cores
3. **Early termination** — if no sort, apply limit during filter phase in a parallel scan
4. **Zero-copy field access** — avoid cloning Values until final result assembly
5. **SIMD-friendly numeric comparisons** — Rust compiler auto-vectorizes tight loops
6. **Index-aware query planning** — check BTreeIndex for range queries in Rust, not JS

### TypeScript changes:
- `QueryBuilder.exec()` collects all parameters and calls `native.executeQuery()` in one shot
- Remove `applyFilters()`, `applyPostProcessing()` from JS — they become Rust
- Keep `filter(fn)` for custom JS predicates as a fallback, but native filters get priority

**Expected impact**: Queries go from ~1K ops/s → 50K-200K ops/s

---

## Phase 2: Optimize Array Operations (push/pull)

**Files**: `src/lib.rs`

### Push optimization:
Replace `arr.contains(&value)` (O(N) linear scan) with:

```rust
// In Rust:
pub fn push_dedup(&self, path: String, value: Value) -> Result<bool> {
    let mut data = self.data.write();
    let ptr = Self::normalize_path(&path);
    if let Some(Value::Array(arr)) = data.pointer_mut(&ptr) {
        // Use HashSet for O(1) dedup check on simple types
        // For complex types, use a temporary HashSet<String> of JSON serializations
        let key = serde_json::to_string(&value).unwrap_or_default();
        // Build hash on first push, cache it
        if !arr.iter().any(|v| serde_json::to_string(v).unwrap_or_default() == key) {
            arr.push(value);
            return Ok(true);
        }
        Ok(false)
    } else {
        Err(...)
    }
}
```

Better approach — add a **Set index** for arrays that need dedup:

```rust
// Maintain a HashSet<String> alongside arrays marked for dedup
// O(1) contains check instead of O(N)
```

### Pull optimization:
Move pull entirely to Rust with parallel removal:

```rust
#[napi]
pub fn pull(&self, path: String, items: Vec<Value>) -> Result<u32> {
    let mut data = self.data.write();
    // Build HashSet of items to remove
    let remove_set: HashSet<String> = items.iter()
        .map(|v| serde_json::to_string(v).unwrap_or_default())
        .collect();
    
    // Filter in place - O(N) single pass
    if let Some(Value::Array(arr)) = data.pointer_mut(&ptr) {
        let before = arr.len();
        arr.retain(|v| {
            !remove_set.contains(&serde_json::to_string(v).unwrap_or_default())
        });
        Ok((before - arr.len()) as u32)
    }
}
```

**Expected impact**: push ~4K → 100K+ ops/s, pull ~2.5K → 50K+ ops/s

---

## Phase 3: Smart File Offloading (Auto Memory Management)

**Files**: `src/lib.rs` (new), `src/cold_storage.rs` (new), `index.ts`

### Architecture:

```
┌──────────────────────────────────────────┐
│                HOT TIER                   │
│  (In-Memory serde_json::Value)           │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐    │
│  │ users   │ │ orders  │ │ config  │    │
│  └─────────┘ └─────────┘ └─────────┘    │
│                                          │
│  Memory Monitor (checks every 5s)        │
│  Threshold: 80% of configured limit      │
└──────────────┬───────────────────────────┘
               │ evict coldest
               ▼
┌──────────────────────────────────────────┐
│               COLD TIER                   │
│  (Memory-mapped files on disk)            │
│  LRU tracking per top-level collection    │
│  Auto-restore on access (transparent)     │
│  ┌─────────────┐  ┌─────────────┐        │
│  │ logs.cold.1  │  │ archive.cold│        │
│  └─────────────┘  └─────────────┘        │
└──────────────────────────────────────────┘
```

### Implementation:

```rust
// In NativeDB struct, add:
struct MemoryManager {
    max_memory_bytes: usize,          // Configurable limit
    access_tracker: HashMap<String, u64>,  // path -> last_access_timestamp
    cold_paths: HashSet<String>,       // Currently offloaded paths
    check_interval_ms: u64,
}

impl NativeDB {
    /// Called internally before every get() — track access
    fn track_access(&self, path: &str) {
        // Update LRU timestamp for the top-level key
        let top_key = path.split('.').next().unwrap_or(path);
        // ... update access_tracker
    }
    
    /// Background check — estimate memory usage, evict if needed
    fn check_memory_pressure(&self) -> Result<()> {
        let data = self.data.read();
        let estimated_size = estimate_value_size(&data);
        
        if estimated_size > self.memory_manager.max_memory_bytes * 80 / 100 {
            // Find LRU top-level keys
            let mut entries: Vec<_> = self.memory_manager.access_tracker.iter().collect();
            entries.sort_by_key(|(_, ts)| *ts);
            
            // Offload coldest entries until under threshold
            for (path, _) in entries {
                if estimated_size <= self.memory_manager.max_memory_bytes * 60 / 100 {
                    break;
                }
                self.offload(path.clone())?;
                // Recalculate
            }
        }
        Ok(())
    }
}
```

### Transparent restore:
Modify `get()` to auto-restore cold data:

```rust
pub fn get(&self, path: String) -> Result<Value> {
    self.track_access(&path);
    let data = self.data.read();
    let ptr = Self::normalize_path(&path);
    match data.pointer(&ptr) {
        Some(v) if is_cold_marker(v) => {
            drop(data); // Release read lock
            self.restore(path.clone())?;
            // Re-read
            let data = self.data.read();
            Ok(data.pointer(&ptr).cloned().unwrap_or(Value::Null))
        }
        Some(v) => Ok(v.clone()),
        None => Ok(Value::Null),
    }
}
```

### Config:
```typescript
const db = new JSONDatabase('db.json', {
    memoryLimit: '512mb',       // Auto-offload when approaching this
    coldStorageDir: './cold',   // Where to store offloaded data
    evictionPolicy: 'lru',     // 'lru' | 'lfu' | 'none'
});
```

---

## Phase 4: Concurrent Write Scaling (Sharded Writes)

**Files**: `src/lib.rs`

### Current Problem:
```rust
// Every write does this:
let mut data = self.data.write();  // GLOBAL write lock
```

### Solution: Collection-Level Locking

```rust
pub struct NativeDB {
    // Replace single data lock with sharded approach
    data: Arc<PLRwLock<Value>>,           // Root object (metadata only)
    shards: Arc<PLRwLock<HashMap<String, Arc<PLRwLock<Value>>>>>,  // Top-level key locks
}

impl NativeDB {
    pub fn set(&self, path: String, value: Value) -> Result<()> {
        let top_key = path.split('.').next().unwrap_or(&path).to_string();
        let sub_path = if path.contains('.') {
            &path[top_key.len() + 1..]
        } else {
            ""
        };
        
        // Get or create shard lock for this top-level key
        let shard = {
            let shards = self.shards.read();
            if let Some(s) = shards.get(&top_key) {
                s.clone()
            } else {
                drop(shards);
                let mut shards = self.shards.write();
                let s = Arc::new(PLRwLock::new(Value::Object(serde_json::Map::new())));
                shards.insert(top_key.clone(), s.clone());
                s
            }
        };
        
        // Only lock the specific shard — other collections can write concurrently
        let mut shard_data = shard.write();
        Self::set_value_at_path(&mut shard_data, sub_path, value)?;
        Ok(())
    }
}
```

This allows `db.set('users.1', ...)` and `db.set('orders.5', ...)` to run **concurrently** without blocking each other.

**Expected impact**: Write throughput scales ~linearly with number of distinct top-level collections being written to concurrently.

---

## Phase 5: Network Protocol Design (DESIGN ONLY — No Implementation)

### Recommendation: Unix Domain Socket + MessagePack

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Service A   │    │  Service B   │    │  Service C   │
│  (Node.js)   │    │  (Python)    │    │  (Go)        │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       │    Unix Socket / TCP                  │
       │    + MessagePack binary protocol      │
       ▼                   ▼                   ▼
┌──────────────────────────────────────────────────────┐
│                  jsondb-high Server                    │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐      │
│  │ Connection │  │ Connection │  │ Connection │      │
│  │ Handler    │  │ Handler    │  │ Handler    │      │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘      │
│        │               │               │              │
│        ▼               ▼               ▼              │
│  ┌─────────────────────────────────────────┐          │
│  │        Request Router / Dispatcher       │          │
│  │  (Parses commands, routes to engine)     │          │
│  └─────────────────────┬───────────────────┘          │
│                        │                              │
│                        ▼                              │
│  ┌─────────────────────────────────────────┐          │
│  │          NativeDB (Rust Core)            │          │
│  │  (Same engine, just network-accessible)  │          │
│  └─────────────────────────────────────────┘          │
└──────────────────────────────────────────────────────┘
```

### Protocol (Binary MessagePack):
```
Request:  [request_id: u64, command: string, args: map]
Response: [request_id: u64, status: u8, data: any]

Commands:
  GET     { path: "users.1" }
  SET     { path: "users.1", value: {...} }
  QUERY   { path: "users", filters: [...], sort: {...}, limit: 10 }
  BATCH   { ops: [...] }
  SUB     { pattern: "users.*" }  → streaming responses
```

### Why this design:
- **MessagePack** is 2-5x faster than JSON for serialization
- **Unix sockets** have ~2μs latency (vs ~50μs for TCP localhost)
- **Request IDs** allow pipelining (multiple in-flight requests)
- **SUB** command enables Pub/Sub over the wire
- Can add **TCP** endpoint for remote access later
- Can add **TLS** for security

### Implementation path (future):
1. Create `src/server.rs` using `tokio` (already in deps)
2. Add `jsondb-high serve` CLI command
3. Create client SDKs: `jsondb-high-client` (Node), Python, Go

---

## Phase 6: Benchmark Overhaul

**Files**: `benchmarks/run.ts`, `benchmarks/RESULTS.md`

### Current problems:
- Only 10K iterations, 1K-record datasets
- No concurrent access testing
- No large dataset tests
- Missing: cold storage, parallel query vs regular, network latency (future)

### New benchmark suite:

```typescript
// Scale tiers
const SCALES = {
    small:  { records: 1_000,    iterations: 10_000 },
    medium: { records: 100_000,  iterations: 5_000  },
    large:  { records: 1_000_000, iterations: 1_000  },
};

// New benchmark categories:
// 1. Core CRUD at all scales
// 2. Query engine (native vs JS fallback) at all scales
// 3. Concurrent write throughput (multi-promise / worker_threads)
// 4. Cold storage offload/restore cycle times
// 5. Memory footprint tracking
// 6. Index performance at scale
// 7. Array operations at scale (10K element arrays)
// 8. Comparison: regular query vs parallelQuery vs execute_query (new)
```

---

## Phase 7: New Features to Close Gaps

### 7a. Compound Indexes
```typescript
const db = new JSONDatabase('db.json', {
    indices: [
        { name: 'user_email', path: 'users', field: 'email' },
        // NEW: compound index
        { name: 'user_age_role', path: 'users', fields: ['age', 'role'], type: 'compound' },
    ]
});
```

### 7b. Cursor/Streaming Results
```typescript
// For large result sets, avoid materializing everything
const cursor = db.queryCursor('users').where('active').eq(true);
for await (const batch of cursor.batchIterator(100)) {
    // Process 100 items at a time
    process(batch);
}
```

### 7c. Full-Text Search (basic)
```typescript
// Built on top of existing index infrastructure
const db = new JSONDatabase('db.json', {
    textIndices: [
        { name: 'search', path: 'articles', fields: ['title', 'body'] }
    ]
});

const results = await db.textSearch('articles', 'rust performance');
```

---

## Execution Order

| # | Phase | Impact | Effort | Priority | Status |
|---|-------|--------|--------|----------|--------|
| 1 | Query Engine → Rust | 🔥🔥🔥🔥🔥 | High | DO FIRST | ✅ **DONE** |
| 2 | Array Ops (push/pull) | 🔥🔥🔥 | Low | Quick Win | ✅ **DONE** |
| 3 | Smart Offloading | 🔥🔥🔥 | Medium | Core Feature | ✅ **DONE** |
| 4 | Concurrent Writes | 🔥🔥🔥🔥 | Medium | Scaling | 🔲 Next |
| 5 | Network Protocol | 🔥🔥 | Design Only | Future | 📋 Designed |
| 6 | Benchmark Overhaul | 🔥🔥🔥 | Medium | Validates All | 🔲 Pending |
| 7 | New Features | 🔥🔥 | High | Feature Richness | 🔲 Pending |

---

## Completed Work

### Phase 1: Native Query Engine ✅
- Created `src/query_engine.rs` (556 lines)
- Pre-compiled filters, enum-based ops, Rayon parallel processing
- Integrated via `execute_query()` and `execute_aggregate()` N-API methods
- Updated `QueryBuilder.exec()` with 3-strategy approach (Index → Native → JS fallback)
- Native aggregation fast paths for count/sum/avg/min/max

### Phase 2: Optimized Array Operations ✅
- `push_batch()`: single lock, HashSet dedup
- `pull_items()`: single-pass O(N) removal
- Updated TypeScript push/pull with automatic fallback

### Phase 3: Smart Memory Management ✅
- Created `src/memory_manager.rs` (259 lines)
- LRU-based eviction, configurable via `DBOptions.memoryLimit`
- `checkMemoryPressure()`, `memoryStats()`, `configureMemory()` N-API methods

---

## Files Created/Modified

| File | Action | Description |
|------|--------|-------------|
| `src/query_engine.rs` | ✅ CREATED | Full native query pipeline (556 lines) |
| `src/memory_manager.rs` | ✅ CREATED | Smart memory management (259 lines) |
| `src/lib.rs` | ✅ MODIFIED | Integrated modules, 10 new N-API methods |
| `index.ts` | ✅ MODIFIED | Native query delegation, memory config, optimized push/pull |
| `index.d.ts` | 🔲 TODO | Update type definitions |
| `benchmarks/run.ts` | 🔲 TODO | Scale testing |
| `README.md` | 🔲 TODO | Document new features |

## Next Steps

1. **Phase 4: Concurrent Writes** — Collection-level sharded locks
2. **Phase 6: Benchmark Overhaul** — Validate all improvements
3. **Phase 7: New Features** — Compound indexes, cursors, full-text search
4. **Type definitions** — Update `index.d.ts`
5. **Documentation** — Update README

