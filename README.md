# jsondb-high 🚀

A blazing fast, feature-rich JSON database for Node.js with a Rust-powered native query engine via N-API.

## ✨ Features

- ⚡ **Blazing Fast**: Native Rust query engine via N-API (~2.5M ops/s reads, ~510k ops/s writes)
- 🔎 **Native Query Engine** *(v5.5)*: Full query pipeline in Rust — filter → sort → skip → limit → select in one call
- 🧠 **Smart Memory Management** *(v5.5)*: LRU-based cold storage eviction with transparent restore on access
- 🔐 **Striped Write Locks** *(v5.5)*: 64-stripe collection-level concurrency — writes to different collections proceed in parallel
- 🔒 **Multi-Process Safe**: OS-level advisory file locking prevents data corruption across multiple processes
- 🧵 **Multi-Core Processing**: Adaptive parallelism using Rayon — automatically scales with your CPU
- 🛡️ **Atomic Operations**: Group Commit Write-Ahead Logging (WAL) ensures ACID durability with near-zero overhead
- 🔍 **O(1) Indexing**: In-memory Map indices for instant, constant-time lookups
- 📝 **Schema Validation**: JSON Schema-like validation for data integrity (v5.1+)
- 🔒 **Encryption**: AES-256-GCM encryption for data at rest
- 📦 **Zero Dependencies**: Self-contained native binary; no external database servers required
- 🔄 **Middleware**: Support for before and after hooks on all operations
- ⏱️ **TTL Support**: Auto-expire keys after a specified time (like Redis)
- 📡 **Pub/Sub**: EventEmitter-style subscriptions to data changes
- 📊 **Aggregations**: Built-in sum, avg, min, max, groupBy, distinct
- 🔗 **Parallel Joins**: High-performance left outer join (lookup) operations

## 📦 Installation

```bash
bun add jsondb-high
# or
npm install jsondb-high
```

> **Note**: This package builds its native core from source during installation. You must have [Rust and Cargo](https://rustup.rs/) installed on your system.

## 🛠️ Requirements

- **Node.js**: >= 16.0.0
- **Rust Toolchain**: [Installed and in PATH](https://rustup.rs/) (Required for initial build)
- **C++ Build Tools**: Required by Cargo on some platforms (e.g., Visual Studio Build Tools on Windows)

## 🚀 Quick Start

```typescript
import JSONDatabase from 'jsondb-high';

// Initialize (Auto-creates file if missing)
const db = new JSONDatabase('db.json');

// Write
await db.set('user.1', { name: 'Alice', role: 'admin' });

// Read
const user = await db.get('user.1');
console.log(user); // { name: 'Alice', role: 'admin' }
```

## 🏗️ Hybrid Architecture (v4.5+)

jsondb-high offers multiple storage and safety modes. Choose based on your performance and durability needs.

### 🔒 Safety & Locking
Prevent corruption from multiple processes using the same file.

```typescript
const db = new JSONDatabase('db.json', {
    lockMode: 'exclusive' // 'exclusive' | 'shared' | 'none'
});
```

### 💾 Durability Modes
Configure the Write-Ahead Log (WAL) to balance speed and safety.

```typescript
const db = new JSONDatabase('db.json', {
    durability: 'batched',   // 'none' | 'lazy' | 'batched' | 'sync'
    walFlushMs: 10,          // Sync every 10ms (Group Commit)
    lockMode: 'exclusive'
});
```

| Mode | Throughput | Latency | Durability Window |
|------|------------|---------|-------------------|
| `none` | ~545k ops/s | 0.002ms | Manual save only |
| `lazy` | ~400k ops/s | 0.003ms | 100ms |
| `batched`| ~525k ops/s | 0.002ms | 10ms (Recommended) |
| `sync` | ~2k ops/s | 0.5ms | Immediate |

## 📝 Schema Validation (v5.1+)

Define schemas to enforce data structure and validation rules at specific paths.

```typescript
const db = new JSONDatabase('db.json', {
    schemas: {
        'users': {
            type: 'object',
            properties: {
                id: { type: 'number' },
                email: { 
                    type: 'string', 
                    pattern: '^[\w.-]+@[\w.-]+\.\w+$' 
                },
                age: { 
                    type: 'number', 
                    minimum: 0, 
                    maximum: 150 
                },
                role: { 
                    type: 'string', 
                    enum: ['admin', 'user', 'guest'] 
                },
                tags: {
                    type: 'array',
                    items: { type: 'string' },
                    uniqueItems: true
                }
            },
            required: ['id', 'email']
        }
    }
});

// This will throw validation error (missing required field)
await db.set('users.1', { id: 1 }); // ❌ Error: Missing required property: email

// This will throw validation error (invalid email pattern)
await db.set('users.1', { 
    id: 1, 
    email: 'invalid-email' 
}); // ❌ Error: String does not match pattern

// Valid data
await db.set('users.1', { 
    id: 1, 
    email: 'alice@example.com',
    age: 25,
    role: 'admin',
    tags: ['premium', 'beta']
}); // ✅ Success
```

### Schema Types & Constraints

| Type | Constraints |
|------|-------------|
| `string` | `minLength`, `maxLength`, `pattern` (regex) |
| `number` | `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum` |
| `array` | `minItems`, `maxItems`, `uniqueItems`, `items` (item schema) |
| `object` | `properties`, `required` |
| All types | `enum` (allowed values) |

## 📖 API Reference

### Basic Operations

#### `set(path, value)`

Writes data. Creates nested paths automatically.

```typescript
await db.set('config.theme', 'dark');
await db.set('users.1.settings.notifications', true);
```

#### `get(path, defaultValue?)`

Retrieves data. Returns `defaultValue` if path doesn't exist.

```typescript
const val = await db.get('config.theme', 'light');
```

#### `has(path)`

Checks existence.

```typescript
if (await db.has('users.1')) {
    // User exists
}
```

#### `delete(path)`

Removes a key or object property.

```typescript
await db.delete('users.1.settings'); // Delete nested property
await db.delete('users.1');          // Delete entire object
```

### Arrays

#### `push(path, ...items)`

Adds items to an array. Dedupes automatically.

```typescript
await db.push('users.1.tags', 'premium', 'beta');
```

#### `pull(path, ...items)`

Removes items from an array (deep equality).

```typescript
await db.pull('users.1.tags', 'beta');
```

### Math Operations (Atomic)

#### `add(path, amount)`

Atomic increment. Returns new value.

```typescript
const newCount = await db.add('users.1.loginCount', 1);
```

#### `subtract(path, amount)`

Atomic decrement. Returns new value.

```typescript
const newCredits = await db.subtract('users.1.credits', 50);
```

### 🔍 Indices (O(1) Lookups)

Define indices in the constructor for O(1) read performance.

```typescript
const db = new JSONDatabase('db.json', {
    indices: [{ name: 'email', path: 'users', field: 'email' }]
});

// Instant Lookup
const user = await db.findByIndex('email', 'alice@corp.com');
```

### 🔎 Advanced Query Cursor

Chainable query builder with aggregation support.

```typescript
const results = await db.query('users')
    .where('age').gt(18)
    .where('role').eq('admin')
    .limit(10)
    .skip(0)
    .sort({ age: -1 }) // Descending
    .select(['id', 'name', 'email'])
    .exec();
```

#### Where Clauses

```typescript
.where('field').eq(value)      // Equal
.where('field').ne(value)      // Not equal
.where('field').gt(value)      // Greater than
.where('field').gte(value)     // Greater or equal
.where('field').lt(value)      // Less than
.where('field').lte(value)     // Less or equal
.where('field').between(1, 10) // Between range
.where('field').in([1, 2, 3])  // In array
.where('field').notIn([1, 2])  // Not in array
.where('field').contains('x')  // String contains
.where('field').startsWith('x')// String starts with
.where('field').endsWith('x')  // String ends with
.where('field').matches(/^x/)  // Regex match
.where('field').exists()       // Field exists
.where('field').isNull()       // Is null
.where('field').isNotNull()    // Is not null
```

#### Aggregations

```typescript
const count = db.query('users').count();
const total = db.query('orders').sum('amount');
const average = db.query('orders').avg('amount');
const min = db.query('orders').min('amount');
const max = db.query('orders').max('amount');
const unique = db.query('users').distinct('role');
const grouped = db.query('users').groupBy('department');
```

### Find (Simple)

```typescript
// With function predicate
const user = await db.find('users', u => u.age > 18);

// With object matcher
const admin = await db.find('users', { role: 'admin' });

// Find all matching
const adults = await db.findAll('users', u => u.age >= 18);
```

### 📄 Paginate

Helper for API endpoints.

```typescript
const page = await db.paginate('users', 1, 20);
// Returns: { 
//   data: [...], 
//   meta: { total, pages, page, limit, hasNext, hasPrev } 
// }
```

### 📦 Batch Operations

Execute multiple writes in a single IO tick.

```typescript
await db.batch([
    { type: 'set', path: 'logs.1', value: 'log data' },
    { type: 'delete', path: 'temp.cache' },
    { type: 'add', path: 'stats.visits', value: 1 }
]);
```

### 🧵 Multi-Core Parallel Processing

The database automatically detects available CPU cores and uses parallel processing for large datasets (≥100 items). Falls back to efficient single-threaded operation for small workloads to avoid overhead.

#### System Info

Check system capabilities for parallel processing.

```typescript
const info = db.getSystemInfo();
console.log(info);
// {
//   availableCores: 8,
//   parallelEnabled: true,
//   recommendedBatchSize: 1000
// }
```

#### Parallel Batch Set

Execute thousands of set operations efficiently using all available cores.

```typescript
const operations = [];
for (let i = 0; i < 10000; i++) {
    operations.push({
        path: `users.${i}`,
        value: { id: i, name: `User ${i}`, active: true }
    });
}

const result = await db.batchSetParallel(operations);
console.log(`Completed ${result.count} operations`);
// Automatically parallelized when ≥100 items
```

#### Parallel Query

High-performance filtering using native Rust parallel iteration.

```typescript
// Filter with multiple conditions - uses parallel processing for large collections
const activeAdults = await db.parallelQuery('users', [
    { field: 'age', op: 'gte', value: 18 },
    { field: 'status', op: 'eq', value: 'active' }
]);

// Available operators: eq, ne, gt, gte, lt, lte, contains, startswith, endswith, in, notin, regex, containsAll, containsAny
```

#### Parallel Aggregation

Compute aggregations efficiently across large datasets.

```typescript
const count = await db.parallelAggregate('orders', 'count');
const totalRevenue = await db.parallelAggregate('orders', 'sum', 'amount');
const avgOrderValue = await db.parallelAggregate('orders', 'avg', 'amount');
const minOrder = await db.parallelAggregate('orders', 'min', 'amount');
const maxOrder = await db.parallelAggregate('orders', 'max', 'amount');
```

#### Parallel Joins (Lookups)

Perform high-performance left outer joins across collections.

```typescript
// Join users with their orders
const usersWithOrders = await db.parallelQuery('users', [])
    .then(users => {
        return db.parallelLookup(
            'users',      // left collection
            'orders',     // right collection  
            'id',         // left field (users.id)
            'userId',     // right field (orders.userId)
            'orders'      // output field name
        );
    });
// Result: Users with embedded 'orders' array containing matching orders
```

#### How It Works

- **Adaptive**: Automatically uses 1-N cores based on workload size and system resources
- **Efficient**: Small workloads (<100 items) use single-threaded to avoid parallel overhead
- **Resource-Aware**: Leaves 1 core free for system/main thread
- **Scalable**: Performance scales linearly with available cores for large datasets

## 🧠 Smart Memory Management (v5.5+)

Automatically offload least-recently-used data to disk when memory limits are approached. Data is transparently restored on access.

### Configuration

```typescript
const db = new JSONDatabase('db.json', {
    memoryLimit: '512mb',        // Max memory (e.g. '256mb', '1gb')
    coldStorageDir: './cold',    // Where offloaded data is stored
    evictionThresholdPct: 80,    // Start evicting at 80% usage
    evictionTargetPct: 60        // Evict until 60% usage
});
```

### Manual Offload & Restore

```typescript
// Offload a collection to disk to free memory
const id = await db.offload('largeCollection');

// Data is automatically restored on access:
const data = await db.get('largeCollection.key1'); // Transparent restore

// Or manually restore:
await db.restore('largeCollection');
```

### Memory Stats

```typescript
const stats = await db.memoryStats();
console.log(stats);
// {
//   totalEstimatedBytes: 52428800,
//   maxMemoryBytes: 536870912,
//   coldKeysCount: 3,
//   hotKeysCount: 12,
//   utilizationPct: 10
// }

// Manually trigger eviction check
const evicted = await db.checkMemoryPressure();
console.log('Evicted keys:', evicted);
```

### How It Works

- **LRU Eviction**: Least-recently-accessed collections are evicted first
- **Per-Key Size Estimation**: Tracks estimated memory per top-level key
- **Transparent Restore**: `get()` automatically restores cold data on access
- **Cold File Storage**: Evicted data is written as JSON to disk, deleted on restore

## 🔐 Striped Write Locks (v5.5+)

Writes to different top-level collections proceed concurrently using a 64-stripe lock manager.

```typescript
// These writes can run in parallel because they target different collections
await Promise.all([
    db.set('users.1', { name: 'Alice' }),
    db.set('orders.1', { total: 99 }),
    db.set('logs.1', { event: 'login' }),
]);
```

- **64 Lock Stripes**: Collection keys are hashed to stripes for concurrent access
- **Deadlock-Free Batch Locking**: Batch operations sort stripe indices before acquisition
- **Zero Contention on Reads**: Readers never block writers

### 🔒 Transactions

Atomic read-modify-write with automatic rollback on error.

```typescript
await db.transaction(async (data) => {
    if (data.bank.balance >= 100) {
        data.bank.balance -= 100;
        data.users['1'].wallet += 100;
    }
    return data;
});
```

### 📸 Snapshots

Create and restore backups.

```typescript
const backupPath = await db.createSnapshot('daily');
console.log('Backup saved to:', backupPath);

// Restore later
await db.restoreSnapshot(backupPath);
```

### 🔧 Middleware

Intercept operations before/after they happen.

```typescript
// Before hook - modify data before write
db.before('set', 'users.*', (ctx) => {
    ctx.value.updatedAt = Date.now();
    return ctx;
});

// After hook - react after write
db.after('set', 'users.*', (ctx) => {
    console.log('User updated:', ctx.path);
    return ctx;
});
```

### ⏱️ TTL (Time to Live)

Auto-expire keys like Redis.

```typescript
// Set with TTL (expires in 60 seconds)
await db.setWithTTL('session.abc123', { userId: 1 }, 60);

// Set TTL on existing key
db.setTTL('temp.data', 300);

// Get remaining TTL (-1 = no TTL, -2 = key doesn't exist)
const ttl = await db.getTTL('session.abc123');

// Remove TTL (make persistent)
db.clearTTL('session.abc123');

// Check if key has TTL
if (db.hasTTL('session.abc123')) { ... }

// Listen for expirations
db.on('ttl:expired', ({ path }) => {
    console.log('Key expired:', path);
});
```

### 📡 Pub/Sub (Subscriptions)

Subscribe to key changes with pattern matching.

```typescript
// Subscribe to all user changes
const unsubscribe = db.subscribe('users.*', (newValue, oldValue) => {
    console.log('User changed:', newValue);
});

// Subscribe to specific path
db.subscribe('config.theme', (value) => {
    applyTheme(value);
});

// Wildcards supported
db.subscribe('**', (value, old) => {
    // Called for ALL changes
});

// Unsubscribe when done
unsubscribe();

// Or use event emitter style
db.on('change', ({ path, value, oldValue }) => {
    console.log(`${path} changed`);
});
```

### 🔐 Encryption

AES-256-GCM encryption for data at rest.

```typescript
const db = new JSONDatabase('secure.json', {
    encryptionKey: 'your-32-character-secret-key!!'
});

// All data is encrypted before writing to disk
await db.set('secrets', { apiKey: 'xyz123' });
```

### 🛠️ Utility Methods

```typescript
// Get all keys under a path
const keys = await db.keys('users');

// Get all values under a path
const values = await db.values('users');

// Count items
const count = await db.count('users');

// Clear all data
await db.clear();

// Get database statistics
const stats = await db.stats();
// { size: 1234, keys: 10, indices: 2, ttlKeys: 5, subscriptions: 3 }

// Force save to disk (Durable write)
await db.save();

// Explicit durability sync (v4.5+)
await db.sync();

// Check WAL status (v4.5+)
const wal = db.walStatus();
// { enabled: true, committedLsn: 12345 }

// Clean shutdown
await db.close();
```

## 🎯 Events

```typescript
db.on('change', ({ path, value, oldValue }) => { ... });
db.on('batch', ({ operations }) => { ... });
db.on('transaction:commit', () => { ... });
db.on('transaction:rollback', ({ error }) => { ... });
db.on('snapshot:created', ({ path, name }) => { ... });
db.on('snapshot:restored', ({ path }) => { ... });
db.on('ttl:expired', ({ path }) => { ... });
db.on('error', (error) => { ... });
```

## 🔧 Development

```bash
# Build native module
bun run build

# Run tests
bun test

# Run benchmarks
bun run bench

# Build debug version
bun run build:debug
```

## 📊 Performance Benchmarks

> See [benchmarks/RESULTS.md](./benchmarks/RESULTS.md) and [benchmarks/RESULTS_V55.md](./benchmarks/RESULTS_V55.md) for detailed benchmark data.

### Core CRUD

| Operation         | In-Memory Mode    | WAL (Batched)     | Avg Latency   |
| ----------------- | ----------------- | ----------------- | ------------- |
| set (simple)      | 545,512 ops/s     | 525,713 ops/s     | 0.0018ms      |
| set (nested)      | 484,991 ops/s     | 408,078 ops/s     | 0.0021ms      |
| get (existing)    | 1,181,674 ops/s   | 1,298,581 ops/s   | 0.0008ms      |
| has (existing)    | 2,301,343 ops/s   | 2,380,147 ops/s   | 0.0004ms      |
| has (missing)     | 2,643,143 ops/s   | 2,580,611 ops/s   | 0.0004ms      |
| delete            | 621,390 ops/s     | 560,561 ops/s     | 0.0016ms      |
| add/subtract      | 614,138 ops/s     | 571,588 ops/s     | 0.0016ms      |
| findByIndex       | 696,810 ops/s     | 639,626 ops/s     | 0.0014ms      |
| batch (10 ops)    | 264,029 ops/s     | 178,946 ops/s     | 0.0038ms      |

### v5.5 Native Query Engine

| Operation                          | 1K Dataset   | 10K Dataset  | 50K Dataset  |
| ---------------------------------- | ------------ | ------------ | ------------ |
| Native: where(age > 30)            | 971 ops/s    | 104 ops/s    | 17 ops/s     |
| Native: multi-filter+sort+limit    | 6,625 ops/s  | 1,116 ops/s  | 214 ops/s    |
| Native: filter+select(3 fields)    | 10,594 ops/s | 3,656 ops/s  | 1,426 ops/s  |
| JS Fallback: filter(fn)            | 519 ops/s    | 39 ops/s     | 5 ops/s      |

### v5.5 Concurrent Write Scaling

| Test                                          | Ops/s     | Avg Latency |
| --------------------------------------------- | --------- | ----------- |
| Sequential writes (same collection)           | 427,333   | 0.0023ms    |
| Concurrent 10 writes (different collections)  | 44,174    | 0.0226ms    |
| Batch mixed (100 ops, 10 collections)         | 10,968    | 0.0912ms    |

### Key Insights

- **Read operations** (`get`, `has`) are now near-instant (~2.5M ops/s) thanks to zero-copy lookups. 🔥
- **Write throughput doubled** in v5.5 (~545k ops/s vs ~260k in v4.x) with striped write locks.
- **Native query engine** runs filter+select at **10,594 ops/s** — up to **20x faster** than JS fallback.
- **Group Commit WAL** maintains ~525k write ops/s with full ACID durability.
- **Smart memory management** transparently offloads cold data with sub-ms restore latency.
- **Index lookups** provide O(1) performance regardless of dataset size (~700k ops/s).

## 📄 License

MIT

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
