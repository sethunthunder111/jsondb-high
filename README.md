# jsondb-high 🚀

A blazing fast, feature-rich JSON database for Node.js with a Rust-powered core via N-API.

## ✨ Features

- ⚡ **Blazing Fast**: Core logic written in Rust via N-API for native performance (~90k ops/sec in-memory mode)
- 🧵 **Multi-Core Processing**: Adaptive parallelism using Rayon - automatically uses all CPU cores for large datasets
- 🛡️ **Atomic Operations**: Uses Write-Ahead Logging (WAL) and atomic file swaps to prevent data corruption
- 🔍 **O(1) Indexing**: In-memory Map indices allow for instant lookups by field
- 🔒 **Encryption**: Optional AES-256-GCM encryption for data at rest
- 📦 **Zero Runtime Dependencies**: Self-contained native binary; no heavy external DB servers required
- 🔄 **Middleware**: Support for before and after hooks on operations
- 💾 **JSON Compatible**: Stores data in a simple, portable JSON file
- ⏱️ **TTL Support**: Auto-expire keys after a specified time (like Redis)
- 📡 **Pub/Sub**: EventEmitter-style subscriptions to key changes
- 📊 **Aggregations**: Built-in sum, avg, min, max, groupBy, distinct

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

## 🏗️ Hybrid Architecture

v4 offers two storage consistency modes. Choose based on your durability needs.

### MODE: DURABLE (WAL)

Writes are appended to a WAL file immediately. Crash-safe.

```typescript
const db = new JSONDatabase('db.json', {
    wal: true
});
```

### MODE: IN-MEMORY

Max throughput (~90k ops/sec). Writes update RAM instantly, disk periodically.

```typescript
const db = new JSONDatabase('db.json', {
    wal: false  // Default
});
```

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

// Available operators: eq, ne, gt, gte, lt, lte, contains, startswith, endswith, in, notin
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

#### How It Works

- **Adaptive**: Automatically uses 1-N cores based on workload size and system resources
- **Efficient**: Small workloads (<100 items) use single-threaded to avoid parallel overhead
- **Resource-Aware**: Leaves 1 core free for system/main thread
- **Scalable**: Performance scales linearly with available cores for large datasets

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

// Force save to disk
await db.save();

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

> See [benchmarks/RESULTS.md](./benchmarks/RESULTS.md) for detailed benchmark data.

| Operation         | In-Memory Mode  | WAL Mode        | Avg Latency   |
| ----------------- | --------------- | --------------- | ------------- |
| set (simple)      | 136,994 ops/s   | 1,913 ops/s     | 0.007ms       |
| set (nested)      | 158,449 ops/s   | 2,456 ops/s     | 0.006ms       |
| get               | 801,700 ops/s   | 389,956 ops/s   | 0.001ms       |
| has               | 730,311 ops/s   | 332,594 ops/s   | 0.001ms       |
| delete            | 153,035 ops/s   | 2,907 ops/s     | 0.007ms       |
| add/subtract      | 136,590 ops/s   | 3,019 ops/s     | 0.007ms       |
| findByIndex       | 193,306 ops/s   | 274,882 ops/s   | 0.005ms       |
| batch (10 ops)    | 72,515 ops/s    | 254 ops/s       | 0.014ms       |
| query.where()     | 475 ops/s       | 381 ops/s       | 2.11ms        |

### Key Insights

- **Read operations** (`get`, `has`) are blazing fast (~730k-800k ops/s) 🔥
- **Write operations** in In-Memory mode achieve ~140k+ ops/s
- **WAL mode** trades write speed for crash safety (~2-3k write ops/s)
- **Index lookups** provide O(1) performance regardless of dataset size

## 📄 License

MIT

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
