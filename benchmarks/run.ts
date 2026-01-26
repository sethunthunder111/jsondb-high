/**
 * jsondb-high Performance Benchmark Suite
 * 
 * Tests core operations and measures:
 * - Operations per second (ops/s)
 * - Average latency (ms)
 * - Throughput characteristics
 */

import { JSONDatabase } from '../index.ts';
import { existsSync, unlinkSync, writeFileSync } from 'fs';

// ============================================
// Configuration
// ============================================

const ITERATIONS = 10000;
const WARMUP_ITERATIONS = 1000;
const DB_FILE = 'benchmarks/bench_db.json';
const RESULTS_FILE = 'benchmarks/RESULTS.md';

interface BenchmarkResult {
    name: string;
    iterations: number;
    totalTimeMs: number;
    avgLatencyMs: number;
    opsPerSecond: number;
    mode: string;
}

const results: BenchmarkResult[] = [];

// ============================================
// Utilities
// ============================================

function cleanup() {
    const files = [DB_FILE, `${DB_FILE}.wal`, `${DB_FILE}.tmp`];
    for (const f of files) {
        if (existsSync(f)) unlinkSync(f);
    }
}

function formatNumber(n: number): string {
    return n.toLocaleString('en-US');
}

async function benchmark(
    name: string,
    mode: string,
    iterations: number,
    fn: () => Promise<void> | void
): Promise<BenchmarkResult> {
    // Warmup
    for (let i = 0; i < WARMUP_ITERATIONS; i++) {
        await fn();
    }

    // Actual benchmark
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
        await fn();
    }
    const end = performance.now();

    const totalTimeMs = end - start;
    const avgLatencyMs = totalTimeMs / iterations;
    const opsPerSecond = Math.round((iterations / totalTimeMs) * 1000);

    const result: BenchmarkResult = {
        name,
        mode,
        iterations,
        totalTimeMs: Math.round(totalTimeMs * 100) / 100,
        avgLatencyMs: Math.round(avgLatencyMs * 10000) / 10000,
        opsPerSecond
    };

    console.log(`  ✅ ${name}: ${formatNumber(opsPerSecond)} ops/s (avg: ${avgLatencyMs.toFixed(4)}ms)`);
    results.push(result);
    return result;
}

// ============================================
// Benchmark Suites
// ============================================

async function runCoreBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n📊 Core Operations (${mode})`);
    console.log('─'.repeat(50));

    let counter = 0;

    // SET - Simple value
    await benchmark(`set (simple)`, mode, ITERATIONS, async () => {
        await db.set(`bench.key_${counter++}`, 'value');
    });

    // SET - Nested path
    counter = 0;
    await benchmark(`set (nested)`, mode, ITERATIONS, async () => {
        await db.set(`users.${counter++}.profile.name`, 'Test User');
    });

    // GET - Existing key
    await db.set('existing.key', { data: 'test value', num: 42 });
    await benchmark(`get (existing)`, mode, ITERATIONS, async () => {
        await db.get('existing.key');
    });

    // GET - With default
    await benchmark(`get (default)`, mode, ITERATIONS, async () => {
        await db.get('nonexistent.key', 'default');
    });

    // HAS - Existing
    await benchmark(`has (existing)`, mode, ITERATIONS, async () => {
        await db.has('existing.key');
    });

    // HAS - Non-existing
    await benchmark(`has (missing)`, mode, ITERATIONS, async () => {
        await db.has('nonexistent.path.key');
    });

    // DELETE
    counter = 0;
    for (let i = 0; i < ITERATIONS; i++) {
        await db.set(`delete_test.${i}`, i);
    }
    await benchmark(`delete`, mode, ITERATIONS, async () => {
        await db.delete(`delete_test.${counter++}`);
    });
}

async function runMathBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n🔢 Math Operations (${mode})`);
    console.log('─'.repeat(50));

    await db.set('counter', 0);

    await benchmark(`add`, mode, ITERATIONS, async () => {
        await db.add('counter', 1);
    });

    await benchmark(`subtract`, mode, ITERATIONS, async () => {
        await db.subtract('counter', 1);
    });
}

async function runArrayBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n📚 Array Operations (${mode})`);
    console.log('─'.repeat(50));

    await db.set('array', []);
    let counter = 0;

    await benchmark(`push`, mode, ITERATIONS / 10, async () => {
        await db.push('array', `item_${counter++}`);
    });

    // Pull is slower due to filtering
    await benchmark(`pull`, mode, ITERATIONS / 100, async () => {
        await db.pull('array', `item_${--counter}`);
    });
}

async function runQueryBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n🔍 Query Operations (${mode})`);
    console.log('─'.repeat(50));

    // Setup test data
    const users: Record<string, { id: number; name: string; age: number; role: string }> = {};
    for (let i = 0; i < 1000; i++) {
        users[`user_${i}`] = {
            id: i,
            name: `User ${i}`,
            age: 18 + (i % 50),
            role: i % 3 === 0 ? 'admin' : 'user'
        };
    }
    await db.set('query_users', users);

    await benchmark(`query.where().exec()`, mode, ITERATIONS / 10, async () => {
        await db.query('query_users')
            .where('age').gt(30)
            .where('role').eq('admin')
            .exec();
    });

    await benchmark(`query.sort().limit()`, mode, ITERATIONS / 10, async () => {
        await db.query('query_users')
            .sort({ age: -1 })
            .limit(10)
            .exec();
    });

    await benchmark(`query.count()`, mode, ITERATIONS, () => {
        db.query('query_users').count();
    });

    await benchmark(`query.sum()`, mode, ITERATIONS, () => {
        db.query('query_users').sum('age');
    });

    await benchmark(`find (predicate)`, mode, ITERATIONS / 10, async () => {
        await db.find('query_users', (u: { age: number }) => u.age > 40);
    });

    await benchmark(`find (object)`, mode, ITERATIONS / 10, async () => {
        await db.find('query_users', { role: 'admin' });
    });

    await benchmark(`paginate`, mode, ITERATIONS / 10, async () => {
        await db.paginate('query_users', 1, 20);
    });
}

async function runIndexBenchmarks(mode: string) {
    console.log(`\n⚡ Index Operations (${mode})`);
    console.log('─'.repeat(50));

    cleanup();
    const db = new JSONDatabase(DB_FILE, {
        wal: mode === 'WAL',
        indices: [{ name: 'email', path: 'indexed_users', field: 'email' }]
    });

    // Setup indexed data
    for (let i = 0; i < 1000; i++) {
        await db.set(`indexed_users.user_${i}`, {
            id: i,
            email: `user${i}@example.com`,
            name: `User ${i}`
        });
    }
    db.rebuildIndex();

    await benchmark(`findByIndex`, mode, ITERATIONS, async () => {
        await db.findByIndex('email', 'user500@example.com');
    });

    await db.close();
}

async function runBatchBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n📦 Batch Operations (${mode})`);
    console.log('─'.repeat(50));

    await benchmark(`batch (10 ops)`, mode, ITERATIONS / 10, async () => {
        await db.batch([
            { type: 'set', path: 'batch.a', value: 1 },
            { type: 'set', path: 'batch.b', value: 2 },
            { type: 'set', path: 'batch.c', value: 3 },
            { type: 'set', path: 'batch.d', value: 4 },
            { type: 'set', path: 'batch.e', value: 5 },
            { type: 'add', path: 'batch.a', value: 1 },
            { type: 'add', path: 'batch.b', value: 1 },
            { type: 'delete', path: 'batch.c' },
            { type: 'set', path: 'batch.f', value: 6 },
            { type: 'set', path: 'batch.g', value: 7 }
        ]);
    });
}

async function runParallelBenchmarks(db: JSONDatabase, mode: string) {
    console.log(`\n🧵 Parallel Processing (${mode})`);
    console.log('─'.repeat(50));

    // Show system info
    const sysInfo = db.getSystemInfo();
    console.log(`  System: ${sysInfo.availableCores} cores, parallel=${sysInfo.parallelEnabled}`);

    // Setup large dataset for parallel benchmarks
    const largeUsers: Record<string, { id: number; name: string; age: number; active: boolean }> = {};
    for (let i = 0; i < 5000; i++) {
        largeUsers[`user_${i}`] = {
            id: i,
            name: `User ${i}`,
            age: 18 + (i % 60),
            active: i % 2 === 0
        };
    }
    await db.set('parallel_users', largeUsers);

    // Parallel Batch Set (500 items)
    await benchmark(`parallelBatchSet (500 ops)`, mode, ITERATIONS / 100, async () => {
        const ops: Array<{ path: string; value: unknown }> = [];
        for (let i = 0; i < 500; i++) {
            ops.push({
                path: `parallel_batch.item_${i}`,
                value: { id: i, data: `test_${i}` }
            });
        }
        await db.batchSetParallel(ops);
    });

    // Parallel Query vs Regular Query
    await benchmark(`parallelQuery (2 filters)`, mode, ITERATIONS / 10, async () => {
        await db.parallelQuery('parallel_users', [
            { field: 'age', op: 'gte', value: 40 },
            { field: 'active', op: 'eq', value: true }
        ]);
    });

    // Compare with regular query
    await benchmark(`regularQuery (2 filters)`, mode, ITERATIONS / 10, async () => {
        await db.query<{ age: number; active: boolean }>('parallel_users')
            .where('age').gte(40)
            .filter((u) => u.active === true)
            .exec();
    });

    // Parallel Aggregations
    await benchmark(`parallelAggregate (count)`, mode, ITERATIONS, async () => {
        await db.parallelAggregate('parallel_users', 'count');
    });

    await benchmark(`parallelAggregate (sum)`, mode, ITERATIONS, async () => {
        await db.parallelAggregate('parallel_users', 'sum', 'age');
    });

    await benchmark(`parallelAggregate (avg)`, mode, ITERATIONS, async () => {
        await db.parallelAggregate('parallel_users', 'avg', 'age');
    });

    // Compare with regular aggregation
    await benchmark(`regularAggregate (sum)`, mode, ITERATIONS, () => {
        db.query('parallel_users').sum('age');
    });

    // Cleanup
    await db.delete('parallel_users');
    await db.delete('parallel_batch');
}

// ============================================
// Results Generation
// ============================================

function generateMarkdownReport(): string {
    const now = new Date().toISOString();
    
    let md = `# 📊 jsondb-high Benchmark Results

> Generated: ${now}  
> Iterations per test: ${formatNumber(ITERATIONS)}  
> Warmup iterations: ${formatNumber(WARMUP_ITERATIONS)}

## System Information

- **Platform**: ${process.platform}
- **Architecture**: ${process.arch}
- **Node Version**: ${process.version}

## Summary

| Operation | In-Memory (ops/s) | WAL Mode (ops/s) | Avg Latency (ms) |
| --------- | ----------------- | ---------------- | ---------------- |
`;

    // Group results by operation name
    const grouped = new Map<string, { inMemory?: BenchmarkResult; wal?: BenchmarkResult }>();
    for (const r of results) {
        if (!grouped.has(r.name)) grouped.set(r.name, {});
        const entry = grouped.get(r.name)!;
        if (r.mode === 'In-Memory') entry.inMemory = r;
        else entry.wal = r;
    }

    for (const [name, { inMemory, wal }] of grouped) {
        const imOps = inMemory ? formatNumber(inMemory.opsPerSecond) : 'N/A';
        const walOps = wal ? formatNumber(wal.opsPerSecond) : 'N/A';
        const latency = inMemory?.avgLatencyMs ?? wal?.avgLatencyMs ?? 0;
        md += `| ${name} | ${imOps} | ${walOps} | ${latency.toFixed(4)} |\n`;
    }

    md += `
## Detailed Results

### In-Memory Mode

In-memory mode prioritizes speed. Data is kept in RAM and flushed to disk periodically.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
`;

    for (const r of results.filter(r => r.mode === 'In-Memory')) {
        md += `| ${r.name} | ${formatNumber(r.iterations)} | ${r.totalTimeMs.toFixed(2)} | ${r.avgLatencyMs.toFixed(4)} | ${formatNumber(r.opsPerSecond)} |\n`;
    }

    md += `
### WAL Mode (Durable)

WAL mode provides crash safety by appending operations to a write-ahead log before applying.

| Operation | Iterations | Total Time (ms) | Avg Latency (ms) | Ops/Second |
| --------- | ---------- | --------------- | ---------------- | ---------- |
`;

    for (const r of results.filter(r => r.mode === 'WAL')) {
        md += `| ${r.name} | ${formatNumber(r.iterations)} | ${r.totalTimeMs.toFixed(2)} | ${r.avgLatencyMs.toFixed(4)} | ${formatNumber(r.opsPerSecond)} |\n`;
    }

    md += `
## Interpretation

### Key Takeaways

1. **Read Operations** (\`get\`, \`has\`) are extremely fast in both modes since they only access in-memory data.
2. **Write Operations** (\`set\`, \`delete\`) are faster in In-Memory mode but still performant in WAL mode.
3. **Index Lookups** (\`findByIndex\`) provide O(1) performance regardless of dataset size.
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
`;

    return md;
}

// ============================================
// Main
// ============================================

async function main() {
    console.log('🚀 jsondb-high Benchmark Suite');
    console.log('═'.repeat(50));
    console.log(`Iterations: ${formatNumber(ITERATIONS)}`);
    console.log(`Warmup: ${formatNumber(WARMUP_ITERATIONS)}`);

    // In-Memory Mode
    console.log('\n\n🔵 IN-MEMORY MODE');
    console.log('═'.repeat(50));
    cleanup();
    const dbInMemory = new JSONDatabase(DB_FILE, { wal: false });
    
    await runCoreBenchmarks(dbInMemory, 'In-Memory');
    await runMathBenchmarks(dbInMemory, 'In-Memory');
    await runArrayBenchmarks(dbInMemory, 'In-Memory');
    await runQueryBenchmarks(dbInMemory, 'In-Memory');
    await runIndexBenchmarks('In-Memory');
    cleanup();
    const dbInMemory2 = new JSONDatabase(DB_FILE, { wal: false });
    await runBatchBenchmarks(dbInMemory2, 'In-Memory');
    await runParallelBenchmarks(dbInMemory2, 'In-Memory');
    await dbInMemory2.close();

    // WAL Mode
    console.log('\n\n🟡 WAL MODE (DURABLE)');
    console.log('═'.repeat(50));
    cleanup();
    const dbWal = new JSONDatabase(DB_FILE, { wal: true });
    
    await runCoreBenchmarks(dbWal, 'WAL');
    await runMathBenchmarks(dbWal, 'WAL');
    await runArrayBenchmarks(dbWal, 'WAL');
    await runQueryBenchmarks(dbWal, 'WAL');
    await runIndexBenchmarks('WAL');
    cleanup();
    const dbWal2 = new JSONDatabase(DB_FILE, { wal: true });
    await runBatchBenchmarks(dbWal2, 'WAL');
    await runParallelBenchmarks(dbWal2, 'WAL');
    await dbWal2.close();

    cleanup();
    console.log('\n🎉 Benchmark complete!');
}

main().catch(console.error);
