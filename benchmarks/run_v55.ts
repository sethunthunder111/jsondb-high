/// <reference types="node" />
/**
 * jsondb-high v5.5 Enterprise Benchmark Suite
 * 
 * Tests new v5.5 features and validates performance improvements:
 * - Native Query Engine vs JS fallback
 * - Optimized array operations (pushBatch/pullItems)
 * - Concurrent write scaling (striped locks)
 * - Memory management
 * - Multi-scale testing (1K, 10K, 100K records)
 */

import { JSONDatabase } from '../index.ts';
import { existsSync, unlinkSync, writeFileSync } from 'fs';
import { performance } from 'perf_hooks';

// ============================================
// Configuration
// ============================================

const DB_FILE = 'benchmarks/bench_v55.json';

interface BenchmarkResult {
    name: string;
    category: string;
    iterations: number;
    totalTimeMs: number;
    avgLatencyMs: number;
    opsPerSecond: number;
    datasetSize: number;
    notes?: string;
}

const results: BenchmarkResult[] = [];

// ============================================
// Utilities
// ============================================

function cleanup() {
    const patterns = [DB_FILE, `${DB_FILE}.wal`, `${DB_FILE}.tmp`];
    for (const f of patterns) {
        if (existsSync(f)) unlinkSync(f);
    }
    // Clean cold storage files
    const { readdirSync } = require('fs');
    const dir = require('path').dirname(DB_FILE);
    try {
        for (const file of readdirSync(dir)) {
            if (file.includes('bench_v55') && (file.endsWith('.cold') || file.includes('.cold.'))) {
                unlinkSync(require('path').join(dir, file));
            }
        }
    } catch { /* ignore */ }
}

function formatNumber(n: number): string {
    return n.toLocaleString('en-US');
}

function bar(pct: number, width: number = 30): string {
    const filled = Math.round(pct * width);
    return '█'.repeat(filled) + '░'.repeat(width - filled);
}

async function bench(
    name: string,
    category: string,
    iterations: number,
    datasetSize: number,
    fn: () => Promise<void> | void,
    notes?: string,
    warmupIterations: number = Math.min(100, Math.floor(iterations / 10)),
): Promise<BenchmarkResult> {
    // Warmup
    for (let i = 0; i < warmupIterations; i++) {
        await fn();
    }

    // GC hint
    if (typeof global !== 'undefined' && (global as any).gc) {
        (global as any).gc();
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
        category,
        iterations,
        datasetSize,
        totalTimeMs: Math.round(totalTimeMs * 100) / 100,
        avgLatencyMs: Math.round(avgLatencyMs * 10000) / 10000,
        opsPerSecond,
        notes,
    };

    const speedColor = opsPerSecond > 100000 ? '🟢' : opsPerSecond > 10000 ? '🟡' : '🔴';
    console.log(`  ${speedColor} ${name}: ${formatNumber(opsPerSecond)} ops/s  (${avgLatencyMs.toFixed(4)}ms avg, ${formatNumber(datasetSize)} items)`);
    results.push(result);
    return result;
}

// ============================================
// Data Generators
// ============================================

function generateUsers(count: number): Record<string, any> {
    const users: Record<string, any> = {};
    const roles = ['admin', 'user', 'editor', 'viewer', 'moderator'];
    const cities = ['New York', 'London', 'Tokyo', 'Berlin', 'Paris', 'Sydney', 'Toronto'];
    
    for (let i = 0; i < count; i++) {
        users[`u_${i}`] = {
            id: i,
            name: `User ${i}`,
            email: `user${i}@example.com`,
            age: 18 + (i % 60),
            salary: 30000 + (i % 100) * 1000,
            role: roles[i % roles.length],
            city: cities[i % cities.length],
            active: i % 3 !== 0,
            score: Math.round(Math.random() * 1000) / 10,
            tags: [`tag${i % 10}`, `group${i % 5}`],
            createdAt: new Date(2020 + (i % 5), i % 12, (i % 28) + 1).toISOString(),
        };
    }
    return users;
}

// ============================================
// Benchmark: Native Query Engine vs JS Fallback
// ============================================

async function benchQueryEngine(db: JSONDatabase) {
    console.log('\n\n🔥 NATIVE QUERY ENGINE vs JS FALLBACK');
    console.log('═'.repeat(60));

    for (const size of [1000, 10000, 50000]) {
        console.log(`\n  📦 Dataset size: ${formatNumber(size)} records`);
        console.log('  ' + '─'.repeat(55));

        const users = generateUsers(size);
        await db.set('qe_users', users);
        
        // 1. Native query: simple filter
        await bench(
            `Native: where(age > 30)`,
            'query-native',
            500,
            size,
            async () => {
                await db.query('qe_users')
                    .where('age').gt(30)
                    .exec();
            },
            'Uses Rust executeQuery()',
        );

        // 2. JS fallback: custom filter function (forces JS path)
        await bench(
            `JS Fallback: filter(fn)`,
            'query-js',
            500,
            size,
            async () => {
                await db.query<{ age: number }>('qe_users')
                    .filter((u) => u.age > 30)
                    .exec();
            },
            'Forces JS-side filtering',
        );

        // 3. Native: multi-filter + sort + limit
        await bench(
            `Native: multi-filter+sort+limit`,
            'query-native',
            500,
            size,
            async () => {
                await db.query('qe_users')
                    .where('age').gte(25)
                    .where('role').eq('admin')
                    .sort({ salary: -1 })
                    .limit(20)
                    .exec();
            },
            'Full pipeline in Rust',
        );

        // 4. Native: select fields
        await bench(
            `Native: filter+select(3 fields)`,
            'query-native',
            500,
            size,
            async () => {
                await db.query('qe_users')
                    .where('active').eq(true)
                    .select(['name', 'email', 'age'])
                    .limit(50)
                    .exec();
            },
            'Projection in Rust',
        );

        // 5. Native aggregation: count with filter
        await bench(
            `Native: count(age > 30)`,
            'aggregate-native',
            1000,
            size,
            () => {
                db.query('qe_users').where('age').gt(30).count();
            },
            'Uses Rust executeAggregate()',
        );

        // 6. Native aggregation: avg
        await bench(
            `Native: avg(salary)`,
            'aggregate-native',
            1000,
            size,
            () => {
                db.query('qe_users').where('role').eq('admin').avg('salary');
            },
            'Avg aggregation in Rust',
        );

        // Cleanup for next size
        await db.delete('qe_users');
    }
}

// ============================================
// Benchmark: Optimized Array Operations
// ============================================

async function benchArrayOps(db: JSONDatabase) {
    console.log('\n\n📚 OPTIMIZED ARRAY OPERATIONS');
    console.log('═'.repeat(60));

    for (const size of [100, 1000, 5000]) {
        console.log(`\n  📦 Array size: ${formatNumber(size)} elements`);
        console.log('  ' + '─'.repeat(55));

        // Setup: create array
        const arr: any[] = [];
        for (let i = 0; i < size; i++) {
            arr.push({ id: i, value: `item_${i}` });
        }
        await db.set('arr_bench', arr);

        // Push single items
        let pushCounter = size;
        await bench(
            `push (single item)`,
            'array',
            500,
            size,
            async () => {
                await db.push('arr_bench', { id: pushCounter++, value: `new_${pushCounter}` });
            },
        );

        // Push batch (10 items at once)
        await bench(
            `push (batch 10 items)`,
            'array-batch',
            200,
            size,
            async () => {
                const items = [];
                for (let j = 0; j < 10; j++) {
                    items.push({ id: pushCounter++, value: `batch_${pushCounter}` });
                }
                await db.push('arr_bench', ...items);
            },
            'Uses native pushBatch()',
        );

        // Pull items (remove from array)
        // Reset array first
        await db.set('arr_bench', arr.slice());
        let pullCounter = 0;
        await bench(
            `pull (single item)`,
            'array',
            Math.min(200, size / 2),
            size,
            async () => {
                await db.pull('arr_bench', { id: pullCounter++, value: `item_${pullCounter - 1}` });
            },
            'Uses native pullItems()',
        );

        await db.delete('arr_bench');
    }
}

// ============================================
// Benchmark: Concurrent Write Scaling
// ============================================

async function benchConcurrentWrites(db: JSONDatabase) {
    console.log('\n\n🧵 CONCURRENT WRITE SCALING (Striped Locks)');
    console.log('═'.repeat(60));

    // Sequential writes to SAME collection
    let counter = 0;
    await bench(
        `Sequential writes (same collection)`,
        'concurrent',
        5000,
        1,
        async () => {
            await db.set(`single_coll.key_${counter++}`, { data: counter });
        },
        'All writes to "single_coll"',
    );

    // Sequential writes to DIFFERENT collections
    counter = 0;
    await bench(
        `Sequential writes (different collections)`,
        'concurrent',
        5000,
        1,
        async () => {
            const coll = `coll_${counter % 8}`;
            await db.set(`${coll}.key_${counter++}`, { data: counter });
        },
        'Writes spread across 8 collections',
    );

    // Concurrent writes using Promise.all (same collection)
    await bench(
        `Concurrent 10 writes (same collection)`,
        'concurrent-parallel',
        500,
        10,
        async () => {
            const promises = [];
            for (let i = 0; i < 10; i++) {
                promises.push(db.set(`same_coll.k_${counter++}`, { v: i }));
            }
            await Promise.all(promises);
        },
        'Promise.all with 10 ops to same collection',
    );

    // Concurrent writes using Promise.all (different collections)
    await bench(
        `Concurrent 10 writes (different collections)`,
        'concurrent-parallel',
        500,
        10,
        async () => {
            const promises = [];
            for (let i = 0; i < 10; i++) {
                promises.push(db.set(`dc_${i}.k_${counter++}`, { v: i }));
            }
            await Promise.all(promises);
        },
        'Promise.all with 10 ops across different collections',
    );

    // Batch mixed operations
    await bench(
        `Batch mixed (100 ops, 10 collections)`,
        'concurrent-batch',
        100,
        100,
        async () => {
            const ops: any[] = [];
            for (let i = 0; i < 100; i++) {
                ops.push({ type: 'set', path: `batch_c${i % 10}.item_${counter++}`, value: { x: i } });
            }
            await db.batch(ops);
        },
        '100 set ops across 10 collections per batch',
    );

    // Cleanup
    for (let i = 0; i < 10; i++) {
        try { await db.delete(`coll_${i}`); } catch {}
        try { await db.delete(`dc_${i}`); } catch {}
        try { await db.delete(`batch_c${i}`); } catch {}
    }
    try { await db.delete('single_coll'); } catch {}
    try { await db.delete('same_coll'); } catch {}
}

// ============================================
// Benchmark: Memory Management
// ============================================

async function benchMemoryManagement(db: JSONDatabase) {
    console.log('\n\n💾 MEMORY MANAGEMENT');
    console.log('═'.repeat(60));

    // Setup: create several collections of different sizes
    for (let c = 0; c < 5; c++) {
        const data: Record<string, any> = {};
        for (let i = 0; i < 1000; i++) {
            data[`item_${i}`] = {
                id: i,
                collection: c,
                payload: `data_${i}_`.repeat(10),
            };
        }
        await db.set(`mem_coll_${c}`, data);
    }

    // Offload speed
    await bench(
        `offload (1K items)`,
        'memory',
        5,
        1000,
        async () => {
            await db.offload(`mem_coll_4`);
            // Restore it back for next iteration
            await db.restore(`mem_coll_4`);
        },
        'Offload + restore cycle',
        1,
    );

    // Restore speed
    await db.offload('mem_coll_3');
    await bench(
        `restore (1K items)`,
        'memory',
        5,
        1000,
        async () => {
            await db.restore('mem_coll_3');
            // Re-offload for next iteration
            await db.offload('mem_coll_3');
        },
        'Cold → hot restore',
        1,
    );
    await db.restore('mem_coll_3');

    // Memory stats
    await bench(
        `memoryStats()`,
        'memory',
        1000,
        5000,
        async () => {
            await db.memoryStats();
        },
        'Read memory utilization',
    );

    // Cleanup
    for (let c = 0; c < 5; c++) {
        try { await db.delete(`mem_coll_${c}`); } catch {}
    }
}

// ============================================
// Core CRUD at Scale
// ============================================

async function benchCoreAtScale(db: JSONDatabase) {
    console.log('\n\n⚡ CORE CRUD AT SCALE');
    console.log('═'.repeat(60));

    for (const size of [1000, 10000]) {
        console.log(`\n  📦 Dataset size: ${formatNumber(size)} records`);
        console.log('  ' + '─'.repeat(55));

        // Seed data
        const users = generateUsers(size);
        await db.set('crud_users', users);

        // GET (random reads)
        await bench(
            `get (random key)`,
            'crud',
            5000,
            size,
            async () => {
                const idx = Math.floor(Math.random() * size);
                await db.get(`crud_users.u_${idx}`);
            },
        );

        // SET (update existing)
        let setCounter = 0;
        await bench(
            `set (update existing)`,
            'crud',
            5000,
            size,
            async () => {
                await db.set(`crud_users.u_${setCounter++ % size}.score`, Math.random() * 100);
            },
        );

        // HAS (random checks)
        await bench(
            `has (random key)`,
            'crud',
            5000,
            size,
            async () => {
                const idx = Math.floor(Math.random() * size * 1.5); // 33% miss rate
                await db.has(`crud_users.u_${idx}`);
            },
        );

        await db.delete('crud_users');
    }
}

// ============================================
// Report Generator
// ============================================

function generateReport(): string {
    const now = new Date().toISOString();
    
    let md = `# 📊 jsondb-high v5.5 Enterprise Benchmark Results

> Generated: ${now}  
> Node.js: ${process.version}  
> Platform: ${process.platform} ${process.arch}

## Performance Summary

### 🔥 Native Query Engine

The v5.5 native query engine executes the entire query pipeline (filter → sort → skip → limit → select) in a single Rust call, eliminating JS↔Rust boundary crossings.

| Test | Dataset | Ops/s | Avg Latency | Engine |
|------|---------|-------|-------------|--------|
`;

    for (const r of results.filter(r => r.category.startsWith('query') || r.category.startsWith('aggregate'))) {
        const engine = r.category.includes('native') ? '🟢 Rust' : '🔴 JS';
        md += `| ${r.name} | ${formatNumber(r.datasetSize)} | ${formatNumber(r.opsPerSecond)} | ${r.avgLatencyMs.toFixed(4)}ms | ${engine} |\n`;
    }

    md += `
### 📚 Array Operations

Optimized push/pull with HashSet-based dedup and single-pass removal.

| Test | Array Size | Ops/s | Avg Latency | Notes |
|------|-----------|-------|-------------|-------|
`;

    for (const r of results.filter(r => r.category.startsWith('array'))) {
        md += `| ${r.name} | ${formatNumber(r.datasetSize)} | ${formatNumber(r.opsPerSecond)} | ${r.avgLatencyMs.toFixed(4)}ms | ${r.notes || ''} |\n`;
    }

    md += `
### 🧵 Concurrent Write Scaling

Striped lock manager allows concurrent writes to different top-level collections.

| Test | Ops/s | Avg Latency | Notes |
|------|-------|-------------|-------|
`;

    for (const r of results.filter(r => r.category.startsWith('concurrent'))) {
        md += `| ${r.name} | ${formatNumber(r.opsPerSecond)} | ${r.avgLatencyMs.toFixed(4)}ms | ${r.notes || ''} |\n`;
    }

    md += `
### 💾 Memory Management

Smart offloading with LRU-based eviction and transparent restore.

| Test | Dataset | Ops/s | Avg Latency | Notes |
|------|---------|-------|-------------|-------|
`;

    for (const r of results.filter(r => r.category === 'memory')) {
        md += `| ${r.name} | ${formatNumber(r.datasetSize)} | ${formatNumber(r.opsPerSecond)} | ${r.avgLatencyMs.toFixed(4)}ms | ${r.notes || ''} |\n`;
    }

    md += `
### ⚡ Core CRUD at Scale

| Test | Dataset | Ops/s | Avg Latency |
|------|---------|-------|-------------|
`;

    for (const r of results.filter(r => r.category === 'crud')) {
        md += `| ${r.name} | ${formatNumber(r.datasetSize)} | ${formatNumber(r.opsPerSecond)} | ${r.avgLatencyMs.toFixed(4)}ms |\n`;
    }

    md += `

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
- \`pushBatch\`: Single lock + HashSet dedup for O(1) contains
- \`pullItems\`: Single-pass O(N) removal with HashSet lookup

### Smart Memory Management
- LRU-based eviction when memory limit approached
- Transparent cold storage restore on access
- Per-key access tracking and size estimation

---

*Benchmarks run using [jsondb-high](https://github.com/sethunthunder111/jsondb-high) v5.5*
`;

    return md;
}

// ============================================
// Main
// ============================================

async function main() {
    console.log('');
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║     jsondb-high v5.5 Enterprise Benchmark Suite         ║');
    console.log('╚══════════════════════════════════════════════════════════╝');

    const sysInfo = {
        platform: process.platform,
        arch: process.arch,
        node: process.version,
    };
    console.log(`\n  Platform: ${sysInfo.platform} ${sysInfo.arch}`);
    console.log(`  Node.js:  ${sysInfo.node}`);

    cleanup();
    const db = new JSONDatabase(DB_FILE, { wal: false });

    const systemInfo = db.getSystemInfo();
    console.log(`  Cores:    ${systemInfo.availableCores}`);
    console.log(`  Parallel: ${systemInfo.parallelEnabled}`);

    try {
        await benchCoreAtScale(db);
        await benchQueryEngine(db);
        await benchArrayOps(db);
        await benchConcurrentWrites(db);
        await benchMemoryManagement(db);
    } catch (err) {
        console.error('\n❌ Benchmark error:', err);
    }

    // Generate report
    const report = generateReport();
    writeFileSync('benchmarks/RESULTS_V55.md', report);
    console.log('\n\n📝 Results written to benchmarks/RESULTS_V55.md');

    await db.close();
    cleanup();
    console.log('🎉 Benchmark suite complete!\n');
}

main().catch(console.error);
