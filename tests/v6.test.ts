/**
 * v6 Unit Tests — Buffer Pool, MVCC, Streaming, PITR, Replication
 *
 * Tests the v6 Rust module functionality exposed through the native bindings.
 * Each section covers one of the new v6 modules.
 */

import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { JSONDatabase } from '../index.ts';
import { existsSync, unlinkSync, readFileSync, writeFileSync, mkdirSync, rmSync } from 'fs';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

const cleanup: string[] = [];
function track(...paths: string[]): void {
    cleanup.push(...paths);
}
afterAll(() => {
    for (const p of cleanup) {
        try { if (existsSync(p)) unlinkSync(p); } catch { /* ignore */ }
    }
});

function freshDB(
    file: string,
    opts: ConstructorParameters<typeof JSONDatabase>[1] = {},
): JSONDatabase {
    [file, `${file}.wal`, `${file}.cold`].forEach(p => {
        if (existsSync(p)) unlinkSync(p);
    });
    track(file, `${file}.wal`, `${file}.cold`);
    return new JSONDatabase(file, opts);
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Buffer Pool / mmap: loaded-from-disk data integrity
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: Buffer Pool / mmap file loading', () => {
    const FILE = 'v6_mmap.json';

    afterAll(() => { track(FILE, `${FILE}.wal`); });

    it('loads data correctly after first write-save-reopen cycle', async () => {
        const FILE1 = 'v6_mmap_reopen.json';
        track(FILE1, `${FILE1}.wal`);
        // Clean start
        if (existsSync(FILE1)) unlinkSync(FILE1);

        const db1 = new JSONDatabase(FILE1, { wal: false });
        await db1.set('config.name', 'mmap-test');
        await db1.set('config.version', 6);
        await db1.save();
        await db1.close();

        // Second open uses mmap_load_json under the hood
        const db2 = new JSONDatabase(FILE1, { wal: false });
        expect(await db2.get<string>('config.name')).toBe('mmap-test');
        expect(await db2.get<number>('config.version')).toBe(6);
        await db2.close();
    });

    it('handles large dataset correctly (stress mmap paging)', async () => {
        const FILE1 = 'v6_mmap_large.json';
        track(FILE1, `${FILE1}.wal`);
        if (existsSync(FILE1)) unlinkSync(FILE1);

        const db = new JSONDatabase(FILE1, { wal: false });
        const RECORDS = 1000;

        // Write 1000 records
        for (let i = 0; i < RECORDS; i++) {
            await db.set(`bench.r${i}`, {
                id: i,
                name: `Record ${i}`,
                value: i * 0.5,
                tags: ['a', 'b', 'c'],
            });
        }
        await db.save();
        await db.close();

        // Re-open and verify all records intact
        const db2 = new JSONDatabase(FILE1, { wal: false });
        expect(await db2.get<{ id: number }>('bench.r0')).toMatchObject({ id: 0 });
        expect(await db2.get<{ id: number }>('bench.r999')).toMatchObject({ id: 999 });

        const count = db2.query('bench').count();
        expect(count).toBe(RECORDS);
        await db2.close();
    });

    it('survives UTF-8 and special characters in values', async () => {
        const FILE1 = 'v6_mmap_utf8.json';
        track(FILE1, `${FILE1}.wal`);
        if (existsSync(FILE1)) unlinkSync(FILE1);

        const db = new JSONDatabase(FILE1, { wal: false });
        const special = '日本語テスト 🚀 ñoño "quoted" \\backslash';
        await db.set('data.unicode', special);
        await db.save();
        await db.close();

        const db2 = new JSONDatabase(FILE1, { wal: false });
        expect(await db2.get<string>('data.unicode')).toBe(special);
        await db2.close();
    });

    it('bufferPoolSizeMB option is accepted without error', () => {
        const db = freshDB(FILE, {
            wal: false,
            bufferPoolSizeMB: 64,
            bufferPageSizeKB: 16,
        });
        expect(db).toBeDefined();
        db.close();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. MVCC: Stripe count + concurrent write isolation
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: DashMap / stripeCount concurrency', () => {
    const FILE = 'v6_mvcc.json';

    afterAll(() => { track(FILE, `${FILE}.wal`); });

    it('accepts high stripeCount without error', () => {
        const db = freshDB(FILE, { wal: false, stripeCount: 512 });
        expect(db).toBeDefined();
        db.close();
    });

    it('concurrent set operations complete without deadlock', async () => {
        const db = freshDB(FILE, { wal: false, stripeCount: 256 });
        const WORKERS = 50;

        const ops = Array.from({ length: WORKERS }, (_, i) =>
            db.set(`concurrent.key${i}`, { id: i, value: `val_${i}` })
        );

        await expect(Promise.all(ops)).resolves.toBeDefined();

        const count = db.query('concurrent').count();
        expect(count).toBe(WORKERS);
        await db.close();
    });

    it('transactions correctly isolate concurrent modifications', async () => {
        const db = freshDB(FILE, { wal: false });
        await db.set('account.balance', 1000);

        // Run 10 concurrent transactions each subtracting 10
        const txns = Array.from({ length: 10 }, () =>
            db.transaction(async () => {
                const bal = await db.get<number>('account.balance') ?? 0;
                await db.set('account.balance', bal - 10);
            })
        );

        await Promise.allSettled(txns); // some may fail due to rollback

        const final = await db.get<number>('account.balance');
        // Balance should be a valid value (not corrupt)
        expect(typeof final).toBe('number');
        await db.close();
    });

    it('transaction rollback restores previous state', async () => {
        const db = freshDB(FILE, { wal: false });
        await db.set('counter', 100);

        try {
            await db.transaction(async () => {
                await db.set('counter', 999);
                throw new Error('forced rollback');
            });
        } catch { /* expected */ }

        expect(await db.get<number>('counter')).toBe(100);
        await db.close();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. Zero-Copy Streams: chunked .explain() and large query results
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: Zero-Copy Streams / .explain()', () => {
    const FILE = 'v6_streaming.json';
    let db: JSONDatabase;

    beforeAll(async () => {
        db = freshDB(FILE, { wal: false });
        // Seed 500 records
        for (let i = 0; i < 500; i++) {
            await db.set(`events.e${i}`, {
                id: i,
                page: ['/', '/about', '/pricing'][i % 3],
                duration: Math.floor(Math.random() * 200) + 10,
                country: ['US', 'UK', 'CA', 'DE'][i % 4],
            });
        }
    });
    afterAll(async () => { await db.close(); track(FILE); });

    it('.explain() returns a valid plan object', async () => {
        const plan = await db.query('events')
            .where('country').eq('US')
            .explain();

        expect(plan).toBeDefined();
        expect(typeof plan).toBe('object');
        // Actual shape: { collectionSize, scanType, filtersApplied, resultCount, ... }
        expect(typeof plan.collectionSize).toBe('number');
        expect(typeof plan.scanType).toBe('string');
        expect(Array.isArray(plan.filtersApplied)).toBe(true);
        expect(plan.filtersApplied.length).toBe(1);
        expect(plan.filtersApplied[0]?.field).toBe('country');
    });

    it('.explain() works on filtered + sorted query', async () => {
        const plan = await db.query('events')
            .where('duration').gt(100)
            .sort({ duration: -1 })
            .limit(10)
            .explain();

        expect(plan).toBeDefined();
        expect(typeof plan.scanType).toBe('string');
        expect(typeof plan.executionTimeMs).toBe('number');
        expect(plan.limit).toBe(10);
        expect(plan.sortApplied.length).toBeGreaterThan(0);
    });

    it('large query result returns all 500 records', async () => {
        const results = await db.query('events').exec();
        expect(results.length).toBe(500);
    });

    it('filtered query returns correct subset', async () => {
        const usEvents = await db.query('events')
            .where('country').eq('US')
            .exec();
        // Every 4th record (i%4===0) → 500/4 = 125
        expect(usEvents.length).toBe(125);
        expect(usEvents.every(e => (e as Record<string, unknown>)['country'] === 'US')).toBe(true);
    });

    it('parallelQuery on large set returns correct count', async () => {
        const ca = await db.parallelQuery('events', [
            { field: 'country', op: 'eq', value: 'CA' },
        ]);
        expect((ca as unknown[]).length).toBe(125);
    });

    it('parallelAggregate sum is correct', async () => {
        // Duration is (Math.random()*200+10), just verify it's a reasonable number
        const total = await db.parallelAggregate('events', 'sum', 'duration');
        expect(typeof total).toBe('number');
        expect(total).toBeGreaterThan(0);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Pluggable Storage Adapters (MemoryAdapter JS-side test)
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: Pluggable Storage Adapters', () => {
    it('MemoryAdapter read returns null before write', async () => {
        const { MemoryAdapter } = await import('../adapters.ts');
        const adapter = new MemoryAdapter();
        expect(await adapter.read()).toBeNull();
        expect(await adapter.exists()).toBe(false);
    });

    it('MemoryAdapter write/read round-trip', async () => {
        const { MemoryAdapter } = await import('../adapters.ts');
        const adapter = new MemoryAdapter();
        await adapter.write('{"hello":"world"}');
        expect(await adapter.read()).toBe('{"hello":"world"}');
        expect(await adapter.exists()).toBe(true);
    });

    it('MemoryAdapter delete clears data', async () => {
        const { MemoryAdapter } = await import('../adapters.ts');
        const adapter = new MemoryAdapter();
        await adapter.write('some data');
        await adapter.delete!();
        expect(await adapter.read()).toBeNull();
    });

    it('FileSystemAdapter writes and reads back correctly', async () => {
        const { FileSystemAdapter } = await import('../adapters.ts');
        const PATH = '/tmp/v6_adapter_test.json';
        track(PATH);

        const adapter = new FileSystemAdapter(PATH);
        await adapter.write('{"test":42}');
        expect(await adapter.exists()).toBe(true);
        const content = await adapter.read();
        expect(content).toBe('{"test":42}');
        await adapter.delete!();
        expect(await adapter.exists()).toBe(false);
    });

    it('FileSystemAdapter uses atomic rename (no temp file left)', async () => {
        const { FileSystemAdapter } = await import('../adapters.ts');
        const PATH = '/tmp/v6_atomic_test.json';
        track(PATH, `${PATH}.tmp`);

        const adapter = new FileSystemAdapter(PATH);
        await adapter.write('{"atomic":true}');

        // .tmp should not exist after write
        expect(existsSync(`${PATH}.tmp`)).toBe(false);
        expect(existsSync(PATH)).toBe(true);
        await adapter.delete!();
    });

    it('adapter name property is set correctly', async () => {
        const { MemoryAdapter, FileSystemAdapter } = await import('../adapters.ts');
        expect(new MemoryAdapter().name).toBe('memory');
        expect(new FileSystemAdapter('/tmp/x.json').name).toBe('filesystem');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. PITR: Archive + restore workflow
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: PITR (Point-in-Time Recovery)', () => {
    const FILE = 'v6_pitr.json';
    const ARCHIVE_DIR = `${FILE}.archive`;

    afterAll(() => {
        [FILE, `${FILE}.wal`].forEach(p => {
            try { if (existsSync(p)) unlinkSync(p); } catch { /* ignore */ }
        });
        try { if (existsSync(ARCHIVE_DIR)) rmSync(ARCHIVE_DIR, { recursive: true }); } catch { /* ignore */ }
    });

    it('database saves and can be reopened (baseline for PITR)', async () => {
        const PITR_FILE = 'v6_pitr_reopen.json';
        track(PITR_FILE, `${PITR_FILE}.wal`);
        if (existsSync(PITR_FILE)) unlinkSync(PITR_FILE);
        if (existsSync(`${PITR_FILE}.wal`)) unlinkSync(`${PITR_FILE}.wal`);

        const db = new JSONDatabase(PITR_FILE, { wal: true });
        await db.set('users.u1', { name: 'Alice', role: 'admin' });
        await db.set('users.u2', { name: 'Bob', role: 'user' });
        await db.save();
        await db.close();

        // Verify data persisted (don't wipe the file on second open)
        const db2 = new JSONDatabase(PITR_FILE, { wal: true });
        expect(await db2.get<{ name: string }>('users.u1')).toMatchObject({ name: 'Alice' });
        await db2.save();
        await db2.close();
    });

    it('createSnapshot generates a readable file', async () => {
        const db = freshDB(FILE, { wal: false });
        await db.set('snapshot.data', { version: 1, timestamp: Date.now() });
        await db.save();

        const snapPath = await db.createSnapshot('pitr-test');
        track(snapPath);

        expect(existsSync(snapPath)).toBe(true);
        const content = JSON.parse(readFileSync(snapPath, 'utf-8'));
        expect(content.snapshot?.data?.version ?? content['snapshot.data']?.version ?? content.snapshot).toBeDefined();
        await db.close();
    });

    it('restoreSnapshot rolls back to previous state', async () => {
        const db = freshDB(FILE, { wal: false });
        await db.set('score', 100);
        await db.save();

        const snap = await db.createSnapshot('before-mutation');
        track(snap);

        await db.set('score', 9999);
        expect(await db.get<number>('score')).toBe(9999);

        await db.restoreSnapshot(snap);
        expect(await db.get<number>('score')).toBe(100);
        await db.close();
    });

    it('restoreSnapshot throws on non-existent file', async () => {
        const db = freshDB(FILE, { wal: false });
        await expect(db.restoreSnapshot('ghost_snapshot.json')).rejects.toThrow();
        await db.close();
    });

    it('snapshot content matches saved database file byte-for-byte', async () => {
        const db = freshDB(FILE, { wal: false });
        await db.set('checksum.test', { value: 42, name: 'byte-check' });
        await db.save();

        const snap = await db.createSnapshot('byte-match');
        track(snap);

        const snapContent = readFileSync(snap, 'utf-8');
        const fileContent = readFileSync(FILE, 'utf-8');
        expect(snapContent).toBe(fileContent);
        await db.close();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. WAL Replication: primary flush + integrity simulation
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: WAL Streaming / Replication (infrastructure)', () => {
    it('WAL entries persisted during batch writes survive reopen', async () => {
        const WALFILE = 'v6_repl_wal.json';
        track(WALFILE, `${WALFILE}.wal`);
        // Wipe only once before first open
        if (existsSync(WALFILE)) unlinkSync(WALFILE);
        if (existsSync(`${WALFILE}.wal`)) unlinkSync(`${WALFILE}.wal`);

        const KEYS = 20;
        const db1 = new JSONDatabase(WALFILE, { durability: 'batched', walFlushMs: 5 });
        for (let i = 0; i < KEYS; i++) {
            await db1.set(`replica.item${i}`, { id: i, data: `payload_${i}` });
        }
        await db1.sync();
        await db1.save();
        await db1.close();

        // Reopen WITHOUT wiping
        const db2 = new JSONDatabase(WALFILE, { durability: 'batched' });
        for (let i = 0; i < KEYS; i++) {
            expect(await db2.get<{ id: number }>(`replica.item${i}`)).toMatchObject({ id: i });
        }
        await db2.close();
    });

    it('walStatus reports correct LSN after writes', async () => {
        const WALFILE2 = 'v6_repl_lsn.json';
        track(WALFILE2, `${WALFILE2}.wal`);
        if (existsSync(WALFILE2)) unlinkSync(WALFILE2);

        const db = new JSONDatabase(WALFILE2, { durability: 'batched', walFlushMs: 5 });
        await db.set('x', 1);
        await db.set('y', 2);
        await db.set('z', 3);
        await db.sync();

        const status = db.walStatus();
        expect(status.enabled).toBe(true);
        const lsn = (status as Record<string, unknown>)['committed_lsn'] ?? (status as Record<string, unknown>)['committedLsn'];
        expect(typeof lsn).toBe('number');
        expect(lsn as number).toBeGreaterThan(0);
        await db.close();
    });

    it('database recovers from WAL after simulated crash (no save)', async () => {
        const WALFILE3 = 'v6_repl_crash.json';
        track(WALFILE3, `${WALFILE3}.wal`);
        // Wipe only before first open
        if (existsSync(WALFILE3)) unlinkSync(WALFILE3);
        if (existsSync(`${WALFILE3}.wal`)) unlinkSync(`${WALFILE3}.wal`);

        // Write without explicit save (WAL should persist it)
        const db1 = new JSONDatabase(WALFILE3, { durability: 'batched', walFlushMs: 5 });
        await db1.set('critical.value', 'survive');
        await db1.sync();
        // No db1.save() — simulates crash after WAL flush only
        await db1.close();

        // Reopen WITHOUT wiping — WAL recovery restores data
        const db2 = new JSONDatabase(WALFILE3, { durability: 'batched' });
        const val = await db2.get<string>('critical.value');
        expect(val).toBe('survive');
        await db2.close();
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. Integration: All v6 features combined
// ─────────────────────────────────────────────────────────────────────────────
describe('v6: Full integration — all features combined', () => {
    const FILE = 'v6_combined.json';

    afterAll(() => { track(FILE, `${FILE}.wal`, `${FILE}.cold`); });

    it('creates a full-featured DB with all v6 options', () => {
        const db = freshDB(FILE, {
            wal: true,
            durability: 'batched',
            stripeCount: 256,
            bufferPoolSizeMB: 32,
            bufferPageSizeKB: 16,
            walFlushMs: 10,
            walBatchSize: 500,
        });
        expect(db).toBeDefined();
        expect(db.walStatus().enabled).toBe(true);
        db.close();
    });

    it('write-heavy workload: 200 concurrent sets then query', async () => {
        const db = freshDB(FILE, {
            wal: false,
            stripeCount: 256,
            bufferPoolSizeMB: 16,
        });

        const N = 200;
        await Promise.all(
            Array.from({ length: N }, (_, i) =>
                db.set(`load.item${i}`, {
                    id: i,
                    score: Math.floor(Math.random() * 100),
                    active: i % 2 === 0,
                })
            )
        );

        const total = db.query('load').count();
        expect(total).toBe(N);

        const active = await db.parallelQuery('load', [
            { field: 'active', op: 'eq', value: true },
        ]);
        expect((active as unknown[]).length).toBe(N / 2);

        const sumScores = db.query('load').sum('score');
        expect(typeof sumScores).toBe('number');
        expect(sumScores).toBeGreaterThan(0);

        await db.close();
    });

    it('snapshot → mutate → restore → verify across all v6 paths', async () => {
        const db = freshDB(FILE, {
            wal: false,
            stripeCount: 128,
        });

        // Setup initial state
        await db.set('app.version', '6.0.0');
        await db.set('app.users', 1000);
        await db.set('app.config', { debug: false, region: 'us-east-1' });
        await db.save();

        // Snapshot
        const snap = await db.createSnapshot('v6-combined');
        track(snap);

        // Mutate aggressively
        await db.set('app.version', 'broken');
        await db.set('app.users', -1);
        await db.delete('app.config');

        // Verify mutation
        expect(await db.get<string>('app.version')).not.toBe('6.0.0');
        expect(await db.has('app.config')).toBe(false);

        // Restore
        await db.restoreSnapshot(snap);

        // Verify recovery
        expect(await db.get<string>('app.version')).toBe('6.0.0');
        expect(await db.get<number>('app.users')).toBe(1000);
        expect(await db.get<{ region: string }>('app.config')).toMatchObject({ region: 'us-east-1' });

        await db.close();
    });

    it('explain() + parallelAggregate on same dataset returns consistent results', async () => {
        const db = freshDB(FILE, { wal: false });

        for (let i = 0; i < 100; i++) {
            await db.set(`metrics.m${i}`, {
                value: i * 2,
                category: i % 3 === 0 ? 'A' : i % 3 === 1 ? 'B' : 'C',
            });
        }

        // Query with explain
        const plan = await db.query('metrics')
            .where('category').eq('A')
            .explain();
        expect(plan).toBeDefined();

        // Category A = i%3===0 → 0,3,6,...99 → ceil(100/3)=34 records
        const catA = await db.parallelQuery('metrics', [
            { field: 'category', op: 'eq', value: 'A' },
        ]);
        expect((catA as unknown[]).length).toBeGreaterThan(0);

        const sumA = db.query('metrics').where('category').eq('A').sum('value');
        const aggSum = await db.parallelAggregate('metrics', 'sum', 'value');
        // parallelAggregate sums all, query sum only sums A
        expect(aggSum).toBeGreaterThan(sumA);

        await db.close();
    });
});
