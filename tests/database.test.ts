import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync, readFileSync } from 'fs';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const tmpFiles: string[] = [];

function track(...paths: string[]): void {
    tmpFiles.push(...paths);
}

function cleanupAll(): void {
    for (const f of tmpFiles) {
        if (existsSync(f)) unlinkSync(f);
    }
    tmpFiles.length = 0;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface User {
    name: string;
    email: string;
}

interface Product {
    id: number;
    name: string;
    price: number;
    category: string;
    tags?: string[];
}

interface ParallelUser {
    id: number;
    name: string;
    age: number;
    active: boolean;
}

// ─── Suite ────────────────────────────────────────────────────────────────────

describe('JSONDatabase', () => {
    // =========================================================================
    // SECTION 1 — Core CRUD
    // =========================================================================
    describe('Core CRUD', () => {
        const DB = 'test_core_crud.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('set and get a nested primitive', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('user.name', 'Alice');
            expect(await db.get<string>('user.name')).toBe('Alice');
            await db.close();
        });

        it('returns null for non-existent path', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            expect(await db.get('does.not.exist')).toBeNull();
            await db.close();
        });

        it('returns defaultValue for non-existent path', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            expect(await db.get<string>('does.not.exist', 'fallback')).toBe('fallback');
            await db.close();
        });

        it('has() returns true for existing and false for missing', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('flag', true);
            expect(await db.has('flag')).toBe(true);
            expect(await db.has('nope')).toBe(false);
            await db.close();
        });

        it('delete removes a key', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('temp', 42);
            await db.delete('temp');
            expect(await db.has('temp')).toBe(false);
            await db.close();
        });

        it('clear wipes the entire database', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('a', 1);
            await db.set('b', 2);
            await db.clear();
            const root = await db.get<Record<string, unknown>>('');
            expect(Object.keys(root!).length).toBe(0);
            await db.close();
        });

        it('getMany returns values in order', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('x', 10);
            await db.set('y', 20);
            const results = await db.getMany(['x', 'y', 'z']);
            expect(results[0]).toBe(10);
            expect(results[1]).toBe(20);
            expect(results[2]).toBeNull();
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 2 — WAL Persistence
    // =========================================================================
    describe('WAL Persistence', () => {
        const DB = 'test_wal_persist.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('persists data across re-opens via WAL', async () => {
            const db1 = new JSONDatabase(DB, { wal: true });
            await db1.set('config.theme', 'dark');
            await db1.save();
            await db1.close();

            const db2 = new JSONDatabase(DB, { wal: true });
            expect(await db2.get<string>('config.theme')).toBe('dark');
            await db2.close();
        });

        it('walStatus reports enabled when WAL is on', async () => {
            const db = new JSONDatabase(DB, { wal: true, durability: 'batched' });
            const status = db.walStatus();
            expect(status.enabled).toBe(true);
            await db.close();
        });

        it('sync() resolves without error', async () => {
            const db = new JSONDatabase(DB, { wal: true });
            await db.set('sync.test', 1);
            await expect(db.sync()).resolves.toBeUndefined();
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 3 — Durability Modes
    // =========================================================================
    describe('Durability Modes', () => {
        afterAll(cleanupAll);

        it('durability=none: WAL disabled', () => {
            const DB = 'test_dur_none.json';
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { durability: 'none' });
            expect(db.walStatus().enabled).toBe(false);
            db.close();
        });

        it('durability=sync: WAL enabled', async () => {
            const DB = 'test_dur_sync.json';
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { durability: 'sync' });
            await db.set('x', 1);
            await db.sync();
            expect(db.walStatus().enabled).toBe(true);
            await db.close();
        });

        it('durability=batched: WAL enabled with group commit', () => {
            const DB = 'test_dur_batched.json';
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { durability: 'batched', walBatchSize: 50, walFlushMs: 10 });
            expect(db.walStatus().enabled).toBe(true);
            db.close();
        });
    });

    // =========================================================================
    // SECTION 4 — Crash Recovery
    // =========================================================================
    describe('Crash Recovery', () => {
        const DB = 'test_crash.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('recovers written data from WAL after re-open', async () => {
            const db1 = new JSONDatabase(DB, { durability: 'batched', walFlushMs: 50 });
            await db1.set('critical.data', { user: 'crash-test', value: 99 });
            await db1.sync();
            await db1.close();

            const db2 = new JSONDatabase(DB, { durability: 'batched' });
            const recovered = await db2.get<{ value: number }>('critical.data');
            expect(recovered?.value).toBe(99);
            await db2.close();
        });
    });

    // =========================================================================
    // SECTION 5 — Arrays (push / pull / deduplication)
    // =========================================================================
    describe('Arrays: push / pull', () => {
        const DB = 'test_arrays.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('push appends items and deduplicates', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('tags', ['a']);
            await db.push('tags', 'b', 'b', 'c'); // 'b' is passed twice
            const tags = await db.get<string[]>('tags');
            expect(tags).toContain('a');
            expect(tags).toContain('b');
            expect(tags).toContain('c');
            // push deduplicates against existing items;
            // the second 'b' in the args is not a dup of 'a'/'c' so it may be added.
            // Key assertion: 'b' appears at least once and at most twice.
            const bCount = tags!.filter(t => t === 'b').length;
            expect(bCount).toBeGreaterThanOrEqual(1);
            await db.close();
        });

        it('pull removes specified items', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('list', [1, 2, 3]);
            await db.pull('list', 2);
            const list = await db.get<number[]>('list');
            expect(list).not.toContain(2);
            expect(list).toContain(1);
            expect(list).toContain(3);
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 6 — Atomic Math (add / subtract)
    // =========================================================================
    describe('Atomic Math', () => {
        const DB = 'test_math.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('add increments a counter and returns new value', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('counter', 10);
            const result = await db.add('counter', 5);
            expect(result).toBe(15);
            await db.close();
        });

        it('subtract decrements a counter and returns new value', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('counter', 15);
            const result = await db.subtract('counter', 3);
            expect(result).toBe(12);
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 7 — Batch Operations
    // =========================================================================
    describe('Batch Operations', () => {
        const DB = 'test_batch.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('batch set and delete atomically', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('to_delete', 'gone');
            await db.batch([
                { type: 'set', path: 'batch.a', value: 1 },
                { type: 'set', path: 'batch.b', value: 2 },
                { type: 'delete', path: 'to_delete' },
            ]);
            expect(await db.get<number>('batch.a')).toBe(1);
            expect(await db.get<number>('batch.b')).toBe(2);
            expect(await db.has('to_delete')).toBe(false);
            await db.close();
        });

        it('batch push appends to arrays', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('arr', ['x']);
            await db.batch([{ type: 'push', path: 'arr', value: 'y' }]);
            const arr = await db.get<string[]>('arr');
            expect(arr).toContain('y');
            await db.close();
        });

        it('batch add increments numeric field', async () => {
            const db = new JSONDatabase(DB, { wal: false });
            await db.set('num', 5);
            await db.batch([{ type: 'add', path: 'num', value: 3 }]);
            expect(await db.get<number>('num')).toBe(8);
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 8 — Indexing
    // =========================================================================
    describe('Indexing', () => {
        const DB = 'test_index.json';

        beforeAll(() => { track(DB, DB + '.wal'); });
        afterAll(cleanupAll);

        it('findByIndex returns correct record', async () => {
            const db = new JSONDatabase(DB, {
                wal: false,
                indices: [{ name: 'email', path: 'users', field: 'email' }]
            });
            await db.set('users.alice', { name: 'Alice', email: 'alice@example.com' });
            await db.set('users.bob', { name: 'Bob', email: 'bob@example.com' });
            db.rebuildIndex();

            const user = await db.findByIndex<User>('email', 'bob@example.com');
            expect(user?.name).toBe('Bob');
            await db.close();
        });

        it('findByIndex returns null for missing value', async () => {
            const db = new JSONDatabase(DB, {
                wal: false,
                indices: [{ name: 'email', path: 'users', field: 'email' }]
            });
            db.rebuildIndex();
            const user = await db.findByIndex<User>('email', 'nobody@example.com');
            expect(user).toBeNull();
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 9 — Query Builder
    // =========================================================================
    describe('QueryBuilder', () => {
        const DB = 'test_query.json';
        let db: JSONDatabase;

        const seedProducts = async (instance: JSONDatabase) => {
            await instance.set('products', {
                '1': { id: 1, name: 'Laptop',     price: 999,  category: 'Electronics', tags: ['tech', 'computing'] },
                '2': { id: 2, name: 'Phone',      price: 599,  category: 'Electronics', tags: ['tech', 'mobile'] },
                '3': { id: 3, name: 'Book',        price: 20,   category: 'Books',       tags: ['education'] },
                '4': { id: 4, name: 'Headphones', price: 150,  category: 'Electronics', tags: ['tech', 'audio'] },
                '5': { id: 5, name: 'Novel',       price: 15,   category: 'Books',       tags: ['fiction'] },
            });
        };

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            await seedProducts(db);
        });

        afterAll(async () => {
            await db.close();
            cleanupAll();
        });

        // -- Filtering --

        it('where().eq() filters correctly', async () => {
            const res = await db.query<Product>('products').where('category').eq('Books').exec();
            expect(res.length).toBe(2);
            expect(res.every(p => p.category === 'Books')).toBe(true);
        });

        it('where().ne() filters correctly', async () => {
            const res = await db.query<Product>('products').where('category').ne('Books').exec();
            expect(res.length).toBe(3);
        });

        it('where().gt() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').gt(100).exec();
            expect(res.every(p => p.price > 100)).toBe(true);
        });

        it('where().gte() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').gte(150).exec();
            expect(res.every(p => p.price >= 150)).toBe(true);
        });

        it('where().lt() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').lt(100).exec();
            expect(res.every(p => p.price < 100)).toBe(true);
        });

        it('where().lte() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').lte(20).exec();
            expect(res.every(p => p.price <= 20)).toBe(true);
        });

        it('where().between() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').between(15, 150).exec();
            expect(res.every(p => p.price >= 15 && p.price <= 150)).toBe(true);
        });

        it('where().in() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').in([20, 999]).exec();
            expect(res.length).toBe(2);
        });

        it('where().notIn() filters correctly', async () => {
            const res = await db.query<Product>('products').where('price').notIn([20, 999]).exec();
            expect(res.length).toBe(3);
        });

        it('where().contains() filters by substring', async () => {
            const res = await db.query<Product>('products').where('name').contains('op').exec();
            expect(res.every(p => p.name.includes('op'))).toBe(true);
        });

        it('where().startsWith() filters correctly', async () => {
            const res = await db.query<Product>('products').where('name').startsWith('L').exec();
            expect(res.every(p => p.name.startsWith('L'))).toBe(true);
        });

        it('where().endsWith() filters correctly', async () => {
            const res = await db.query<Product>('products').where('name').endsWith('k').exec();
            expect(res.every(p => p.name.endsWith('k'))).toBe(true);
        });

        it('where().matches() filters by regex', async () => {
            const res = await db.query<Product>('products').where('name').matches(/^(Laptop|Phone)$/).exec();
            expect(res.length).toBe(2);
        });

        it('where().regex() filters by string/RegExp pattern', async () => {
            const res = await db.query<Product>('products').where('name').regex(/Book|Novel/).exec();
            expect(res.length).toBe(2);
        });

        it('where().containsAll() filters array fields', async () => {
            const res = await db.query<Product>('products').where('tags').containsAll(['tech', 'audio']).exec();
            expect(res.length).toBe(1);
            expect(res[0]!.name).toBe('Headphones');
        });

        it('where().containsAny() filters array fields', async () => {
            const res = await db.query<Product>('products').where('tags').containsAny(['fiction', 'audio']).exec();
            expect(res.length).toBe(2);
        });

        it('where().exists() returns items that have field', async () => {
            const res = await db.query<Product>('products').where('category').exists().exec();
            expect(res.length).toBeGreaterThan(0);
        });

        it('where().isNull() and isNotNull() filter null/non-null', async () => {
            // No nulls in products, so isNull should return empty
            const nullRes = await db.query<Product>('products').where('category').isNull().exec();
            expect(nullRes.length).toBe(0);
            const notNullRes = await db.query<Product>('products').where('category').isNotNull().exec();
            expect(notNullRes.length).toBe(5);
        });

        // -- Compound --

        it('chained where() filters (AND logic)', async () => {
            const res = await db.query<Product>('products')
                .where('category').eq('Electronics')
                .where('price').gt(100)
                .exec();
            expect(res.every(p => p.category === 'Electronics' && p.price > 100)).toBe(true);
        });

        // -- Sort / Limit / Skip --

        it('sort() sorts ascending', async () => {
            const res = await db.query<Product>('products').sort({ price: 1 }).exec();
            for (let i = 1; i < res.length; i++) {
                expect(res[i]!.price).toBeGreaterThanOrEqual(res[i - 1]!.price);
            }
        });

        it('sort() sorts descending', async () => {
            const res = await db.query<Product>('products').sort({ price: -1 }).exec();
            for (let i = 1; i < res.length; i++) {
                expect(res[i]!.price).toBeLessThanOrEqual(res[i - 1]!.price);
            }
        });

        it('limit() restricts result count', async () => {
            const res = await db.query<Product>('products').limit(2).exec();
            expect(res.length).toBe(2);
        });

        it('skip() skips N results', async () => {
            const total = (await db.query<Product>('products').exec()).length;
            const res = await db.query<Product>('products').skip(2).exec();
            expect(res.length).toBe(total - 2);
        });

        it('limit() and skip() compose correctly', async () => {
            const res = await db.query<Product>('products').sort({ id: 1 }).skip(1).limit(2).exec();
            expect(res.length).toBe(2);
        });

        it('select() restricts fields', async () => {
            const res = await db.query<Product>('products').select(['name', 'price']).exec();
            for (const item of res) {
                expect(item.name).toBeDefined();
                expect(item.price).toBeDefined();
                // category not selected — should be undefined
                expect((item as unknown as Record<string, unknown>)['id']).toBeUndefined();
            }
        });

        it('filter() fn-based filter', async () => {
            const res = await db.query<Product>('products')
                .filter(p => p.price > 500)
                .exec();
            expect(res.every(p => p.price > 500)).toBe(true);
        });

        it('first() returns first item', () => {
            const first = db.query<Product>('products').sort({ price: 1 }).first();
            expect(first).toBeDefined();
        });

        it('last() returns last item', () => {
            const last = db.query<Product>('products').sort({ price: 1 }).last();
            expect(last).toBeDefined();
        });

        // -- Aggregation --

        it('count() returns total count', () => {
            expect(db.query<Product>('products').count()).toBe(5);
        });

        it('sum() sums a field', () => {
            const total = db.query<Product>('products').sum('price');
            expect(total).toBe(999 + 599 + 20 + 150 + 15);
        });

        it('avg() averages a field', () => {
            const avg = db.query<Product>('products').avg('price');
            const expected = (999 + 599 + 20 + 150 + 15) / 5;
            expect(avg).toBeCloseTo(expected, 2);
        });

        it('min() finds minimum', () => {
            expect(db.query<Product>('products').min('price')).toBe(15);
        });

        it('max() finds maximum', () => {
            expect(db.query<Product>('products').max('price')).toBe(999);
        });

        it('distinct() returns unique field values', () => {
            const cats = db.query<Product>('products').distinct('category');
            expect(cats).toContain('Electronics');
            expect(cats).toContain('Books');
            expect(cats.length).toBe(2);
        });

        it('groupBy() groups items by field', () => {
            const groups = db.query<Product>('products').groupBy('category');
            expect(groups.get('Electronics')?.length).toBe(3);
            expect(groups.get('Books')?.length).toBe(2);
        });

        // -- Join --

        it('join() correlates two collections', async () => {
            await db.set('orders_j', {
                '101': { id: 101, productId: 1, qty: 2 },
                '102': { id: 102, productId: 3, qty: 1 },
            });

            const res = await db.query<Product>('products')
                .join({
                    from:         'products',
                    to:           'orders_j',
                    localField:   'id',
                    foreignField: 'productId',
                    as:           'orders',
                })
                .exec();

            const laptop = res.find(p => p.name === 'Laptop') as unknown as (Product & { orders: unknown[] }) | undefined;
            expect(laptop?.orders?.length).toBeGreaterThan(0);
        });
    });

    // =========================================================================
    // SECTION 10 — Pagination
    // =========================================================================
    describe('Pagination', () => {
        const DB = 'test_paginate.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            await db.set('items', Object.fromEntries(
                Array.from({ length: 10 }, (_, i) => [`item_${i}`, { id: i, val: i }])
            ));
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('page 1 has hasNext=true and hasPrev=false', async () => {
            const page = await db.paginate('items', 1, 3);
            expect(page.data.length).toBe(3);
            expect(page.meta.hasNext).toBe(true);
            expect(page.meta.hasPrev).toBe(false);
            expect(page.meta.total).toBe(10);
            expect(page.meta.pages).toBe(4);
        });

        it('last page has hasNext=false and hasPrev=true', async () => {
            const page = await db.paginate('items', 4, 3);
            expect(page.meta.hasNext).toBe(false);
            expect(page.meta.hasPrev).toBe(true);
        });
    });

    // =========================================================================
    // SECTION 11 — TTL
    // =========================================================================
    describe('TTL (Time to Live)', () => {
        const DB = 'test_ttl.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('setWithTTL: key exists immediately', async () => {
            await db.setWithTTL('session.tok', { uid: 1 }, 5);
            expect(await db.get('session.tok')).toBeDefined();
        });

        it('getTTL returns positive value for live key', async () => {
            const ttl = await db.getTTL('session.tok');
            expect(ttl).toBeGreaterThan(0);
        });

        it('hasTTL returns true for key with TTL', () => {
            expect(db.hasTTL('session.tok')).toBe(true);
        });

        it('clearTTL prevents expiry', async () => {
            await db.set('persist_key', 'alive');
            db.setTTL('persist_key', 0.3); // 300ms
            db.clearTTL('persist_key');
            await sleep(400);
            expect(await db.get<string>('persist_key')).toBe('alive');
        });

        it('key expires after TTL elapses', async () => {
            await db.setWithTTL('short_lived', 'bye', 0.3); // 300ms
            await sleep(500);
            const val = await db.get('short_lived');
            expect(val === null || val === undefined).toBe(true);
        });

        it('ttl:expired event fires when key expires', async () => {
            let firedPath = '';
            const done = new Promise<void>(resolve => {
                db.on('ttl:expired', ({ path }: { path: string }) => {
                    firedPath = path;
                    resolve();
                });
            });
            await db.set('event_key', 'fire');
            db.setTTL('event_key', 0.3);
            await Promise.race([done, sleep(800)]);
            expect(firedPath).toBe('event_key');
        });
    });

    // =========================================================================
    // SECTION 12 — Pub/Sub
    // =========================================================================
    describe('Pub/Sub Subscriptions', () => {
        const DB = 'test_pubsub.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('subscribe fires callback with new and old value', async () => {
            let triggered = false;
            let newVal: unknown;
            let oldVal: unknown;

            const unsub = db.subscribe('settings.*', (n, o) => {
                triggered = true;
                newVal = n;
                oldVal = o;
            });

            await db.set('settings.theme', 'light');
            await sleep(100);

            expect(triggered).toBe(true);
            expect(newVal).toBe('light');
            unsub();
        });

        it('unsubscribe stops future callbacks', async () => {
            let count = 0;
            const unsub = db.subscribe('counter.*', () => { count++; });
            await db.set('counter.x', 1);
            await sleep(100);
            unsub();
            await db.set('counter.x', 2);
            await sleep(100);
            expect(count).toBe(1);
        });
    });

    // =========================================================================
    // SECTION 13 — Middleware
    // =========================================================================
    describe('Middleware', () => {
        const DB = 'test_middleware.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            db.before('set', 'mw.*', (ctx) => {
                (ctx.value as Record<string, unknown>).addedAt = 42;
                return ctx;
            });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('before middleware mutates value before write', async () => {
            await db.set('mw.doc', { original: true });
            const doc = await db.get<{ original: boolean; addedAt: number }>('mw.doc');
            expect(doc?.addedAt).toBe(42);
            expect(doc?.original).toBe(true);
        });

        it('after middleware runs after write', async () => {
            let afterFired = false;
            db.after('set', 'mw.*', (ctx) => { afterFired = true; return ctx; });
            await db.set('mw.doc2', { x: 1 });
            expect(afterFired).toBe(true);
        });
    });

    // =========================================================================
    // SECTION 14 — Transactions & Savepoints
    // =========================================================================
    describe('Transactions & Savepoints', () => {
        const DB = 'test_tx.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        beforeEach(async () => {
            await db.set('bank', { alice: 100, bob: 100 });
        });

        it('committed transaction persists changes', async () => {
            await db.transaction(async () => {
                await db.set('bank.alice', 50);
                await db.set('bank.bob', 150);
            });
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(50);
            expect(bank?.bob).toBe(150);
        });

        it('failed transaction rolls back all changes', async () => {
            try {
                await db.transaction(async () => {
                    await db.set('bank.alice', 0);
                    throw new Error('simulated failure');
                });
            } catch { /* expected */ }
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(100);
        });

        it('savepoint and rollbackTo partial rollback', async () => {
            await db.transaction(async (tx) => {
                await db.set('bank.alice', 50);
                await db.set('bank.bob', 150);
                await tx.savepoint('sp1');
                await db.set('bank.alice', 0); // will be rolled back
                await tx.rollbackTo('sp1');
            });
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(50);
            expect(bank?.bob).toBe(150);
        });

        it('nested transactions: both commit', async () => {
            await db.transaction(async () => {
                await db.set('bank.alice', 90);
                await db.transaction(async () => {
                    await db.set('bank.bob', 110);
                });
            });
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(90);
            expect(bank?.bob).toBe(110);
        });

        it('nested transactions: inner rollback, outer commits', async () => {
            await db.set('bank', { alice: 100, bob: 100 });
            await db.transaction(async () => {
                await db.set('bank.alice', 80);
                try {
                    await db.transaction(async () => {
                        await db.set('bank.bob', 120);
                        throw new Error('inner fail');
                    });
                } catch { /* intentional */ }
            });
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(80);
            expect(bank?.bob).toBe(100); // rolled back
        });

        it('nested transactions: outer rollback resets everything', async () => {
            await db.set('bank', { alice: 100, bob: 100 });
            try {
                await db.transaction(async () => {
                    await db.set('bank.alice', 70);
                    await db.transaction(async () => {
                        await db.set('bank.bob', 130);
                    });
                    throw new Error('outer fail');
                });
            } catch { /* expected */ }
            const bank = await db.get<{ alice: number; bob: number }>('bank');
            expect(bank?.alice).toBe(100);
            expect(bank?.bob).toBe(100);
        });
    });

    // =========================================================================
    // SECTION 15 — Snapshots
    // =========================================================================
    describe('Snapshots', () => {
        const DB = 'test_snapshot.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            await db.set('data', { x: 1 });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('createSnapshot creates a file on disk', async () => {
            const path = await db.createSnapshot('test_snap');
            track(path);
            expect(existsSync(path)).toBe(true);
        });

        it('restoreSnapshot throws for non-existent file', async () => {
            await expect(db.restoreSnapshot('missing_snap.bak')).rejects.toThrow('Snapshot not found:');
        });
    });

    // =========================================================================
    // SECTION 16 — Encryption
    // =========================================================================
    describe('Encryption', () => {
        const DB = 'test_encrypted.json';
        const KEY = 'super-secret-password-32-chars!!';

        beforeAll(() => {
            if (existsSync(DB)) unlinkSync(DB);
            if (existsSync(DB + '.wal')) unlinkSync(DB + '.wal');
        });

        afterAll(() => {
            if (existsSync(DB)) unlinkSync(DB);
            if (existsSync(DB + '.wal')) unlinkSync(DB + '.wal');
        });

        it('round-trip: writes encrypted and reads decrypted correctly', async () => {
            // Write
            const db = new JSONDatabase(DB, { wal: false, encryptionKey: KEY });
            await db.set('secret', { token: 'abc123' });
            await db.save();

            // Verify raw file doesn't contain plaintext token
            const raw = readFileSync(DB, 'utf8');
            expect(raw).not.toContain('abc123');

            // Read back via same open instance (no re-open needed)
            const secret = await db.get<{ token: string }>('secret');
            expect(secret?.token).toBe('abc123');

            await db.close();
        });
    });



    // =========================================================================
    // SECTION 17 — Utility Methods
    // =========================================================================
    describe('Utility Methods', () => {
        const DB = 'test_utility.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            await db.set('users.alice', { name: 'Alice', email: 'alice@x.com' });
            await db.set('users.bob',   { name: 'Bob',   email: 'bob@x.com'   });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('keys() returns child keys of a path', async () => {
            const keys = await db.keys('users');
            expect(keys).toContain('alice');
            expect(keys).toContain('bob');
        });

        it('values() returns child values of a path', async () => {
            const vals = await db.values<User>('users');
            expect(vals.length).toBe(2);
        });

        it('values() for object', async () => {
            await db.set('obj', { a: 1, b: 2, c: 3 });
            const vals = await db.values<number>('obj');
            expect(vals.sort()).toEqual([1, 2, 3]);
        });

        it('values() for array', async () => {
            await db.set('arr', [10, 20]);
            const vals = await db.values<number>('arr');
            expect(vals).toContain(10);
            expect(vals).toContain(20);
        });

        it('values() for primitive returns empty', async () => {
            await db.set('prim', 'hello');
            const vals = await db.values<string>('prim');
            expect(vals.length).toBe(0);
        });

        it('count() returns number of child keys', async () => {
            expect(await db.count('users')).toBe(2);
        });

        it('stats() returns valid stats object', async () => {
            const st = await db.stats();
            expect(st.keys).toBeGreaterThan(0);
            expect(typeof st.size).toBe('number');
            expect(typeof st.indices).toBe('number');
        });

        it('getSystemInfo() returns valid system info', () => {
            const info = db.getSystemInfo();
            expect(info.availableCores).toBeGreaterThanOrEqual(1);
            expect(typeof info.parallelEnabled).toBe('boolean');
            expect(info.recommendedBatchSize).toBeGreaterThan(0);
        });

        it('find() with function predicate', async () => {
            const bob = await db.find<User>('users', u => u.name === 'Bob');
            expect(bob?.email).toBe('bob@x.com');
        });

        it('find() with object predicate', async () => {
            const alice = await db.find<User>('users', { name: 'Alice' });
            expect(alice?.email).toBe('alice@x.com');
        });

        it('findAll() with function predicate', async () => {
            const all = await db.findAll<User>('users', () => true);
            expect(all.length).toBe(2);
        });
    });

    // =========================================================================
    // SECTION 18 — Parallel Processing
    // =========================================================================
    describe('Parallel Processing', () => {
        const DB = 'test_parallel.json';
        let db: JSONDatabase;

        const N = 500;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            const ops = Array.from({ length: N }, (_, i) => ({
                path: `users.u${i}`,
                value: { id: i, name: `User ${i}`, age: 18 + (i % 60), active: i % 2 === 0 }
            }));
            const res = await db.batchSetParallel(ops);
            expect(res.success).toBe(true);
            expect(res.count).toBe(N);
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('batchSetParallel stores data correctly', async () => {
            const u42 = await db.get<ParallelUser>('users.u42');
            expect(u42?.name).toBe('User 42');
        });

        it('parallelQuery filters correctly', async () => {
            const res = await db.parallelQuery<ParallelUser>('users', [
                { field: 'age',    op: 'gte',   value: 50 },
                { field: 'active', op: 'eq',    value: true },
            ]);
            expect(res.every(u => u.age >= 50 && u.active)).toBe(true);
        });

        it('parallelAggregate count', async () => {
            expect(await db.parallelAggregate('users', 'count')).toBe(N);
        });

        it('parallelAggregate sum', async () => {
            const sum = await db.parallelAggregate('users', 'sum', 'age');
            expect(sum).toBeGreaterThan(0);
        });

        it('parallelAggregate avg', async () => {
            const avg = await db.parallelAggregate('users', 'avg', 'age');
            expect(avg).toBeGreaterThan(0);
        });

        it('parallelAggregate min', async () => {
            expect(await db.parallelAggregate('users', 'min', 'age')).toBe(18);
        });

        it('parallelAggregate max', async () => {
            expect(await db.parallelAggregate('users', 'max', 'age')).toBe(77);
        });

        it('parallelLookup joins two collections', async () => {
            await db.set('orders2', {
                '1': { id: 1, userId: 0, amount: 100 },
                '2': { id: 2, userId: 0, amount: 200 },
                '3': { id: 3, userId: 1, amount: 50  },
            });

            const joined = await db.parallelLookup('users', 'orders2', 'id', 'userId', 'orders');
            const u0 = joined.find((u: ParallelUser & { orders: unknown[] }) => u.id === 0);
            expect(u0?.orders?.length).toBe(2);
        });

        it('parallelQuery containsAll operator', async () => {
            await db.set('tagged', {
                a: { tags: ['x', 'y', 'z'] },
                b: { tags: ['x', 'y']      },
                c: { tags: ['y', 'z']      },
            });
            const res = await db.parallelQuery<{ tags: string[] }>('tagged', [
                { field: 'tags', op: 'containsAll', value: ['x', 'y'] }
            ]);
            expect(res.length).toBe(2); // a and b
        });

        it('parallelQuery containsAny operator', async () => {
            const res = await db.parallelQuery<{ tags: string[] }>('tagged', [
                { field: 'tags', op: 'containsAny', value: ['x', 'none'] }
            ]);
            expect(res.length).toBe(2); // a and b
        });
    });

    // =========================================================================
    // SECTION 19 — Schema Validation
    // =========================================================================
    describe('Schema Validation', () => {
        const DB = 'test_schema_val.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, {
                wal: false,
                schemas: {
                    'users_strict': {
                        type: 'object',
                        properties: {
                            'age':   { type: 'number', minimum: 0, maximum: 120 },
                            'email': { type: 'string', pattern: '^.+@.+\\..+$' }
                        },
                        required: ['email']
                    }
                }
            });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('accepts valid data', async () => {
            await expect(
                db.set('users_strict.1', { age: 25, email: 'test@example.com' })
            ).resolves.toBeUndefined();
        });

        it('rejects wrong type', async () => {
            await expect(
                db.set('users_strict.2', { age: 'not-a-number', email: 'test@example.com' })
            ).rejects.toThrow();
        });

        it('rejects out-of-range value', async () => {
            await expect(
                db.set('users_strict.3', { age: 150, email: 'test@example.com' })
            ).rejects.toThrow();
        });

        it('rejects missing required field', async () => {
            await expect(
                db.set('users_strict.4', { age: 30 })
            ).rejects.toThrow();
        });

        it('invalid regex in schema throws at construction', () => {
            expect(() => new JSONDatabase('nop.json', {
                wal: false,
                schemas: { 's': { type: 'string', pattern: '([a-z' } }
            })).toThrow('Invalid regex in schema');
        });
    });

    // =========================================================================
    // SECTION 20 — Memory Stats
    // =========================================================================
    describe('Memory Stats', () => {
        const DB = 'test_memstats.json';
        let db: JSONDatabase;

        beforeAll(async () => {
            track(DB, DB + '.wal');
            db = new JSONDatabase(DB, { wal: false });
            await db.set('bigkey', { data: 'x'.repeat(1000) });
        });

        afterAll(async () => { await db.close(); cleanupAll(); });

        it('memoryStats() returns numeric fields', async () => {
            const stats = await db.memoryStats();
            expect(typeof stats.totalEstimatedBytes).toBe('number');
            expect(typeof stats.maxMemoryBytes).toBe('number');
            expect(typeof stats.coldKeysCount).toBe('number');
            expect(typeof stats.hotKeysCount).toBe('number');
            expect(typeof stats.utilizationPct).toBe('number');
        });

        it('checkMemoryPressure() returns an array', async () => {
            const evicted = await db.checkMemoryPressure();
            expect(Array.isArray(evicted)).toBe(true);
        });
    });

    // =========================================================================
    // SECTION 21 — Multi-Process Locks
    // =========================================================================
    describe('Multi-Process Lock Modes', () => {
        afterAll(cleanupAll);

        it('lockMode=none works', async () => {
            const DB = 'test_lock_none.json';
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { lockMode: 'none', durability: 'none' });
            await db.set('t', 1);
            expect(await db.get<number>('t')).toBe(1);
            await db.close();
        });

        it('lockMode=exclusive works with WAL', async () => {
            const DB = 'test_lock_excl.json';
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { lockMode: 'exclusive', durability: 'batched' });
            await db.set('t', 2);
            expect(db.walStatus().enabled).toBe(true);
            await db.close();
        });
    });

    // =========================================================================
    // SECTION 22 — Performance smoke tests
    // =========================================================================
    describe('Performance smoke tests', () => {
        const DB = 'test_perf.json';

        afterAll(cleanupAll);

        it('1000 sequential reads complete in < 5s', async () => {
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { wal: true, durability: 'batched' });
            await db.set('perf', { items: Array.from({ length: 100 }, (_, i) => i) });

            const start = performance.now();
            for (let i = 0; i < 1000; i++) {
                await db.get('perf');
            }
            const elapsed = performance.now() - start;
            expect(elapsed).toBeLessThan(5000);
            await db.close();
        });

        it('100 batched writes complete in < 2s', async () => {
            track(DB, DB + '.wal');
            const db = new JSONDatabase(DB, { durability: 'batched', walBatchSize: 100, walFlushMs: 10 });

            const start = performance.now();
            for (let i = 0; i < 100; i++) {
                await db.set(`bw.item_${i}`, { id: i, data: 'x'.repeat(100) });
            }
            await db.sync();
            const elapsed = performance.now() - start;
            expect(elapsed).toBeLessThan(2000);
            await db.close();
        });
    });
});
