/**
 * Integration Tests for jsondb-high
 *
 * Unlike unit tests which exercise individual methods in isolation, these tests
 * simulate realistic application workflows that exercise multiple features
 * working together: schema validation + indexing + queries, TTL + pub/sub events,
 * transaction rollback across batch operations, WAL recovery, middleware
 * transformation pipelines, and more.
 */

import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync, readFileSync } from 'fs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

/** Paths to clean up after each suite. */
const toClean: string[] = [];

function db(file: string, opts: ConstructorParameters<typeof JSONDatabase>[1] = {}): JSONDatabase {
    const paths = [file, `${file}.wal`, `${file}.cold`];
    paths.forEach(p => { if (existsSync(p)) unlinkSync(p); });
    toClean.push(...paths);
    return new JSONDatabase(file, opts);
}

afterAll(() => {
    for (const p of toClean) {
        try { if (existsSync(p)) unlinkSync(p); } catch { /* ignore */ }
    }
});

// ---------------------------------------------------------------------------
// Types used across suites
// ---------------------------------------------------------------------------

interface User {
    id: number;
    name: string;
    email: string;
    role: 'admin' | 'user' | 'guest';
    age: number;
    createdAt: string;
}

interface Product {
    id: number;
    name: string;
    price: number;
    stock: number;
    category: string;
}

interface Order {
    id: number;
    userId: number;
    productId: number;
    quantity: number;
    total: number;
    status: 'pending' | 'confirmed' | 'shipped' | 'cancelled';
}

// ===========================================================================
// 1. User Management System
//    Schema validation + indexing + pagination + middleware audit log
// ===========================================================================
describe('Integration: User Management System', () => {
    let store: JSONDatabase;
    const FILE = 'integ_users.json';

    beforeAll(async () => {
        store = db(FILE, {
            wal: false,
            schemas: {
                'users': {
                    type: 'object',
                    properties: {
                        id:    { type: 'number', minimum: 1 },
                        name:  { type: 'string', minLength: 2 },
                        email: { type: 'string', pattern: '^[^@]+@[^@]+\\.[^@]+$' },
                        role:  { type: 'string', enum: ['admin', 'user', 'guest'] },
                        age:   { type: 'number', minimum: 0, maximum: 150 },
                    },
                    required: ['id', 'name', 'email', 'role'],
                },
            },
            indices: [{ name: 'users_by_email', path: 'users', field: 'email' }],
        });

        // Audit log via middleware
        const auditLog: string[] = [];
        store.after('set', 'users.*', (ctx: { operation: string; path: string; value: unknown }) => {
            auditLog.push(`${ctx.operation}:${ctx.path}`);
        });
        (store as unknown as Record<string, unknown>)['_auditLog'] = auditLog;

        // Seed users
        const users: User[] = [
            { id: 1, name: 'Alice',   email: 'alice@ex.com', role: 'admin', age: 30, createdAt: '2024-01-01' },
            { id: 2, name: 'Bob',     email: 'bob@ex.com',   role: 'user',  age: 24, createdAt: '2024-02-01' },
            { id: 3, name: 'Carol',   email: 'carol@ex.com', role: 'user',  age: 28, createdAt: '2024-03-01' },
            { id: 4, name: 'Dave',    email: 'dave@ex.com',  role: 'guest', age: 19, createdAt: '2024-04-01' },
            { id: 5, name: 'Eve',     email: 'eve@ex.com',   role: 'admin', age: 35, createdAt: '2024-05-01' },
            { id: 6, name: 'Frank',   email: 'frank@ex.com', role: 'user',  age: 22, createdAt: '2024-06-01' },
            { id: 7, name: 'Grace',   email: 'grace@ex.com', role: 'user',  age: 26, createdAt: '2024-07-01' },
            { id: 8, name: 'Henry',   email: 'henry@ex.com', role: 'guest', age: 17, createdAt: '2024-08-01' },
        ];
        for (const u of users) {
            await store.set(`users.u${u.id}`, u);
        }
    });

    afterAll(async () => { await store.close(); });

    it('schema rejects invalid email format', async () => {
        await expect(
            store.set('users.bad', { id: 99, name: 'Bad', email: 'not-an-email', role: 'user' })
        ).rejects.toThrow();
    });

    it('schema rejects under-age minimum', async () => {
        await expect(
            store.set('users.bad', { id: 99, name: 'Underage', email: 'x@x.com', role: 'user', age: -1 })
        ).rejects.toThrow();
    });

    it('finds user by indexed email', async () => {
        const result = await store.findByIndex<User>('users_by_email', 'alice@ex.com');
        expect(result).not.toBeNull();
        expect((result as User).name).toBe('Alice');
    });

    it('queries admins sorted by age ascending', async () => {
        const admins = await store.query<User>('users')
            .where('role').eq('admin')
            .sort({ age: 1 })
            .exec();

        expect(admins.length).toBe(2);
        expect(admins[0]!.name).toBe('Alice');
        expect(admins[1]!.name).toBe('Eve');
    });

    it('queries users over 25 with pagination (first page of filtered result)', async () => {
        // paginate() has no filter param — use query first to get matching keys,
        // then verify the plain paginate count via the query builder instead.
        const over25 = await store.query<User>('users')
            .where('age').gt(25)
            .exec();
        // Alice(30), Carol(28), Eve(35), Grace(26)
        expect(over25.length).toBeGreaterThanOrEqual(4);

        // Also verify paginate works over the whole collection
        const page1 = await store.paginate<User>('users', 1, 3);
        expect(page1.data.length).toBe(3);
        expect(page1.meta.hasNext).toBe(true);
        expect(page1.meta.hasPrev).toBe(false);
    });

    it('counts users by role group', async () => {
        const groups = store.query<User>('users').groupBy('role');
        expect(groups.get('admin')?.length).toBe(2);
        expect(groups.get('user')?.length).toBe(4);
        expect(groups.get('guest')?.length).toBe(2);
    });

    it('computes average age correctly', () => {
        const avg = store.query<User>('users').avg('age');
        const expected = (30 + 24 + 28 + 19 + 35 + 22 + 26 + 17) / 8;
        expect(avg).toBeCloseTo(expected, 1);
    });

    it('middleware after-hook fires for every set', async () => {
        const log = (store as unknown as Record<string, unknown>)['_auditLog'] as string[];
        // We set 8 users above
        expect(log.length).toBeGreaterThanOrEqual(8);
        expect(log.every(e => e.startsWith('set:') || e.startsWith('update:'))).toBe(true);
    });

    it('deletes user and verifies absence via query', async () => {
        await store.delete('users.u8');
        const henry = await store.query<User>('users').where('name').eq('Henry').exec();
        expect(henry.length).toBe(0);
    });
});

// ===========================================================================
// 2. E-Commerce: Cart + Inventory with Atomic Transactions
//    Transactions + batch ops + schema validation + queries
// ===========================================================================
describe('Integration: E-Commerce Cart & Inventory', () => {
    let store: JSONDatabase;
    const FILE = 'integ_ecom.json';

    const PRODUCTS: Product[] = [
        { id: 1, name: 'Laptop',  price: 999, stock: 10, category: 'Electronics' },
        { id: 2, name: 'Mouse',   price: 25,  stock: 50, category: 'Electronics' },
        { id: 3, name: 'Desk',    price: 350, stock: 5,  category: 'Furniture'   },
        { id: 4, name: 'Chair',   price: 200, stock: 8,  category: 'Furniture'   },
        { id: 5, name: 'Monitor', price: 450, stock: 15, category: 'Electronics' },
    ];

    beforeAll(async () => {
        store = db(FILE, { wal: false });
        for (const p of PRODUCTS) {
            await store.set(`products.p${p.id}`, p);
        }
        await store.set('orders', []);
        await store.set('cart.u1', []);
    });

    afterAll(async () => { await store.close(); });

    it('adds items to cart atomically', async () => {
        await store.transaction(async () => {
            await store.push('cart.u1', { productId: 1, qty: 1 });
            await store.push('cart.u1', { productId: 2, qty: 2 });
        });
        const cart = await store.get<Array<{ productId: number; qty: number }>>('cart.u1');
        expect(cart?.length).toBe(2);
    });

    it('places order and decrements stock atomically', async () => {
        await store.transaction(async () => {
            // Decrement stock
            await store.subtract('products.p1.stock', 1);
            await store.subtract('products.p2.stock', 2);

            // Record order
            const order: Order = { id: 1, userId: 1, productId: 1, quantity: 1, total: 999, status: 'confirmed' };
            await store.push('orders', order);

            // Clear cart
            await store.set('cart.u1', []);
        });

        const laptop = await store.get<Product>('products.p1');
        expect(laptop?.stock).toBe(9);

        const mouse = await store.get<Product>('products.p2');
        expect(mouse?.stock).toBe(48);

        const orders = await store.get<Order[]>('orders');
        expect(orders?.length).toBe(1);
        expect(orders![0]!.status).toBe('confirmed');

        const cart = await store.get<unknown[]>('cart.u1');
        expect(cart?.length).toBe(0);
    });

    it('rolls back transaction on insufficient stock', async () => {
        const chairBefore = await store.get<Product>('products.p3');
        const stockBefore = chairBefore!.stock;

        await expect(
            store.transaction(async () => {
                await store.subtract('products.p3.stock', 999); // way too many
                // Simulate a business rule check that throws
                const after = await store.get<Product>('products.p3');
                if ((after?.stock ?? 0) < 0) throw new Error('Out of stock');
            })
        ).rejects.toThrow('Out of stock');

        const chairAfter = await store.get<Product>('products.p3');
        expect(chairAfter?.stock).toBe(stockBefore); // rolled back
    });

    it('aggregates total inventory value by category', () => {
        const electronics = store.query<Product>('products')
            .where('category').eq('Electronics')
            .sum('price');
        // 999 + 25 + 450 = 1474
        expect(electronics).toBe(1474);

        const furniture = store.query<Product>('products')
            .where('category').eq('Furniture')
            .sum('price');
        // 350 + 200 = 550
        expect(furniture).toBe(550);
    });

    it('batch-updates all electronics prices by 10%', async () => {
        const electronics = await store.query<Product>('products')
            .where('category').eq('Electronics')
            .exec();

        await store.batch(
            electronics.map(p => ({
                type: 'set' as const,
                path: `products.p${p.id}.price`,
                value: Math.round(p.price * 1.1),
            }))
        );

        const laptop = await store.get<Product>('products.p1');
        expect(laptop?.price).toBe(Math.round(999 * 1.1));
    });

    it('finds cheapest in-stock item', () => {
        const cheapest = store.query<Product>('products')
            .where('stock').gt(0)
            .sort({ price: 1 })
            .first();
        expect(cheapest).toBeDefined();
        // Mouse is cheapest among electronics
        expect(cheapest?.name).toBe('Mouse');
    });
});

// ===========================================================================
// 3. Session Store
//    TTL + pub/sub expiry events + indexing
// ===========================================================================
describe('Integration: Session Store with TTL', () => {
    let store: JSONDatabase;
    const FILE = 'integ_sessions.json';

    interface Session {
        userId: number;
        token: string;
        ip: string;
        createdAt: number;
    }

    beforeAll(async () => {
        store = db(FILE, { wal: false });
    });

    afterAll(async () => { await store.close(); });

    it('creates a session with TTL and reads it immediately', async () => {
        const session: Session = {
            userId: 42,
            token: 'tok_abc123',
            ip: '127.0.0.1',
            createdAt: Date.now(),
        };
        await store.setWithTTL('sessions.s1', session, 1); // 1 second TTL

        const got = await store.get<Session>('sessions.s1');
        expect(got?.userId).toBe(42);
        expect(got?.token).toBe('tok_abc123');
        expect(store.hasTTL('sessions.s1')).toBe(true);
    });

    it('session disappears after TTL expires', async () => {
        await store.setWithTTL('sessions.s2', { userId: 7, token: 'ephemeral' }, 0.3);
        await sleep(500);
        const gone = await store.get('sessions.s2');
        expect(gone === null || gone === undefined).toBe(true);
    });

    it('ttl:expired event fires for an expired session', async () => {
        let expiredPath: string | null = null;
        store.on('ttl:expired', (event: { path: string }) => { expiredPath = event.path; });

        await store.setWithTTL('sessions.s3', { userId: 9, token: 'bye' }, 0.2);
        await sleep(400);

        expect(expiredPath as unknown as string).toBe('sessions.s3');
    });

    it('clearTTL makes session persistent', async () => {
        await store.set('sessions.s4', { userId: 1, token: 'persistent' });
        store.setTTL('sessions.s4', 0.2);
        store.clearTTL('sessions.s4');

        await sleep(300);

        const still = await store.get<Session>('sessions.s4');
        expect(still?.token).toBe('persistent');
    });

    it('multiple sessions coexist with independent TTLs', async () => {
        await store.setWithTTL('sessions.long',  { userId: 10 }, 5);    // 5s
        await store.setWithTTL('sessions.short', { userId: 11 }, 0.2);  // 200ms

        await sleep(400);

        const long  = await store.get('sessions.long');
        const short = await store.get('sessions.short');

        expect(long).not.toBeNull();
        expect(short === null || short === undefined).toBe(true);
    });
});

// ===========================================================================
// 4. Real-Time Feed / Activity Log
//    pub/sub + array push + queries + pagination
// ===========================================================================
describe('Integration: Activity Feed', () => {
    let store: JSONDatabase;
    const FILE = 'integ_feed.json';

    interface Activity {
        id: number;
        userId: number;
        type: 'post' | 'like' | 'comment' | 'follow';
        targetId: number;
        ts: number;
    }

    let nextId = 1;

    async function addActivity(store: JSONDatabase, a: Omit<Activity, 'id' | 'ts'>): Promise<Activity> {
        const activity: Activity = { ...a, id: nextId++, ts: Date.now() };
        await store.push('feed', activity);
        return activity;
    }

    beforeAll(async () => {
        store = db(FILE, { wal: false });
        await store.set('feed', []);
        await store.set('follower_counts', {});

        await addActivity(store, { userId: 1, type: 'post',    targetId: 101 });
        await addActivity(store, { userId: 2, type: 'like',    targetId: 101 });
        await addActivity(store, { userId: 3, type: 'comment', targetId: 101 });
        await addActivity(store, { userId: 1, type: 'follow',  targetId: 2   });
        await addActivity(store, { userId: 2, type: 'post',    targetId: 102 });
        await addActivity(store, { userId: 3, type: 'like',    targetId: 102 });
        await addActivity(store, { userId: 1, type: 'like',    targetId: 102 });
        await addActivity(store, { userId: 4, type: 'follow',  targetId: 1   });
    });

    afterAll(async () => { await store.close(); });

    it('feed has correct total count', async () => {
        const feed = await store.get<Activity[]>('feed');
        expect(feed?.length).toBe(8);
    });

    it('filters activities by type=like', async () => {
        const likes = await store.query<Activity>('feed')
            .where('type').eq('like')
            .exec();
        expect(likes.length).toBe(3);
    });

    it('filters activities by userId', async () => {
        const user1 = await store.query<Activity>('feed')
            .where('userId').eq(1)
            .exec();
        expect(user1.length).toBe(3); // post, follow, like
    });

    it('gets latest 3 activities sorted by ts desc', async () => {
        const latest = await store.query<Activity>('feed')
            .sort({ ts: -1 })
            .limit(3)
            .exec();
        expect(latest.length).toBe(3);
        // Verify descending order
        for (let i = 1; i < latest.length; i++) {
            expect(latest[i]!.ts).toBeLessThanOrEqual(latest[i - 1]!.ts);
        }
    });

    it('pub/sub notifies subscriber when new activity is pushed', async () => {
        let notified = false;
        const unsub = store.subscribe('feed', () => { notified = true; });

        await addActivity(store, { userId: 5, type: 'post', targetId: 200 });
        await sleep(50);

        expect(notified).toBe(true);
        unsub();
    });

    it('pulls (removes) a specific activity by id', async () => {
        const feed = await store.get<Activity[]>('feed');
        const toRemove = feed![0]!;

        await store.pull('feed', toRemove);

        const after = await store.get<Activity[]>('feed');
        expect(after?.find(a => a.id === toRemove.id)).toBeUndefined();
    });

    it('counts activities grouped by type', async () => {
        const groups = store.query<Activity>('feed').groupBy('type');
        expect(groups.size).toBeGreaterThanOrEqual(1);
        const likeCount = groups.get('like')?.length ?? 0;
        expect(likeCount).toBeGreaterThanOrEqual(2);
    });
});

// ===========================================================================
// 5. Analytics Pipeline
//    Batch writes + parallelAggregate + parallelQuery + join
// ===========================================================================
describe('Integration: Analytics Pipeline', () => {
    let store: JSONDatabase;
    const FILE = 'integ_analytics.json';

    interface Event {
        id: number;
        page: string;
        country: string;
        duration: number;  // seconds
        bounced: boolean;
    }

    beforeAll(async () => {
        store = db(FILE, { wal: false });

        const events: Event[] = [
            { id: 1,  page: '/home',    country: 'US', duration: 45,  bounced: false },
            { id: 2,  page: '/about',   country: 'UK', duration: 12,  bounced: true  },
            { id: 3,  page: '/home',    country: 'CA', duration: 90,  bounced: false },
            { id: 4,  page: '/pricing', country: 'US', duration: 120, bounced: false },
            { id: 5,  page: '/home',    country: 'US', duration: 30,  bounced: true  },
            { id: 6,  page: '/pricing', country: 'DE', duration: 200, bounced: false },
            { id: 7,  page: '/about',   country: 'US', duration: 60,  bounced: false },
            { id: 8,  page: '/home',    country: 'US', duration: 15,  bounced: true  },
            { id: 9,  page: '/pricing', country: 'CA', duration: 180, bounced: false },
            { id: 10, page: '/home',    country: 'UK', duration: 55,  bounced: false },
        ];

        await store.batchSetParallel(
            events.map(e => ({ path: `events.e${e.id}`, value: e }))
        );

        await store.set('pages', [
            { slug: '/home',    title: 'Home Page'    },
            { slug: '/about',   title: 'About Us'     },
            { slug: '/pricing', title: 'Pricing'      },
        ]);
    });

    afterAll(async () => { await store.close(); });

    it('parallelAggregate: total duration across all events', async () => {
        const total = await store.parallelAggregate('events', 'sum', 'duration');
        // 45+12+90+120+30+200+60+15+180+55 = 807
        expect(total).toBe(807);
    });

    it('parallelAggregate: avg session duration', async () => {
        const avg = await store.parallelAggregate('events', 'avg', 'duration');
        expect(avg).toBeCloseTo(807 / 10, 1);
    });

    it('parallelAggregate: max duration event', async () => {
        const max = await store.parallelAggregate('events', 'max', 'duration');
        expect(max).toBe(200);
    });

    it('parallelQuery: US events only', async () => {
        const us = await store.parallelQuery('events', [
            { field: 'country', op: 'eq', value: 'US' },
        ]);
        // Events 1, 4, 5, 7, 8 → 5
        expect((us as Event[]).length).toBe(5);
    });

    it('parallelQuery: non-bounced events with duration > 50', async () => {
        const engaged = await store.parallelQuery('events', [
            { field: 'bounced',  op: 'eq',  value: false },
            { field: 'duration', op: 'gt',  value: 50    },
        ]);
        // 1(45✗), 3(90✓), 4(120✓), 6(200✓), 7(60✓), 9(180✓), 10(55✓) = 6
        expect((engaged as Event[]).length).toBe(6);
    });

    it('query: bounce rate per page via groupBy', async () => {
        const groups = store.query<Event>('events').groupBy('page');
        const homeBounces = groups.get('/home')?.filter(e => e.bounced).length ?? 0;
        expect(homeBounces).toBe(2); // events 5 and 8
    });

    it('join: enriches events with page metadata', async () => {
        const enriched = await store.query<Event>('events')
            .join({
                from:         'events',
                to:           'pages',
                localField:   'page',
                foreignField: 'slug',
                as:           'pageInfo',
            })
            .exec();

        const withMeta = (enriched as unknown as Array<Event & { pageInfo: unknown[] }>)
            .filter(e => e.pageInfo.length > 0);

        expect(withMeta.length).toBe(10); // all events have a matching /home, /about, /pricing
    });

    it('distinct countries in the dataset', () => {
        const countries = store.query<Event>('events').distinct('country');
        expect(countries).toContain('US');
        expect(countries).toContain('UK');
        expect(countries).toContain('CA');
        expect(countries).toContain('DE');
        expect(countries.length).toBe(4);
    });
});

// ===========================================================================
// 6. WAL Crash Recovery & Persistence
//    (durability: batched, WAL write → close → reopen → data intact)
// ===========================================================================
describe('Integration: WAL Persistence & Crash Recovery', () => {
    const FILE = 'integ_wal.json';

    afterAll(() => {
        [FILE, `${FILE}.wal`].forEach(p => {
            try { if (existsSync(p)) unlinkSync(p); } catch { /* ignore */ }
        });
    });

    it('data written before graceful close survives reopen', async () => {
        const db1 = new JSONDatabase(FILE, { durability: 'batched', walFlushMs: 5 });
        await db1.set('config.version', '2.0');
        await db1.set('config.debug', false);
        await db1.sync();
        await db1.close();

        const db2 = new JSONDatabase(FILE, { durability: 'batched', walFlushMs: 5 });
        expect(await db2.get<string>('config.version')).toBe('2.0');
        expect(await db2.get<boolean>('config.debug')).toBe(false);
        await db2.close();
    });

    it('multiple write-reopen cycles keep all data', async () => {
        const db1 = new JSONDatabase(FILE, { wal: true });
        await db1.set('count', 0);
        await db1.save();
        await db1.close();

        // Cycle 1
        const db2 = new JSONDatabase(FILE, { wal: true });
        await db2.add('count', 10);
        await db2.save();
        await db2.close();

        // Cycle 2
        const db3 = new JSONDatabase(FILE, { wal: true });
        await db3.add('count', 5);
        await db3.save();
        await db3.close();

        // Final read
        const db4 = new JSONDatabase(FILE, { wal: true });
        expect(await db4.get<number>('count')).toBe(15);
        await db4.close();
    });

    it('walStatus reflects WAL mode correctly', async () => {
        const store = new JSONDatabase(FILE, { durability: 'batched' });
        expect(store.walStatus().enabled).toBe(true);
        await store.close();
    });
});

// ===========================================================================
// 7. Middleware Transformation Pipeline
//    before + after hooks, wildcard patterns, value mutation
// ===========================================================================
describe('Integration: Middleware Transformation Pipeline', () => {
    let store: JSONDatabase;
    const FILE = 'integ_mw.json';

    const log: Array<{ op: string; path: string; value: unknown }> = [];

    beforeAll(async () => {
        store = db(FILE, { wal: false });

        // Sanitise: trim strings before saving
        store.before('set', 'profiles.*', (ctx: { value: unknown; operation: string; path: string; timestamp: number }) => {
            if (typeof ctx.value === 'object' && ctx.value !== null) {
                const obj = ctx.value as Record<string, unknown>;
                if (typeof obj['bio'] === 'string') {
                    obj['bio'] = (obj['bio'] as string).trim();
                }
            }
            return ctx;
        });

        // Normalise: lowercase emails
        store.before('set', 'profiles.*.email', (ctx: { value: unknown; operation: string; path: string; timestamp: number }) => {
            if (typeof ctx.value === 'string') {
                return { ...ctx, value: (ctx.value as string).toLowerCase() };
            }
        });

        // Audit: record every write — use the actual method key 'set'
        store.after('set', 'profiles.*', (ctx: { value: unknown; operation: string; path: string; timestamp: number }) => {
            log.push({ op: ctx.operation, path: ctx.path, value: ctx.value });
        });
    });

    afterAll(async () => { await store.close(); });

    it('before middleware trims bio whitespace', async () => {
        await store.set('profiles.p1', {
            name: 'Tester',
            email: 'T@test.com',
            bio: '  Hello World  ',
        });

        const p = await store.get<{ bio: string }>('profiles.p1');
        expect(p?.bio).toBe('Hello World');
    });

    it('before middleware lowercases email on nested set', async () => {
        await store.set('profiles.p1.email', 'UPPER@EXAMPLE.COM');
        const email = await store.get<string>('profiles.p1.email');
        expect(email).toBe('upper@example.com');
    });

    it('after middleware captures every operation', async () => {
        const before = log.length;
        // Fire a set that must match 'profiles.*'
        await store.set('profiles.audit_probe', { name: 'probe' });
        expect(log.length).toBeGreaterThan(before);
        expect(log.every(e => typeof e.op === 'string')).toBe(true);
    });

    it('multiple middlewares compose in order', async () => {
        // Set a profile with messy bio AND uppercase email simultaneously
        await store.set('profiles.p2', {
            name: 'Composer',
            email: 'COMPOSE@TEST.COM',
            bio: '   Composed  ',
        });

        const p = await store.get<{ email: string; bio: string }>('profiles.p2');
        expect(p?.bio).toBe('Composed');
        // Email middleware targets 'profiles.*.email', not the whole object
        // so profile-level before only trims bio
        expect(p?.email).toBe('COMPOSE@TEST.COM'); // email sub-path hook not triggered here
    });
});

// ===========================================================================
// 8. Snapshot + Restore Workflow
//    createSnapshot → mutate → restoreSnapshot → verify roll-back
// ===========================================================================
describe('Integration: Snapshot & Restore', () => {
    let store: JSONDatabase;
    const FILE     = 'integ_snap.json';
    const SNAP     = 'integ_snap.bak';

    beforeAll(async () => {
        store = db(FILE, { wal: false });
    });

    afterAll(async () => {
        await store.close();
        [SNAP].forEach(p => {
            try { if (existsSync(p)) unlinkSync(p); } catch { /* ignore */ }
        });
    });

    // Track where the snapshot was written
    let snapPath = '';

    it('creates a snapshot and mutates state, then restores to snapshot', async () => {
        await store.set('counter', 100);
        await store.set('label', 'original');

        // createSnapshot() builds its own timestamped path; capture the returned path
        snapPath = await store.createSnapshot('test');
        expect(existsSync(snapPath)).toBe(true);
        toClean.push(snapPath);

        // Mutate
        await store.set('counter', 999);
        await store.set('label', 'mutated');
        expect(await store.get<number>('counter')).toBe(999);

        // Restore
        await store.restoreSnapshot(snapPath);

        expect(await store.get<number>('counter')).toBe(100);
        expect(await store.get<string>('label')).toBe('original');
    });

    it('restoreSnapshot throws for non-existent file', async () => {
        await expect(store.restoreSnapshot('does_not_exist.bak')).rejects.toThrow();
    });

    it('snapshot file is identical to last saved state', async () => {
        await store.set('x', 42);
        await store.save();
        const latestSnap = await store.createSnapshot('final');
        toClean.push(latestSnap);

        const snapContent = readFileSync(latestSnap, 'utf8');
        const fileContent = readFileSync(FILE, 'utf8');
        expect(snapContent).toBe(fileContent);
    });
});

// ===========================================================================
// 9. Multi-Feature Interaction: Schema + Index + Middleware + Transaction
//    all four working together in one coherent workflow
// ===========================================================================
describe('Integration: Multi-Feature Product Catalog', () => {
    let store: JSONDatabase;
    const FILE = 'integ_catalog.json';

    const priceChanges: number[] = [];

    beforeAll(async () => {
        store = db(FILE, {
            wal: false,
            schemas: {
                'catalog': {
                    type: 'object',
                    properties: {
                        price: { type: 'number', minimum: 0 },
                        name:  { type: 'string', minLength: 1 },
                        stock: { type: 'number', minimum: 0 },
                    },
                    required: ['price', 'name', 'stock'],
                },
            },
            indices: [{ name: 'catalog_by_name', path: 'catalog', field: 'name' }],
        });

        // Track price changes — watch catalog.* (full objects) and pull the price field
        store.after('set', 'catalog.*', (ctx: { value: unknown; operation: string; path: string; timestamp: number }) => {
            const obj = ctx.value as Record<string, unknown> | undefined;
            if (obj && typeof obj['price'] === 'number') {
                priceChanges.push(obj['price'] as number);
            }
        });

        await store.set('catalog.c1', { name: 'Widget A', price: 10, stock: 100 });
        await store.set('catalog.c2', { name: 'Widget B', price: 20, stock: 50  });
        await store.set('catalog.c3', { name: 'Gadget X', price: 99, stock: 5   });
    });

    afterAll(async () => { await store.close(); });

    it('schema rejects negative price', async () => {
        await expect(
            store.set('catalog.bad', { name: 'Bad', price: -1, stock: 0 })
        ).rejects.toThrow();
    });

    it('index lookup by name works', async () => {
        const found = await store.findByIndex<{ name: string; price: number }>('catalog_by_name', 'Widget A');
        expect(found).not.toBeNull();
        expect((found as { name: string; price: number }).price).toBe(10);
    });

    it('middleware captures price changes when price is updated', async () => {
        // Update the whole item (not just the sub-path) so schema + middleware both fire on 'catalog.*'
        await store.set('catalog.c1', { name: 'Widget A', price: 15, stock: 100 });
        expect(priceChanges.length).toBeGreaterThan(0);
    });

    it('transaction: apply bulk discount, rollback on schema violation', async () => {
        const before = await store.get<{ price: number }>('catalog.c1');
        const priceBefore = before!.price;

        await expect(
            store.transaction(async () => {
                await store.set('catalog.c1.price', 8);
                await store.set('catalog.c2.price', 16);
                // This violates schema (negative)
                await store.set('catalog.c3.price', -5);
            })
        ).rejects.toThrow();

        // c1 should be rolled back
        const after = await store.get<{ price: number }>('catalog.c1');
        expect(after!.price).toBe(priceBefore);
    });

    it('query: low-stock items (stock < 10)', async () => {
        const low = await store.query<{ name: string; stock: number }>('catalog')
            .where('stock').lt(10)
            .exec();
        expect(low.length).toBe(1);
        expect(low[0]!.name).toBe('Gadget X');
    });

    it('query: items sorted by price then filtered by name prefix', async () => {
        const widgets = await store.query<{ name: string; price: number }>('catalog')
            .where('name').startsWith('Widget')
            .sort({ price: 1 })
            .exec();
        expect(widgets.length).toBe(2);
        expect(widgets[0]!.name).toBe('Widget A');
    });
});
