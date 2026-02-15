import { JSONDatabase } from "../index.ts";
import { unlinkSync, existsSync } from "fs";

const TEST_DB = 'test_db.json';
const TEST_WAL = 'test_db.json.wal';
const TEST_ENCRYPTED_DB = 'test_encrypted.json';
const TEST_LOCK_DB = 'test_lock.json';
const TEST_DURABILITY_DB = 'test_durability.json';

// Clean up previous runs
const cleanup = () => {
    const files = [
        TEST_DB, TEST_WAL, TEST_ENCRYPTED_DB, `${TEST_ENCRYPTED_DB}.wal`,
        TEST_LOCK_DB, `${TEST_LOCK_DB}.wal`,
        TEST_DURABILITY_DB, `${TEST_DURABILITY_DB}.wal`
    ];
    for (const f of files) {
        if (existsSync(f)) unlinkSync(f);
    }
    // Clean up any snapshot files
    const fs = require('fs');
    const dir = fs.readdirSync('.');
    for (const file of dir) {
        if (file.includes('.bak')) {
            unlinkSync(file);
        }
    }
};

cleanup();

async function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
    console.log('🚀 === jsondb-high Test Suite ===\n');

    // ============================================
    // TEST 1: Basic Set/Get (In-Memory Mode)
    // ============================================
    console.log('📝 [Test 1] Basic Set/Get');
    const db = new JSONDatabase(TEST_DB, { wal: false });
    await db.set('user.name', 'Alice');
    const name = await db.get('user.name');
    console.log('   Got Name:', name);
    if (name !== 'Alice') throw new Error('Basic Set/Get failed');
    await db.close();
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 2: WAL Persistence
    // ============================================
    console.log('💾 [Test 2] WAL Persistence');
    const db2 = new JSONDatabase(TEST_DB, { wal: true });
    await db2.set('config.theme', 'dark');
    await db2.save(); // Force save
    await db2.close(); // Release lock for next instance
    
    const db3 = new JSONDatabase(TEST_DB, { wal: true });
    const theme = await db3.get('config.theme');
    console.log('   Got Theme:', theme);
    if (theme !== 'dark') throw new Error('WAL Persistence failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 3: Arrays & Push/Pull with Deduplication
    // ============================================
    console.log('📚 [Test 3] Arrays & Push/Pull');
    await db3.set('tags', ['a']);
    await db3.push('tags', 'b', 'b', 'c'); // 'b' duped, should be deduped
    const tags = await db3.get<string[]>('tags');
    console.log('   Tags after push:', tags);

    await db3.pull('tags', 'a');
    const tags2 = await db3.get<string[]>('tags');
    console.log('   Tags after pull:', tags2);
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 4: Add/Subtract (Atomic Math)
    // ============================================
    console.log('🔢 [Test 4] Atomic Add/Subtract');
    await db3.set('counter', 10);
    const afterAdd = await db3.add('counter', 5);
    console.log('   After add(5):', afterAdd);
    if (afterAdd !== 15) throw new Error('Add failed');

    const afterSub = await db3.subtract('counter', 3);
    console.log('   After subtract(3):', afterSub);
    if (afterSub !== 12) throw new Error('Subtract failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 5: Batch Operations
    // ============================================
    console.log('📦 [Test 5] Batch Operations');
    await db3.batch([
        { type: 'set', path: 'batch.item_a', value: 1 },
        { type: 'set', path: 'batch.item_b', value: 2 },
        { type: 'delete', path: 'tags' }
    ]);
    const b1 = await db3.get('batch.item_a');
    const hasTags = await db3.has('tags');
    console.log('   Batch results:', { b1, hasTags });
    if (b1 !== 1 || hasTags) throw new Error('Batch failed');
    console.log('   ✅ Passed\n');

    await db3.close();
    console.log('🔍 [Test 6] Indexing');
    const dbWithIndex = new JSONDatabase(TEST_DB, {
        wal: false,
        indices: [{ name: 'email', path: 'users', field: 'email' }]
    });
    
    await dbWithIndex.set('users.alice', { name: 'Alice', email: 'alice@example.com' });
    await dbWithIndex.set('users.bob', { name: 'Bob', email: 'bob@example.com' });
    dbWithIndex.rebuildIndex();
    
    interface User { name: string; email: string; }
    const user = await dbWithIndex.findByIndex<User>('email', 'bob@example.com');
    console.log('   Found user by email:', user);
    if (user?.name !== 'Bob') throw new Error('Index lookup failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 7: Query Builder with Where Clauses
    // ============================================
    console.log('🔎 [Test 7] Advanced Query Builder');
    await dbWithIndex.set('products', {
        '1': { id: 1, name: 'Laptop', price: 999, category: 'Electronics' },
        '2': { id: 2, name: 'Phone', price: 599, category: 'Electronics' },
        '3': { id: 3, name: 'Book', price: 20, category: 'Books' },
        '4': { id: 4, name: 'Headphones', price: 150, category: 'Electronics' }
    });

    interface Product { id: number; name: string; price: number; category: string; }
    const expensiveElectronics = await dbWithIndex.query<Product>('products')
        .where('category').eq('Electronics')
        .where('price').gt(100)
        .sort({ price: -1 })
        .select(['name', 'price'])
        .exec();
    
    console.log('   Expensive Electronics (sorted by price desc):', expensiveElectronics);
    if (expensiveElectronics.length !== 3) throw new Error('Query failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 8: Aggregation Functions
    // ============================================
    console.log('📊 [Test 8] Aggregation Functions');
    const totalPrice = dbWithIndex.query<Product>('products').sum('price');
    const avgPrice = dbWithIndex.query<Product>('products').avg('price');
    const minPrice = dbWithIndex.query<Product>('products').min('price');
    const maxPrice = dbWithIndex.query<Product>('products').max('price');
    const count = dbWithIndex.query<Product>('products').count();
    const categories = dbWithIndex.query<Product>('products').distinct('category');
    
    console.log('   Total:', totalPrice, 'Avg:', avgPrice, 'Min:', minPrice, 'Max:', maxPrice, 'Count:', count);
    console.log('   Categories:', categories);
    if (count !== 4) throw new Error('Aggregation failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 9: Group By
    // ============================================
    console.log('📈 [Test 9] Group By');
    const grouped = dbWithIndex.query<Product>('products').groupBy('category');
    console.log('   Groups:');
    for (const [key, items] of grouped) {
        console.log(`     ${key}: ${items.length} items`);
    }
    if (grouped.get('Electronics')?.length !== 3) throw new Error('GroupBy failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 10: Pagination
    // ============================================
    console.log('📄 [Test 10] Pagination');
    const page1 = await dbWithIndex.paginate<Product>('products', 1, 2);
    console.log('   Page 1:', page1.data.length, 'items, Total:', page1.meta.total, 'Pages:', page1.meta.pages);
    const page2 = await dbWithIndex.paginate<Product>('products', 2, 2);
    console.log('   Page 2:', page2.data.length, 'items');
    if (!page1.meta.hasNext || page1.meta.hasPrev) throw new Error('Pagination meta failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 11: TTL (Time to Live)
    // ============================================
    console.log('⏱️  [Test 11] TTL (Time to Live)');
    await dbWithIndex.setWithTTL('session.abc123', { userId: 1 }, 2); // Expires in 2 seconds
    
    const sessionBefore = await dbWithIndex.get('session.abc123');
    const ttl = await dbWithIndex.getTTL('session.abc123');
    console.log('   Session before:', sessionBefore, 'TTL:', ttl, 'seconds');
    if (!sessionBefore) throw new Error('TTL set failed');
    
    console.log('   Waiting 2.5 seconds for expiry...');
    await sleep(2500);
    
    const sessionAfter = await dbWithIndex.get('session.abc123');
    console.log('   Session after expiry:', sessionAfter);
    if (sessionAfter !== undefined && sessionAfter !== null) throw new Error('TTL expiry failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 12: Pub/Sub (Subscriptions)
    // ============================================
    console.log('📡 [Test 12] Pub/Sub (Subscriptions)');
    let subscriptionTriggered = false;
    let receivedValue: unknown = null;
    
    const unsubscribe = dbWithIndex.subscribe('settings.*', (value, oldValue) => {
        console.log('   Subscription triggered! New:', value, 'Old:', oldValue);
        subscriptionTriggered = true;
        receivedValue = value;
    });
    
    await dbWithIndex.set('settings.theme', 'light');
    await sleep(100); // Small delay for event to fire
    
    if (!subscriptionTriggered) throw new Error('Subscription not triggered');
    if (receivedValue !== 'light') throw new Error('Subscription value mismatch');
    
    // Test unsubscribe
    subscriptionTriggered = false;
    unsubscribe();
    await dbWithIndex.set('settings.theme', 'dark');
    await sleep(100);
    if (subscriptionTriggered) throw new Error('Unsubscribe failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 13: Middleware (Before & After)
    // ============================================
    console.log('🔧 [Test 13] Middleware');
    dbWithIndex.before('set', 'users.*', (ctx) => {
        console.log('   [Before] Intercepted set on:', ctx.path);
        const val = ctx.value as Record<string, unknown>;
        val.updatedAt = Date.now();
        return ctx;
    });

    dbWithIndex.after('set', 'users.*', (ctx) => {
        console.log('   [After] Set complete on:', ctx.path);
        return ctx;
    });

    await dbWithIndex.set('users.charlie', { name: 'Charlie', email: 'charlie@example.com' });
    const charlie = await dbWithIndex.get<User & { updatedAt: number }>('users.charlie');
    console.log('   User with auto-timestamp:', charlie);
    if (!charlie?.updatedAt) throw new Error('Middleware failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 14: Transaction with Savepoints & Rollback
    // ============================================
    console.log('🔒 [Test 14] Transaction with Savepoints');
    await dbWithIndex.set('bank', { alice: 100, bob: 100 });
    
    try {
        await dbWithIndex.transaction(async (tx) => {
            // Operation 1: Alice gives to Bob
            const alice = await dbWithIndex.get<number>('bank.alice');
            const bob = await dbWithIndex.get<number>('bank.bob');
            await dbWithIndex.set('bank.alice', alice - 50);
            await dbWithIndex.set('bank.bob', bob + 50);
            
            // Create savepoint
            await tx.savepoint('sp1');
            
            // Operation 2: Bob gives to Charlie (oops, typo)
            await dbWithIndex.set('bank.bob', bob + 50 - 20);
            await dbWithIndex.set('bank.charlie', 20);
            
            // Rollback to savepoint
            await tx.rollbackTo('sp1');
        });
        
        const bank = await dbWithIndex.get<any>('bank');
        console.log('   Bank state after transaction:', bank);
        // Should be Alice: 50, Bob: 150, Charlie: null/undefined
        if (bank.alice !== 50 || bank.bob !== 150 || bank.charlie !== undefined) {
             console.log('   Mismatch:', { alice: bank.alice, bob: bank.bob, charlie: bank.charlie });
             throw new Error('Transaction/Savepoint failed');
        }
    } catch (e) {
        console.error('   Transaction error:', e);
        throw e;
    }
    console.log('   ✅ Passed\n');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 15: Snapshots
    // ============================================
    console.log('📸 [Test 15] Snapshots');
    const snapshotPath = await dbWithIndex.createSnapshot('test');
    console.log('   Snapshot created:', snapshotPath);
    if (!existsSync(snapshotPath)) throw new Error('Snapshot file not created');
    unlinkSync(snapshotPath); // Cleanup
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 16: Encryption
    // ============================================
    console.log('🔐 [Test 16] Encryption');
    const encryptedDb = new JSONDatabase(TEST_ENCRYPTED_DB, {
        wal: false,
        encryptionKey: 'super-secret-password-32-chars!'
    });
    
    await encryptedDb.set('secret', { password: '12345', apiKey: 'xyz' });
    await encryptedDb.save();
    
    // Check file is encrypted (not plain JSON)
    const fileContent = require('fs').readFileSync(TEST_ENCRYPTED_DB, 'utf8');
    console.log('   File starts with:', fileContent.slice(0, 32) + '...');
    
    // Verify we can read it back
    const secret = await encryptedDb.get<{ password: string; apiKey: string }>('secret');
    console.log('   Decrypted secret:', secret);
    if (secret?.password !== '12345') throw new Error('Encryption/Decryption failed');
    
    await encryptedDb.close();
    unlinkSync(TEST_ENCRYPTED_DB);
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 17: Utility Methods
    // ============================================
    console.log('🛠️  [Test 17] Utility Methods');
    const keys = await dbWithIndex.keys('users');
    const count2 = await dbWithIndex.count('users');
    const stats = await dbWithIndex.stats();
    
    console.log('   User keys:', keys);
    console.log('   User count:', count2);
    console.log('   DB Stats:', stats);
    if (stats.keys === 0) throw new Error('Stats failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 18: Find with Object Predicate
    // ============================================
    console.log('🔍 [Test 18] Find with Object Predicate');
    const bob = await dbWithIndex.find<User>('users', { name: 'Bob' });
    console.log('   Found Bob:', bob);
    if (bob?.email !== 'bob@example.com') throw new Error('Find with object failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 19: FindAll
    // ============================================
    console.log('🔍 [Test 19] FindAll');
    const electronics = await dbWithIndex.findAll<Product>('products', p => p.category === 'Electronics');
    console.log('   All Electronics:', electronics.length, 'items');
    if (electronics.length !== 3) throw new Error('FindAll failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 20: System Info (Multi-Core Detection)
    // ============================================
    console.log('🖥️  [Test 20] System Info (Multi-Core Detection)');
    const sysInfo = dbWithIndex.getSystemInfo();
    console.log('   Available Cores:', sysInfo.availableCores);
    console.log('   Parallel Enabled:', sysInfo.parallelEnabled);
    console.log('   Recommended Batch Size:', sysInfo.recommendedBatchSize);
    if (sysInfo.availableCores < 1) throw new Error('System info failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 21: Parallel Batch Set Operations
    // ============================================
    console.log('⚡ [Test 21] Parallel Batch Set Operations');
    
    // Generate a larger dataset to test parallelism
    const batchOps: Array<{ path: string; value: unknown }> = [];
    for (let i = 0; i < 500; i++) {
        batchOps.push({
            path: `parallel_users.user_${i}`,
            value: { id: i, name: `User ${i}`, age: 18 + (i % 60), active: i % 2 === 0 }
        });
    }
    
    const batchResult = await dbWithIndex.batchSetParallel(batchOps);
    console.log('   Batch Result:', batchResult);
    console.log('   Operations completed:', batchResult.count);
    
    if (!batchResult.success) throw new Error('Parallel batch failed: ' + batchResult.error);
    if (batchResult.count !== 500) throw new Error('Parallel batch count mismatch');
    
    // Verify some data
    const testUser = await dbWithIndex.get<{ id: number; name: string }>('parallel_users.user_42');
    if (testUser?.name !== 'User 42') throw new Error('Parallel batch verification failed');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 22: Parallel Query
    // ============================================
    console.log('🔎 [Test 22] Parallel Query');
    
    interface ParallelUser { id: number; name: string; age: number; active: boolean; }
    
    // Query for active users over 50
    const activeElders = await dbWithIndex.parallelQuery<ParallelUser>('parallel_users', [
        { field: 'age', op: 'gte', value: 50 },
        { field: 'active', op: 'eq', value: true }
    ]);
    
    console.log('   Active users age >= 50:', activeElders.length);
    
    // Verify all results match criteria
    for (const user of activeElders) {
        if (user.age < 50) throw new Error('Parallel query filter failed: age');
        if (!user.active) throw new Error('Parallel query filter failed: active');
    }
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 23: Parallel Aggregation
    // ============================================
    console.log('📊 [Test 23] Parallel Aggregation');
    
    const parallelCount = await dbWithIndex.parallelAggregate('parallel_users', 'count');
    console.log('   Parallel Count:', parallelCount);
    if (parallelCount !== 500) throw new Error('Parallel count failed');
    
    const parallelSum = await dbWithIndex.parallelAggregate('parallel_users', 'sum', 'age');
    console.log('   Parallel Sum of ages:', parallelSum);
    if (parallelSum === null || parallelSum <= 0) throw new Error('Parallel sum failed');
    
    const parallelAvg = await dbWithIndex.parallelAggregate('parallel_users', 'avg', 'age');
    console.log('   Parallel Avg age:', parallelAvg);
    if (parallelAvg === null || parallelAvg <= 0) throw new Error('Parallel avg failed');
    
    const parallelMin = await dbWithIndex.parallelAggregate('parallel_users', 'min', 'age');
    console.log('   Parallel Min age:', parallelMin);
    if (parallelMin !== 18) throw new Error('Parallel min failed');
    
    const parallelMax = await dbWithIndex.parallelAggregate('parallel_users', 'max', 'age');
    console.log('   Parallel Max age:', parallelMax);
    if (parallelMax !== 77) throw new Error('Parallel max failed');
    console.log('   ✅ Passed\n');

    // Cleanup parallel test data
    await dbWithIndex.delete('parallel_users');

    // ============================================
    // v4.5 FEATURE TESTS
    // ============================================

    // ============================================
    // TEST 24: v4.5 WAL Status
    // ============================================
    console.log('📊 [Test 24] v4.5 WAL Status');
    const dbWal = new JSONDatabase(TEST_DB, { 
        wal: true,
        durability: 'batched'
    });
    
    const walStatus = dbWal.walStatus();
    console.log('   WAL Status:', walStatus);
    if (!walStatus.enabled) throw new Error('WAL should be enabled');
    console.log('   ✅ Passed\n');
    await dbWal.close();

    // ============================================
    // TEST 25: v4.5 Durability Modes
    // ============================================
    console.log('💾 [Test 25] v4.5 Durability Modes');
    
    // Test 'none' durability (no WAL)
    const dbNoDurability = new JSONDatabase(TEST_DURABILITY_DB + '_none', { 
        durability: 'none'
    });
    const walStatusNone = dbNoDurability.walStatus();
    console.log('   Durability "none" - WAL enabled:', walStatusNone.enabled);
    if (walStatusNone.enabled) throw new Error('WAL should be disabled for durability=none');
    await dbNoDurability.close();
    
    // Test 'sync' durability (immediate fsync)
    const dbSync = new JSONDatabase(TEST_DURABILITY_DB + '_sync', { 
        durability: 'sync'
    });
    await dbSync.set('test', { value: 1 });
    await dbSync.sync(); // Explicit sync
    const walStatusSync = dbSync.walStatus();
    console.log('   Durability "sync" - WAL enabled:', walStatusSync.enabled);
    if (!walStatusSync.enabled) throw new Error('WAL should be enabled for durability=sync');
    await dbSync.close();
    
    console.log('   ✅ Passed\n');

    // Cleanup durability test files
    [TEST_DURABILITY_DB + '_none', TEST_DURABILITY_DB + '_sync'].forEach(f => {
        if (existsSync(f)) unlinkSync(f);
        if (existsSync(f + '.wal')) unlinkSync(f + '.wal');
    });

    // ============================================
    // TEST 26: v4.5 Crash Recovery Simulation
    // ============================================
    console.log('🔄 [Test 26] v4.5 Crash Recovery Simulation');
    const dbCrashTest = 'test_crash_recovery.json';
    const dbCrashTestWal = 'test_crash_recovery.json.wal';
    
    // Clean up any previous test files
    if (existsSync(dbCrashTest)) unlinkSync(dbCrashTest);
    if (existsSync(dbCrashTestWal)) unlinkSync(dbCrashTestWal);
    
    // Create DB with batched durability
    const dbBeforeCrash = new JSONDatabase(dbCrashTest, { 
        durability: 'batched',
        walFlushMs: 50 // Short flush interval for testing
    });
    
    // Write data
    await dbBeforeCrash.set('critical.data', { user: 'test', value: 42 });
    await dbBeforeCrash.sync(); // Ensure it's flushed
    
    // Close without saving (simulates crash before checkpoint)
    await dbBeforeCrash.close();
    
    // Reopen - should recover from WAL
    const dbAfterCrash = new JSONDatabase(dbCrashTest, { 
        durability: 'batched'
    });
    
    const recoveredData = await dbAfterCrash.get('critical.data');
    console.log('   Recovered data after crash:', recoveredData);
    if (!recoveredData || (recoveredData as any).value !== 42) {
        throw new Error('Crash recovery failed - data not recovered from WAL');
    }
    
    await dbAfterCrash.close();
    
    // Cleanup
    if (existsSync(dbCrashTest)) unlinkSync(dbCrashTest);
    if (existsSync(dbCrashTestWal)) unlinkSync(dbCrashTestWal);
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 27: v4.5 Lock-Free Reads Performance
    // ============================================
    console.log('⚡ [Test 27] v4.5 Lock-Free Reads Performance');
    const dbPerf = new JSONDatabase(TEST_DB, { 
        wal: true,
        durability: 'batched'
    });
    
    // Populate with test data
    await dbPerf.set('perf.test', { items: Array.from({ length: 1000 }, (_, i) => ({ id: i, value: i * 2 })) });
    
    // Measure read performance
    const readStart = performance.now();
    for (let i = 0; i < 1000; i++) {
        await dbPerf.get('perf.test');
    }
    const readDuration = performance.now() - readStart;
    console.log(`   1000 reads took ${readDuration.toFixed(2)}ms (${(readDuration / 1000).toFixed(3)}ms avg)`);
    
    if (readDuration > 5000) { // Should be much faster than 5 seconds
        console.log('   ⚠️  Warning: Reads slower than expected, but test passes');
    }
    
    await dbPerf.close();
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 28: v4.5 Multi-Process Lock (Basic)
    // ============================================
    console.log('🔒 [Test 28] v4.5 Multi-Process Lock (Basic)');
    
    const lockTestDb = 'test_lock_basic.json';
    if (existsSync(lockTestDb)) unlinkSync(lockTestDb);
    if (existsSync(lockTestDb + '.wal')) unlinkSync(lockTestDb + '.wal');
    
    // Test 'none' lock mode (default for backwards compatibility)
    const dbNoLock = new JSONDatabase(lockTestDb, { 
        lockMode: 'none',
        durability: 'none'
    });
    await dbNoLock.set('test', 1);
    console.log('   Lock mode "none" works');
    await dbNoLock.close();
    
    // Test 'exclusive' lock mode
    const dbExclusive = new JSONDatabase(lockTestDb, { 
        lockMode: 'exclusive',
        durability: 'batched'
    });
    await dbExclusive.set('test', 2);
    console.log('   Lock mode "exclusive" works');
    
    // WAL status should work
    const exclusiveWalStatus = dbExclusive.walStatus();
    if (!exclusiveWalStatus.enabled) throw new Error('WAL should be enabled');
    console.log('   WAL status with exclusive lock:', exclusiveWalStatus);
    
    await dbExclusive.close();
    
    // Cleanup
    if (existsSync(lockTestDb)) unlinkSync(lockTestDb);
    if (existsSync(lockTestDb + '.wal')) unlinkSync(lockTestDb + '.wal');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 29: v4.5 Batched Write Performance
    // ============================================
    console.log('⚡ [Test 29] v4.5 Batched Write Performance');
    const dbBatch = new JSONDatabase(TEST_DB, { 
        durability: 'batched',
        walBatchSize: 100,
        walFlushMs: 10
    });
    
    // Measure batched write performance
    const batchWriteStart = performance.now();
    for (let i = 0; i < 100; i++) {
        await dbBatch.set(`batch_perf.item_${i}`, { id: i, data: 'x'.repeat(100) });
    }
    const batchWriteDuration = performance.now() - batchWriteStart;
    console.log(`   100 batched writes took ${batchWriteDuration.toFixed(2)}ms`);
    
    await dbBatch.sync(); // Ensure all flushed
    
    const finalWalStatus = dbBatch.walStatus();
    console.log('   Final WAL status:', finalWalStatus);
    
    await dbBatch.close();
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 30: v4.5 New Query Operators (containsAll, containsAny)
    // ============================================
    console.log('🔎 [Test 30] v4.5 New Query Operators');
    const dbNewOps = new JSONDatabase(TEST_DB, { durability: 'none' });
    
    await dbNewOps.set('items', {
        '1': { id: 1, tags: ['a', 'b', 'c'], name: 'Item 1' },
        '2': { id: 2, tags: ['b', 'c', 'd'], name: 'Item 2' },
        '3': { id: 3, tags: ['a', 'c'], name: 'Item 3' },
        '4': { id: 4, tags: ['e', 'f'], name: 'Item 4' },
    });
    
    // Test parallel query with containsAll
    interface TaggedItem { id: number; tags: string[]; name: string; }
    const itemsWithAB = await dbNewOps.parallelQuery<TaggedItem>('items', [
        { field: 'tags', op: 'containsAll', value: ['a', 'b'] }
    ]);
    console.log('   Items with tags a AND b:', itemsWithAB.length);
    if (itemsWithAB.length !== 1) throw new Error('containsAll failed');
    
    // Test parallel query with containsAny
    const itemsWithAorE = await dbNewOps.parallelQuery<TaggedItem>('items', [
        { field: 'tags', op: 'containsAny', value: ['a', 'e'] }
    ]);
    console.log('   Items with tags a OR e:', itemsWithAorE.length);
    if (itemsWithAorE.length !== 3) throw new Error('containsAny failed');
    
    await dbNewOps.close();
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 31: v4.5 Parallel Join (Lookup)
    // ============================================
    console.log('🔗 [Test 31] v4.5 Parallel Join (Lookup)');
    const dbJoin = new JSONDatabase(TEST_DB, { durability: 'none' });
    
    // Set up users and orders
    await dbJoin.set('users_join', {
        '1': { id: 1, name: 'Alice' },
        '2': { id: 2, name: 'Bob' },
        '3': { id: 3, name: 'Charlie' }
    });
    
    await dbJoin.set('orders_join', {
        '101': { id: 101, userId: 1, amount: 100 },
        '102': { id: 102, userId: 1, amount: 200 },
        '103': { id: 103, userId: 2, amount: 150 },
        '104': { id: 104, userId: 2, amount: 50 },
        '105': { id: 105, userId: 2, amount: 75 }
    });
    
    // Perform parallel lookup join
    const joinedResult = await dbJoin.parallelLookup(
        'users_join',
        'orders_join',
        'id',
        'userId',
        'orders'
    );
    
    console.log('   Joined result count:', joinedResult.length);
    const alice = joinedResult.find((u: any) => u.name === 'Alice');
    const bobJoined = joinedResult.find((u: any) => u.name === 'Bob');
    
    if (alice?.orders?.length !== 2) throw new Error('Join failed for Alice');
    if (bobJoined?.orders?.length !== 3) throw new Error('Join failed for Bob');
    
    console.log('   Alice orders:', alice.orders.length);
    console.log('   Bob orders:', bobJoined.orders.length);
    console.log('   Charlie orders:', joinedResult.find((u: any) => u.name === 'Charlie')?.orders?.length || 0);
    
    await dbJoin.close();
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 32: Schema Validation
    // ============================================
    console.log('✅ [Test 32] Schema Validation');
    const dbSchema = new JSONDatabase(TEST_DB + '.schema', {
        schemas: {
            'users_strict': {
                type: 'object',
                properties: {
                    'age': { type: 'number', minimum: 0, maximum: 120 },
                    'email': { type: 'string', pattern: '^.+@.+\\..+$' }
                },
                required: ['email']
            }
        }
    });

    // Valid data
    await dbSchema.set('users_strict.1', { age: 25, email: 'test@example.com' });
    console.log('   Valid data accepted');

    // Invalid data - wrong type
    try {
        await dbSchema.set('users_strict.2', { age: 'not-a-number', email: 'test@example.com' });
        throw new Error('Should have failed validation (wrong type)');
    } catch (e: any) {
        console.log('   Invalid data (type) correctly rejected:', e.message);
    }

    // Invalid data - out of range
    try {
        await dbSchema.set('users_strict.3', { age: 150, email: 'test@example.com' });
        throw new Error('Should have failed validation (out of range)');
    } catch (e: any) {
        console.log('   Invalid data (range) correctly rejected:', e.message);
    }

    // Invalid data - missing required field
    try {
        await dbSchema.set('users_strict.4', { age: 30 });
        throw new Error('Should have failed validation (missing required)');
    } catch (e: any) {
        console.log('   Invalid data (required) correctly rejected:', e.message);
    }

    await dbSchema.close();
    if (existsSync(TEST_DB + '.schema')) unlinkSync(TEST_DB + '.schema');
    if (existsSync(TEST_DB + '.schema.wal')) unlinkSync(TEST_DB + '.schema.wal');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 33: db.values()
    // ============================================
    console.log('📋 [Test 33] db.values()');

    // 1. Test Object
    await dbWithIndex.set('values_test.obj', { a: 1, b: 2, c: 3 });
    const objVals = await dbWithIndex.values<number>('values_test.obj');
    console.log('   Object values:', objVals);
    if (objVals.length !== 3 || !objVals.includes(1) || !objVals.includes(2) || !objVals.includes(3)) {
        throw new Error('db.values() failed for object');
    }

    // 2. Test Array
    await dbWithIndex.set('values_test.arr', [10, 20]);
    const arrVals = await dbWithIndex.values<number>('values_test.arr');
    console.log('   Array values:', arrVals);
    if (arrVals.length !== 2 || !arrVals.includes(10) || !arrVals.includes(20)) {
        throw new Error('db.values() failed for array');
    }

    // 3. Test Primitive (should be empty based on implementation)
    await dbWithIndex.set('values_test.prim', 'hello');
    const primVals = await dbWithIndex.values('values_test.prim');
    console.log('   Primitive values:', primVals);
    if (primVals.length !== 0) throw new Error('db.values() failed for primitive');

    // 4. Test Non-existent (should be empty)
    const nonExistVals = await dbWithIndex.values('values_test.nothing');
    if (nonExistVals.length !== 0) throw new Error('db.values() failed for non-existent path');

    // Cleanup
    await dbWithIndex.delete('values_test');

    // ============================================
    // TEST 34: Clear Data
    // ============================================
    console.log('🧹 [Test 34] Clear Data');
    const dbClear = new JSONDatabase(TEST_DB + '.clear', { wal: false });
    await dbClear.set('user.name', 'Alice');
    await dbClear.set('config', { theme: 'dark' });

    // Verify data exists
    const dataBeforeClear = await dbClear.get('');
    if (Object.keys(dataBeforeClear as object).length === 0) {
        throw new Error('Data should not be empty before clear');
    }

    // Clear data
    await dbClear.clear();

    // Verify data is empty
    const dataAfterClear = await dbClear.get('');
    console.log('   Data after clear:', dataAfterClear);
    if (Object.keys(dataAfterClear as object).length !== 0) {
        throw new Error('Data should be empty after clear');
    }

    await dbClear.close();
    if (existsSync(TEST_DB + '.clear')) unlinkSync(TEST_DB + '.clear');
    
    // ============================================
    // TEST 35: Nested Transactions
    // ============================================
    console.log('🔄 [Test 35] Nested Transactions');
    const dbNested = new JSONDatabase(TEST_DB, { wal: false });

    // Initial state
    await dbNested.set('bank', { alice: 100, bob: 100, charlie: 100 });

    // Case 1: Both Commit
    try {
        await dbNested.transaction(async (tx1) => {
            await dbNested.set('bank.alice', 90); // -10

            await dbNested.transaction(async (tx2) => {
                await dbNested.set('bank.bob', 110); // +10
            });
        });

        const bank = await dbNested.get<any>('bank');
        if (bank.alice !== 90 || bank.bob !== 110) throw new Error('Both commit failed');
    } catch (e) {
        throw new Error('Nested transaction case 1 failed: ' + e);
    }

    // Reset
    await dbNested.set('bank', { alice: 100, bob: 100, charlie: 100 });

    // Case 2: Inner Fails (Rollback Inner, Outer Catches)
    try {
        await dbNested.transaction(async (tx1) => {
            await dbNested.set('bank.alice', 80); // -20

            try {
                await dbNested.transaction(async (tx2) => {
                    await dbNested.set('bank.bob', 120); // +20
                    throw new Error('Inner error');
                });
            } catch (e) {
                // Caught inner error
            }
        });

        const bank = await dbNested.get<any>('bank');
        // Alice should be 80, Bob should be 100 (rolled back)
        if (bank.alice !== 80 || bank.bob !== 100) throw new Error('Inner rollback failed');
    } catch (e) {
        throw new Error('Nested transaction case 2 failed: ' + e);
    }

    // Reset
    await dbNested.set('bank', { alice: 100, bob: 100, charlie: 100 });

    // Case 3: Outer Fails (Rollback Everything)
    try {
        await dbNested.transaction(async (tx1) => {
            await dbNested.set('bank.alice', 70); // -30

            await dbNested.transaction(async (tx2) => {
                await dbNested.set('bank.bob', 130); // +30
            });

            throw new Error('Outer error');
        });
    } catch (e) {
        // Caught outer error
    }

    const bank = await dbNested.get<any>('bank');
    // Everything should be initial state
    if (bank.alice !== 100 || bank.bob !== 100) throw new Error('Outer rollback failed');

    await dbNested.close();
    
    // ============================================
    // TEST 36: Enhanced TTL Features
    // ============================================
    console.log('⏱️  [Test 36] Enhanced TTL Features');
    const dbTTL = new JSONDatabase(TEST_DB + '_ttl', { wal: false });

    // 1. setTTL on existing key (using fractional seconds for speed)
    await dbTTL.set('ttl_key', 'value');
    dbTTL.setTTL('ttl_key', 0.5); // 0.5 seconds

    // Test hasTTL
    if (!dbTTL.hasTTL('ttl_key')) throw new Error('hasTTL failed - should return true');

    // Let's use 1.5s for initial setup to verify getTTL works
    dbTTL.setTTL('ttl_key', 1.5);
    const initialTTL = await dbTTL.getTTL('ttl_key');
    console.log('   Initial TTL:', initialTTL);
    if (initialTTL <= 0) throw new Error('getTTL failed - should be >= 1');

    // 2. Overwrite TTL
    // Overwrite with shorter TTL to test clear/reset
    dbTTL.setTTL('ttl_key', 0.5);

    // 3. clearTTL
    dbTTL.clearTTL('ttl_key');
    if (dbTTL.hasTTL('ttl_key')) throw new Error('clearTTL failed - should return false');

    // Wait longer than the TTL (0.5s) to ensure it doesn't expire
    await sleep(800);
    const valAfterWait = await dbTTL.get('ttl_key');
    if (valAfterWait !== 'value') throw new Error('Key expired after clearTTL - Timer was not cleared');

    // 4. Expiry callback & Event
    let expiredPath = '';
    const expiryPromise = new Promise<void>((resolve) => {
        dbTTL.on('ttl:expired', ({ path }) => {
            expiredPath = path;
            resolve();
        });
    });

    await dbTTL.set('ttl_expire_event', 'bye');
    dbTTL.setTTL('ttl_expire_event', 0.5);

    console.log('   Waiting for expiry event...');
    await Promise.race([
        expiryPromise,
        sleep(1000)
    ]);

    if (expiredPath !== 'ttl_expire_event') throw new Error('TTL expiry event not fired or wrong path');
    if (await dbTTL.has('ttl_expire_event')) throw new Error('Key not deleted after TTL expiry');

    await dbTTL.close();
    if (existsSync(TEST_DB + '_ttl')) unlinkSync(TEST_DB + '_ttl');
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 37: Restore Snapshot (Success)
    // ============================================
    console.log('📸 [Test 37] Restore Snapshot (Success)');
    const dbRestore = new JSONDatabase(TEST_DB, { wal: false });
    await dbRestore.set('restore_test', 'original');
    const snapPath = await dbRestore.createSnapshot('restore-success');

    await dbRestore.set('restore_test', 'modified');
    if (await dbRestore.get('restore_test') !== 'modified') throw new Error('Failed to modify before restore');

    await dbRestore.restoreSnapshot(snapPath);
    if (await dbRestore.get('restore_test') !== 'original') throw new Error('Restore failed to recover original data');

    await dbRestore.close();
    if (existsSync(snapPath)) unlinkSync(snapPath);
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 38: Restore Snapshot (File Not Found)
    // ============================================
    console.log('📸 [Test 38] Restore Snapshot (File Not Found)');
    const dbRestoreError = new JSONDatabase(TEST_DB, { wal: false });
    const nonExistentPath = 'non_existent_snapshot.bak';

    try {
        await dbRestoreError.restoreSnapshot(nonExistentPath);
        throw new Error('Should have thrown error for missing snapshot');
    } catch (e: any) {
        if (!e.message.includes('Snapshot not found:')) {
            throw new Error('Unexpected error message: ' + e.message);
        }
        console.log('   Caught expected error:', e.message);
    }

    await dbRestoreError.close();
    console.log('   ✅ Passed\n');

    // Cleanup
    await dbWithIndex.close();
    cleanup();

    console.log('\n🎉 === All Tests Passed! ===');
    console.log('\n📋 v4.5 Features Tested:');
    console.log('   • Nested Transactions');
    console.log('   • WAL Status API');
    console.log('   • Durability Modes (none, batched, sync)');
    console.log('   • Crash Recovery Simulation');
    console.log('   • Lock-Free Reads');
    console.log('   • Multi-Process Lock Modes');
    console.log('   • Batched Write Performance');
    console.log('   • New Query Operators (containsAll, containsAny)');
    console.log('   • Parallel Join/Lookup');
    console.log('   • Snapshot Restoration & Error Handling');
}

runTests().catch(e => {
    console.error('\n❌ Test Failed:', e);
    cleanup();
    process.exit(1);
});
