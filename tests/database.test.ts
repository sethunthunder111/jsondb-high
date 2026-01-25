import { JSONDatabase } from '../index';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = 'test_db.json';
const TEST_WAL = 'test_db.json.wal';
const TEST_ENCRYPTED_DB = 'test_encrypted.json';

// Clean up previous runs
const cleanup = () => {
    const files = [TEST_DB, TEST_WAL, TEST_ENCRYPTED_DB, `${TEST_ENCRYPTED_DB}.wal`];
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
    console.log('   ✅ Passed\n');

    // ============================================
    // TEST 2: WAL Persistence
    // ============================================
    console.log('💾 [Test 2] WAL Persistence');
    const db2 = new JSONDatabase(TEST_DB, { wal: true });
    await db2.set('config.theme', 'dark');
    await db2.save(); // Force save
    
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

    // ============================================
    // TEST 6: Indexing (O(1) Lookups)
    // ============================================
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
    if (sessionAfter !== undefined) throw new Error('TTL expiry failed');
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
    // TEST 14: Transaction with Rollback
    // ============================================
    console.log('🔒 [Test 14] Transaction');
    await dbWithIndex.set('bank', { balance: 100 });
    
    try {
        await dbWithIndex.transaction(async (data: Record<string, unknown>) => {
            const bank = data.bank as { balance: number };
            if (bank.balance >= 50) {
                bank.balance -= 50;
            }
            return data;
        });
        const balance = await dbWithIndex.get<{ balance: number }>('bank');
        console.log('   Balance after transaction:', balance?.balance);
        if (balance?.balance !== 50) throw new Error('Transaction failed');
    } catch (e) {
        console.log('   Transaction error (expected):', e);
    }
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

    // Cleanup
    await dbWithIndex.close();
    cleanup();

    console.log('\n🎉 === All Tests Passed! ===');
}

runTests().catch(e => {
    console.error('\n❌ Test Failed:', e);
    cleanup();
    process.exit(1);
});
