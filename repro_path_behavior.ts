import { JSONDatabase } from './index.ts';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = 'repro_path.json';

// Clean up previous runs
const cleanup = () => {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
    if (existsSync(`${TEST_DB}.wal`)) unlinkSync(`${TEST_DB}.wal`);
};

cleanup();

async function runTests() {
    console.log('🚀 === Path Behavior Repro ===\n');

    const db = new JSONDatabase(TEST_DB, { wal: false });
    await db.set('users', { 'alice': { age: 30 }, 'bob': { age: 40 } });

    // 1. parallelAggregate with empty path
    // Current implementation: ptr = "/". collection = root.
    // If root is object with "users", count should be 1 (users key)? Or values of root?
    // "users" key is one value.
    try {
        const rootCount = await db.parallelAggregate('', 'count');
        console.log(`parallelAggregate('') count: ${JSON.stringify(rootCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('') failed: ${e}`);
    }

    // 2. parallelAggregate with "/" path
    try {
        const rootSlashCount = await db.parallelAggregate('/', 'count');
        console.log(`parallelAggregate('/') count: ${JSON.stringify(rootSlashCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('/') failed: ${e}`);
    }

    // 3. parallelAggregate with "users"
    try {
        const usersCount = await db.parallelAggregate('users', 'count');
        console.log(`parallelAggregate('users') count: ${JSON.stringify(usersCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('users') failed: ${e}`);
    }

    // 4. parallelAggregate with "/users"
    try {
        const usersSlashCount = await db.parallelAggregate('/users', 'count');
        console.log(`parallelAggregate('/users') count: ${JSON.stringify(usersSlashCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('/users') failed: ${e}`);
    }

    // 5. parallelAggregate with "users.alice"
    // "users.alice" -> "/users/alice" -> points to { age: 30 }
    // If it's an object, it iterates values. Values are [30]. Count should be 1.
    try {
        const aliceCount = await db.parallelAggregate('users.alice', 'count');
        console.log(`parallelAggregate('users.alice') count: ${JSON.stringify(aliceCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('users.alice') failed: ${e}`);
    }

     // 6. parallelAggregate with "/users/alice"
    try {
        const aliceSlashCount = await db.parallelAggregate('/users/alice', 'count');
        console.log(`parallelAggregate('/users/alice') count: ${JSON.stringify(aliceSlashCount)}`);
    } catch (e) {
        console.log(`parallelAggregate('/users/alice') failed: ${e}`);
    }

    // 7. get("/")
    try {
        const rootSlash = await db.get('/');
        console.log(`get('/') value: ${JSON.stringify(rootSlash)}`);
    } catch (e) {
        console.log(`get('/') failed: ${e}`);
    }

    await db.close();

    // 8. push to root array
    const dbArray = new JSONDatabase('repro_array.json', { wal: false });

    // Test set("")
    try {
        await dbArray.set('', []);
        console.log(`set('') to [] success`);
    } catch (e) {
        console.log(`set('') failed: ${e}`);
    }

    try {
        await dbArray.push('', 'item1');
        const root = await dbArray.get('');
        console.log(`push('') result: ${JSON.stringify(root)}`);
    } catch (e) {
        console.log(`push('') failed: ${e}`);
    }

    // cleanup repro_array
    if (existsSync('repro_array.json')) unlinkSync('repro_array.json');
    if (existsSync('repro_array.json.wal')) unlinkSync('repro_array.json.wal');
    cleanup();
}

runTests().catch(e => {
    console.error('\n❌ Test Failed:', e);
    cleanup();
    process.exit(1);
});
