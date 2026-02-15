import { JSONDatabase } from '../index.ts';
import { performance } from 'perf_hooks';
import { unlinkSync, existsSync } from 'fs';

const DB_PATH = './bench_data.json';

// Setup Data
async function setup() {
    if (existsSync(DB_PATH)) unlinkSync(DB_PATH);
    const db = new JSONDatabase(DB_PATH);

    console.log('Generating 100,000 items...');
    const items = [];
    for (let i = 0; i < 100000; i++) {
        items.push({
            id: i,
            age: Math.floor(Math.random() * 100),
            active: Math.random() > 0.5,
            name: `User_${i}`,
            data: 'x'.repeat(100) // add some payload size
        });
    }
    await db.set('users', items);
    await db.save();
    return db;
}

async function runBenchmark() {
    const db = await setup();

    // Warmup
    await db.query('users').where('age').gt(50).exec();

    console.log('Running benchmark...');
    const start = performance.now();

    for (let i = 0; i < 100; i++) {
        await db.query('users').where('age').gt(80).exec();
    }

    const end = performance.now();
    const duration = end - start;
    const avg = duration / 100;

    console.log(`Total time: ${duration.toFixed(2)}ms`);
    console.log(`Average query time: ${avg.toFixed(2)}ms`);

    // Verify count to ensure correctness later
    const count = (await db.query('users').where('age').gt(80).exec()).length;
    console.log(`Matches found: ${count}`);

    // Cleanup
    if (existsSync(DB_PATH)) unlinkSync(DB_PATH);
}

runBenchmark().catch(console.error);
