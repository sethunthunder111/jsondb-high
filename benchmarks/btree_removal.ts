import { JSONDatabase } from '../index';
import { unlinkSync, existsSync } from 'fs';

const DB_PATH = 'bench_btree.json';

// Cleanup
if (existsSync(DB_PATH)) unlinkSync(DB_PATH);
const idxPath = `${DB_PATH}.idx_value.idx`;
if (existsSync(idxPath)) unlinkSync(idxPath);

const db = new JSONDatabase(DB_PATH, {
    wal: false,
    indices: [{ name: 'idx_value', path: 'items', field: 'value' }]
});

async function run() {
    const N = 100000; // 100000 items
    console.log(`Inserting ${N} items with same indexed value...`);

    // Batch in chunks to avoid memory issues or too large payload
    const CHUNK = 10000;
    for (let i = 0; i < N; i += CHUNK) {
        const ops = [];
        for (let j = 0; j < CHUNK && i + j < N; j++) {
            ops.push({ type: 'set', path: `items.${i+j}`, value: { id: i+j, value: 'common' } } as any);
        }
        await db.batch(ops);
        process.stdout.write('+');
    }
    console.log('\nInsertion complete.');

    console.log('Starting removal benchmark...');
    const start = performance.now();

    for (let i = 0; i < N; i++) {
        await db.delete(`items.${i}`);
        if (i % 10000 === 0) process.stdout.write('.');
    }
    console.log();

    const end = performance.now();
    console.log(`Removal of ${N} items took ${(end - start).toFixed(2)}ms`);

    // Cleanup
    if (existsSync(DB_PATH)) unlinkSync(DB_PATH);
    if (existsSync(idxPath)) unlinkSync(idxPath);
}

run();
