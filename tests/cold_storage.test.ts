
import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { JSONDatabase } from '../index';
import { join } from 'path';
import { existsSync, unlinkSync, rmSync } from 'fs';

const TEST_DB_PATH = join(process.cwd(), 'test_cold.json');

describe('Cold Storage (Offloading)', () => {
    let db: JSONDatabase;

    beforeAll(async () => {
        if (existsSync(TEST_DB_PATH)) unlinkSync(TEST_DB_PATH);
        if (existsSync(TEST_DB_PATH + '.wal')) unlinkSync(TEST_DB_PATH + '.wal');
        // Clean up any left over cold files
        const files = require('fs').readdirSync(process.cwd());
        for (const f of files) {
            if (f.startsWith('test_cold.json.cold.')) {
                unlinkSync(join(process.cwd(), f));
            }
        }

        db = new JSONDatabase(TEST_DB_PATH, { wal: false });
    });

    afterAll(async () => {
        await db.close();
        if (existsSync(TEST_DB_PATH)) unlinkSync(TEST_DB_PATH);
        if (existsSync(TEST_DB_PATH + '.wal')) unlinkSync(TEST_DB_PATH + '.wal');
         // Clean up cold files
        const files = require('fs').readdirSync(process.cwd());
        for (const f of files) {
            if (f.startsWith('test_cold.json.cold.')) {
                unlinkSync(join(process.cwd(), f));
            }
        }
    });

    it('should offload data to disk', async () => {
        const largeData = {
            name: 'Big Data',
            payload: 'x'.repeat(1024 * 1024) // 1MB string
        };

        await db.set('huge', largeData);
        
        // Verify it's there
        const val1 = await db.get<any>('huge');
        expect(val1.name).toBe('Big Data');

        // Offload
        const id = await db.offload('huge');
        expect(id.length).toBeGreaterThan(0);
        
        // Check if file exists
        const coldPath = `${TEST_DB_PATH}.cold.${id}`;
        expect(existsSync(coldPath)).toBe(true);
        
        // Verify native get returns marker (internal check, not exposed via public get)
        // But since we modified public get to auto-restore, we can't easily check the marker via public API
        // unless we add a specific method to "peek" without restore, or we check the file system.
        // We already checked the file system.
    });

    it('should automatically restore offloaded data on get', async () => {
        // Now 'huge' is offloaded.
        // Accessing it should transparently restore it.
        const start = performance.now();
        const val = await db.get<any>('huge');
        const end = performance.now();
        
        expect(val).toBeDefined();
        expect(val.name).toBe('Big Data');
        expect(val.payload.length).toBe(1024 * 1024);
        
        console.log(`Restored 1MB in ${end - start}ms`);

        // Verify cold file is gone
        // We need to know the ID, but we don't store it here easily.
        // However, we can check if directory has any cold files for this DB
        const files = require('fs').readdirSync(process.cwd());
        const coldFiles = files.filter((f: string) => f.startsWith('test_cold.json.cold.'));
        expect(coldFiles.length).toBe(0);
    });

    it('should handle manual restore', async () => {
        await db.set('manual', { foo: 'bar' });
        const id = await db.offload('manual');
        const coldPath = `${TEST_DB_PATH}.cold.${id}`;
        expect(existsSync(coldPath)).toBe(true);

        const restored = await db.restore('manual');
        expect(restored).toBe(true);
        expect(existsSync(coldPath)).toBe(false); // Should be deleted

        const val = await db.get<any>('manual');
        expect(val.foo).toBe('bar');
    });

    it('should handle offloading nested paths', async () => {
        await db.set('users.u1', { name: 'Alice', bio: 'Long bio...' });
        await db.set('users.u2', { name: 'Bob', bio: 'Another bio...' });

        await db.offload('users.u1');
        
        // u1 offloaded, u2 in memory
        const u2 = await db.get<any>('users.u2');
        expect(u2.name).toBe('Bob');

        const u1 = await db.get<any>('users.u1'); // Auto restore
        expect(u1.name).toBe('Alice');
    });
});
