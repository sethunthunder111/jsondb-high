import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = 'test_subquery.json';

describe('Subquery Operators', () => {
    let db: JSONDatabase;

    beforeAll(async () => {
        if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
        if (existsSync(TEST_DB + '.wal')) unlinkSync(TEST_DB + '.wal');

        db = new JSONDatabase(TEST_DB, { wal: false });

        // Orders: sum=600, avg=200
        await db.set('orders', [
            { id: 1, amount: 100 },
            { id: 2, amount: 200 },
            { id: 3, amount: 300 },
        ]);

        await db.set('items', [
            { name: 'ItemSum',  price: 600 },
            { name: 'ItemAvg',  price: 200 },
            { name: 'ItemHigh', price: 800 },
            { name: 'ItemLow',  price: 50  },
            { name: 'ItemIn',   price: 100 },
        ]);
    });

    afterAll(async () => {
        await db.close();
        if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
        if (existsSync(TEST_DB + '.wal')) unlinkSync(TEST_DB + '.wal');
    });

    it('eqSubquery (sum): finds item whose price equals sum of orders.amount', async () => {
        const qb = await db.query('items')
            .where('price').eqSubquery({ path: 'orders', op: 'sum', field: 'amount' });
        const result = await qb.exec();

        expect(result.length).toBe(1);
        expect((result[0] as { name: string }).name).toBe('ItemSum');
    });

    it('gtSubquery (avg): finds items with price > avg of orders.amount', async () => {
        // avg = 200 → ItemSum (600) and ItemHigh (800)
        const qb = await db.query('items')
            .where('price').gtSubquery({ path: 'orders', op: 'avg', field: 'amount' });
        const result = await qb.exec();

        expect(result.length).toBe(2);

        const names = result.map(i => (i as { name: string }).name).sort();
        expect(names).toEqual(['ItemHigh', 'ItemSum']);
    });

    it('ltSubquery (avg): finds items with price < avg of orders.amount', async () => {
        // avg = 200 → ItemLow (50) and ItemIn (100)
        const qb = await db.query('items')
            .where('price').ltSubquery({ path: 'orders', op: 'avg', field: 'amount' });
        const result = await qb.exec();

        expect(result.length).toBe(2);

        const names = result.map(i => (i as { name: string }).name).sort();
        expect(names).toEqual(['ItemIn', 'ItemLow']);
    });

    it('inSubquery: finds items whose price appears in orders.amount list', async () => {
        // order amounts = [100, 200, 300] → ItemIn (100) and ItemAvg (200)
        const qb = await db.query('items')
            .where('price').inSubquery({ path: 'orders', field: 'amount' });
        const result = await qb.exec();

        expect(result.length).toBe(2);

        const names = result.map(i => (i as { name: string }).name).sort();
        expect(names).toEqual(['ItemAvg', 'ItemIn']);
    });
});
