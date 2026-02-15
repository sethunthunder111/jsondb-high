import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = 'test_subquery.json';

const cleanup = () => {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
    if (existsSync(TEST_DB + '.wal')) unlinkSync(TEST_DB + '.wal');
};

cleanup();

async function runTests() {
    console.log('🚀 === Subquery Test Suite ===\n');
    const db = new JSONDatabase(TEST_DB, { wal: false });

    // Setup data
    // Orders: total amount = 100 + 200 + 300 = 600.
    // Avg amount = 600 / 3 = 200.
    await db.set('orders', [
        { id: 1, amount: 100 },
        { id: 2, amount: 200 },
        { id: 3, amount: 300 }
    ]);

    await db.set('items', [
        { name: 'ItemSum', price: 600 },
        { name: 'ItemAvg', price: 200 },
        { name: 'ItemHigh', price: 800 },
        { name: 'ItemLow', price: 50 },
        { name: 'ItemIn', price: 100 }
    ]);

    // Test eqSubquery (Sum)
    // Should find ItemSum (price 600 == sum(orders.amount))
    console.log('Test eqSubquery (Sum)');
    const qbSum = await db.query('items')
        .where('price').eqSubquery({ path: 'orders', op: 'sum', field: 'amount' });
    const eqSum = await qbSum.exec();
    console.log('   Items with price equal to sum of orders:', eqSum);

    if (eqSum.length !== 1 || (eqSum[0] as any).name !== 'ItemSum') {
        throw new Error(`eqSubquery (Sum) failed. Expected ItemSum, got ${JSON.stringify(eqSum)}`);
    }
    console.log('   ✅ Passed\n');

    // Test gtSubquery (Avg)
    // Avg is 200. Items with price > 200 are ItemSum (600) and ItemHigh (800).
    console.log('Test gtSubquery (Avg)');
    const qbAvg = await db.query('items')
        .where('price').gtSubquery({ path: 'orders', op: 'avg', field: 'amount' });
    const gtAvg = await qbAvg.exec();
    console.log('   Items with price greater than avg of orders:', gtAvg);

    if (gtAvg.length !== 2) {
        throw new Error(`gtSubquery (Avg) failed. Expected 2 items, got ${gtAvg.length}`);
    }
    const gtNames = gtAvg.map(i => (i as any).name).sort();
    if (gtNames[0] !== 'ItemHigh' || gtNames[1] !== 'ItemSum') {
         throw new Error(`gtSubquery (Avg) failed. Expected ItemHigh, ItemSum, got ${gtNames}`);
    }
    console.log('   ✅ Passed\n');

    // Test ltSubquery (Avg)
    // Avg is 200. Items with price < 200 are ItemLow (50) and ItemIn (100).
    console.log('Test ltSubquery (Avg)');
    const qbLt = await db.query('items')
        .where('price').ltSubquery({ path: 'orders', op: 'avg', field: 'amount' });
    const ltAvg = await qbLt.exec();
    console.log('   Items with price less than avg of orders:', ltAvg);

    if (ltAvg.length !== 2) {
        throw new Error(`ltSubquery (Avg) failed. Expected 2 items, got ${ltAvg.length}`);
    }
    const ltNames = ltAvg.map(i => (i as any).name).sort();
    if (ltNames[0] !== 'ItemIn' || ltNames[1] !== 'ItemLow') {
         throw new Error(`ltSubquery (Avg) failed. Expected ItemIn, ItemLow, got ${ltNames}`);
    }
    console.log('   ✅ Passed\n');

    // Test inSubquery
    // Orders have amounts [100, 200, 300].
    // Items with price matching any order amount are ItemIn (100) and ItemAvg (200).
    console.log('Test inSubquery');
    const qbIn = await db.query('items')
        .where('price').inSubquery({ path: 'orders', field: 'amount' });
    const inSub = await qbIn.exec();
    console.log('   Items with price in order amounts:', inSub);

    if (inSub.length !== 2) {
        throw new Error(`inSubquery failed. Expected 2 items, got ${inSub.length}`);
    }
    const inNames = inSub.map(i => (i as any).name).sort();
    if (inNames[0] !== 'ItemAvg' || inNames[1] !== 'ItemIn') {
         throw new Error(`inSubquery failed. Expected ItemAvg, ItemIn, got ${inNames}`);
    }
    console.log('   ✅ Passed\n');

    await db.close();
    cleanup();
    console.log('🎉 === Subquery Tests Passed! ===');
}

runTests().catch(e => {
    console.error('\n❌ Test Failed:', e);
    cleanup();
    process.exit(1);
});
