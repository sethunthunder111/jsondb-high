import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = 'test_schema_regex.json';

const cleanup = () => {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
    if (existsSync(TEST_DB + '.wal')) unlinkSync(TEST_DB + '.wal');
};

cleanup();

async function runTest() {
    console.log('🚀 === Schema Regex Validation Test ===\n');

    // Test 1: Invalid Regex in Schema
    console.log('📝 [Test 1] Invalid Regex Registration');
    try {
        new JSONDatabase(TEST_DB, {
            wal: false,
            schemas: {
                'invalid_regex_schema': {
                    type: 'string',
                    pattern: '([a-z' // Unclosed group
                }
            }
        });
        throw new Error('Should have failed to register invalid regex schema');
    } catch (e: any) {
        console.log('   Caught expected error:', e.message);
        if (!e.message.includes('Invalid regex in schema')) {
            throw new Error('Unexpected error message: ' + e.message);
        }
    }
    console.log('   ✅ Passed\n');

    // Test 2: Valid Regex
    console.log('📝 [Test 2] Valid Regex Registration');
    const db = new JSONDatabase(TEST_DB, {
        wal: false,
        schemas: {
            'valid_regex_schema': {
                type: 'string',
                pattern: '^[a-z]+$'
            }
        }
    });
    console.log('   ✅ Passed\n');

    // Test 3: Validation with Compiled Regex
    console.log('📝 [Test 3] Validation with Compiled Regex');

    await db.set('valid_regex_schema', 'abc'); // Should pass

    try {
        await db.set('valid_regex_schema', '123'); // Should fail (pattern ^[a-z]+$)
        throw new Error('Should have failed validation');
    } catch (e: any) {
         console.log('   Caught expected error:', e.message);
         // Expect: Validation failed at valid_regex_schema: String does not match pattern: ^[a-z]+$
         if (!e.message.includes('String does not match pattern')) {
             throw new Error('Unexpected error message: ' + e.message);
         }
    }
    console.log('   ✅ Passed\n');

    await db.close();
    cleanup();
    console.log('🎉 === All Schema Regex Tests Passed! ===');
}

runTest().catch(e => {
    console.error('\n❌ Test Failed:', e);
    cleanup();
    process.exit(1);
});
