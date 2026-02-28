import { describe, it, expect, afterAll } from 'bun:test';
import { JSONDatabase } from '../index.ts';
import { unlinkSync, existsSync } from 'fs';

describe('Schema Regex Validation', () => {
    const DB = 'test_schema_regex.json';

    afterAll(() => {
        if (existsSync(DB)) unlinkSync(DB);
        if (existsSync(DB + '.wal')) unlinkSync(DB + '.wal');
    });

    it('throws on construction when schema pattern is an invalid regex', () => {
        expect(() => {
            new JSONDatabase(DB, {
                wal: false,
                schemas: {
                    'bad_schema': {
                        type: 'string',
                        pattern: '([a-z', // unclosed group
                    },
                },
            });
        }).toThrow('Invalid regex in schema');
    });

    it('constructs without error when pattern is valid regex', () => {
        expect(() => {
            new JSONDatabase(DB, {
                wal: false,
                schemas: {
                    'ok_schema': {
                        type: 'string',
                        pattern: '^[a-z]+$',
                    },
                },
            });
        }).not.toThrow();
    });

    it('accepts data that matches a compiled schema pattern', async () => {
        const db = new JSONDatabase(DB, {
            wal: false,
            schemas: {
                'alpha': { type: 'string', pattern: '^[a-z]+$' },
            },
        });

        await expect(db.set('alpha', 'abc')).resolves.toBeUndefined();
        await db.close();
    });

    it('rejects data that does not match the schema pattern', async () => {
        const db = new JSONDatabase(DB, {
            wal: false,
            schemas: {
                'alpha': { type: 'string', pattern: '^[a-z]+$' },
            },
        });

        await expect(db.set('alpha', '123')).rejects.toThrow('String does not match pattern');
        await db.close();
    });
});
