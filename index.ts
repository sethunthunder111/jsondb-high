import { join } from 'path';
import { existsSync, copyFileSync, writeFileSync, readFileSync } from 'fs';
import { copyFile } from 'fs/promises';
import { EventEmitter } from 'events';
import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from 'crypto';
import { performance } from 'perf_hooks';
import { isDeepStrictEqual } from 'util';

// @ts-ignore
import { NativeDb } from './index.js';

export interface IndexConfig {
    name: string;
    path: string;
    field: string;
}

export interface JoinConfig {
    from: string;
    to: string;
    localField: string;
    foreignField: string;
    as: string;
}

export interface SubqueryConfig {
    path: string;
    field?: string;
    op?: 'avg' | 'sum' | 'min' | 'max' | 'values';
}

export type SchemaType = 'object' | 'array' | 'string' | 'number' | 'boolean' | 'null';

export interface Schema {
    type: SchemaType;
    properties?: Record<string, Schema>;
    required?: string[];
    minLength?: number;
    maxLength?: number;
    pattern?: string;
    minimum?: number;
    maximum?: number;
    exclusiveMinimum?: number;
    exclusiveMaximum?: number;
    items?: Schema;
    minItems?: number;
    maxItems?: number;
    uniqueItems?: boolean;
    enum?: unknown[];
}

export interface MiddlewareContext<T = unknown> {
    path: string;
    value: T;
    operation: string;
    timestamp: number;
}

export type MiddlewareFn<T = unknown> = (ctx: MiddlewareContext<T>) => MiddlewareContext<T> | void;

export interface DBOptions {
    indices?: IndexConfig[];
    wal?: boolean;
    encryptionKey?: string;
    autoSaveInterval?: number;
    
    lockMode?: 'exclusive' | 'shared' | 'none';
    
    lockTimeoutMs?: number;
    
    durability?: 'none' | 'lazy' | 'batched' | 'sync';
    
    walBatchSize?: number;
    
    walFlushMs?: number;
    
    schemas?: Record<string, Schema>;

    slowQueryThresholdMs?: number;

    memoryLimit?: string;

    coldStorageDir?: string;

    evictionThresholdPct?: number;

    evictionTargetPct?: number;

    /** v6: Number of write lock stripes for concurrent writes (default: 64). Increase for high-core-count servers. */
    stripeCount?: number;

    /** v6: Buffer pool size in MB (0 = disabled, default: 0). Set to e.g. 256 for large databases. */
    bufferPoolSizeMB?: number;

    /** v6: Buffer pool page size in KB (default: 16). Smaller = finer granularity, larger = fewer pages. */
    bufferPageSizeKB?: number;
}

export interface TTLEntry {
    path: string;
    expiresAt: number;
}

export interface PaginationMeta {
    total: number;
    pages: number;
    page: number;
    limit: number;
    hasNext: boolean;
    hasPrev: boolean;
}

export interface PaginationResult<T> {
    data: T[];
    meta: PaginationMeta;
}

export interface BatchOperation {
    type: 'set' | 'delete' | 'push' | 'add' | 'subtract';
    path: string;
    value?: unknown;
}

export type SortDirection = 1 | -1;
export interface SortOptions {
    [key: string]: SortDirection;
}

export interface SystemInfo {
    availableCores: number;
    parallelEnabled: boolean;
    recommendedBatchSize: number;
}

export interface ParallelConfig {
    enabled?: boolean;
    threshold?: number;
    maxThreads?: number;
}

export interface QueryFilter {
    field: string;
    op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'startswith' | 'endswith' | 'in' | 'notin' | 'regex' | 'containsAll' | 'containsAny';
    value: unknown;
}

export interface ParallelResult {
    success: boolean;
    count: number;
    error?: string;
}

export interface Transaction {
    savepoint(name: string): Promise<void>;
    rollbackTo(name: string): Promise<void>;
}

function deepEqual(a: unknown, b: unknown): boolean {
    return isDeepStrictEqual(a, b);
}

const patternCache = new Map<string, RegExp>();

function matchesPattern(pattern: string, path: string): boolean {
    let regex = patternCache.get(pattern);
    if (!regex) {
        regex = new RegExp('^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]*').replace(/\*\*/g, '.*') + '$');
        patternCache.set(pattern, regex);
    }
    return regex.test(path);
}

const ENCRYPTION_ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const AUTH_TAG_LENGTH = 16;
const SALT_LENGTH = 32;

function deriveKey(password: string, salt: Buffer): Buffer {
    return scryptSync(password, salt, 32);
}

function encrypt(data: string, password: string): string {
    const salt = randomBytes(SALT_LENGTH);
    const key = deriveKey(password, salt);
    const iv = randomBytes(IV_LENGTH);
    const cipher = createCipheriv(ENCRYPTION_ALGORITHM, key, iv);
    
    let encrypted = cipher.update(data, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    const authTag = cipher.getAuthTag();
    
    return salt.toString('hex') + iv.toString('hex') + authTag.toString('hex') + encrypted;
}

function decrypt(encryptedData: string, password: string): string {
    const salt = Buffer.from(encryptedData.slice(0, SALT_LENGTH * 2), 'hex');
    const iv = Buffer.from(encryptedData.slice(SALT_LENGTH * 2, SALT_LENGTH * 2 + IV_LENGTH * 2), 'hex');
    const authTag = Buffer.from(encryptedData.slice(SALT_LENGTH * 2 + IV_LENGTH * 2, SALT_LENGTH * 2 + IV_LENGTH * 2 + AUTH_TAG_LENGTH * 2), 'hex');
    const encrypted = encryptedData.slice(SALT_LENGTH * 2 + IV_LENGTH * 2 + AUTH_TAG_LENGTH * 2);
    
    const key = deriveKey(password, salt);
    const decipher = createDecipheriv(ENCRYPTION_ALGORITHM, key, iv);
    decipher.setAuthTag(authTag);
    
    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
}

type FilterFn<T> = (item: T) => boolean;

export class WhereClause<T> {
    private queryBuilder: QueryBuilder<T>;
    private field: string;

    constructor(queryBuilder: QueryBuilder<T>, field: string) {
        this.queryBuilder = queryBuilder;
        this.field = field;
    }

    private getFieldValue(item: T): unknown {
        const parts = this.field.split('.');
        let value: unknown = item;
        for (const part of parts) {
            if (value && typeof value === 'object') {
                value = (value as Record<string, unknown>)[part];
            } else {
                return undefined;
            }
        }
        return value;
    }

    eq(value: any): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'eq', value });
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return deepEqual(v, value);
        });
    }

    ne(value: any): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'ne', value });
        return this.queryBuilder.filter((item: T) => !deepEqual(this.getFieldValue(item), value));
    }

    gt(value: number): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'gt', value });
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v > value;
        });
    }

    gte(value: number): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'gte', value });
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v >= value;
        });
    }

    lt(value: number): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'lt', value });
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v < value;
        });
    }

    lte(value: number): QueryBuilder<T> {
        this.queryBuilder.addQueryFilter({ field: this.field, op: 'lte', value });
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v <= value;
        });
    }

    between(min: number, max: number): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v >= min && v <= max;
        });
    }

    in(values: unknown[]): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => values.includes(this.getFieldValue(item)));
    }

    notIn(values: unknown[]): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => !values.includes(this.getFieldValue(item)));
    }

    async eqSubquery(config: SubqueryConfig): Promise<QueryBuilder<T>> {
        const val = await this.queryBuilder.db.parallelAggregate(config.path, (config.op as any) || 'sum', config.field);
        return this.eq(val);
    }

    async gtSubquery(config: SubqueryConfig): Promise<QueryBuilder<T>> {
        const val = await this.queryBuilder.db.parallelAggregate(config.path, (config.op as any) || 'avg', config.field);
        return this.gt(val as number);
    }

    async ltSubquery(config: SubqueryConfig): Promise<QueryBuilder<T>> {
        const val = await this.queryBuilder.db.parallelAggregate(config.path, (config.op as any) || 'avg', config.field);
        return this.lt(val as number);
    }

    async inSubquery(config: SubqueryConfig): Promise<QueryBuilder<T>> {
        const values = await this.queryBuilder.db.query(config.path).exec();
        const extracted = values.map(v => (v as any)[config.field!]);
        return this.in(extracted);
    }

    contains(substring: string): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'string' && v.includes(substring);
        });
    }

    startsWith(prefix: string): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'string' && v.startsWith(prefix);
        });
    }

    endsWith(suffix: string): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'string' && v.endsWith(suffix);
        });
    }

    matches(regex: RegExp): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'string' && regex.test(v);
        });
    }

    exists(): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => this.getFieldValue(item) !== undefined);
    }

    isNull(): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => this.getFieldValue(item) === null);
    }

    isNotNull(): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => this.getFieldValue(item) !== null);
    }

    containsAll(values: unknown[]): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return Array.isArray(v) && values.every(val => v.some(arrVal => deepEqual(arrVal, val)));
        });
    }

    containsAny(values: unknown[]): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return Array.isArray(v) && values.some(val => v.some(arrVal => deepEqual(arrVal, val)));
        });
    }

    regex(pattern: string | RegExp): QueryBuilder<T> {
        const regex = typeof pattern === 'string' ? new RegExp(pattern) : pattern;
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'string' && regex.test(v);
        });
    }

    before(date: Date | string | number): QueryBuilder<T> {
        const targetTime = new Date(date).getTime();
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return (typeof v === 'string' || typeof v === 'number' || v instanceof Date) && new Date(v).getTime() < targetTime;
        });
    }

    after(date: Date | string | number): QueryBuilder<T> {
        const targetTime = new Date(date).getTime();
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return (typeof v === 'string' || typeof v === 'number' || v instanceof Date) && new Date(v).getTime() > targetTime;
        });
    }
}

export class QueryBuilder<T = unknown> {
    private items: T[] | Record<string, T> | null;
    public db: JSONDatabase;
    private _limit?: number;
    private _skip?: number;
    private _sortOptions?: SortOptions;
    private _selectFields?: string[];
    private filters: FilterFn<T>[] = [];
    private queryFilters: QueryFilter[] = [];
    private path: string = '';

    constructor(items: T[] | Record<string, T> | null, db: JSONDatabase) {
        this.items = items;
        this.db = db;
    }
    
    private ensureData(): void {
        if (this.items !== null) return;

        const data = this.db.getSync(this.path);
        if (Array.isArray(data)) {
            this.items = data as T[];
        } else if (typeof data === 'object' && data !== null) {
            this.items = data as Record<string, T>;
        } else {
            this.items = [];
        }
    }

    setPath(path: string): QueryBuilder<T> {
        this.path = path;
        return this;
    }

    addQueryFilter(f: QueryFilter): void {
        this.queryFilters.push(f);
    }
  
  join<U>(config: JoinConfig): QueryBuilder<T & { [K in string]: U[] }> {
        if (!this.db) {
            throw new Error("Database instance required for join operations");
        }
        
        this.ensureData();

        const targetCollection = (this.db as any).native.get(config.to);
        const targetItems: any[] = Array.isArray(targetCollection) 
            ? targetCollection 
            : Object.values(targetCollection ?? {});
            
        const lookup = new Map<string, any[]>();
        for (const item of targetItems) {
            const rawKey = item[config.foreignField];
            if (rawKey === undefined || rawKey === null) continue;
            
            const key = String(rawKey);
            if (!lookup.has(key)) {
                lookup.set(key, []);
            }
            lookup.get(key)!.push(item);
        }
        
        const currentItems: T[] = Array.isArray(this.items)
            ? this.items
            : Object.values(this.items as Record<string, T>);

        this.items = currentItems.map(item => {
            const rawKey = (item as any)[config.localField];
            const key = (rawKey === undefined || rawKey === null) ? null : String(rawKey);
            
            const matches = (key !== null) ? (lookup.get(key) || []) : [];
            
            return {
                ...item,
                [config.as]: matches
            };
        }) as any;
        
        return this as any;
    }

    where(field: string): WhereClause<T> {
        return new WhereClause(this, field);
    }

    filter(fn: FilterFn<T>): QueryBuilder<T> {
        this.filters.push(fn);
        return this;
    }

    limit(n: number): QueryBuilder<T> {
        this._limit = n;
        return this;
    }

    skip(n: number): QueryBuilder<T> {
        this._skip = n;
        return this;
    }

    sort(options: SortOptions): QueryBuilder<T> {
        this._sortOptions = options;
        return this;
    }

    select(fields: string[]): QueryBuilder<T> {
        this._selectFields = fields;
        return this;
    }

    count(): number {
        if (this.db && this.items === null && this.queryFilters.length > 0 && this.filters.length === 0) {
            try {
                const result = JSON.parse((this.db as any).native.executeAggregateFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    'count',
                    null
                ));
                if (typeof result === 'number') return result;
            } catch { }
        }
        return this.applyFilters().length;
    }

    sum(field: string): number {
        if (this.db && this.items === null && this.queryFilters.length > 0 && this.filters.length === 0) {
            try {
                const result = JSON.parse((this.db as any).native.executeAggregateFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    'sum',
                    field
                ));
                if (typeof result === 'number') return result;
            } catch { }
        }
        return this.applyFilters().reduce((acc, item) => {
            const value = this.getFieldValue(item, field);
            return acc + (typeof value === 'number' ? value : 0);
        }, 0);
    }

    avg(field: string): number {
        if (this.db && this.items === null && this.queryFilters.length > 0 && this.filters.length === 0) {
            try {
                const result = JSON.parse((this.db as any).native.executeAggregateFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    'avg',
                    field
                ));
                if (typeof result === 'number') return result;
            } catch { }
        }
        const items = this.applyFilters();
        if (items.length === 0) return 0;
        return this.sum(field) / items.length;
    }

    min(field: string): number | undefined {
        if (this.db && this.items === null && this.queryFilters.length > 0 && this.filters.length === 0) {
            try {
                const result = JSON.parse((this.db as any).native.executeAggregateFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    'min',
                    field
                ));
                if (typeof result === 'number') return result;
            } catch { }
        }
        const items = this.applyFilters();
        if (items.length === 0) return undefined;
        return Math.min(...items.map(item => {
            const v = this.getFieldValue(item, field);
            return typeof v === 'number' ? v : Infinity;
        }));
    }

    max(field: string): number | undefined {
        if (this.db && this.items === null && this.queryFilters.length > 0 && this.filters.length === 0) {
            try {
                const result = JSON.parse((this.db as any).native.executeAggregateFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    'max',
                    field
                ));
                if (typeof result === 'number') return result;
            } catch { }
        }
        const items = this.applyFilters();
        if (items.length === 0) return undefined;
        return Math.max(...items.map(item => {
            const v = this.getFieldValue(item, field);
            return typeof v === 'number' ? v : -Infinity;
        }));
    }

    distinct(field: string): unknown[] {
        const items = this.applyFilters();
        const seen = new Set<string>();
        const result: unknown[] = [];
        for (const item of items) {
            const v = this.getFieldValue(item, field);
            const key = JSON.stringify(v);
            if (!seen.has(key)) {
                seen.add(key);
                result.push(v);
            }
        }
        return result;
    }

    groupBy(field: string): Map<unknown, T[]> {
        const items = this.applyFilters();
        const groups = new Map<unknown, T[]>();
        for (const item of items) {
            const key = this.getFieldValue(item, field);
            if (!groups.has(key)) {
                groups.set(key, []);
            }
            groups.get(key)!.push(item);
        }
        return groups;
    }

    private getFieldValue(item: T, field: string): unknown {
        const parts = field.split('.');
        let value: unknown = item;
        for (const part of parts) {
            if (value && typeof value === 'object') {
                value = (value as Record<string, unknown>)[part];
            } else {
                return undefined;
            }
        }
        return value;
    }

private applyFilters(limit?: number): T[] {
        this.ensureData();

        if (Array.isArray(this.items)) {
            if (this.filters.length === 0 && limit === undefined) {
                 return [...this.items];
            }

            const result: T[] = [];
            let count = 0;
            for (const item of this.items) {
                let match = true;
                for (const filter of this.filters) {
                    if (!filter(item)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    result.push(item);
                    count++;
                    if (limit !== undefined && count >= limit) break;
                }
            }
            return result;
        }

        const items = this.items as Record<string, T>;
        if (this.filters.length === 0 && limit === undefined) {
            return Object.values(items);
        }

        const result: T[] = [];
        let count = 0;
        for (const key in items) {
            if (Object.prototype.hasOwnProperty.call(items, key)) {
                const item = items[key];
                if (item === undefined) continue;
                let match = true;
                for (const filter of this.filters) {
                    if (!filter(item)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    result.push(item);
                    count++;
                    if (limit !== undefined && count >= limit) break;
                }
            }
        }
        return result;
    }

    async exec(): Promise<T[]> {
        const startTime = performance.now();
        let result: T[] = [];
        let usedIndex = false;
        let usedNativeEngine = false;

        if (this.db && this.queryFilters.length > 0) {
            for (const f of this.queryFilters) {
                if (f.op === 'eq') {
                    const index = (this.db as any).indices.find((idx: any) => idx.path === this.path && idx.field === f.field);
                    if (index) {
                        const paths = (this.db as any).native.findIndexPaths(index.name, f.value);
                        if (paths) {
                            const indexedItems = await this.db.getMany<T>(paths);
                            result = indexedItems.filter((x): x is T => x !== null && x !== undefined);
                            usedIndex = true;
                            break;
                        }
                    }
                } else if (['gt', 'gte', 'lt', 'lte'].includes(f.op)) {
                    const index = (this.db as any).indices.find((idx: any) => idx.path === this.path && idx.field === f.field);
                    if (index && typeof (this.db as any).native.findIndexRange === 'function') {
                        let start = null;
                        let end = null;
                        
                        if (f.op === 'gt' || f.op === 'gte') start = f.value;
                        if (f.op === 'lt' || f.op === 'lte') end = f.value;
                        
                        const paths = (this.db as any).native.findIndexRange(index.name, start, end);
                        if (paths) {
                            const indexedItems = await this.db.getMany<T>(paths);
                            result = indexedItems.filter((x): x is T => x !== null && x !== undefined);
                            usedIndex = true;
                            break;
                        }
                    }
                }
            }
        }

        if (!usedIndex && this.items === null && this.db) {
            const hasCustomJSFilters = this.filters.length > this.queryFilters.length;
            if (this.queryFilters.length > 0 && !hasCustomJSFilters) {
                try {
                    const sortJson = this._sortOptions ? JSON.stringify(this._sortOptions) : null;
                    const jsonStr = (this.db as any).native.executeQueryFast(
                        this.path,
                        JSON.stringify(this.queryFilters),
                        sortJson,
                        this._limit ?? null,
                        this._skip ?? null,
                        this._selectFields ?? null,
                    );
                    
                    if (typeof jsonStr === 'string') {
                        result = JSON.parse(jsonStr) as T[];
                        usedNativeEngine = true;
                    }
                } catch {
                    usedNativeEngine = false;
                }
            }
            
            if (!usedNativeEngine && this.queryFilters.length > 0) {
                result = await this.db.parallelQuery<T>(this.path, this.queryFilters);

                if (this.filters.length > 0) {
                    for (const filter of this.filters) {
                        result = result.filter(filter);
                    }
                }
            }
        }

        if (!usedIndex && !usedNativeEngine && result.length === 0) {
            this.ensureData();
            let effectiveLimit: number | undefined;
            if (!this._sortOptions && this._limit !== undefined) {
                effectiveLimit = (this._skip || 0) + this._limit;
            }
            result = this.applyFilters(effectiveLimit);
        }

        let finalResult: T[];
        if (usedNativeEngine) {
            finalResult = result;
        } else if (usedIndex) {
            for (const filter of this.filters) {
                result = result.filter(filter);
            }
            finalResult = this.applyPostProcessing(result);
        } else {
            finalResult = this.applyPostProcessing(result);
        }

        const duration = performance.now() - startTime;
        
        const threshold = (this.db as any).slowQueryThresholdMs ?? 100;
        if (duration > threshold) {
            this.db.emit('slow_query', {
                path: this.path,
                filters: this.queryFilters,
                duration,
                usedIndex,
                usedNativeEngine,
            });
        }

        return finalResult;
    }

    private applyPostProcessing(result: T[]): T[] {
        if (this._sortOptions) {
            const sortEntries = Object.entries(this._sortOptions);
            result.sort((a, b) => {
                for (const [key, dir] of sortEntries) {
                    const aVal = this.getFieldValue(a, key);
                    const bVal = this.getFieldValue(b, key);
                    if (aVal === bVal) continue;
                    if (aVal === undefined || aVal === null) return dir;
                    if (bVal === undefined || bVal === null) return -dir;
                    if (aVal < bVal) return -dir;
                    if (aVal > bVal) return dir;
                }
                return 0;
            });
        }

        if (this._skip !== undefined) {
            result = result.slice(this._skip);
        }

        if (this._limit !== undefined) {
            result = result.slice(0, this._limit);
        }

        if (this._selectFields) {
            result = result.map(item => {
                const newItem: Record<string, unknown> = {};
                for (const f of this._selectFields!) {
                    newItem[f] = this.getFieldValue(item, f);
                }
                return newItem as T;
            });
        }

        return result;
    }

    first(): T | undefined {
        if (this._sortOptions) {
            let items = this.applyFilters(); 
            items = this.applyPostProcessing(items);
            return items[0];
        }
        
        const items = this.applyFilters(1);
        return items[0];
    }

    last(): T | undefined {
        let items = this.applyFilters();
        items = this.applyPostProcessing(items);
        return items[items.length - 1];
    }

    /** v6: Returns the execution plan for this query without fetching results.
     * Shows scan type, collection size, matched count, parallelism, timing, etc.
     */
    async explain(): Promise<{
        scanType: string;
        collectionSize: number;
        filtersApplied: Array<{ field: string; op: string }>;
        matchedCount: number;
        sortApplied: Array<{ field: string; direction: string }>;
        skip: number;
        limit: number | null;
        projectedFields: string[];
        resultCount: number;
        parallelExecution: boolean;
        executionTimeMs: number;
    }> {
        // Try native explain first
        if (this.db && this.queryFilters.length > 0) {
            try {
                const sortJson = this._sortOptions ? JSON.stringify(this._sortOptions) : null;
                const jsonStr = (this.db as any).native.explainQueryFast(
                    this.path,
                    JSON.stringify(this.queryFilters),
                    sortJson,
                    this._limit ?? null,
                    this._skip ?? null,
                    this._selectFields ?? null,
                );

                if (typeof jsonStr === 'string') {
                    return JSON.parse(jsonStr);
                }
            } catch {
                // Fall through to JS-based explain
            }
        }

        // Fallback: JS-based explain
        this.ensureData();
        const collectionSize = Array.isArray(this.items)
            ? this.items.length
            : (this.items ? Object.keys(this.items).length : 0);

        return {
            scanType: this.queryFilters.length > 0 ? 'FILTER_SCAN' : 'FULL_SCAN',
            collectionSize,
            filtersApplied: this.queryFilters.map(f => ({ field: f.field, op: f.op })),
            matchedCount: -1, // Unknown without executing
            sortApplied: this._sortOptions
                ? Object.entries(this._sortOptions).map(([field, dir]) => ({
                    field,
                    direction: dir < 0 ? 'DESC' : 'ASC',
                }))
                : [],
            skip: this._skip ?? 0,
            limit: this._limit ?? null,
            projectedFields: this._selectFields ?? [],
            resultCount: -1,
            parallelExecution: false,
            executionTimeMs: 0,
        };
    }
}

export class JSONDatabase extends EventEmitter {
    private native: InstanceType<typeof NativeDb>;
    private indices: IndexConfig[] = [];
    private beforeMiddlewares: Map<string, MiddlewareFn[]> = new Map();
    private afterMiddlewares: Map<string, MiddlewareFn[]> = new Map();
    private wal: boolean = false;
    private saveTimeout: NodeJS.Timeout | null = null;
    private autoSaveInterval: number;
    private encryptionKey?: string;
    
    private ttlMap: Map<string, NodeJS.Timeout> = new Map();
    private ttlEntries: Map<string, number> = new Map();
    
    private subscriptions: Map<string, Set<(value: unknown, oldValue: unknown) => void>> = new Map();

    private lockMode: 'exclusive' | 'shared' | 'none';
    private durability: 'none' | 'lazy' | 'batched' | 'sync';
    private walBatchSize: number;
    private walFlushMs: number;
    private slowQueryThresholdMs: number;
    private stripeCount: number;
    private bufferPoolSizeMB: number;
    private bufferPageSizeKB: number;


    private filePath: string;

    constructor(filePath: string, options: DBOptions = {}) {
        super();
        this.filePath = filePath;

        this.wal = options.wal ?? false;
        this.encryptionKey = options.encryptionKey;
        this.autoSaveInterval = options.autoSaveInterval ?? 1000;
        
        this.lockMode = options.lockMode ?? (this.wal ? 'exclusive' : 'none');
        this.durability = options.durability ?? (this.wal ? 'batched' : 'none');
        this.walBatchSize = options.walBatchSize ?? 1000;
        this.walFlushMs = options.walFlushMs ?? 10;
        this.slowQueryThresholdMs = options.slowQueryThresholdMs ?? 100;
        this.stripeCount = options.stripeCount ?? 64;
        this.bufferPoolSizeMB = options.bufferPoolSizeMB ?? 0;
        this.bufferPageSizeKB = options.bufferPageSizeKB ?? 16;
        
        if (typeof (NativeDb as any).newWithOptions === 'function') {
            this.native = (NativeDb as any).newWithOptions(
                filePath,
                this.lockMode,
                this.durability,
                this.walBatchSize,
                this.walFlushMs,
                undefined, // lockTimeoutMs (use default)
                this.stripeCount,
                this.bufferPoolSizeMB,
                this.bufferPageSizeKB,
            );
        } else {
            this.native = new NativeDb(filePath, this.wal);
        }
        
        if (options.schemas && typeof this.native.registerSchema === 'function') {
            for (const [path, schema] of Object.entries(options.schemas)) {
                this.native.registerSchema(path, JSON.stringify(schema));
            }
        }
        
        this.loadData();
        
        if (options.indices) {
            this.indices = options.indices;
            if (typeof this.native.registerIndex === 'function') {
                for (const idx of this.indices) {
                     this.native.registerIndex(idx.name, idx.field);
                     const idxPath = `${this.filePath}.${idx.name}.idx`;
                     if (!existsSync(idxPath)) {
                          this.rebuildIndexByName(idx);
                     }
                }
            }
        }

        if (options.memoryLimit && typeof this.native.configureMemory === 'function') {
            this.native.configureMemory(
                options.memoryLimit,
                options.coldStorageDir ?? null,
                options.evictionThresholdPct ?? null,
                options.evictionTargetPct ?? null,
            );
        }
        
        process.on('beforeExit', () => this.close());
    }

    private loadData(forceReload: boolean = false): void {
        if (this.encryptionKey && existsSync(this.filePath)) {
            try {
                const encrypted = readFileSync(this.filePath, 'utf8');
                const decrypted = decrypt(encrypted, this.encryptionKey);
                const tempPath = `${this.filePath}.tmp`;
                writeFileSync(tempPath, decrypted);

                if (this.native && typeof this.native.close === 'function') {
                    this.native.close();
                }
                this.native = new NativeDb(tempPath, this.wal);
                this.native.load();

                try { require('fs').unlinkSync(tempPath); } catch { }
            } catch (err) {
                this.native.load();
            }
        } else if (forceReload) {

            if (this.native && typeof this.native.close === 'function') {
                this.native.close();
            }

            if (typeof (NativeDb as any).newWithOptions === 'function') {
                this.native = (NativeDb as any).newWithOptions(
                    this.filePath,
                    this.lockMode,
                    this.durability,
                    this.walBatchSize,
                    this.walFlushMs,
                    undefined, // lockTimeoutMs
                    this.stripeCount,
                );
            } else {
                this.native = new NativeDb(this.filePath, this.wal);
            }
        }
    }

    private triggerSave(): void {
        if (this.wal) {
            if (this.saveTimeout) clearTimeout(this.saveTimeout);
            this.saveTimeout = setTimeout(() => {
                this.saveInternal();
            }, this.autoSaveInterval * 5);
        } else {
            if (this.saveTimeout) clearTimeout(this.saveTimeout);
            this.saveTimeout = setTimeout(() => {
                this.saveInternal();
            }, this.autoSaveInterval);
        }
    }

    private saveInternal(): void {
        if (this.encryptionKey) {
            const data = this.native.get('');
            const jsonStr = JSON.stringify(data, null, 2);
            const encrypted = encrypt(jsonStr, this.encryptionKey);
            writeFileSync(this.filePath, encrypted);
        } else {
            this.native.save();
        }
    }

    public async save(): Promise<void> {
        if (this.saveTimeout) {
            clearTimeout(this.saveTimeout);
            this.saveTimeout = null;
        }
        this.saveInternal();
    }

    public async sync(): Promise<void> {
        if (typeof this.native.sync === 'function') {
            await this.native.sync();
        }
    }

    public walStatus(): { enabled: boolean; committedLsn?: number } {
        if (typeof this.native.walStatus === 'function') {
            return this.native.walStatus();
        }
        return { enabled: this.wal };
    }

    public async close(): Promise<void> {
        if (!this.native) return;
        
        for (const timeout of this.ttlMap.values()) {
            clearTimeout(timeout);
        }
        this.ttlMap.clear();
        
        await this.save();
        
        this.subscriptions.clear();
        this.removeAllListeners();
        
        if (this.native && typeof this.native.close === 'function') {
            this.native.close();
        }
        (this as any).native = null;
    }

    private rebuildIndexByName(idx: IndexConfig): void {
        if (typeof this.native.clearIndex !== 'function') return;
        this.native.clearIndex(idx.name);
        const collection = this.native.get(idx.path);
        if (collection && typeof collection === 'object') {
            for (const [key, item] of Object.entries(collection as Record<string, unknown>)) {
                if (item && typeof item === 'object') {
                    const val = (item as Record<string, unknown>)[idx.field];
                    if (val !== undefined && typeof this.native.updateIndex === 'function') {
                        this.native.updateIndex(idx.name, val, `${idx.path}.${key}`, false);
                    }
                }
            }
        }
    }
    
    private rebuildIndices(): void {
        for (const idx of this.indices) {
            this.rebuildIndexByName(idx);
        }
    }

    private updateIndicesForPath(path: string, value: unknown, isDelete: boolean = false): void {
        if (this.indices.length === 0) return;
        if (typeof this.native.updateIndex !== 'function') return;
        
        const parts = path.split('.');
        if (parts.length < 2) return;
        
        const collectionPath = parts.slice(0, -1).join('.');
        
        for (const idx of this.indices) {
            if (collectionPath === idx.path) {
                if (isDelete) {
                    if (value && typeof value === 'object') {
                         const fieldValue = (value as Record<string, unknown>)[idx.field];
                         if (fieldValue !== undefined) {
                             this.native.updateIndex(idx.name, fieldValue, path, true);
                         }
                    }
                } else if (value && typeof value === 'object') {
                    const fieldValue = (value as Record<string, unknown>)[idx.field];
                    if (fieldValue !== undefined) {
                        this.native.updateIndex(idx.name, fieldValue, path, false);
                    }
                }
            }
        }
    }
    
    public before<T = unknown>(method: string, pathPattern: string, fn: MiddlewareFn<T>): void {
        const key = `${method}:${pathPattern}`;
        if (!this.beforeMiddlewares.has(key)) this.beforeMiddlewares.set(key, []);
        this.beforeMiddlewares.get(key)!.push(fn as MiddlewareFn);
    }
    
    public after<T = unknown>(method: string, pathPattern: string, fn: MiddlewareFn<T>): void {
        const key = `${method}:${pathPattern}`;
        if (!this.afterMiddlewares.has(key)) this.afterMiddlewares.set(key, []);
        this.afterMiddlewares.get(key)!.push(fn as MiddlewareFn);
    }
    
    private runMiddleware(
        type: 'before' | 'after', 
        method: string, 
        path: string, 
        value?: unknown
    ): unknown {
        const middlewares = type === 'before' ? this.beforeMiddlewares : this.afterMiddlewares;
        
        for (const [key, fns] of middlewares) {
            const [mMethod, mPattern] = key.split(':');
            if (mMethod === method && matchesPattern(mPattern as string, path)) {
                let ctx: MiddlewareContext = { 
                    path, 
                    value, 
                    operation: method,
                    timestamp: Date.now()
                };
                for (const fn of fns) {
                    const result = fn(ctx);
                    if (result) ctx = result;
                }
                value = ctx.value;
            }
        }
        return value;
    }

    public subscribe(
        pathPattern: string, 
        callback: (value: unknown, oldValue: unknown) => void
    ): () => void {
        if (!this.subscriptions.has(pathPattern)) {
            this.subscriptions.set(pathPattern, new Set());
        }
        this.subscriptions.get(pathPattern)!.add(callback);
        
        return () => {
            const subs = this.subscriptions.get(pathPattern);
            if (subs) {
                subs.delete(callback);
                if (subs.size === 0) {
                    this.subscriptions.delete(pathPattern);
                }
            }
        };
    }

    private notifySubscribers(path: string, newValue: unknown, oldValue: unknown): void {
        for (const [pattern, callbacks] of this.subscriptions) {
            if (matchesPattern(pattern, path)) {
                for (const callback of callbacks) {
                    try {
                        callback(newValue, oldValue);
                    } catch (err) {
                        this.emit('error', err);
                    }
                }
            }
        }
        
        this.emit('change', { path, value: newValue, oldValue });
    }

    public async setWithTTL(path: string, value: unknown, ttlSeconds: number): Promise<void> {
        await this.set(path, value);
        this.setTTL(path, ttlSeconds);
    }

    public setTTL(path: string, ttlSeconds: number): void {
        this.clearTTL(path);
        
        const expiresAt = Date.now() + ttlSeconds * 1000;
        this.ttlEntries.set(path, expiresAt);
        
        const timeout = setTimeout(async () => {
            this.ttlMap.delete(path);
            this.ttlEntries.delete(path);
            await this.delete(path);
            this.emit('ttl:expired', { path });
        }, ttlSeconds * 1000);
        
        this.ttlMap.set(path, timeout);
    }

    public async getTTL(path: string): Promise<number> {
        if (!(await this.has(path))) return -2;
        const expiresAt = this.ttlEntries.get(path);
        if (!expiresAt) return -1;
        return Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
    }

    public clearTTL(path: string): void {
        const timeout = this.ttlMap.get(path);
        if (timeout) {
            clearTimeout(timeout);
            this.ttlMap.delete(path);
        }
        this.ttlEntries.delete(path);
    }

    public hasTTL(path: string): boolean {
        return this.ttlEntries.has(path);
    }

    public async get<T = unknown>(path: string, defaultValue: T | null = null): Promise<T> {
        let val = this.native.get(path);
        
        if (this.isColdPointer(val)) {
            const restored = this.native.restore(path);
            if (restored) {
                 val = this.native.get(path);
            }
        }
        
        return (val === null || val === undefined ? defaultValue : val) as T;
    }

    public async getMany<T = unknown>(paths: string[]): Promise<(T | null)[]> {
        if (typeof this.native.getMany === 'function') {
            const results = this.native.getMany(paths);
            return results as (T | null)[];
        }
        return Promise.all(paths.map(p => this.get<T>(p)));
    }

    public getSync<T = unknown>(path: string, defaultValue: T | null = null): T {
        let val = this.native.get(path);
        
        if (this.isColdPointer(val)) {
            const restored = this.native.restore(path);
            if (restored) {
                 val = this.native.get(path);
            }
        }
        
        return (val === null || val === undefined ? defaultValue : val) as T;
    }

    public async set(path: string, value: unknown): Promise<void> {
        if (typeof this.native.validatePath === 'function') {
            this.native.validatePath(path, value);
        }
        
        const needsFetch = this.subscriptions.size > 0 || this.listenerCount('change') > 0 || this.indices.length > 0;
        const oldValue = needsFetch ? this.native.get(path) : undefined;
        
        value = this.runMiddleware('before', 'set', path, value);
        this.native.set(path, value);
        this.runMiddleware('after', 'set', path, value);
        this.triggerSave();
        this.updateIndicesForPath(path, value, false);
        
        if (needsFetch) {
            this.notifySubscribers(path, value, oldValue);
        }
    }

    public async has(path: string): Promise<boolean> {
        return this.native.has(path);
    }

    public async offload(path: string): Promise<string> {
        return this.native.offload(path);
    }

    public async restore(path: string): Promise<boolean> {
        return this.native.restore(path);
    }

    public async memoryStats(): Promise<{
        totalEstimatedBytes: number;
        maxMemoryBytes: number;
        coldKeysCount: number;
        hotKeysCount: number;
        utilizationPct: number;
    }> {
        if (typeof this.native.memoryStats === 'function') {
            return this.native.memoryStats();
        }
        return { totalEstimatedBytes: 0, maxMemoryBytes: 0, coldKeysCount: 0, hotKeysCount: 0, utilizationPct: 0 };
    }

    public async checkMemoryPressure(): Promise<string[]> {
        if (typeof this.native.checkMemoryPressure === 'function') {
            return this.native.checkMemoryPressure();
        }
        return [];
    }

    private isColdPointer(value: unknown): boolean {
        return typeof value === 'object' && value !== null && (value as any).__cold__ === true && typeof (value as any).id === 'string';
    }

    public async delete(path: string): Promise<void> {
        const needsFetch = this.subscriptions.size > 0 || this.listenerCount('change') > 0 || this.indices.length > 0;
        const oldValue = needsFetch ? this.native.get(path) : undefined;
        
        this.runMiddleware('before', 'delete', path, undefined);
        this.native.delete(path);
        this.runMiddleware('after', 'delete', path, undefined);
        this.triggerSave();
        this.updateIndicesForPath(path, oldValue, true);
        this.clearTTL(path);
        
        if (needsFetch) {
            this.notifySubscribers(path, undefined, oldValue);
        }
    }

    public async push(path: string, ...items: unknown[]): Promise<void> {
        const needsFetch = this.subscriptions.size > 0 || this.listenerCount('change') > 0;
        const oldValue = needsFetch ? this.native.get(path) : undefined;
        
        if (items.length > 1 && typeof this.native.pushBatch === 'function') {
            try {
                this.native.pushBatch(path, items);
            } catch {
                for (const item of items) {
                    this.native.push(path, item);
                }
            }
        } else {
            for (const item of items) {
                this.native.push(path, item);
            }
        }
        
        this.triggerSave();
        
        if (needsFetch) {
            const newValue = this.native.get(path);
            this.notifySubscribers(path, newValue, oldValue);
        }
    }

    public async pull(path: string, ...items: unknown[]): Promise<void> {
        if (typeof this.native.pullItems === 'function') {
            try {
                const needsFetch = this.subscriptions.size > 0 || this.listenerCount('change') > 0;
                const oldValue = needsFetch ? this.native.get(path) : undefined;
                
                this.native.pullItems(path, items);
                this.triggerSave();
                
                if (needsFetch) {
                    const newValue = this.native.get(path);
                    this.notifySubscribers(path, newValue, oldValue);
                }
                return;
            } catch {
            }
        }
        
        const arr = await this.get<unknown[]>(path);
        if (Array.isArray(arr)) {
            const newArr = arr.filter(x => !items.some(i => deepEqual(x, i)));
            await this.set(path, newArr);
        }
    }

    public async add(path: string, amount: number): Promise<number> {
        const val = await this.get<number>(path, 0);
        if (typeof val === 'number') {
            const newVal = val + amount;
            await this.set(path, newVal);
            return newVal;
        }
        return val;
    }

    public async subtract(path: string, amount: number): Promise<number> {
        return this.add(path, -amount);
    }

    public async findByIndex<T = unknown>(indexName: string, value: unknown): Promise<T | null> {
        if (typeof this.native.findIndexPaths !== 'function') return null;
        const paths = this.native.findIndexPaths(indexName, value);
        if (paths && paths.length > 0) {
            return this.get<T>(paths[0]);
        }
        return null;
    }

    public rebuildIndex(): void {
        this.rebuildIndices();
    }

    public query<T = unknown>(path: string): QueryBuilder<T> {
        return new QueryBuilder<T>(null, this).setPath(path);
    }

    public async find<T = unknown>(
        path: string, 
        predicate: ((item: T) => boolean) | Record<string, unknown>
    ): Promise<T | undefined> {
        const data = await this.get<unknown>(path);
        let items: T[] = [];
        if (Array.isArray(data)) {
            items = data as T[];
        } else if (typeof data === 'object' && data !== null) {
            items = Object.values(data) as T[];
        }

        if (typeof predicate === 'function') {
            return items.find(predicate);
        } else {
            return items.find(item => {
                const itemObj = item as Record<string, unknown>;
                for (const [key, val] of Object.entries(predicate)) {
                    if (itemObj[key] !== val) return false;
                }
                return true;
            });
        }
    }

    public async findAll<T = unknown>(
        path: string, 
        predicate: ((item: T) => boolean) | Record<string, unknown>
    ): Promise<T[]> {
        const data = await this.get<unknown>(path);
        let items: T[] = [];
        if (Array.isArray(data)) {
            items = data as T[];
        } else if (typeof data === 'object' && data !== null) {
            items = Object.values(data) as T[];
        }

        if (typeof predicate === 'function') {
            return items.filter(predicate);
        } else {
            return items.filter(item => {
                const itemObj = item as Record<string, unknown>;
                for (const [key, val] of Object.entries(predicate)) {
                    if (itemObj[key] !== val) return false;
                }
                return true;
            });
        }
    }

    public async paginate<T = unknown>(
        path: string, 
        page: number, 
        limit: number
    ): Promise<PaginationResult<T>> {
        const data = await this.get<unknown>(path);
        let items: T[] = [];
        if (Array.isArray(data)) {
            items = data as T[];
        } else if (typeof data === 'object' && data !== null) {
            items = Object.values(data) as T[];
        }
        
        const total = items.length;
        const totalPages = Math.ceil(total / limit);
        const start = (page - 1) * limit;
        const end = start + limit;
        const sliced = items.slice(start, end);
        
        return {
            data: sliced,
            meta: { 
                total, 
                pages: totalPages, 
                page, 
                limit,
                hasNext: page < totalPages,
                hasPrev: page > 1
            }
        };
    }

    public async batch(ops: BatchOperation[]): Promise<void> {
        for (const op of ops) {
            switch (op.type) {
                case 'set':
                    this.native.set(op.path, op.value);
                    this.updateIndicesForPath(op.path, op.value, false);
                    break;
                case 'delete':
                    this.native.delete(op.path);
                    this.updateIndicesForPath(op.path, undefined, true);
                    break;
                case 'push':
                    this.native.push(op.path, op.value);
                    break;
                case 'add': {
                    const val = (this.native.get(op.path) as number) ?? 0;
                    const newVal = val + (op.value as number);
                    this.native.set(op.path, newVal);
                    this.updateIndicesForPath(op.path, newVal, false);
                    break;
                }
                case 'subtract': {
                    const val = (this.native.get(op.path) as number) ?? 0;
                    const newVal = val - (op.value as number);
                    this.native.set(op.path, newVal);
                    this.updateIndicesForPath(op.path, newVal, false);
                    break;
                }
            }
        }
        this.triggerSave();
        this.emit('batch', { operations: ops });
    }

    private inTransaction = false;

    public async transaction<T = unknown>(
        fn: (tx: Transaction) => Promise<T> | T
    ): Promise<T> {
        const hasNativeTransactions = typeof this.native.beginTransaction === 'function';
        
        if (this.inTransaction) {
            const savepointName = `nested_${Math.random().toString(36).slice(2, 9)}`;
            if (hasNativeTransactions) {
                this.native.createSavepoint(savepointName);
            }
            try {
                const result = await fn({
                    savepoint: async (name) => hasNativeTransactions && this.native.createSavepoint(name),
                    rollbackTo: async (name) => hasNativeTransactions && this.native.rollbackToSavepoint(name)
                });
                return result;
            } catch (error) {
                if (hasNativeTransactions) {
                    this.native.rollbackToSavepoint(savepointName);
                }
                throw error;
            }
        }

        this.inTransaction = true;
        if (hasNativeTransactions) {
            this.native.beginTransaction();
        }
        
        const tx: Transaction = {
            savepoint: async (name) => hasNativeTransactions && this.native.createSavepoint(name),
            rollbackTo: async (name) => hasNativeTransactions && this.native.rollbackToSavepoint(name)
        };

        try {
            const result = await fn(tx);
            if (hasNativeTransactions) {
                this.native.commitTransaction();
            }
            this.inTransaction = false;
            this.emit('transaction:commit');
            return result;
        } catch (error) {
            if (hasNativeTransactions) {
                this.native.rollbackTransaction();
            }
            this.inTransaction = false;
            this.emit('transaction:rollback', { error });
            throw error;
        }
    }

    public async createSnapshot(name: string): Promise<string> {
        await this.save();
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupPath = `${this.filePath}.${name}.${timestamp}.bak`;
        await copyFile(this.filePath, backupPath);
        this.emit('snapshot:created', { path: backupPath, name });
        return backupPath;
    }

    public async restoreSnapshot(snapshotPath: string): Promise<void> {
        if (!existsSync(snapshotPath)) {
            throw new Error(`Snapshot not found: ${snapshotPath}`);
        }
        copyFileSync(snapshotPath, this.filePath);
        this.loadData(true);
        this.rebuildIndices();
        this.emit('snapshot:restored', { path: snapshotPath });
    }

    public async keys(path: string = ''): Promise<string[]> {
        const data = await this.get<unknown>(path);
        if (typeof data === 'object' && data !== null) {
            return Object.keys(data);
        }
        return [];
    }

    public async values<T = unknown>(path: string = ''): Promise<T[]> {
        const data = await this.get<unknown>(path);
        if (typeof data === 'object' && data !== null) {
            return Object.values(data) as T[];
        }
        return [];
    }

    public async count(path: string = ''): Promise<number> {
        const data = await this.get<unknown>(path);
        if (Array.isArray(data)) {
            return data.length;
        } else if (typeof data === 'object' && data !== null) {
            return Object.keys(data).length;
        }
        return 0;
    }

    public async clear(): Promise<void> {
        await this.set('', {});
    }

    public async stats(): Promise<{
        size: number;
        keys: number;
        indices: number;
        ttlKeys: number;
        subscriptions: number;
    }> {
        const data = await this.get<unknown>('');
        const jsonSize = JSON.stringify(data).length;
        
        return {
            size: jsonSize,
            keys: await this.count(''),
            indices: this.indices.length,
            ttlKeys: this.ttlEntries.size,
            subscriptions: Array.from(this.subscriptions.values())
                .reduce((acc, set) => acc + set.size, 0)
        };
    }

    public getSystemInfo(): SystemInfo {
        const nativeInfo = this.native.getSystemInfo();
        return {
            availableCores: nativeInfo.availableCores,
            parallelEnabled: nativeInfo.parallelEnabled,
            recommendedBatchSize: nativeInfo.recommendedBatchSize
        };
    }

    public async batchSetParallel(
        operations: Array<{ path: string; value: unknown }>
    ): Promise<ParallelResult> {
        const tuples: Array<[string, unknown]> = operations.map(op => [op.path, op.value]);
        
        const result = this.native.batchSetParallel(tuples);
        
        this.triggerSave();
        
        this.emit('batch', { 
            operations: operations.map(op => ({ type: 'set', path: op.path, value: op.value }))
        });
        
        return {
            success: result.success,
            count: result.count,
            error: result.error
        };
    }

    public async parallelQuery<T = unknown>(
        path: string, 
        filters: QueryFilter[]
    ): Promise<T[]> {
        if (typeof this.native.executeQueryFast === 'function') {
            try {
                const jsonStr = this.native.executeQueryFast(
                    path,
                    JSON.stringify(filters),
                    null,
                    null,
                    null,
                    null
                );
                return JSON.parse(jsonStr) as T[];
            } catch (e) {
            }
        }
        
        const result = this.native.parallelQuery(path, filters);
        return result as T[];
    }

    public async parallelAggregate(
        path: string,
        operation: 'sum' | 'avg' | 'min' | 'max' | 'count',
        field?: string
    ): Promise<number | null> {
        if (typeof this.native.executeAggregateFast === 'function') {
            try {
                const jsonStr = this.native.executeAggregateFast(
                    path,
                    JSON.stringify([]),
                    operation,
                    field
                );
                const result = JSON.parse(jsonStr);
                return result === null || result === undefined ? null : result;
            } catch {
            }
        }

        const result = this.native.parallelAggregate(path, operation, field);
        return result === null || result === undefined ? null : result;
    }

    public async parallelLookup(
        leftPath: string,
        rightPath: string,
        leftField: string,
        rightField: string,
        asField: string
    ): Promise<any[]> {
        if (typeof this.native.parallelLookup !== 'function') {
            throw new Error('parallelLookup not supported by native module');
        }
        const result = this.native.parallelLookup(leftPath, rightPath, leftField, rightField, asField);
        return Array.isArray(result) ? result : [];
    }
}

export default JSONDatabase;
