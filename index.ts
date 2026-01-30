import { join } from 'path';
import { existsSync, copyFileSync, writeFileSync, readFileSync } from 'fs';
import { EventEmitter } from 'events';
import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from 'crypto';
import { performance } from 'perf_hooks';

// Load native binding
// @ts-ignore
import { NativeDb } from './index.js';

// ============================================
// TYPES & INTERFACES
// ============================================

export interface IndexConfig {
    name: string;
    path: string; // e.g. 'users'
    field: string; // e.g. 'email'
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
    encryptionKey?: string; // 32 character password for AES-256-GCM
    autoSaveInterval?: number; // ms, default 1000
    
    // ============================================
    // v4.5: Process Locking
    // ============================================
    /**
     * Multi-process locking mode
     * - 'exclusive': Acquire exclusive lock, prevent other processes (default for multi-process)
     * - 'shared': Open read-only, fail if exclusive lock exists
     * - 'none': No locking (fastest, single-process only, default for backwards compat)
     */
    lockMode?: 'exclusive' | 'shared' | 'none';
    
    /**
     * Timeout to wait for lock (ms)
     * Default: 0 (fail immediately if locked)
     */
    lockTimeoutMs?: number;
    
    // ============================================
    // v4.5: Durability / WAL
    // ============================================
    /**
     * Durability mode for writes
     * - 'none': No WAL, manual save only (fastest, unsafe)
     * - 'lazy': Write WAL, fsync every 100ms (~120k ops/sec, 100ms window)
     * - 'batched': Group commit every 10ms (~80k ops/sec, 10ms window, recommended)
     * - 'sync': Every write fsynced (~2k ops/sec, full ACID per op)
     * Default: 'batched' if wal=true, 'none' otherwise
     */
    durability?: 'none' | 'lazy' | 'batched' | 'sync';
    
    /**
     * WAL batch size for 'batched' mode
     * Default: 1000 operations
     */
    walBatchSize?: number;
    
    /**
     * WAL flush interval in ms for 'batched' mode
     * Default: 10ms
     */
    walFlushMs?: number;
    
    /**
     * Path-based schemas for validation
     * e.g. { 'users': { type: 'object', properties: { ... } } }
     */
    schemas?: Record<string, Schema>;

    /**
     * Threshold for slow query logging in ms
     * Default: 100ms
     */
    slowQueryThresholdMs?: number;
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

// ============================================
// PARALLEL PROCESSING INTERFACES
// ============================================

export interface SystemInfo {
    availableCores: number;
    parallelEnabled: boolean;
    recommendedBatchSize: number;
}

export interface ParallelConfig {
    /** Enable parallel processing (auto-detected by default) */
    enabled?: boolean;
    /** Minimum items before using parallel processing (default: 100) */
    threshold?: number;
    /** Maximum threads to use (default: auto-detected cores - 1) */
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

// ============================================
// UTILITY FUNCTIONS
// ============================================

/**
 * Simple Deep Equal Implementation
 */
function deepEqual(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
    const aObj = a as Record<string, unknown>;
    const bObj = b as Record<string, unknown>;
    const keysA = Object.keys(aObj), keysB = Object.keys(bObj);
    if (keysA.length !== keysB.length) return false;
    for (const key of keysA) {
        if (!keysB.includes(key) || !deepEqual(aObj[key], bObj[key])) return false;
    }
    return true;
}

/**
 * Pattern matching for middleware paths (supports wildcards)
 * Uses a cache to avoid regex recompilation
 */
const patternCache = new Map<string, RegExp>();

function matchesPattern(pattern: string, path: string): boolean {
    let regex = patternCache.get(pattern);
    if (!regex) {
        regex = new RegExp('^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]*').replace(/\*\*/g, '.*') + '$');
        patternCache.set(pattern, regex);
    }
    return regex.test(path);
}

// ============================================
// ENCRYPTION HELPERS
// ============================================

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
    
    // Format: salt (hex) + iv (hex) + authTag (hex) + encrypted data
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

// ============================================
// QUERY BUILDER (Fluent API)
// ============================================

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
    private items: T[];
    public db: JSONDatabase;
    private _limit?: number;
    private _skip?: number;
    private _sortOptions?: SortOptions;
    private _selectFields?: string[];
    private filters: FilterFn<T>[] = [];
    private queryFilters: QueryFilter[] = [];
    private path: string = '';

    constructor(items: T[], db: JSONDatabase) {
        this.items = items;
        this.db = db;
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
        
        // Fetch target collection directly from native to avoid async overhead if possible
        // using (db as any).native access pattern since native is private
        const targetCollection = (this.db as any).native.get(config.to);
        const targetItems: any[] = Array.isArray(targetCollection) 
            ? targetCollection 
            : Object.values(targetCollection ?? {});
            
        // Build lookup map (Hash Join)
        const lookup = new Map<string, any[]>();
        for (const item of targetItems) {
            const key = String(item[config.foreignField]);
            if (!lookup.has(key)) {
                lookup.set(key, []);
            }
            lookup.get(key)!.push(item);
        }
        
        // Perform join
        this.items = this.items.map(item => {
            const key = String((item as any)[config.localField]);
            const matches = lookup.get(key) || [];
            return {
                ...item,
                [config.as]: matches
            };
        }) as any;
        
        // Return this as filtered/modified query builder
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

    // Aggregation methods
    count(): number {
        return this.applyFilters().length;
    }

    sum(field: string): number {
        return this.applyFilters().reduce((acc, item) => {
            const value = this.getFieldValue(item, field);
            return acc + (typeof value === 'number' ? value : 0);
        }, 0);
    }

    avg(field: string): number {
        const items = this.applyFilters();
        if (items.length === 0) return 0;
        return this.sum(field) / items.length;
    }

    min(field: string): number | undefined {
        const items = this.applyFilters();
        if (items.length === 0) return undefined;
        return Math.min(...items.map(item => {
            const v = this.getFieldValue(item, field);
            return typeof v === 'number' ? v : Infinity;
        }));
    }

    max(field: string): number | undefined {
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

    private applyFilters(): T[] {
        let result = this.items;
        for (const filter of this.filters) {
            result = result.filter(filter);
        }
        return result;
    }

    async exec(): Promise<T[]> {
        const startTime = performance.now();
        let result: T[] = [];
        let usedIndex = false;

        if (this.db && this.queryFilters.length > 0) {
            for (const f of this.queryFilters) {
                if (f.op === 'eq') {
                    const index = (this.db as any).indices.find((idx: any) => idx.path === this.path && idx.field === f.field);
                    if (index) {
                        const paths = (this.db as any).native.findIndexPaths(index.name, f.value);
                        if (paths) {
                            const indexedItems = await Promise.all(paths.map((p: string) => this.db.get<T>(p)));
                            result = indexedItems.filter(x => x !== null) as T[];
                            usedIndex = true;
                            break;
                        }
                    }
                }
            }
        }

        if (!usedIndex) {
            result = this.applyFilters();
        } else {
             // If we used index, we still need to apply other filters
             // Note: the 'eq' filter that used the index is already satisfied, 
             // but applyFilters() will run it again in JS which is fine for correctness.
             for (const filter of this.filters) {
                 result = result.filter(filter);
             }
        }

        const finalResult = this.applyPostProcessing(result);
        const duration = performance.now() - startTime;
        
        // Slow query detection
        const threshold = (this.db as any).slowQueryThresholdMs ?? 100;
        if (duration > threshold) {
            this.db.emit('slow_query', {
                path: this.path,
                filters: this.queryFilters,
                duration,
                usedIndex
            });
        }

        return finalResult;
    }

    private applyPostProcessing(result: T[]): T[] {
        // Sort
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

        // Skip
        if (this._skip !== undefined) {
            result = result.slice(this._skip);
        }

        // Limit
        if (this._limit !== undefined) {
            result = result.slice(0, this._limit);
        }

        // Select
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
        const items = this.applyFilters();
        return items[0];
    }

    last(): T | undefined {
        const items = this.applyFilters();
        return items[items.length - 1];
    }
}

// ============================================
// MAIN DATABASE CLASS
// ============================================

export class JSONDatabase extends EventEmitter {
    private native: InstanceType<typeof NativeDb>;
    private indices: IndexConfig[] = [];
    private beforeMiddlewares: Map<string, MiddlewareFn[]> = new Map();
    private afterMiddlewares: Map<string, MiddlewareFn[]> = new Map();
    private wal: boolean = false;
    private saveTimeout: NodeJS.Timeout | null = null;
    private autoSaveInterval: number;
    private encryptionKey?: string;
    
    // TTL Management
    private ttlMap: Map<string, NodeJS.Timeout> = new Map();
    private ttlEntries: Map<string, number> = new Map(); // path -> expiresAt timestamp
    
    // Subscriptions (Pub/Sub)
    private subscriptions: Map<string, Set<(value: unknown, oldValue: unknown) => void>> = new Map();

    // v4.5: New options
    private lockMode: 'exclusive' | 'shared' | 'none';
    private durability: 'none' | 'lazy' | 'batched' | 'sync';
    private walBatchSize: number;
    private walFlushMs: number;
    private slowQueryThresholdMs: number;

    constructor(private filePath: string, options: DBOptions = {}) {
        super();
        this.wal = options.wal ?? false;
        this.encryptionKey = options.encryptionKey;
        this.autoSaveInterval = options.autoSaveInterval ?? 1000;
        
        // v4.5: Initialize new options
        this.lockMode = options.lockMode ?? (this.wal ? 'exclusive' : 'none');
        this.durability = options.durability ?? (this.wal ? 'batched' : 'none');
        this.walBatchSize = options.walBatchSize ?? 1000;
        this.walFlushMs = options.walFlushMs ?? 10;
        this.slowQueryThresholdMs = options.slowQueryThresholdMs ?? 100;
        
        // v4.5: Use new constructor with options if available
        if (typeof (NativeDb as any).newWithOptions === 'function') {
            this.native = (NativeDb as any).newWithOptions(
                filePath,
                this.lockMode,
                this.durability,
                this.walBatchSize,
                this.walFlushMs
            );
        } else {
            // Fallback to legacy constructor
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
        
        // Cleanup on process exit
        process.on('beforeExit', () => this.close());
    }

    private loadData(): void {
        if (this.encryptionKey && existsSync(this.filePath)) {
            try {
                const encrypted = readFileSync(this.filePath, 'utf8');
                const decrypted = decrypt(encrypted, this.encryptionKey);
                // Write decrypted temporarily for native to load
                const tempPath = `${this.filePath}.tmp`;
                writeFileSync(tempPath, decrypted);
                this.native = new NativeDb(tempPath, this.wal);
                this.native.load();
                // Clean up temp file
                try { require('fs').unlinkSync(tempPath); } catch { /* ignore */ }
            } catch (err) {
                // If decryption fails, might be first run or corrupted
                this.native.load();
            }
        } else {
            this.native.load();
        }
    }

    private triggerSave(): void {
        if (this.wal) {
            // WAL mode: immediate writes are appended to WAL by Rust.
            // Periodic checkpoint to consolidate
            if (this.saveTimeout) clearTimeout(this.saveTimeout);
            this.saveTimeout = setTimeout(() => {
                this.saveInternal();
            }, this.autoSaveInterval * 5); // Less frequent checkpoints in WAL mode
        } else {
            // In-Memory mode: Debounce save
            if (this.saveTimeout) clearTimeout(this.saveTimeout);
            this.saveTimeout = setTimeout(() => {
                this.saveInternal();
            }, this.autoSaveInterval);
        }
    }

    private saveInternal(): void {
        if (this.encryptionKey) {
            // Get data, encrypt, and write
            const data = this.native.get('');
            const jsonStr = JSON.stringify(data, null, 2);
            const encrypted = encrypt(jsonStr, this.encryptionKey);
            writeFileSync(this.filePath, encrypted);
        } else {
            this.native.save();
        }
    }

    /**
     * Force save to disk immediately
     */
    public async save(): Promise<void> {
        if (this.saveTimeout) {
            clearTimeout(this.saveTimeout);
            this.saveTimeout = null;
        }
        this.saveInternal();
    }

    /**
     * v4.5: Explicit sync for durability
     * 
     * Ensures all pending writes are flushed to the WAL and fsynced.
     * Use this when you need guaranteed durability before continuing.
     * 
     * @example
     * await db.set('critical.data', value);
     * await db.sync(); // Guaranteed durable
     */
    public async sync(): Promise<void> {
        if (typeof this.native.sync === 'function') {
            await this.native.sync();
        }
    }

    /**
     * v4.5: Get WAL status
     * 
     * Returns information about the Write-Ahead Log state.
     */
    public walStatus(): { enabled: boolean; committedLsn?: number } {
        if (typeof this.native.walStatus === 'function') {
            return this.native.walStatus();
        }
        return { enabled: this.wal };
    }

    /**
     * Close the database gracefully
     */
    public async close(): Promise<void> {
        if (!this.native) return;
        
        // Clear all TTL timers
        for (const timeout of this.ttlMap.values()) {
            clearTimeout(timeout);
        }
        this.ttlMap.clear();
        
        // Force save
        await this.save();
        
        // Clear subscriptions
        this.subscriptions.clear();
        this.removeAllListeners();
        
        // v4.5: Release native resources (locks, WAL handles)
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
    
    // Legacy method name kept for internal compatibility references, replaced implementation
    private rebuildIndices(): void {
        for (const idx of this.indices) {
            this.rebuildIndexByName(idx);
        }
    }

    /**
     * Incrementally update indices for a single path change
     * Much faster than full rebuild for single-item operations
     */
    /**
     * Incrementally update indices for a single path change
     */
    private updateIndicesForPath(path: string, value: unknown, isDelete: boolean = false): void {
        if (this.indices.length === 0) return;
        if (typeof this.native.updateIndex !== 'function') return;
        
        const parts = path.split('.');
        if (parts.length < 2) return;
        
        const collectionPath = parts.slice(0, -1).join('.');
        
        for (const idx of this.indices) {
            // Check if path matches collection (e.g. users.123 updates index on users)
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
    
    // ============================================
    // MIDDLEWARE
    // ============================================
    
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

    // ============================================
    // PUB/SUB (Subscriptions)
    // ============================================

    /**
     * Subscribe to changes on a path pattern
     * @param pathPattern - Path pattern with optional wildcards (* for single segment, ** for multiple)
     * @param callback - Function called when value changes
     * @returns Unsubscribe function
     */
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
        
        // Also emit generic events
        this.emit('change', { path, value: newValue, oldValue });
    }

    // ============================================
    // TTL (Time to Live)
    // ============================================

    /**
     * Set a key with TTL (expires after specified seconds)
     */
    public async setWithTTL(path: string, value: unknown, ttlSeconds: number): Promise<void> {
        await this.set(path, value);
        this.setTTL(path, ttlSeconds);
    }

    /**
     * Set TTL on an existing key
     */
    public setTTL(path: string, ttlSeconds: number): void {
        // Clear existing TTL if any
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

    /**
     * Get remaining TTL for a key in seconds (returns -1 if no TTL, -2 if key doesn't exist)
     */
    public async getTTL(path: string): Promise<number> {
        if (!(await this.has(path))) return -2;
        const expiresAt = this.ttlEntries.get(path);
        if (!expiresAt) return -1;
        return Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
    }

    /**
     * Remove TTL from a key (make it persistent)
     */
    public clearTTL(path: string): void {
        const timeout = this.ttlMap.get(path);
        if (timeout) {
            clearTimeout(timeout);
            this.ttlMap.delete(path);
        }
        this.ttlEntries.delete(path);
    }

    /**
     * Check if a key has TTL set
     */
    public hasTTL(path: string): boolean {
        return this.ttlEntries.has(path);
    }

    // ============================================
    // CORE API
    // ============================================

    public async get<T = unknown>(path: string, defaultValue: T | null = null): Promise<T> {
        const val = this.native.get(path);
        return (val === null || val === undefined ? defaultValue : val) as T;
    }

    public async set(path: string, value: unknown): Promise<void> {
        // Run validation (if native module supports it)
        if (typeof this.native.validatePath === 'function') {
            this.native.validatePath(path, value);
        }
        
        const oldValue = this.native.get(path);
        value = this.runMiddleware('before', 'set', path, value);
        this.native.set(path, value);
        this.runMiddleware('after', 'set', path, value);
        this.triggerSave();
        this.updateIndicesForPath(path, value, false);
        this.notifySubscribers(path, value, oldValue);
    }

    public async has(path: string): Promise<boolean> {
        return this.native.has(path);
    }

    public async delete(path: string): Promise<void> {
        const oldValue = this.native.get(path);
        this.runMiddleware('before', 'delete', path, undefined);
        this.native.delete(path);
        this.runMiddleware('after', 'delete', path, undefined);
        this.triggerSave();
        this.updateIndicesForPath(path, oldValue, true);
        this.clearTTL(path);
        this.notifySubscribers(path, undefined, oldValue);
    }

    public async push(path: string, ...items: unknown[]): Promise<void> {
        // Validation for each item if path matches a schema
        // Note: push adds to array, so we should ideally validate the item against schema.items 
        // if the path itself is a collection with a schema.
        // For simplicity, we can validate the whole new collection after pushing? 
        // No, let's validate each item if possible, or skip for now and rely on full collection validation.
        
        const oldValue = this.native.get(path);
        for (const item of items) {
             // Basic validation attempt: we don't know if 'item' matches 'path' or 'path' is parent.
             // Native validatePath handles prefix matching.
            this.native.push(path, item);
        }
        const newValue = this.native.get(path);
        this.triggerSave();
        // Arrays don't need index updates (indices are for object collections)
        this.notifySubscribers(path, newValue, oldValue);
    }

    public async pull(path: string, ...items: unknown[]): Promise<void> {
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

    // ============================================
    // INDEXING
    // ============================================
    
    public async findByIndex<T = unknown>(indexName: string, value: unknown): Promise<T | null> {
        if (typeof this.native.findIndexPaths !== 'function') return null;
        const paths = this.native.findIndexPaths(indexName, value);
        if (paths && paths.length > 0) {
            // Return first match for compatibility
            return this.get<T>(paths[0]);
        }
        return null;
    }

    /**
     * Manually trigger index rebuild
     */
    public rebuildIndex(): void {
        this.rebuildIndices();
    }

    // ============================================
    // QUERY
    // ============================================

    public query<T = unknown>(path: string): QueryBuilder<T> {
        const data = this.native.get(path);
        let items: T[] = [];
        if (Array.isArray(data)) {
            items = [...data] as T[];
        } else if (typeof data === 'object' && data !== null) {
            items = Object.values(data) as T[];
        }
        return new QueryBuilder<T>(items, this).setPath(path);
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

    // ============================================
    // BATCH OPERATIONS
    // ============================================

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

    // ============================================
    // TRANSACTIONS
    // ============================================

    private inTransaction = false;

    /**
     * Execute a function within a transaction.
     * Supports nested transactions using savepoints.
     * All operations (set, delete, etc.) called on this DB instance within the 
     * transaction function will be atomic.
     */
    public async transaction<T = unknown>(
        fn: (tx: Transaction) => Promise<T> | T
    ): Promise<T> {
        // Check if native module supports transactions
        const hasNativeTransactions = typeof this.native.beginTransaction === 'function';
        
        if (this.inTransaction) {
            // Nested transaction: use a savepoint
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

    // ============================================
    // SNAPSHOTS
    // ============================================

    public async createSnapshot(name: string): Promise<string> {
        await this.save();
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupPath = `${this.filePath}.${name}.${timestamp}.bak`;
        copyFileSync(this.filePath, backupPath);
        this.emit('snapshot:created', { path: backupPath, name });
        return backupPath;
    }

    public async restoreSnapshot(snapshotPath: string): Promise<void> {
        if (!existsSync(snapshotPath)) {
            throw new Error(`Snapshot not found: ${snapshotPath}`);
        }
        copyFileSync(snapshotPath, this.filePath);
        this.loadData();
        this.rebuildIndices();
        this.emit('snapshot:restored', { path: snapshotPath });
    }

    // ============================================
    // UTILITY METHODS
    // ============================================

    /**
     * Get all keys under a path
     */
    public async keys(path: string = ''): Promise<string[]> {
        const data = await this.get<unknown>(path);
        if (typeof data === 'object' && data !== null) {
            return Object.keys(data);
        }
        return [];
    }

    /**
     * Get all values under a path
     */
    public async values<T = unknown>(path: string = ''): Promise<T[]> {
        const data = await this.get<unknown>(path);
        if (typeof data === 'object' && data !== null) {
            return Object.values(data) as T[];
        }
        return [];
    }

    /**
     * Get count of items under a path
     */
    public async count(path: string = ''): Promise<number> {
        const data = await this.get<unknown>(path);
        if (Array.isArray(data)) {
            return data.length;
        } else if (typeof data === 'object' && data !== null) {
            return Object.keys(data).length;
        }
        return 0;
    }

    /**
     * Clear all data
     */
    public async clear(): Promise<void> {
        await this.set('', {});
    }

    /**
     * Get database statistics
     */
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

    // ============================================
    // PARALLEL PROCESSING
    // ============================================

    /**
     * Get system resource information for parallel processing decisions
     * Returns info about available cores and whether parallel mode is enabled
     */
    public getSystemInfo(): SystemInfo {
        const nativeInfo = this.native.getSystemInfo();
        return {
            availableCores: nativeInfo.availableCores,
            parallelEnabled: nativeInfo.parallelEnabled,
            recommendedBatchSize: nativeInfo.recommendedBatchSize
        };
    }

    /**
     * Execute batch set operations with automatic parallel optimization.
     * Uses multiple CPU cores when workload is large enough (≥100 items).
     * Falls back to sequential processing for small batches to avoid overhead.
     * 
     * @param operations - Array of {path, value} objects to set
     * @returns ParallelResult with success status and count of operations completed
     * 
     * @example
     * ```typescript
     * const result = await db.batchSetParallel([
     *     { path: 'users.1', value: { name: 'Alice' } },
     *     { path: 'users.2', value: { name: 'Bob' } },
     *     // ... potentially thousands more
     * ]);
     * console.log(`Completed ${result.count} operations`);
     * ```
     */
    public async batchSetParallel(
        operations: Array<{ path: string; value: unknown }>
    ): Promise<ParallelResult> {
        // Convert to tuple array for native call
        const tuples: Array<[string, unknown]> = operations.map(op => [op.path, op.value]);
        
        const result = this.native.batchSetParallel(tuples);
        
        // Trigger save after batch
        this.triggerSave();
        
        // Emit batch event
        this.emit('batch', { 
            operations: operations.map(op => ({ type: 'set', path: op.path, value: op.value }))
        });
        
        return {
            success: result.success,
            count: result.count,
            error: result.error
        };
    }

    /**
     * Execute parallel query with native Rust filtering.
     * More efficient than JS-based queries for large datasets (≥100 items).
     * Automatically uses parallel iteration when beneficial.
     * 
     * @param path - Path to the collection to query
     * @param filters - Array of filter conditions to apply
     * @returns Filtered results array
     * 
     * @example
     * ```typescript
     * const adults = await db.parallelQuery('users', [
     *     { field: 'age', op: 'gte', value: 18 },
     *     { field: 'status', op: 'eq', value: 'active' }
     * ]);
     * ```
     */
    public async parallelQuery<T = unknown>(
        path: string, 
        filters: QueryFilter[]
    ): Promise<T[]> {
        const result = this.native.parallelQuery(path, filters);
        return result as T[];
    }

    /**
     * Parallel aggregation operations using native Rust processing.
     * Efficiently computes sum, avg, min, max, or count over large datasets.
     * 
     * @param path - Path to the collection
     * @param operation - Aggregation type: 'sum', 'avg', 'min', 'max', or 'count'
     * @param field - Optional field to aggregate (required for sum, avg, min, max)
     * @returns Aggregation result or null if no data
     * 
     * @example
     * ```typescript
     * const totalSales = await db.parallelAggregate('orders', 'sum', 'amount');
     * const avgAge = await db.parallelAggregate('users', 'avg', 'age');
     * const userCount = await db.parallelAggregate('users', 'count');
     * ```
     */
    public async parallelAggregate(
        path: string,
        operation: 'sum' | 'avg' | 'min' | 'max' | 'count',
        field?: string
    ): Promise<number | null> {
        const result = this.native.parallelAggregate(path, operation, field);
        return result === null || result === undefined ? null : result;
    }

    /**
     * Perform a parallel left outer join (lookup) between two collections using Rust.
     * efficient for large datasets as it avoids passing data through JS.
     * 
     * @param leftPath - Path to the source collection
     * @param rightPath - Path to the target collection
     * @param leftField - Field in source collection
     * @param rightField - Field in target collection
     * @param asField - Name of the field to store matches
     */
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

// Default export
export default JSONDatabase;
