import { join } from 'path';
import { existsSync, copyFileSync, writeFileSync, readFileSync } from 'fs';
import { EventEmitter } from 'events';
import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from 'crypto';

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

    eq(value: unknown): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => this.getFieldValue(item) === value);
    }

    ne(value: unknown): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => this.getFieldValue(item) !== value);
    }

    gt(value: number): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v > value;
        });
    }

    gte(value: number): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v >= value;
        });
    }

    lt(value: number): QueryBuilder<T> {
        return this.queryBuilder.filter((item: T) => {
            const v = this.getFieldValue(item);
            return typeof v === 'number' && v < value;
        });
    }

    lte(value: number): QueryBuilder<T> {
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
}

export class QueryBuilder<T = unknown> {
    private items: T[];
    private _limit?: number;
    private _skip?: number;
    private _sortOptions?: SortOptions;
    private _selectFields?: string[];
    private filters: FilterFn<T>[] = [];

    constructor(items: T[]) {
        this.items = [...items];
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
        let result = this.applyFilters();

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
    private indexMaps: Map<string, Map<string, string>> = new Map();
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

    constructor(private filePath: string, options: DBOptions = {}) {
        super();
        this.wal = options.wal ?? false;
        this.encryptionKey = options.encryptionKey;
        this.autoSaveInterval = options.autoSaveInterval ?? 1000;
        
        this.native = new NativeDb(filePath, this.wal);
        this.loadData();
        
        if (options.indices) {
            this.indices = options.indices;
            this.rebuildIndices();
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
     * Close the database gracefully
     */
    public async close(): Promise<void> {
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
    }

    private rebuildIndices(): void {
        for (const idx of this.indices) {
            const map = new Map<string, string>();
            const collection = this.native.get(idx.path);
            if (collection && typeof collection === 'object') {
                for (const [key, item] of Object.entries(collection as Record<string, unknown>)) {
                    if (item && typeof item === 'object') {
                        const val = (item as Record<string, unknown>)[idx.field];
                        if (val !== undefined) {
                            map.set(String(val), `${idx.path}.${key}`);
                        }
                    }
                }
            }
            this.indexMaps.set(idx.name, map);
        }
    }

    /**
     * Incrementally update indices for a single path change
     * Much faster than full rebuild for single-item operations
     */
    private updateIndicesForPath(path: string, value: unknown, isDelete: boolean = false): void {
        if (this.indices.length === 0) return;
        
        // Parse path: e.g., "users.123" -> collection="users", key="123"
        const parts = path.split('.');
        if (parts.length < 2) return;
        
        const collectionPath = parts.slice(0, -1).join('.');
        const itemKey = parts[parts.length - 1];
        
        for (const idx of this.indices) {
            // Only update if this path is within an indexed collection
            if (collectionPath === idx.path || path.startsWith(idx.path + '.')) {
                const map = this.indexMaps.get(idx.name);
                if (!map) continue;
                
                if (isDelete) {
                    // Remove any entries pointing to this path
                    for (const [indexVal, storedPath] of map.entries()) {
                        if (storedPath === path || storedPath.startsWith(path + '.')) {
                            map.delete(indexVal);
                        }
                    }
                } else if (value && typeof value === 'object') {
                    const fieldValue = (value as Record<string, unknown>)[idx.field];
                    if (fieldValue !== undefined) {
                        // Remove old entry if exists
                        for (const [indexVal, storedPath] of map.entries()) {
                            if (storedPath === path) {
                                map.delete(indexVal);
                                break;
                            }
                        }
                        // Add new entry
                        map.set(String(fieldValue), path);
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

    public async set(path: string, value: unknown): Promise<void> {
        const oldValue = this.native.get(path);
        value = this.runMiddleware('before', 'set', path, value);
        this.native.set(path, value);
        this.runMiddleware('after', 'set', path, value);
        this.triggerSave();
        this.updateIndicesForPath(path, value, false);
        this.notifySubscribers(path, value, oldValue);
    }

    public async get<T = unknown>(path: string, defaultValue?: T): Promise<T> {
        const val = this.native.get(path);
        return (val === null || val === undefined ? defaultValue : val) as T;
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
        this.updateIndicesForPath(path, undefined, true);
        this.clearTTL(path);
        this.notifySubscribers(path, undefined, oldValue);
    }

    public async push(path: string, ...items: unknown[]): Promise<void> {
        const oldValue = this.native.get(path);
        for (const item of items) {
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
        const map = this.indexMaps.get(indexName);
        if (!map) throw new Error(`Index '${indexName}' not found`);
        const path = map.get(String(value));
        if (!path) return null;
        return this.get<T>(path);
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
        return new QueryBuilder<T>(items);
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

    public async transaction<T = unknown>(
        fn: (data: T) => Promise<T> | T
    ): Promise<T> {
        // Get snapshot for rollback
        const snapshot = JSON.stringify(this.native.get(''));
        
        try {
            const root = await this.get<T>('');
            const result = await fn(root);
            await this.set('', result);
            this.emit('transaction:commit');
            return result;
        } catch (error) {
            // Rollback on error
            const rollbackData = JSON.parse(snapshot);
            this.native.set('', rollbackData);
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
}

// Default export
export default JSONDatabase;
