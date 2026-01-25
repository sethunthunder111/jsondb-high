import { EventEmitter } from 'events';

// ============================================
// Configuration Interfaces
// ============================================

export interface IndexConfig {
    name: string;
    path: string;
    field: string;
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
// Query Builder Classes
// ============================================

export declare class WhereClause<T> {
    constructor(queryBuilder: QueryBuilder<T>, field: string);
    eq(value: unknown): QueryBuilder<T>;
    ne(value: unknown): QueryBuilder<T>;
    gt(value: number): QueryBuilder<T>;
    gte(value: number): QueryBuilder<T>;
    lt(value: number): QueryBuilder<T>;
    lte(value: number): QueryBuilder<T>;
    between(min: number, max: number): QueryBuilder<T>;
    in(values: unknown[]): QueryBuilder<T>;
    notIn(values: unknown[]): QueryBuilder<T>;
    contains(substring: string): QueryBuilder<T>;
    startsWith(prefix: string): QueryBuilder<T>;
    endsWith(suffix: string): QueryBuilder<T>;
    matches(regex: RegExp): QueryBuilder<T>;
    exists(): QueryBuilder<T>;
    isNull(): QueryBuilder<T>;
    isNotNull(): QueryBuilder<T>;
}

export declare class QueryBuilder<T = unknown> {
    constructor(items: T[]);
    where(field: string): WhereClause<T>;
    filter(fn: (item: T) => boolean): QueryBuilder<T>;
    limit(n: number): QueryBuilder<T>;
    skip(n: number): QueryBuilder<T>;
    sort(options: SortOptions): QueryBuilder<T>;
    select(fields: string[]): QueryBuilder<T>;
    count(): number;
    sum(field: string): number;
    avg(field: string): number;
    min(field: string): number | undefined;
    max(field: string): number | undefined;
    distinct(field: string): unknown[];
    groupBy(field: string): Map<unknown, T[]>;
    exec(): Promise<T[]>;
    first(): T | undefined;
    last(): T | undefined;
}

// ============================================
// Main Database Class
// ============================================

export declare class JSONDatabase extends EventEmitter {
    constructor(filePath: string, options?: DBOptions);

    // Core Operations
    set(path: string, value: unknown): Promise<void>;
    get<T = unknown>(path: string, defaultValue?: T): Promise<T>;
    has(path: string): Promise<boolean>;
    delete(path: string): Promise<void>;
    push(path: string, ...items: unknown[]): Promise<void>;
    pull(path: string, ...items: unknown[]): Promise<void>;
    add(path: string, amount: number): Promise<number>;
    subtract(path: string, amount: number): Promise<number>;

    // TTL (Time to Live)
    setWithTTL(path: string, value: unknown, ttlSeconds: number): Promise<void>;
    setTTL(path: string, ttlSeconds: number): void;
    getTTL(path: string): Promise<number>;
    clearTTL(path: string): void;
    hasTTL(path: string): boolean;

    // Pub/Sub (Subscriptions)
    subscribe(
        pathPattern: string,
        callback: (value: unknown, oldValue: unknown) => void
    ): () => void;

    // Middleware
    before<T = unknown>(method: string, pathPattern: string, fn: MiddlewareFn<T>): void;
    after<T = unknown>(method: string, pathPattern: string, fn: MiddlewareFn<T>): void;

    // Indexing
    findByIndex<T = unknown>(indexName: string, value: unknown): Promise<T | null>;
    rebuildIndex(): void;

    // Query
    query<T = unknown>(path: string): QueryBuilder<T>;
    find<T = unknown>(
        path: string,
        predicate: ((item: T) => boolean) | Record<string, unknown>
    ): Promise<T | undefined>;
    findAll<T = unknown>(
        path: string,
        predicate: ((item: T) => boolean) | Record<string, unknown>
    ): Promise<T[]>;
    paginate<T = unknown>(path: string, page: number, limit: number): Promise<PaginationResult<T>>;

    // Batch Operations
    batch(ops: BatchOperation[]): Promise<void>;

    // Transactions
    transaction<T = unknown>(fn: (data: T) => Promise<T> | T): Promise<T>;

    // Snapshots
    createSnapshot(name: string): Promise<string>;
    restoreSnapshot(snapshotPath: string): Promise<void>;

    // Persistence
    save(): Promise<void>;
    close(): Promise<void>;

    // Utility Methods
    keys(path?: string): Promise<string[]>;
    values<T = unknown>(path?: string): Promise<T[]>;
    count(path?: string): Promise<number>;
    clear(): Promise<void>;
    stats(): Promise<{
        size: number;
        keys: number;
        indices: number;
        ttlKeys: number;
        subscriptions: number;
    }>;
}

export default JSONDatabase;
