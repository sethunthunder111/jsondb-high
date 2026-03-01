/// <reference lib="dom" />
/**
 * v6: Pluggable Storage Adapters
 * 
 * Decouples the database engine from Node.js `fs` module.
 * This enables jsondb-high to run in:
 * - Node.js (default, uses native fs)
 * - Browser (localStorage, IndexedDB)
 * - Serverless Edge (Cloudflare KV, Vercel Edge Config)
 * - React Native (AsyncStorage)
 * - Custom backends (S3, HTTP, etc.)
 * 
 * Usage:
 * ```typescript
 * import { JSONDatabase, MemoryAdapter } from 'jsondb-high';
 * const db = new JSONDatabase('mydb', { adapter: new MemoryAdapter() });
 * ```
 */

/** Storage adapter interface — implement this for custom backends */
export interface StorageAdapter {
    /** Read the entire database content */
    read(): Promise<string | null>;
    
    /** Write the entire database content */
    write(data: string): Promise<void>;
    
    /** Check if the database exists */
    exists(): Promise<boolean>;
    
    /** Delete the database */
    delete?(): Promise<void>;
    
    /** Get the adapter name (for diagnostics) */
    readonly name: string;
}

// ─── Built-in Adapters ──────────────────────────────────────────────────────

/**
 * FileSystemAdapter — default Node.js adapter using native `fs`.
 * This is what jsondb-high uses when no adapter is specified.
 */
export class FileSystemAdapter implements StorageAdapter {
    readonly name = 'filesystem';
    private filePath: string;

    constructor(filePath: string) {
        this.filePath = filePath;
    }

    async read(): Promise<string | null> {
        const fs = await import('fs');
        try {
            return fs.readFileSync(this.filePath, 'utf-8');
        } catch {
            return null;
        }
    }

    async write(data: string): Promise<void> {
        const fs = await import('fs');
        const tmpPath = `${this.filePath}.tmp`;
        fs.writeFileSync(tmpPath, data, 'utf-8');
        fs.renameSync(tmpPath, this.filePath);
    }

    async exists(): Promise<boolean> {
        const fs = await import('fs');
        return fs.existsSync(this.filePath);
    }

    async delete(): Promise<void> {
        const fs = await import('fs');
        try {
            fs.unlinkSync(this.filePath);
        } catch {
            // ignore
        }
    }
}

/**
 * MemoryAdapter — stores data entirely in memory.
 * Useful for testing, ephemeral state, and browser environments.
 */
export class MemoryAdapter implements StorageAdapter {
    readonly name = 'memory';
    private data: string | null = null;

    async read(): Promise<string | null> {
        return this.data;
    }

    async write(data: string): Promise<void> {
        this.data = data;
    }

    async exists(): Promise<boolean> {
        return this.data !== null;
    }

    async delete(): Promise<void> {
        this.data = null;
    }
}

/**
 * LocalStorageAdapter — browser localStorage backend.
 * Limited to ~5MB in most browsers.
 */
export class LocalStorageAdapter implements StorageAdapter {
    readonly name = 'localStorage';
    private key: string;

    constructor(key: string) {
        this.key = `jsondb_${key}`;
    }

    async read(): Promise<string | null> {
        if (typeof localStorage === 'undefined') return null;
        return localStorage.getItem(this.key);
    }

    async write(data: string): Promise<void> {
        if (typeof localStorage === 'undefined') {
            throw new Error('localStorage is not available in this environment');
        }
        localStorage.setItem(this.key, data);
    }

    async exists(): Promise<boolean> {
        if (typeof localStorage === 'undefined') return false;
        return localStorage.getItem(this.key) !== null;
    }

    async delete(): Promise<void> {
        if (typeof localStorage !== 'undefined') {
            localStorage.removeItem(this.key);
        }
    }
}

/**
 * IndexedDBAdapter — browser IndexedDB backend.
 * Supports much larger datasets than localStorage (~unlimited).
 */
export class IndexedDBAdapter implements StorageAdapter {
    readonly name = 'indexeddb';
    private dbName: string;
    private storeName = 'jsondb_store';

    constructor(dbName: string) {
        this.dbName = `jsondb_${dbName}`;
    }

    private openDB(): Promise<IDBDatabase> {
        return new Promise((resolve, reject) => {
            if (typeof indexedDB === 'undefined') {
                reject(new Error('IndexedDB is not available'));
                return;
            }
            const request = indexedDB.open(this.dbName, 1);
            request.onupgradeneeded = () => {
                const db = request.result;
                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName);
                }
            };
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    async read(): Promise<string | null> {
        const db = await this.openDB();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readonly');
            const store = tx.objectStore(this.storeName);
            const req = store.get('data');
            req.onsuccess = () => resolve(req.result ?? null);
            req.onerror = () => reject(req.error);
        });
    }

    async write(data: string): Promise<void> {
        const db = await this.openDB();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite');
            const store = tx.objectStore(this.storeName);
            const req = store.put(data, 'data');
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
        });
    }

    async exists(): Promise<boolean> {
        const data = await this.read();
        return data !== null;
    }

    async delete(): Promise<void> {
        const db = await this.openDB();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite');
            const store = tx.objectStore(this.storeName);
            const req = store.delete('data');
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
        });
    }
}

/**
 * HttpAdapter — reads/writes to a remote HTTP endpoint.
 * Useful for edge functions or serverless environments.
 */
export class HttpAdapter implements StorageAdapter {
    readonly name = 'http';
    private url: string;
    private headers: Record<string, string>;

    constructor(url: string, headers: Record<string, string> = {}) {
        this.url = url;
        this.headers = {
            'Content-Type': 'application/json',
            ...headers,
        };
    }

    async read(): Promise<string | null> {
        try {
            const res = await fetch(this.url, { headers: this.headers });
            if (!res.ok) return null;
            return res.text();
        } catch {
            return null;
        }
    }

    async write(data: string): Promise<void> {
        const res = await fetch(this.url, {
            method: 'PUT',
            headers: this.headers,
            body: data,
        });
        if (!res.ok) {
            throw new Error(`HTTP write failed: ${res.status} ${res.statusText}`);
        }
    }

    async exists(): Promise<boolean> {
        try {
            const res = await fetch(this.url, {
                method: 'HEAD',
                headers: this.headers,
            });
            return res.ok;
        } catch {
            return false;
        }
    }
}

export default {
    FileSystemAdapter,
    MemoryAdapter,
    LocalStorageAdapter,
    IndexedDBAdapter,
    HttpAdapter,
};
