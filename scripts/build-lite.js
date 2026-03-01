#!/usr/bin/env node

/**
 * v6: Lite Mode Build Script
 * 
 * Builds a stripped-down version of jsondb-high for size-constrained environments
 * (AWS Lambda, Cloudflare Workers, etc.).
 * 
 * Lite mode:
 * - Uses the WASM engine instead of native binaries (no .node files needed)
 * - Strips heavy dependencies (rayon parallelism, full index engine)
 * - Target size: < 2MB (vs ~30MB for native)
 * 
 * Usage:
 *   node scripts/build-lite.js
 *   import { JSONDatabase } from 'jsondb-high/lite';
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const LITE_DIR = path.join(__dirname, '..', 'lite');
const WASM_PKG = path.join(__dirname, '..', 'wasm', 'pkg');

console.log('🪶 Building jsondb-high Lite mode...\n');

// Step 1: Build WASM if not already built
if (!fs.existsSync(path.join(WASM_PKG, 'jsondb_high_wasm_bg.wasm'))) {
    console.log('→ WASM package not found, building...');
    try {
        execSync('node scripts/build-wasm.js', {
            cwd: path.join(__dirname, '..'),
            stdio: 'inherit',
        });
    } catch (e) {
        console.error('❌ Failed to build WASM. Run `bun run build:wasm` first.');
        process.exit(1);
    }
}

// Step 2: Create lite directory
if (fs.existsSync(LITE_DIR)) {
    fs.rmSync(LITE_DIR, { recursive: true });
}
fs.mkdirSync(LITE_DIR, { recursive: true });

// Step 3: Copy WASM package files
const wasmFiles = ['jsondb_high_wasm_bg.wasm', 'jsondb_high_wasm.js', 'jsondb_high_wasm_bg.wasm.d.ts'];
for (const file of wasmFiles) {
    const src = path.join(WASM_PKG, file);
    if (fs.existsSync(src)) {
        fs.copyFileSync(src, path.join(LITE_DIR, file));
        console.log(`  ✓ Copied ${file}`);
    }
}

// Step 4: Copy the WASM shim
const shimSrc = path.join(__dirname, '..', 'wasm-shim.js');
if (fs.existsSync(shimSrc)) {
    fs.copyFileSync(shimSrc, path.join(LITE_DIR, 'wasm-shim.js'));
    console.log('  ✓ Copied wasm-shim.js');
}

// Step 5: Copy adapters
const adaptersSrc = path.join(__dirname, '..', 'adapters.ts');
if (fs.existsSync(adaptersSrc)) {
    fs.copyFileSync(adaptersSrc, path.join(LITE_DIR, 'adapters.ts'));
    console.log('  ✓ Copied adapters.ts');
}

// Step 6: Generate lite entry point
const liteEntry = `
/**
 * jsondb-high/lite — Lightweight WASM-only mode
 * 
 * This entry point skips native binary loading entirely and goes
 * straight to the WASM engine. Perfect for:
 * - AWS Lambda (small cold starts)
 * - Cloudflare Workers
 * - Browser environments
 * - Any size-constrained deployment
 * 
 * Trade-off: No multi-threading (rayon), slightly slower than native.
 * But significantly smaller bundle size (< 2MB vs ~30MB).
 */

const path = require('path');
const { createWasmShim } = require('./wasm-shim');

class LiteDatabase {
    constructor(name, options = {}) {
        this.name = name;
        this.options = options;
        this.data = {};
        this._native = null;
    }

    /**
     * Initialize the lite database.
     * Must be called before any operations.
     */
    async init() {
        const wasmPath = path.join(__dirname, 'jsondb_high_wasm_bg.wasm');
        this._native = await createWasmShim(this.name, wasmPath);
        return this;
    }

    /** Get a value by dot-notation path */
    get(keyPath) {
        if (!this._native) throw new Error('Call .init() first');
        return this._native.get(keyPath);
    }

    /** Set a value at a dot-notation path */
    set(keyPath, value) {
        if (!this._native) throw new Error('Call .init() first');
        this._native.set(keyPath, value);
    }

    /** Delete a value at a dot-notation path */
    delete(keyPath) {
        if (!this._native) throw new Error('Call .init() first');
        this._native.delete(keyPath);
    }

    /** Check if a key exists */
    has(keyPath) {
        if (!this._native) throw new Error('Call .init() first');
        return this._native.has(keyPath);
    }

    /** Save to disk */
    save() {
        if (!this._native) throw new Error('Call .init() first');
        this._native.save();
    }

    /** Get system info */
    getSystemInfo() {
        return {
            engine: 'wasm-lite',
            mode: 'lite',
            parallelism: false,
            bundleSize: '< 2MB',
        };
    }
}

module.exports = { LiteDatabase };
`;

fs.writeFileSync(path.join(LITE_DIR, 'index.js'), liteEntry.trim());
console.log('  ✓ Generated lite/index.js');

// Step 7: Generate package.json for the lite subpackage
const litePkg = {
    name: 'jsondb-high-lite',
    version: '6.0.0',
    description: 'Lightweight WASM-only build of jsondb-high for size-constrained environments',
    main: 'index.js',
    keywords: ['json', 'database', 'wasm', 'lite', 'serverless', 'edge'],
};
fs.writeFileSync(path.join(LITE_DIR, 'package.json'), JSON.stringify(litePkg, null, 2));
console.log('  ✓ Generated lite/package.json');

// Step 8: Report sizes
let totalSize = 0;
const files = fs.readdirSync(LITE_DIR);
console.log('\n📦 Lite bundle contents:');
for (const file of files) {
    const stat = fs.statSync(path.join(LITE_DIR, file));
    totalSize += stat.size;
    const sizeKB = (stat.size / 1024).toFixed(1);
    console.log(`   ${file.padEnd(40)} ${sizeKB} KB`);
}

const totalMB = (totalSize / 1024 / 1024).toFixed(2);
console.log(`\n✅ Lite build complete! Total size: ${totalMB} MB`);

if (totalSize > 2 * 1024 * 1024) {
    console.warn('⚠️  Bundle exceeds 2MB target. Consider further stripping.');
} else {
    console.log('🎯 Under 2MB target — perfect for serverless!');
}
