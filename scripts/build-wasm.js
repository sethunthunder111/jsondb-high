#!/usr/bin/env node

/**
 * Build script for the WASM fallback module.
 * Compiles the Rust wasm crate using wasm-pack, outputs to wasm/pkg/.
 *
 * Usage: node scripts/build-wasm.js
 */

const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs');

const WASM_DIR = path.join(__dirname, '..', 'wasm');
const PKG_DIR = path.join(WASM_DIR, 'pkg');

function buildWasm() {
  console.log('[wasm] Building WASM fallback module...');

  // Check for wasm-pack
  try {
    execSync('wasm-pack --version', { stdio: 'ignore' });
  } catch {
    console.log('[wasm] wasm-pack not found, installing...');
    try {
      execSync('cargo install wasm-pack', { stdio: 'inherit' });
    } catch (e) {
      console.error('[wasm] Failed to install wasm-pack:', e.message);
      process.exit(1);
    }
  }

  // Build with wasm-pack targeting Node.js
  try {
    execSync('wasm-pack build --target nodejs --release --out-dir pkg', {
      stdio: 'inherit',
      cwd: WASM_DIR,
    });
  } catch (e) {
    console.error('[wasm] wasm-pack build failed:', e.message);
    process.exit(1);
  }

  // Verify output
  const wasmFile = path.join(PKG_DIR, 'jsondb_high_wasm_bg.wasm');
  if (fs.existsSync(wasmFile)) {
    const stats = fs.statSync(wasmFile);
    const sizeKB = (stats.size / 1024).toFixed(1);
    console.log(`[wasm] ✓ Built successfully: ${sizeKB} KB`);
  } else {
    console.error('[wasm] Build produced no output!');
    process.exit(1);
  }
}

buildWasm();
