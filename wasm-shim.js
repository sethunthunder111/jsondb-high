/**
 * WASM Shim for jsondb-high
 *
 * This module wraps the WASM-compiled database engine to provide
 * the same interface as the native N-API `NativeDb` class.
 * File I/O is handled here in JS since WASM cannot access the filesystem.
 *
 * This is the universal fallback — slower than native, but works everywhere.
 */

const fs = require('fs');
const path = require('path');

let wasmModule = null;

function loadWasmModule() {
  if (wasmModule) return wasmModule;

  // Try to load the pre-compiled WASM package
  const wasmPkgPath = path.join(__dirname, 'wasm', 'pkg');
  const wasmJsPath = path.join(wasmPkgPath, 'jsondb_high_wasm.js');

  if (!fs.existsSync(wasmJsPath)) {
    throw new Error(
      'WASM fallback not available. Neither native binary nor WASM package found. ' +
      'Please install Rust (https://rustup.rs/) and run: bun run build'
    );
  }

  wasmModule = require(wasmJsPath);
  return wasmModule;
}

/**
 * WasmNativeDb — drop-in replacement for the native NativeDb class.
 * Handles file I/O in JS, delegates all data operations to WASM.
 */
class WasmNativeDb {
  constructor(filePath, wal) {
    const wasm = loadWasmModule();
    this._wasm = new wasm.WasmDB();
    this._filePath = filePath;
    this._wal = wal || false;

    // Load existing data if file exists
    if (fs.existsSync(filePath)) {
      try {
        const contents = fs.readFileSync(filePath, 'utf8');
        this._wasm.loadFromString(contents);
      } catch (err) {
        // File might be empty or corrupted — start fresh
        console.warn(`[jsondb-high/wasm] Could not load ${filePath}: ${err.message}`);
      }
    }
  }

  // --- Core CRUD ---

  get(pathStr) {
    return this._wasm.get(pathStr);
  }

  set(pathStr, value) {
    this._wasm.set(pathStr, value);
  }

  has(pathStr) {
    return this._wasm.has(pathStr);
  }

  delete(pathStr) {
    this._wasm.delete(pathStr);
  }

  push(pathStr, value) {
    this._wasm.push(pathStr, value);
  }

  pushBatch(pathStr, items) {
    for (const item of items) {
      this._wasm.push(pathStr, item);
    }
  }

  getMany(paths) {
    return paths.map(p => this.get(p));
  }

  // --- Persistence (handled in JS) ---

  load() {
    if (fs.existsSync(this._filePath)) {
      const contents = fs.readFileSync(this._filePath, 'utf8');
      this._wasm.loadFromString(contents);
    }
  }

  save() {
    const json = this._wasm.saveToString();
    const tmpPath = `${this._filePath}.tmp`;
    fs.writeFileSync(tmpPath, json);
    fs.renameSync(tmpPath, this._filePath);
  }

  sync() {
    // No WAL in WASM mode — save directly
    this.save();
  }

  close() {
    // No resources to release in WASM mode
  }

  // --- Query Engine ---

  executeQueryFast(path, filtersJson, sortJson, limit, skip, selectFields) {
    return this._wasm.executeQueryFast(
      path, filtersJson, sortJson || null,
      limit ?? null, skip ?? null, selectFields || null
    );
  }

  explainQueryFast(path, filtersJson, sortJson, limit, skip, selectFields) {
    return this._wasm.explainQueryFast(
      path, filtersJson, sortJson || null,
      limit ?? null, skip ?? null, selectFields || null
    );
  }

  executeAggregateFast(path, filtersJson, operation, field) {
    return this._wasm.executeAggregateFast(
      path, filtersJson, operation, field || null
    );
  }

  // --- System Info ---

  getSystemInfo() {
    return this._wasm.getSystemInfo();
  }

  walStatus() {
    return this._wasm.walStatus();
  }

  // --- Schema ---

  registerSchema(path, schemaJson) {
    this._wasm.registerSchema(path, schemaJson);
  }

  // --- Stubs for features not available in WASM mode ---

  validatePath(_path, _value) {
    // Schema validation simplified in WASM mode
  }

  registerIndex(_name, _field) {
    // Indexes not supported in WASM fallback
  }

  clearIndex(_name) {}
  updateIndex(_name, _value, _path, _isDelete) {}
  findIndexPaths(_name, _value) { return null; }
  findIndexRange(_name, _start, _end) { return null; }

  configureMemory(_limit, _dir, _threshold, _target) {}
  offload(_path) { return ''; }
  restore(_path) { return false; }
  memoryStats() {
    return {
      totalEstimatedBytes: 0,
      maxMemoryBytes: 0,
      coldKeysCount: 0,
      hotKeysCount: 0,
      utilizationPct: 0,
    };
  }
  checkMemoryPressure() { return []; }
}

/**
 * Static factory method matching NativeDb.newWithOptions()
 */
WasmNativeDb.newWithOptions = function(
  filePath, _lockMode, _durability,
  _walBatchSize, _walFlushMs, _lockTimeoutMs, _stripeCount
) {
  // WASM mode ignores lock/durability settings — single-threaded, no file locks
  return new WasmNativeDb(filePath, false);
};

module.exports = { NativeDb: WasmNativeDb };
