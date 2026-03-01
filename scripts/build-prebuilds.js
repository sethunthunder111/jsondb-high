#!/usr/bin/env node

/**
 * Build prebuilt native binaries for all supported platforms.
 * 
 * Uses `cross` (Docker-based) for Linux & Windows targets,
 * and native `cargo` for the current host platform.
 * 
 * macOS builds require actual macOS hardware (GitHub Actions).
 *
 * Usage:
 *   node scripts/build-prebuilds.js           # Build all targets
 *   node scripts/build-prebuilds.js linux      # Build only Linux targets
 *   node scripts/build-prebuilds.js host       # Build only for current platform
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');

// ============================================
// Target definitions
// ============================================

const TARGETS = {
  // Linux glibc targets (via cross/Docker) — RELIABLE
  'linux-x64-gnu': {
    triple: 'x86_64-unknown-linux-gnu',
    tool: 'cross',
    lib: 'libjsondb_high.so',
    nodeFile: 'index.linux-x64-gnu.node',
  },
  'linux-arm64-gnu': {
    triple: 'aarch64-unknown-linux-gnu',
    tool: 'cross',
    lib: 'libjsondb_high.so',
    nodeFile: 'index.linux-arm64-gnu.node',
  },

  // NOTE: The following targets require special toolchains:
  //
  // linux-x64-musl / linux-arm64-musl:
  //   musl targets can't produce cdylib via cross. Use NAPI-RS CLI with zig:
  //   npx @napi-rs/cli build --target x86_64-unknown-linux-musl --zig --release
  //
  // win32-x64-msvc:
  //   Windows needs libnode.dll — build on actual Windows CI, or use:
  //   npx @napi-rs/cli build --target x86_64-pc-windows-msvc --release
  //
  // darwin-arm64 / darwin-x64:
  //   macOS needs actual macOS hardware. Use GitHub Actions.
  //
  // All of these platforms are covered by the WASM fallback until
  // CI prebuilds are set up.
};

// ============================================
// Build logic
// ============================================

function ensureRustTarget(triple) {
  try {
    const installed = execSync('rustup target list --installed', { encoding: 'utf8' });
    if (!installed.includes(triple)) {
      console.log(`  Installing Rust target: ${triple}`);
      execSync(`rustup target add ${triple}`, { stdio: 'inherit' });
    }
  } catch {
    // cross handles its own toolchain, skip
  }
}

function buildTarget(name, config) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Building: ${name} (${config.triple})`);
  console.log(`${'='.repeat(60)}`);

  const { triple, tool, lib, nodeFile } = config;
  const destPath = path.join(ROOT, nodeFile);

  // Skip if already built
  if (fs.existsSync(destPath)) {
    const stats = fs.statSync(destPath);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(1);
    console.log(`  ⏭ Already exists (${sizeMB} MB), skipping. Delete to rebuild.`);
    return true;
  }

  // Ensure target is installed for native builds
  if (tool === 'cargo') {
    ensureRustTarget(triple);
  }

  // Build
  const cmd = `${tool} build --release --target ${triple}`;
  console.log(`  $ ${cmd}`);

  try {
    execSync(cmd, {
      stdio: 'inherit',
      cwd: ROOT,
      env: {
        ...process.env,
        // cross needs Docker
        CROSS_CONTAINER_ENGINE: 'docker',
      },
    });
  } catch (e) {
    console.error(`  ✗ Build failed for ${name}: ${e.message}`);
    return false;
  }

  // Copy built library to root as .node file
  const srcPath = path.join(ROOT, 'target', triple, 'release', lib);
  if (!fs.existsSync(srcPath)) {
    console.error(`  ✗ Built library not found: ${srcPath}`);
    return false;
  }

  fs.copyFileSync(srcPath, destPath);
  const stats = fs.statSync(destPath);
  const sizeMB = (stats.size / (1024 * 1024)).toFixed(1);
  console.log(`  ✓ ${nodeFile} (${sizeMB} MB)`);
  return true;
}

function buildHost() {
  console.log('\n--- Building for current host platform ---');
  const cmd = 'cargo build --release';
  console.log(`  $ ${cmd}`);

  try {
    execSync(cmd, { stdio: 'inherit', cwd: ROOT });
  } catch (e) {
    console.error(`  ✗ Host build failed: ${e.message}`);
    return false;
  }

  // Determine native lib name and node file name
  const { platform, arch } = process;
  let lib, nodeFile;

  if (platform === 'linux') {
    lib = 'libjsondb_high.so';
    const musl = isMusl();
    nodeFile = `index.linux-${arch}-${musl ? 'musl' : 'gnu'}.node`;
  } else if (platform === 'darwin') {
    lib = 'libjsondb_high.dylib';
    nodeFile = `index.darwin-${arch}.node`;
  } else if (platform === 'win32') {
    lib = 'jsondb_high.dll';
    nodeFile = `index.win32-${arch}-msvc.node`;
  } else {
    console.error(`  ✗ Unsupported platform: ${platform}`);
    return false;
  }

  const srcPath = path.join(ROOT, 'target', 'release', lib);
  const destPath = path.join(ROOT, nodeFile);

  if (fs.existsSync(srcPath)) {
    fs.copyFileSync(srcPath, destPath);
    const sizeMB = (fs.statSync(destPath).size / (1024 * 1024)).toFixed(1);
    console.log(`  ✓ ${nodeFile} (${sizeMB} MB)`);
    return true;
  }

  console.error(`  ✗ Built library not found: ${srcPath}`);
  return false;
}

function isMusl() {
  try {
    const lddPath = execSync('which ldd').toString().trim();
    return fs.readFileSync(lddPath, 'utf8').includes('musl');
  } catch {
    return false;
  }
}

// ============================================
// Main
// ============================================

function main() {
  const filter = process.argv[2]; // optional: 'linux', 'windows', 'host', or specific target name

  console.log('╔══════════════════════════════════════════╗');
  console.log('║  jsondb-high Prebuild Builder            ║');
  console.log('╚══════════════════════════════════════════╝');

  if (filter === 'host') {
    buildHost();
    return;
  }

  let results = [];

  // Always build host first
  console.log('\n--- Host build ---');
  const hostOk = buildHost();
  results.push({ name: 'host', ok: hostOk });

  // Build cross-compiled targets
  for (const [name, config] of Object.entries(TARGETS)) {
    if (filter && !name.startsWith(filter) && name !== filter) {
      continue;
    }

    const ok = buildTarget(name, config);
    results.push({ name, ok });
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('Build Summary:');
  console.log('='.repeat(60));
  for (const { name, ok } of results) {
    console.log(`  ${ok ? '✓' : '✗'} ${name}`);
  }

  const failed = results.filter(r => !r.ok);
  if (failed.length > 0) {
    console.log(`\n${failed.length} target(s) failed. WASM fallback covers these platforms.`);
  } else {
    console.log('\nAll targets built successfully!');
  }

  // List all .node files
  console.log('\nPrebuilt files:');
  const nodeFiles = fs.readdirSync(ROOT).filter(f => f.endsWith('.node'));
  for (const f of nodeFiles) {
    const sizeMB = (fs.statSync(path.join(ROOT, f)).size / (1024 * 1024)).toFixed(1);
    console.log(`  ${f} (${sizeMB} MB)`);
  }
}

main();
