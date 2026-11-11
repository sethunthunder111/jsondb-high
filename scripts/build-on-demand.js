const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

function isMusl() {
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      const lddPath = execSync('which ldd').toString().trim();
      return fs.readFileSync(lddPath, 'utf8').includes('musl');
    } catch (e) {
      return true;
    }
  } else {
    const { glibcVersionRuntime } = process.report.getReport().header;
    return !glibcVersionRuntime;
  }
}

function getPlatformTriple() {
  const { platform, arch } = process;

  switch (platform) {
    case 'android':
      // android-arm64, android-arm-eabi
      if (arch === 'arm') return 'android-arm-eabi';
      return `android-${arch}`;

    case 'win32':
      // win32-x64-msvc, win32-ia32-msvc, win32-arm64-msvc
      return `win32-${arch}-msvc`;

    case 'darwin':
      // darwin-x64, darwin-arm64
      return `darwin-${arch}`;

    case 'freebsd':
      // freebsd-x64
      return `freebsd-${arch}`;

    case 'linux': {
      const musl = isMusl();
      if (arch === 'arm') {
        // linux-arm-gnueabihf, linux-arm-musleabihf
        return musl ? 'linux-arm-musleabihf' : 'linux-arm-gnueabihf';
      }
      // linux-x64-gnu, linux-x64-musl, linux-arm64-gnu, linux-arm64-musl,
      // linux-riscv64-gnu, linux-riscv64-musl, linux-s390x-gnu
      return `linux-${arch}-${musl ? 'musl' : 'gnu'}`;
    }

    default:
      // Fallback
      return `${platform}-${arch}`;
  }
}

// Name that the NAPI-RS loader (index.js) expects for local resolution
function getNativeName() {
  return `index.${getPlatformTriple()}.node`;
}

function buildOnDemand() {
  const binaryName = getNativeName();
  const prebuildsDir = path.join(__dirname, '..', 'prebuilds');
  // Ensure prebuilds/ exists
  if (!fs.existsSync(prebuildsDir)) fs.mkdirSync(prebuildsDir, { recursive: true });
  const binaryPath = path.join(prebuildsDir, binaryName);

  if (fs.existsSync(binaryPath)) {
    return binaryPath;
  }

  console.log('Native binary not found. Attempting to build from source using cargo...');

  try {
    // Check for cargo
    execSync('cargo --version', { stdio: 'ignore' });
  } catch (e) {
    const msg = `Cargo not found, so jsondb-high cannot build its native module for ${getPlatformTriple()}. ` +
      'Install Rust (https://rustup.rs/) and re-run `npm rebuild jsondb-high` (or `node scripts/build-on-demand.js`).';
    if (process.env.npm_lifecycle_event === 'postinstall') {
      console.error('[jsondb-high] ' + msg);
      return null;
    }
    throw new Error(msg);
  }

  try {
    execSync('cargo build --release', { stdio: 'inherit', cwd: path.join(__dirname, '..') });
    
    const releaseDir = path.join(__dirname, '..', 'target', 'release');
    let libName;
    if (process.platform === 'win32') {
      libName = 'jsondb_high.dll';
    } else if (process.platform === 'darwin') {
      libName = 'libjsondb_high.dylib';
    } else {
      libName = 'libjsondb_high.so';
    }

    const srcPath = path.join(releaseDir, libName);
    if (!fs.existsSync(srcPath)) {
      throw new Error(`Could not find built library at ${srcPath}`);
    }

    fs.copyFileSync(srcPath, binaryPath);
    console.log(`Successfully built and installed native binary: ${binaryName}`);
    return binaryPath;
  } catch (e) {
    console.error(`Failed to build native module: ${e.message}`);
    // Don't throw if we are in postinstall, just log
    if (process.env.npm_lifecycle_event === 'postinstall') {
      return null;
    }
    throw e;
  }
}

if (require.main === module) {
  buildOnDemand();
}

module.exports = { buildOnDemand, getNativeName };
