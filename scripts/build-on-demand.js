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
    // If cargo is missing, try to automatically install rustup (non-Windows).
    if (process.platform === 'win32') {
      throw new Error('Cargo not found. Please install Rust and Cargo to build the native module on Windows: https://rustup.rs/');
    }
    console.log('Cargo not found. Attempting to install Rust toolchain via rustup (non-interactive)...');
    try {
      // Install rustup non-interactively
      execSync('curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y', { stdio: 'inherit' });
      // Source cargo env if present so the current process can find cargo
      const cargoEnv = path.join(process.env.HOME || process.env.USERPROFILE || '', '.cargo', 'env');
      if (fs.existsSync(cargoEnv)) {
        try {
          execSync(`. ${cargoEnv}`, { stdio: 'ignore', shell: '/bin/bash' });
        } catch (e2) {
          // ignore
        }
      }
      // Verify cargo is now available
      execSync('cargo --version', { stdio: 'ignore' });
    } catch (installErr) {
      console.error('Automatic rustup install failed:', installErr && installErr.message ? installErr.message : installErr);
      if (process.env.npm_lifecycle_event === 'postinstall') {
        return null;
      }
      throw new Error('Cargo not found after attempted install. Please install Rust and Cargo manually: https://rustup.rs/');
    }
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
