const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

function getNativeName() {
  const { platform, arch } = process;
  let osName = platform;
  let archName = arch;
  let abi = '';

  if (platform === 'win32') {
    osName = 'win32';
    abi = '-msvc';
  } else if (platform === 'darwin') {
    osName = 'darwin';
  } else if (platform === 'linux') {
    osName = 'linux';
    // Simplified musl check
    try {
      const lddOutput = execSync('ldd --version', { stdio: 'pipe' }).toString();
      if (lddOutput.includes('musl')) {
        abi = '-musl';
      } else {
        abi = '-gnu';
      }
    } catch (e) {
      abi = '-gnu';
    }
  }

  return `jsondb-high.${osName}-${archName}${abi}.node`;
}

function buildOnDemand() {
  const binaryName = getNativeName();
  const binaryPath = path.join(__dirname, '..', binaryName);

  if (fs.existsSync(binaryPath)) {
    return binaryPath;
  }

  console.log('Native binary not found. Attempting to build from source using cargo...');

  try {
    // Check for cargo
    execSync('cargo --version', { stdio: 'ignore' });
  } catch (e) {
    throw new Error('Cargo not found. Please install Rust and Cargo to build the native module: https://rustup.rs/');
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
