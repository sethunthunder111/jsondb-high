import { mkdirSync, writeFileSync, existsSync, readFileSync } from 'fs';
import { join } from 'path';

const rootPackage = JSON.parse(readFileSync(join(process.cwd(), 'package.json'), 'utf-8'));

const VERSION = rootPackage.version;
const PACKAGE_NAME = "jsondb-high";
const AUTHOR = rootPackage.author;
const LICENSE = rootPackage.license;
const DESCRIPTION = rootPackage.description;

const targets = [
  {
    target: "x86_64-apple-darwin",
    name: "jsondb-high-darwin-x64",
    os: ["darwin"],
    cpu: ["x64"],
    binary: "jsondb-high.darwin-x64.node"
  },
  {
    target: "aarch64-apple-darwin",
    name: "jsondb-high-darwin-arm64",
    os: ["darwin"],
    cpu: ["arm64"],
    binary: "jsondb-high.darwin-arm64.node"
  },
  {
    target: "x86_64-pc-windows-msvc",
    name: "jsondb-high-win32-x64-msvc",
    os: ["win32"],
    cpu: ["x64"],
    binary: "jsondb-high.win32-x64-msvc.node"
  },
  {
    target: "aarch64-pc-windows-msvc",
    name: "jsondb-high-win32-arm64-msvc",
    os: ["win32"],
    cpu: ["arm64"],
    binary: "jsondb-high.win32-arm64-msvc.node"
  },
  {
    target: "x86_64-unknown-linux-gnu",
    name: "jsondb-high-linux-x64-gnu",
    os: ["linux"],
    cpu: ["x64"],
    binary: "jsondb-high.linux-x64-gnu.node"
  },
  {
    target: "aarch64-unknown-linux-gnu",
    name: "jsondb-high-linux-arm64-gnu",
    os: ["linux"],
    cpu: ["arm64"],
    binary: "jsondb-high.linux-arm64-gnu.node"
  },
  {
    target: "x86_64-unknown-linux-musl",
    name: "jsondb-high-linux-x64-musl",
    os: ["linux"],
    cpu: ["x64"],
    binary: "jsondb-high.linux-x64-musl.node"
  }
];

// Clean npm directory if exists
const npmDir = join(process.cwd(), 'npm');
if (!existsSync(npmDir)) {
  mkdirSync(npmDir);
}

for (const target of targets) {
  // Extract directory name from package name (remove prefix?)
  // napi-rs convention: npm/<suffix> e.g. npm/win32-x64-msvc
  // The suffix is usually the part after the main package name + hyphen
  // But wait, create-npm-dir generated npm/win32-x64-msvc for jsondb-high-win32-x64-msvc
  
  const suffix = target.name.replace(PACKAGE_NAME + '-', '');
  const pkgDir = join(npmDir, suffix);
  
  if (!existsSync(pkgDir)) {
    mkdirSync(pkgDir, { recursive: true });
  }

  const packageJson = {
    name: target.name,
    version: VERSION,
    os: target.os,
    cpu: target.cpu,
    main: target.binary,
    files: [target.binary],
    license: LICENSE,
    engines: {
      node: ">= 10"
    },
    repository: {
        type: "git",
        url: "git+https://github.com/sethunthunder111/jsondb-high.git"
    },
    description: DESCRIPTION
  };

  if (target.os.includes('linux') && target.name.includes('musl')) {
    // @ts-ignore
    packageJson.libc = ['musl'];
  } else if (target.os.includes('linux') && target.name.includes('gnu')) {
     // @ts-ignore
     packageJson.libc = ['glibc'];
  }

  writeFileSync(join(pkgDir, 'package.json'), JSON.stringify(packageJson, null, 2));
  writeFileSync(join(pkgDir, 'README.md'), `# ${target.name}\n\n${DESCRIPTION}`);
  
  console.log(`Generated ${pkgDir}`);
}
