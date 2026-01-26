import { readdirSync, copyFileSync, existsSync, mkdirSync, statSync } from 'fs';
import { join, basename } from 'path';

const artifactsDir = join(process.cwd(), 'artifacts');
const npmDir = join(process.cwd(), 'npm');

if (!existsSync(artifactsDir)) {
  console.error('Artifacts directory not found');
  process.exit(1);
}

// Function to recursively find .node files
function findNodeFiles(dir: string): string[] {
  let results: string[] = [];
  const list = readdirSync(dir);
  for (const file of list) {
    const fullPath = join(dir, file);
    const stat = statSync(fullPath);
    if (stat && stat.isDirectory()) {
      results = results.concat(findNodeFiles(fullPath));
    } else {
      if (file.endsWith('.node')) {
        results.push(fullPath);
      }
    }
  }
  return results;
}

const nodeFiles = findNodeFiles(artifactsDir);
console.log(`Found ${nodeFiles.length} binary files`);

for (const file of nodeFiles) {
  const fileName = basename(file);
  // fileName is like jsondb-high.win32-arm64-msvc.node
  // We need to extract win32-arm64-msvc
  // Assuming format: <pkgName>.<suffix>.node
  
  const parts = fileName.split('.');
  // parts: ['jsondb-high', 'win32-arm64-msvc', 'node']
  // But what if pkgName has dots? jsondb-high doesn't.
  // Safer: remove first part (pkgName) and last part (node).
  
  // Actually, simpler: regex match
  // jsondb-high\.(.+)\.node
  
  const match = fileName.match(/^jsondb-high\.(.+)\.node$/);
  if (!match) {
    console.warn(`Skipping ${fileName}: does not match pattern`);
    continue;
  }
  
  const suffix = match[1]!;
  const targetDir = join(npmDir, suffix);
  
  if (existsSync(targetDir)) {
    const dest = join(targetDir, fileName);
    console.log(`Copying ${fileName} -> ${targetDir}`);
    copyFileSync(file, dest);
  } else {
    console.error(`Target directory not found for ${fileName}: ${targetDir}`);
    // Check if maybe it's a slightly different mapping?
    // e.g. darwin-x64 matches npm/darwin-x64
    process.exit(1);
  }
}

console.log('Artifacts moved successfully');
