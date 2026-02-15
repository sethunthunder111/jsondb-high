import { unlinkSync, readdirSync, lstatSync, rmSync } from "fs";
import { join } from "path";

const rootFilesToClean = [
  /^test_db\.json.*$/,          // test_db.json, test_db.json.wal, test_db.json.email.idx, etc.
  /^test_encrypted\.json.*$/,   // test_encrypted.json, .wal, etc.
  /^test_lock\.json.*$/,        // test_lock.json, .wal, etc.
  /^test_durability\.json.*$/,  // test_durability.json, .wal, etc.
  /^test_crash_recovery\.json.*$/, // test_crash_recovery.json, .wal, etc.
  /\.bak$/,                     // Backup files
  /\.db.*$/,                    // Database-related files
  /\.wal$/,                     // WAL files
  /\.idx$/,                     // Index files
  /^\.node$/,                  // Compiled native modules
  /^jsondb-high\..*\.node$/,   // Compiled native modules with platform suffix
];

const benchmarkFilesToClean = [
  /bench_db\.json.*$/,          // bench_db.json and all related files (.wal, .idx, etc.)
  /\.json$/,                    // Any json files
  /\.wal$/,                     // WAL files
  /\.tmp$/,                     // Temp files
  /\.idx$/,                     // Index files
  /\.lock$/,                    // Lock files
];

const dirsToClean = [
  "target",                     // Rust build output
  "dist",                       // TypeScript build output
  "build",                      // Build artifacts
  ".next",                      // Next.js build
  "node_modules",               // Node modules
];

function cleanDir(dir: string, patterns: RegExp[]) {
  try {
    const files = readdirSync(dir);
    for (const file of files) {
      const fullPath = join(dir, file);
      if (lstatSync(fullPath).isDirectory()) continue;

      if (patterns.some((p) => p.test(file))) {
        try {
          unlinkSync(fullPath);
          console.log(`  Removed: ${fullPath}`);
        } catch (err) {
          // Ignore errors if file is already gone or locked
        }
      }
    }
  } catch (err) {
    // Directory might not exist or other issues
  }
}

function cleanDirs(dirs: string[]) {
  for (const dir of dirs) {
    try {
      const stats = lstatSync(dir);
      if (stats.isDirectory()) {
        rmSync(dir, { recursive: true, force: true });
        console.log(`  Removed directory: ${dir}`);
      }
    } catch (err) {
      // Directory might not exist or other issues
    }
  }
}

console.log("Cleaning build files and directories...");
cleanDirs(dirsToClean);

console.log("Cleaning generated files...");
cleanDir(".", rootFilesToClean);
cleanDir("benchmarks", benchmarkFilesToClean);

console.log("Cleanup complete!");
