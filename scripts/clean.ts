import { unlinkSync, readdirSync, lstatSync } from "fs";
import { join } from "path";

const rootFilesToClean = [
  /^test_db\.json.*$/,          // test_db.json, test_db.json.wal, test_db.json.email.idx, etc.
  /^test_encrypted\.json.*$/,   // test_encrypted.json, .wal, etc.
  /^test_lock\.json.*$/,        // test_lock.json, .wal, etc.
  /^test_durability\.json.*$/,  // test_durability.json, .wal, etc.
  /^test_crash_recovery\.json.*$/, // test_crash_recovery.json, .wal, etc.
  /\.bak$/,                     // Backup files
  /\.db.*$/,                    // Database-related files
  /\.json\.wal$/,              // WAL files
  /\.json\.idx$/,              // Index files
  /\.json\.lock$/,             // Lock files (only for json databases)
];

const benchmarkFilesToClean = [
  /bench_db\.json.*$/,          // bench_db.json and all related files (.wal, .idx, etc.)
  /\.json$/,                    // Any json files
  /\.wal$/,                     // WAL files
  /\.tmp$/,                     // Temp files
  /\.idx$/,                     // Index files
  /\.lock$/,                    // Lock files
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

console.log("Cleaning generated files...");
cleanDir(".", rootFilesToClean);
cleanDir("benchmarks", benchmarkFilesToClean);
console.log("Cleanup complete!");
