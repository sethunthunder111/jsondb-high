import { unlinkSync, readdirSync, lstatSync } from "fs";
import { join } from "path";

const rootFilesToClean = [
  /^test_db\.json.*$/,
  /^test_encrypted\.json.*$/,
  /^bench_db\.json.*$/,
  /\.bak$/,
  /\.db.*$/,
  /\.json\.wal$/,
];

const benchmarkFilesToClean = [
  /\.json$/,
  /\.wal$/,
  /\.tmp$/,
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
