#![allow(dead_code)]

//! v6: Point-in-Time Recovery (PITR)
//!
//! When the WAL is compacted into the main data file, instead of deleting
//! the old WAL file, it is rotated into an archive folder. A restore API
//! allows replaying the WAL up to a precise timestamp.
//!
//! Architecture:
//! - WAL files are rotated into `{db_path}.archive/` on compaction
//! - Each archived WAL is named `wal_{unix_ms}.log`
//! - Restore replays entries up to a target timestamp

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

/// A WAL entry stored in the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub timestamp_ms: u64,
    pub op: String,
    pub path: String,
    pub value: Option<Value>,
}

/// PITR Manager — handles WAL archiving and restoration.
pub struct PitrManager {
    /// Path to the database file
    db_path: String,
    /// Path to the archive directory
    archive_dir: PathBuf,
    /// Maximum number of archive files to keep (0 = unlimited)
    max_archives: usize,
}

impl PitrManager {
    pub fn new(db_path: &str, max_archives: usize) -> Self {
        let archive_dir = PathBuf::from(format!("{}.archive", db_path));
        PitrManager {
            db_path: db_path.to_string(),
            archive_dir,
            max_archives,
        }
    }

    /// Ensure the archive directory exists.
    pub fn init(&self) -> Result<(), String> {
        fs::create_dir_all(&self.archive_dir)
            .map_err(|e| format!("Failed to create archive dir: {}", e))
    }

    /// Archive a WAL file (rotate it into the archive directory).
    pub fn archive_wal(&self, wal_path: &str) -> Result<String, String> {
        self.init()?;

        let wal = Path::new(wal_path);
        if !wal.exists() {
            return Err("WAL file does not exist".to_string());
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let archive_name = format!("wal_{}.log", timestamp);
        let archive_path = self.archive_dir.join(&archive_name);

        fs::copy(wal_path, &archive_path)
            .map_err(|e| format!("Failed to archive WAL: {}", e))?;

        // Enforce max archives limit
        if self.max_archives > 0 {
            self.prune_old_archives()?;
        }

        Ok(archive_path.to_string_lossy().to_string())
    }

    /// List all archived WAL files, sorted by timestamp (oldest first).
    pub fn list_archives(&self) -> Result<Vec<ArchiveInfo>, String> {
        if !self.archive_dir.exists() {
            return Ok(vec![]);
        }

        let mut archives = Vec::new();

        let entries = fs::read_dir(&self.archive_dir)
            .map_err(|e| format!("Failed to read archive dir: {}", e))?;

        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();

            if name.starts_with("wal_") && name.ends_with(".log") {
                let timestamp_str = name
                    .trim_start_matches("wal_")
                    .trim_end_matches(".log");

                if let Ok(timestamp) = timestamp_str.parse::<u64>() {
                    let metadata = entry.metadata().ok();
                    let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);

                    archives.push(ArchiveInfo {
                        filename: name,
                        path: path.to_string_lossy().to_string(),
                        timestamp_ms: timestamp,
                        size_bytes: size,
                    });
                }
            }
        }

        archives.sort_by_key(|a| a.timestamp_ms);
        Ok(archives)
    }

    /// Prune old archives beyond the max limit.
    fn prune_old_archives(&self) -> Result<(), String> {
        let archives = self.list_archives()?;
        if archives.len() <= self.max_archives {
            return Ok(());
        }

        let to_remove = archives.len() - self.max_archives;
        for archive in archives.iter().take(to_remove) {
            let _ = fs::remove_file(&archive.path);
        }

        Ok(())
    }

    /// Parse entries from an archived WAL file.
    pub fn parse_archive(path: &str) -> Result<Vec<ArchiveEntry>, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read archive: {}", e))?;

        let mut entries = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Ok(entry) = serde_json::from_str::<ArchiveEntry>(line) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Restore data to a specific point in time.
    ///
    /// Starts from an empty state and replays all archive entries
    /// up to `target_timestamp_ms`. Returns the restored data.
    pub fn restore_to(
        &self,
        target_timestamp_ms: u64,
    ) -> Result<RestoreResult, String> {
        let archives = self.list_archives()?;

        let mut data = serde_json::json!({});
        let mut entries_applied = 0u64;
        let mut last_timestamp = 0u64;

        // Also try to load the base data file (latest snapshot before target)
        let db_path = Path::new(&self.db_path);
        if db_path.exists() {
            let content = fs::read_to_string(db_path)
                .map_err(|e| format!("Failed to read base DB: {}", e))?;
            data = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse base DB: {}", e))?;
        }

        // Replay archived WAL entries up to the target timestamp
        for archive in &archives {
            if archive.timestamp_ms > target_timestamp_ms {
                break; // This archive is after the target
            }

            let entries = Self::parse_archive(&archive.path)?;

            for entry in entries {
                if entry.timestamp_ms > target_timestamp_ms {
                    break;
                }

                // Apply the entry to the data
                match entry.op.as_str() {
                    "set" => {
                        if let Some(value) = &entry.value {
                            set_nested(&mut data, &entry.path, value.clone());
                        }
                    }
                    "delete" => {
                        delete_nested(&mut data, &entry.path);
                    }
                    _ => {} // Unsupported op, skip
                }

                entries_applied += 1;
                last_timestamp = entry.timestamp_ms;
            }
        }

        Ok(RestoreResult {
            data,
            entries_applied,
            target_timestamp_ms,
            actual_timestamp_ms: last_timestamp,
            archives_scanned: archives.len() as u64,
        })
    }

    /// Get archive statistics.
    pub fn stats(&self) -> Result<ArchiveStats, String> {
        let archives = self.list_archives()?;
        let total_size: u64 = archives.iter().map(|a| a.size_bytes).sum();
        let oldest = archives.first().map(|a| a.timestamp_ms).unwrap_or(0);
        let newest = archives.last().map(|a| a.timestamp_ms).unwrap_or(0);

        Ok(ArchiveStats {
            archive_count: archives.len(),
            total_size_bytes: total_size,
            oldest_timestamp_ms: oldest,
            newest_timestamp_ms: newest,
            max_archives: self.max_archives,
        })
    }
}

/// Helper: set a value at a dot-notation path in nested JSON.
fn set_nested(data: &mut Value, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = data;

    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part — set the value
            if let Value::Object(map) = current {
                map.insert(part.to_string(), value);
                return;
            }
        } else {
            // Intermediate — ensure object exists
            if let Value::Object(map) = current {
                if !map.contains_key(*part) {
                    map.insert(part.to_string(), serde_json::json!({}));
                }
                current = map.get_mut(*part).unwrap();
            } else {
                return;
            }
        }
    }
}

/// Helper: delete a value at a dot-notation path.
fn delete_nested(data: &mut Value, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = data;

    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Value::Object(map) = current {
                map.remove(*part);
            }
        } else {
            if let Value::Object(map) = current {
                if let Some(next) = map.get_mut(*part) {
                    current = next;
                } else {
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveInfo {
    pub filename: String,
    pub path: String,
    pub timestamp_ms: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct RestoreResult {
    pub data: Value,
    pub entries_applied: u64,
    pub target_timestamp_ms: u64,
    pub actual_timestamp_ms: u64,
    pub archives_scanned: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveStats {
    pub archive_count: usize,
    pub total_size_bytes: u64,
    pub oldest_timestamp_ms: u64,
    pub newest_timestamp_ms: u64,
    pub max_archives: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    fn test_dir() -> String {
        let dir = "/tmp/pitr_test";
        let _ = fs::remove_dir_all(dir);
        let _ = fs::create_dir_all(dir);
        format!("{}/test.json", dir)
    }

    #[test]
    fn test_set_nested() {
        let mut data = json!({});
        set_nested(&mut data, "users.1.name", json!("Alice"));
        assert_eq!(data["users"]["1"]["name"], "Alice");
    }

    #[test]
    fn test_delete_nested() {
        let mut data = json!({"users": {"1": {"name": "Alice", "age": 30}}});
        delete_nested(&mut data, "users.1.age");
        assert!(data["users"]["1"].get("age").is_none());
    }

    #[test]
    fn test_archive_and_list() {
        let db_path = test_dir();
        let mgr = PitrManager::new(&db_path, 0);

        // Create a fake WAL file
        let wal_path = format!("{}.wal", db_path);
        fs::write(&wal_path, "test wal content\n").unwrap();

        let archived = mgr.archive_wal(&wal_path).unwrap();
        assert!(Path::new(&archived).exists());

        let archives = mgr.list_archives().unwrap();
        assert_eq!(archives.len(), 1);
    }

    #[test]
    fn test_stats() {
        let db_path = test_dir();
        let mgr = PitrManager::new(&db_path, 0);

        let stats = mgr.stats().unwrap();
        assert_eq!(stats.archive_count, 0);
    }
}
