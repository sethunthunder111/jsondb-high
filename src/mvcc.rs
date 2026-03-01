#![allow(dead_code)]

//! v6: Optimistic Multi-Version Concurrency Control (MVCC)
//!
//! Provides snapshot isolation for reads and optimistic conflict detection
//! for writes. Key principles:
//!
//! 1. **Readers never block.** They read from a consistent snapshot version.
//! 2. **Writers only lock at commit.** They build changes optimistically,
//!    then attempt an atomic commit.
//! 3. **Conflict detection.** If another writer modified the same keys during
//!    a transaction, a `ConflictError` is returned — no deadlocks.
//!
//! This is similar to PostgreSQL's SERIALIZABLE isolation level.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

/// Global monotonically increasing version counter.
static GLOBAL_VERSION: AtomicU64 = AtomicU64::new(1);

/// Generate the next version number.
fn next_version() -> u64 {
    GLOBAL_VERSION.fetch_add(1, Ordering::SeqCst)
}

/// Get the current version without incrementing.
fn current_version() -> u64 {
    GLOBAL_VERSION.load(Ordering::SeqCst)
}

/// Tracks per-key version numbers for conflict detection.
pub struct VersionMap {
    /// key_path -> last_modified_version
    versions: RwLock<HashMap<String, u64>>,
}

impl VersionMap {
    pub fn new() -> Self {
        VersionMap {
            versions: RwLock::new(HashMap::new()),
        }
    }

    /// Get the version of a key.
    pub fn get_version(&self, key: &str) -> u64 {
        let versions = self.versions.read();
        versions.get(key).copied().unwrap_or(0)
    }

    /// Update the version of a key to the given version.
    pub fn set_version(&self, key: &str, version: u64) {
        let mut versions = self.versions.write();
        versions.insert(key.to_string(), version);
    }

    /// Batch-update versions for multiple keys.
    pub fn set_versions(&self, keys: &[String], version: u64) {
        let mut versions = self.versions.write();
        for key in keys {
            versions.insert(key.clone(), version);
        }
    }

    /// Check if any of the given keys have been modified since `since_version`.
    /// Returns the conflicting keys if any.
    pub fn check_conflicts(&self, read_set: &HashMap<String, u64>) -> Vec<String> {
        let versions = self.versions.read();
        let mut conflicts = Vec::new();

        for (key, read_version) in read_set {
            if let Some(&current_version) = versions.get(key) {
                if current_version > *read_version {
                    conflicts.push(key.clone());
                }
            }
        }

        conflicts
    }
}

/// A read snapshot — captures the version at the time of creation.
/// All reads within this snapshot are consistent.
#[derive(Debug)]
pub struct Snapshot {
    /// The version at which this snapshot was taken.
    pub version: u64,
    /// Timestamp when the snapshot was created.
    pub created_at: u64,
}

impl Snapshot {
    pub fn new() -> Self {
        Snapshot {
            version: current_version(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

/// An MVCC transaction that tracks reads and writes for conflict detection.
pub struct MvccTransaction {
    /// Snapshot version when this transaction started.
    pub snapshot_version: u64,
    /// Keys read during this transaction (key -> version_at_read_time).
    read_set: HashMap<String, u64>,
    /// Keys written during this transaction (key -> new_value).
    write_set: HashMap<String, Option<Value>>,
    /// Whether this transaction has been committed or rolled back.
    status: TransactionStatus,
    /// Transaction ID for logging/debugging.
    pub txn_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    Active,
    Committed,
    RolledBack,
    Conflicted,
}

/// Error type for MVCC conflicts.
#[derive(Debug)]
pub struct ConflictError {
    pub txn_id: u64,
    pub conflicting_keys: Vec<String>,
    pub message: String,
}

impl std::fmt::Display for ConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConflictError(txn={}): {} conflicting key(s): [{}]. {}",
            self.txn_id,
            self.conflicting_keys.len(),
            self.conflicting_keys.join(", "),
            self.message
        )
    }
}

impl MvccTransaction {
    /// Create a new MVCC transaction at the current version.
    pub fn new(version_map: &VersionMap) -> Self {
        let _ = version_map; // Used for consistency, version is global
        MvccTransaction {
            snapshot_version: current_version(),
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            status: TransactionStatus::Active,
            txn_id: next_version(),
        }
    }

    /// Record a read of a key (tracks version for conflict detection).
    pub fn track_read(&mut self, key: &str, version_map: &VersionMap) {
        if self.status != TransactionStatus::Active {
            return;
        }
        let key_version = version_map.get_version(key);
        self.read_set.entry(key.to_string()).or_insert(key_version);
    }

    /// Buffer a write operation (applied at commit time).
    pub fn buffer_write(&mut self, key: String, value: Option<Value>) {
        if self.status != TransactionStatus::Active {
            return;
        }
        self.write_set.insert(key, value);
    }

    /// Get the number of buffered writes.
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    /// Get the number of tracked reads.
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }

    /// Get all keys that were written in this transaction.
    pub fn written_keys(&self) -> Vec<String> {
        self.write_set.keys().cloned().collect()
    }

    /// Check if this transaction is still active.
    pub fn is_active(&self) -> bool {
        self.status == TransactionStatus::Active
    }

    /// Attempt to commit this transaction.
    ///
    /// 1. Check for conflicts (any key in read_set modified since snapshot)
    /// 2. If no conflicts, apply all writes atomically
    /// 3. Update version map for all written keys
    ///
    /// Returns the writes to apply, or a ConflictError.
    pub fn try_commit(
        &mut self,
        version_map: &VersionMap,
    ) -> Result<Vec<(String, Option<Value>)>, ConflictError> {
        if self.status != TransactionStatus::Active {
            return Err(ConflictError {
                txn_id: self.txn_id,
                conflicting_keys: vec![],
                message: format!("Transaction is {:?}, cannot commit", self.status),
            });
        }

        // Step 1: Check for conflicts
        let conflicts = version_map.check_conflicts(&self.read_set);

        if !conflicts.is_empty() {
            self.status = TransactionStatus::Conflicted;
            return Err(ConflictError {
                txn_id: self.txn_id,
                conflicting_keys: conflicts,
                message: "Write-write conflict detected. Retry the transaction.".to_string(),
            });
        }

        // Step 2: Bump version for all written keys
        let commit_version = next_version();
        let keys: Vec<String> = self.write_set.keys().cloned().collect();
        version_map.set_versions(&keys, commit_version);

        // Step 3: Extract writes for the caller to apply
        let writes: Vec<(String, Option<Value>)> = self
            .write_set
            .drain()
            .collect();

        self.status = TransactionStatus::Committed;

        Ok(writes)
    }

    /// Roll back this transaction (discard all buffered writes).
    pub fn rollback(&mut self) {
        self.write_set.clear();
        self.read_set.clear();
        self.status = TransactionStatus::RolledBack;
    }

    /// Get transaction status.
    pub fn status(&self) -> &TransactionStatus {
        &self.status
    }
}

/// MVCC Manager — sits between the database and callers.
/// Manages the version map and transaction lifecycle.
pub struct MvccManager {
    pub version_map: Arc<VersionMap>,
    /// Active transaction count for monitoring
    active_txn_count: AtomicU64,
}

impl MvccManager {
    pub fn new() -> Self {
        MvccManager {
            version_map: Arc::new(VersionMap::new()),
            active_txn_count: AtomicU64::new(0),
        }
    }

    /// Begin a new MVCC transaction.
    pub fn begin(&self) -> MvccTransaction {
        self.active_txn_count.fetch_add(1, Ordering::Relaxed);
        MvccTransaction::new(&self.version_map)
    }

    /// Commit a transaction, returning the writes to apply.
    pub fn commit(
        &self,
        txn: &mut MvccTransaction,
    ) -> Result<Vec<(String, Option<Value>)>, ConflictError> {
        let result = txn.try_commit(&self.version_map);
        self.active_txn_count.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Rollback a transaction.
    pub fn rollback(&self, txn: &mut MvccTransaction) {
        txn.rollback();
        self.active_txn_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Create a read-only snapshot.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new()
    }

    /// Track a key access (for non-transactional writes).
    pub fn touch_key(&self, key: &str) {
        let version = next_version();
        self.version_map.set_version(key, version);
    }

    /// Get the number of active transactions.
    pub fn active_transactions(&self) -> u64 {
        self.active_txn_count.load(Ordering::Relaxed)
    }

    /// Get the current global version.
    pub fn global_version(&self) -> u64 {
        current_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_transaction_commit() {
        let mgr = MvccManager::new();
        let mut txn = mgr.begin();

        txn.track_read("users.1", &mgr.version_map);
        txn.buffer_write("users.1.name".to_string(), Some(json!("Alice")));
        txn.buffer_write("users.1.age".to_string(), Some(json!(30)));

        let writes = mgr.commit(&mut txn).unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(txn.status(), &TransactionStatus::Committed);
    }

    #[test]
    fn test_conflict_detection() {
        let mgr = MvccManager::new();

        // Transaction A reads key
        let mut txn_a = mgr.begin();
        txn_a.track_read("users.1", &mgr.version_map);

        // Another writer modifies the same key
        mgr.touch_key("users.1");

        // Transaction A tries to commit — CONFLICT!
        txn_a.buffer_write("users.1.name".to_string(), Some(json!("Bob")));
        let result = mgr.commit(&mut txn_a);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.conflicting_keys.contains(&"users.1".to_string()));
        assert_eq!(txn_a.status(), &TransactionStatus::Conflicted);
    }

    #[test]
    fn test_non_conflicting_transactions() {
        let mgr = MvccManager::new();

        // Transaction A writes to users
        let mut txn_a = mgr.begin();
        txn_a.track_read("users.1", &mgr.version_map);
        txn_a.buffer_write("users.1.name".to_string(), Some(json!("Alice")));

        // Transaction B writes to orders (different key)
        let mut txn_b = mgr.begin();
        txn_b.track_read("orders.1", &mgr.version_map);
        txn_b.buffer_write("orders.1.total".to_string(), Some(json!(100)));

        // Both should commit successfully
        assert!(mgr.commit(&mut txn_a).is_ok());
        assert!(mgr.commit(&mut txn_b).is_ok());
    }

    #[test]
    fn test_rollback() {
        let mgr = MvccManager::new();
        let mut txn = mgr.begin();

        txn.buffer_write("users.1".to_string(), Some(json!("test")));
        assert_eq!(txn.write_count(), 1);

        mgr.rollback(&mut txn);
        assert_eq!(txn.status(), &TransactionStatus::RolledBack);
        assert_eq!(txn.write_count(), 0);
    }

    #[test]
    fn test_snapshot_versioning() {
        let mgr = MvccManager::new();
        let snap1 = mgr.snapshot();
        mgr.touch_key("foo");
        let snap2 = mgr.snapshot();
        assert!(snap2.version > snap1.version);
    }

    #[test]
    fn test_concurrent_conflict_detection() {
        use std::sync::Arc;
        use std::thread;

        let mgr = Arc::new(MvccManager::new());
        let mgr2 = mgr.clone();

        // Thread 1: begin txn, read key, sleep, then try commit
        let handle = thread::spawn(move || {
            let mut txn = mgr2.begin();
            txn.track_read("shared.key", &mgr2.version_map);
            thread::sleep(std::time::Duration::from_millis(50));
            txn.buffer_write("shared.key".to_string(), Some(json!("from_thread_1")));
            mgr2.commit(&mut txn)
        });

        // Main thread: modify the key while thread 1 is sleeping
        thread::sleep(std::time::Duration::from_millis(10));
        mgr.touch_key("shared.key");

        // Thread 1 should get a conflict
        let result = handle.join().unwrap();
        assert!(result.is_err());
    }
}
