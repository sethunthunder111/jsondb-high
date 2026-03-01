#![allow(dead_code)]

//! v6: WAL Streaming & Node Replication Protocol
//!
//! Enables read-scaling and high availability by streaming WAL entries
//! from a primary node to replica nodes over TCP.
//!
//! Architecture:
//! - Primary: accepts writes, streams WAL entries to connected replicas
//! - Replica: read-only, receives WAL entries and applies to local state
//! - Modes: async (zero write latency) or sync (wait for 1 replica ACK)

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use parking_lot::RwLock;

/// Replication mode for the primary node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Fire-and-forget — zero write latency, eventual consistency
    Async,
    /// Wait for at least 1 replica to ACK before commit returns
    Sync,
}

/// A WAL entry that can be serialized and streamed over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    /// Monotonically increasing sequence number
    pub lsn: u64,
    /// Timestamp in milliseconds
    pub timestamp_ms: u64,
    /// Operation type
    pub op: ReplicationOp,
    /// Dot-notation path of the affected key
    pub path: String,
    /// The value (None for deletes)
    pub value: Option<Value>,
    /// CRC32 checksum for integrity
    pub checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOp {
    Set,
    Delete,
    Push,
    Merge,
}

impl ReplicationEntry {
    /// Create a new replication entry.
    pub fn new(op: ReplicationOp, path: String, value: Option<Value>) -> Self {
        let lsn = REPL_LSN.fetch_add(1, Ordering::SeqCst);
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut entry = ReplicationEntry {
            lsn,
            timestamp_ms,
            op,
            path,
            value,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Compute CRC32 checksum of the entry (excluding the checksum field).
    fn compute_checksum(&self) -> u32 {
        let data = format!(
            "{}:{}:{:?}:{}:{}",
            self.lsn,
            self.timestamp_ms,
            self.op,
            self.path,
            self.value.as_ref().map(|v| v.to_string()).unwrap_or_default()
        );
        crc32fast::hash(data.as_bytes())
    }

    /// Verify the checksum is valid.
    pub fn verify(&self) -> bool {
        let expected = self.compute_checksum();
        self.checksum == expected
    }

    /// Serialize to bytes for network transmission.
    pub fn to_bytes(&self) -> Vec<u8> {
        let json = serde_json::to_string(self).unwrap_or_default();
        let len = json.len() as u32;
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(json.as_bytes());
        buf
    }

    /// Deserialize from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("Parse error: {}", e))
    }
}

/// Global replication LSN counter
static REPL_LSN: AtomicU64 = AtomicU64::new(1);

/// Replication state for the primary node.
pub struct ReplicationPrimary {
    /// Replication mode
    mode: ReplicationMode,
    /// Whether replication is active
    active: AtomicBool,
    /// Buffer of entries waiting to be sent to replicas
    outbound_buffer: RwLock<Vec<ReplicationEntry>>,
    /// Maximum buffer size before flushing
    max_buffer_size: usize,
    /// Number of connected replicas
    replica_count: AtomicU64,
    /// Total entries sent
    entries_sent: AtomicU64,
    /// Last acknowledged LSN from replicas
    last_acked_lsn: AtomicU64,
}

impl ReplicationPrimary {
    pub fn new(mode: ReplicationMode) -> Self {
        ReplicationPrimary {
            mode,
            active: AtomicBool::new(false),
            outbound_buffer: RwLock::new(Vec::with_capacity(1024)),
            max_buffer_size: 1000,
            replica_count: AtomicU64::new(0),
            entries_sent: AtomicU64::new(0),
            last_acked_lsn: AtomicU64::new(0),
        }
    }

    /// Start accepting replica connections.
    pub fn start(&self) {
        self.active.store(true, Ordering::SeqCst);
    }

    /// Stop replication.
    pub fn stop(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    /// Whether replication is currently active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Enqueue a WAL entry for replication.
    pub fn enqueue(&self, entry: ReplicationEntry) {
        if !self.is_active() {
            return;
        }

        let mut buffer = self.outbound_buffer.write();
        buffer.push(entry);

        // Auto-flush if buffer is full
        if buffer.len() >= self.max_buffer_size {
            // In a real implementation, this would trigger TCP send
            self.entries_sent
                .fetch_add(buffer.len() as u64, Ordering::Relaxed);
            buffer.clear();
        }
    }

    /// Flush all buffered entries (would send over TCP in real impl).
    /// Returns the entries that were flushed.
    pub fn flush(&self) -> Vec<ReplicationEntry> {
        let mut buffer = self.outbound_buffer.write();
        let entries: Vec<ReplicationEntry> = buffer.drain(..).collect();
        self.entries_sent
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        entries
    }

    /// Record a replica acknowledgment.
    pub fn ack(&self, lsn: u64) {
        let _ = self.last_acked_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    /// Get replication status.
    pub fn status(&self) -> ReplicationStatus {
        let buffer = self.outbound_buffer.read();
        ReplicationStatus {
            active: self.is_active(),
            mode: self.mode.clone(),
            replica_count: self.replica_count.load(Ordering::Relaxed),
            entries_sent: self.entries_sent.load(Ordering::Relaxed),
            buffer_size: buffer.len() as u64,
            last_acked_lsn: self.last_acked_lsn.load(Ordering::SeqCst),
            current_lsn: REPL_LSN.load(Ordering::SeqCst),
        }
    }
}

/// Replication state for a replica node.
pub struct ReplicationReplica {
    /// Whether this replica is connected to the primary
    connected: AtomicBool,
    /// Inbound buffer of entries received from primary
    inbound_buffer: RwLock<Vec<ReplicationEntry>>,
    /// Last applied LSN
    last_applied_lsn: AtomicU64,
    /// Total entries received
    entries_received: AtomicU64,
    /// Entries that failed checksum verification
    checksum_failures: AtomicU64,
}

impl ReplicationReplica {
    pub fn new() -> Self {
        ReplicationReplica {
            connected: AtomicBool::new(false),
            inbound_buffer: RwLock::new(Vec::with_capacity(1024)),
            last_applied_lsn: AtomicU64::new(0),
            entries_received: AtomicU64::new(0),
            checksum_failures: AtomicU64::new(0),
        }
    }

    /// Receive entries from the primary (would come over TCP in real impl).
    pub fn receive(&self, entries: Vec<ReplicationEntry>) -> Vec<ReplicationEntry> {
        let mut valid = Vec::with_capacity(entries.len());

        for entry in entries {
            self.entries_received.fetch_add(1, Ordering::Relaxed);

            if !entry.verify() {
                self.checksum_failures.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            valid.push(entry);
        }

        valid
    }

    /// Apply a batch of entries to the local database.
    /// Returns the (path, op, value) tuples to apply.
    pub fn apply(&self, entries: &[ReplicationEntry]) -> Vec<(String, ReplicationOp, Option<Value>)> {
        let mut ops = Vec::with_capacity(entries.len());
        let last_applied = self.last_applied_lsn.load(Ordering::SeqCst);

        for entry in entries {
            // Skip already-applied entries
            if entry.lsn <= last_applied {
                continue;
            }

            ops.push((entry.path.clone(), entry.op.clone(), entry.value.clone()));
            self.last_applied_lsn.store(entry.lsn, Ordering::SeqCst);
        }

        ops
    }

    /// Get last applied LSN (for sending ACK back to primary).
    pub fn last_applied_lsn(&self) -> u64 {
        self.last_applied_lsn.load(Ordering::SeqCst)
    }

    /// Get replica status.
    pub fn status(&self) -> ReplicaStatus {
        ReplicaStatus {
            connected: self.connected.load(Ordering::Relaxed),
            last_applied_lsn: self.last_applied_lsn.load(Ordering::SeqCst),
            entries_received: self.entries_received.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicationStatus {
    pub active: bool,
    pub mode: ReplicationMode,
    pub replica_count: u64,
    pub entries_sent: u64,
    pub buffer_size: u64,
    pub last_acked_lsn: u64,
    pub current_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaStatus {
    pub connected: bool,
    pub last_applied_lsn: u64,
    pub entries_received: u64,
    pub checksum_failures: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_replication_entry_round_trip() {
        let entry = ReplicationEntry::new(
            ReplicationOp::Set,
            "users.1.name".to_string(),
            Some(json!("Alice")),
        );

        assert!(entry.verify());

        let bytes = entry.to_bytes();
        assert!(!bytes.is_empty());

        // Extract JSON from bytes (skip 4-byte length prefix)
        let json_str = std::str::from_utf8(&bytes[4..]).unwrap();
        let parsed = ReplicationEntry::from_json(json_str).unwrap();
        assert_eq!(parsed.path, "users.1.name");
        assert!(parsed.verify());
    }

    #[test]
    fn test_primary_enqueue_and_flush() {
        let primary = ReplicationPrimary::new(ReplicationMode::Async);
        primary.start();

        primary.enqueue(ReplicationEntry::new(
            ReplicationOp::Set,
            "users.1".to_string(),
            Some(json!({"name": "Alice"})),
        ));
        primary.enqueue(ReplicationEntry::new(
            ReplicationOp::Delete,
            "users.2".to_string(),
            None,
        ));

        let flushed = primary.flush();
        assert_eq!(flushed.len(), 2);

        let status = primary.status();
        assert!(status.active);
        assert_eq!(status.entries_sent, 2);
    }

    #[test]
    fn test_replica_receive_and_apply() {
        let primary = ReplicationPrimary::new(ReplicationMode::Async);
        primary.start();

        // Primary creates entries
        let entry1 = ReplicationEntry::new(
            ReplicationOp::Set,
            "users.1".to_string(),
            Some(json!({"name": "Alice"})),
        );
        let entry2 = ReplicationEntry::new(
            ReplicationOp::Set,
            "users.2".to_string(),
            Some(json!({"name": "Bob"})),
        );

        // Replica receives and applies
        let replica = ReplicationReplica::new();
        let valid = replica.receive(vec![entry1, entry2]);
        assert_eq!(valid.len(), 2);

        let ops = replica.apply(&valid);
        assert_eq!(ops.len(), 2);

        let status = replica.status();
        assert_eq!(status.entries_received, 2);
        assert_eq!(status.checksum_failures, 0);
    }

    #[test]
    fn test_replica_dedup_applied_entries() {
        let replica = ReplicationReplica::new();

        let entry = ReplicationEntry::new(
            ReplicationOp::Set,
            "users.1".to_string(),
            Some(json!("test")),
        );
        let _entry_lsn = entry.lsn;

        // Apply once
        let valid = replica.receive(vec![entry.clone()]);
        let ops = replica.apply(&valid);
        assert_eq!(ops.len(), 1);

        // Apply same entry again — should be skipped
        let ops2 = replica.apply(&valid);
        assert_eq!(ops2.len(), 0);
    }

    #[test]
    fn test_checksum_failure_detection() {
        let mut entry = ReplicationEntry::new(
            ReplicationOp::Set,
            "users.1".to_string(),
            Some(json!("data")),
        );

        // Corrupt the entry
        entry.path = "tampered.path".to_string();
        assert!(!entry.verify());

        let replica = ReplicationReplica::new();
        let valid = replica.receive(vec![entry]);
        assert_eq!(valid.len(), 0); // Corrupted entry rejected

        let status = replica.status();
        assert_eq!(status.checksum_failures, 1);
    }

    #[test]
    fn test_sync_mode_ack() {
        let primary = ReplicationPrimary::new(ReplicationMode::Sync);
        primary.start();

        assert_eq!(primary.status().last_acked_lsn, 0);

        primary.ack(42);
        assert_eq!(primary.status().last_acked_lsn, 42);

        // ACK should only go forward
        primary.ack(10);
        assert_eq!(primary.status().last_acked_lsn, 42);
    }
}
