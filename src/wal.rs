//! Group Commit WAL (Write-Ahead Logging)
//! 
//! Batches multiple writes into single fsync for durability without blocking.
//! 
//! Format: [LSN:8][CRC32:4][LENGTH:4][DATA:N]
//! - LSN: Log Sequence Number (monotonically increasing)
//! - CRC32: Checksum of DATA
//! - LENGTH: Length of DATA
//! - DATA: JSON-encoded operation

use crossbeam::channel::{bounded, Sender, Receiver, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Read};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::io;

/// WAL operation types
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalOpType {
    Set,
    Delete,
}

/// Single WAL operation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalOp {
    pub timestamp: u64,
    pub op_type: WalOpType,
    pub path: String,
    pub value: Option<Value>,
}

/// WAL command types for channel
pub enum WalCmd {
    Write { lsn: u64, op: WalOp },
    Sync { tx: std::sync::mpsc::Sender<()> },
    #[allow(dead_code)]
    Flush,
    #[allow(dead_code)]
    Shutdown,
}

/// WAL configuration
#[derive(Clone, Copy)]
pub struct WalConfig {
    /// Maximum operations to batch
    pub batch_size: usize,
    /// Maximum time to wait before flush
    pub flush_interval_ms: u64,
    /// Whether to fsync (false = group write, true = group commit)
    pub fsync: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            batch_size: 1000,
            flush_interval_ms: 10,
            fsync: true,
        }
    }
}

/// Group Commit WAL implementation
pub struct GroupCommitWAL {
    cmd_tx: Sender<WalCmd>,
    committed_lsn: Arc<AtomicU64>,
    _next_lsn: Arc<AtomicU64>,
}

impl GroupCommitWAL {
    /// Create new WAL with background commit thread
    pub fn new(wal_path: &str, config: WalConfig) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = bounded(100000);
        let committed_lsn = Arc::new(AtomicU64::new(0));
        let next_lsn = Arc::new(AtomicU64::new(1));
        
        let committed_lsn_clone = committed_lsn.clone();
        let _next_lsn_clone = next_lsn.clone();
        let path = wal_path.to_string();
        
        std::thread::spawn(move || {
            Self::commit_thread(path, cmd_rx, committed_lsn_clone, _next_lsn_clone, config);
        });
        
        Ok(GroupCommitWAL {
            cmd_tx,
            committed_lsn,
            _next_lsn: next_lsn,
        })
    }
    
    /// Append operation to WAL (non-blocking)
    pub fn append(&self, op: WalOp) -> io::Result<u64> {
        let lsn = self._next_lsn.fetch_add(1, Ordering::SeqCst);
        
        self.cmd_tx.send(WalCmd::Write { lsn, op })
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL thread stopped"))?;
            
        Ok(lsn)
    }
    
    /// Wait for all operations up to current point to be committed
    pub fn sync(&self) -> io::Result<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cmd_tx.send(WalCmd::Sync { tx })
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL thread stopped"))?;
        
        rx.recv_timeout(Duration::from_secs(5))
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "WAL sync timeout"))?;
        
        Ok(())
    }
    
    /// Force immediate flush
    #[allow(dead_code)]
    /// Force immediate flush
    pub fn flush(&self) -> io::Result<()> {
        self.cmd_tx.send(WalCmd::Flush)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL thread stopped"))?;
        Ok(())
    }
    
    /// Get last committed LSN
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn.load(Ordering::Acquire)
    }
    
    #[allow(dead_code)]
    /// Shutdown WAL thread
    pub fn shutdown(&self) -> io::Result<()> {
        let _ = self.cmd_tx.send(WalCmd::Shutdown);
        Ok(())
    }
    
    /// Background commit thread
    fn commit_thread(
        wal_path: String,
        rx: Receiver<WalCmd>,
        committed_lsn: Arc<AtomicU64>,
        _next_lsn: Arc<AtomicU64>,
        config: WalConfig,
    ) {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path);
        
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open WAL file: {}", e);
                return;
            }
        };
        
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        let mut batch: Vec<(u64, WalOp)> = Vec::with_capacity(config.batch_size);
        let mut last_flush = Instant::now();
        
        loop {
            let deadline = last_flush + Duration::from_millis(config.flush_interval_ms);
            let timeout = deadline.saturating_duration_since(Instant::now());
            
            // Collect batch
            while batch.len() < config.batch_size {
                match rx.recv_timeout(timeout) {
                    Ok(WalCmd::Write { lsn, op }) => {
                        batch.push((lsn, op));
                    }
                    Ok(WalCmd::Sync { tx }) => {
                        // Flush immediately and signal completion
                        if !batch.is_empty() {
                            Self::flush_batch(&mut writer, &batch, &committed_lsn, config.fsync);
                            batch.clear();
                            last_flush = Instant::now();
                        }
                        let _ = tx.send(());
                    }
                    Ok(WalCmd::Flush) => {
                        if !batch.is_empty() {
                            Self::flush_batch(&mut writer, &batch, &committed_lsn, config.fsync);
                            batch.clear();
                            last_flush = Instant::now();
                        }
                    }
                    Ok(WalCmd::Shutdown) => {
                        // Final flush and exit
                        if !batch.is_empty() {
                            Self::flush_batch(&mut writer, &batch, &committed_lsn, true);
                        }
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // Deadline reached
                        break;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // Channel closed, flush remaining and exit
                        if !batch.is_empty() {
                            Self::flush_batch(&mut writer, &batch, &committed_lsn, true);
                        }
                        return;
                    }
                }
            }
            
            // Flush batch if we have any operations
            if !batch.is_empty() {
                Self::flush_batch(&mut writer, &batch, &committed_lsn, config.fsync);
                batch.clear();
                last_flush = Instant::now();
            }
        }
    }
    
    /// Flush a batch of operations to disk
    fn flush_batch(
        writer: &mut BufWriter<File>,
        batch: &[(u64, WalOp)],
        committed_lsn: &AtomicU64,
        fsync: bool,
    ) {
        let mut buf = Vec::with_capacity(batch.len() * 256);
        let mut max_lsn = 0u64;
        
        for (lsn, op) in batch {
            // Serialize operation
            let data = match serde_json::to_vec(op) {
                Ok(d) => d,
                Err(_) => continue,
            };
            
            let crc = crc32fast::hash(&data);
            
            // Write: [LSN:8][CRC:4][LEN:4][DATA]
            buf.extend_from_slice(&lsn.to_le_bytes());
            buf.extend_from_slice(&crc.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&data);
            
            max_lsn = *lsn;
        }
        
        // Single write syscall
        if let Err(e) = writer.write_all(&buf) {
            eprintln!("WAL write error: {}", e);
            return;
        }
        
        // Single fsync for entire batch (if enabled)
        if fsync {
            if let Err(e) = writer.get_ref().sync_all() {
                eprintln!("WAL fsync error: {}", e);
                return;
            }
        }
        
        // Update committed LSN
        committed_lsn.store(max_lsn, Ordering::Release);
    }
}

/// Recover database state from WAL
pub fn recover_from_wal(wal_path: &str, data: &mut Value) -> io::Result<u64> {
    if !Path::new(wal_path).exists() {
        return Ok(0);
    }
    
    let mut file = File::open(wal_path)?;
    let mut last_valid_lsn = 0u64;
    
    loop {
        // Read header: [LSN:8][CRC:4][LEN:4]
        let mut header = [0u8; 16];
        if file.read_exact(&mut header).is_err() {
            break; // EOF or truncated
        }
        
        let lsn = u64::from_le_bytes([
            header[0], header[1], header[2], header[3],
            header[4], header[5], header[6], header[7]
        ]);
        let crc = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let len = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        
        // Read data
        let mut data_buf = vec![0u8; len as usize];
        if file.read_exact(&mut data_buf).is_err() {
            eprintln!("WAL truncated at LSN {}", lsn);
            break;
        }
        
        // Verify CRC
        if crc32fast::hash(&data_buf) != crc {
            eprintln!("WAL corruption at LSN {}, stopping recovery", lsn);
            break;
        }
        
        // Deserialize and apply
        match serde_json::from_slice::<WalOp>(&data_buf) {
            Ok(op) => {
                apply_wal_op(data, &op);
                last_valid_lsn = lsn;
            }
            Err(e) => {
                eprintln!("WAL deserialization error at LSN {}: {}", lsn, e);
                break;
            }
        }
    }
    
    Ok(last_valid_lsn)
}

/// Apply a single WAL operation to data
fn apply_wal_op(data: &mut Value, op: &WalOp) {
    #[allow(unused_imports)]
    use serde_json::Map;
    
    match op.op_type {
        WalOpType::Set => {
            if let Some(ref value) = op.value {
                set_value_at_path(data, &op.path, value.clone());
            }
        }
        WalOpType::Delete => {
            delete_value_at_path(data, &op.path);
        }
    }
}

/// Set value at path (helper for recovery)
fn set_value_at_path(root: &mut Value, path: &str, value: Value) {
    if path.is_empty() {
        *root = value;
        return;
    }
    
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;
    
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part - set value
            if let Value::Object(map) = current {
                map.insert(part.to_string(), value);
            }
            return;
        }
        
        // Navigate/create path
        match current {
            Value::Object(map) => {
                if !map.contains_key(*part) {
                    map.insert(part.to_string(), Value::Object(Map::new()));
                }
                current = map.get_mut(*part).unwrap();
            }
            _ => return,
        }
    }
}

/// Delete value at path (helper for recovery)
fn delete_value_at_path(root: &mut Value, path: &str) {
    if path.is_empty() {
        return;
    }
    
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;
    
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Value::Object(map) = current {
                map.remove(*part);
            }
            return;
        }
        
        match current {
            Value::Object(map) => {
                if let Some(next) = map.get_mut(*part) {
                    current = next;
                } else {
                    return;
                }
            }
            _ => return,
        }
    }
}

/// Durability mode
#[derive(Clone, Copy, Debug)]
pub enum DurabilityMode {
    /// No WAL, manual save only
    None,
    /// Write WAL, fsync every 100ms
    Lazy,
    /// Group commit every 10ms
    Batched,
    /// Every write fsynced
    Sync,
}

impl DurabilityMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "lazy" => DurabilityMode::Lazy,
            "batched" => DurabilityMode::Batched,
            "sync" => DurabilityMode::Sync,
            _ => DurabilityMode::None,
        }
    }
    
    pub fn to_config(&self) -> Option<WalConfig> {
        match self {
            DurabilityMode::None => None,
            DurabilityMode::Lazy => Some(WalConfig {
                batch_size: 1000,
                flush_interval_ms: 100,
                fsync: true,
            }),
            DurabilityMode::Batched => Some(WalConfig {
                batch_size: 1000,
                flush_interval_ms: 10,
                fsync: true,
            }),
            DurabilityMode::Sync => Some(WalConfig {
                batch_size: 1,
                flush_interval_ms: 0,
                fsync: true,
            }),
        }
    }
}
