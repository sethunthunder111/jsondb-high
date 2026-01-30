#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock as PLRwLock;
use rayon::prelude::*;

// New modules for v4.5
mod fs_lock;
mod wal;
mod btree;
mod schema;

use btree::BTreeIndex;
use schema::{Schema, validate};
use std::collections::HashMap;
use parking_lot::Mutex;

struct TransactionState {
    undo_log: Vec<(String, Option<Value>)>,
    savepoints: HashMap<String, usize>,
}

struct PreparedFilter {
    field: String,
    op: String,
    value: Value,
    regex: Option<regex::Regex>,
}

impl PreparedFilter {
    fn from_query_filter(qf: &QueryFilter) -> Self {
        let regex = if qf.op == "regex" {
            qf.value.as_str().and_then(|p| regex::Regex::new(p).ok())
        } else {
            None
        };
        
        PreparedFilter {
            field: qf.field.clone(),
            op: qf.op.clone(),
            value: qf.value.clone(),
            regex,
        }
    }
}

use fs_lock::{ProcessLock, LockMode};
use wal::{GroupCommitWAL, WalConfig, WalOp, WalOpType, DurabilityMode, recover_from_wal};

// ============================================
// THREAD POOL CONFIGURATION
// ============================================

/// Adaptive thread pool that uses available cores when resources permit
/// Falls back to single-threaded when system is constrained
struct ThreadPoolConfig {
    available_cores: usize,
    use_parallel: bool,
}

impl ThreadPoolConfig {
    fn new() -> Self {
        let available = num_cpus::get();
        // Use parallelism only if we have more than 2 cores
        // and keep 1 core free for the main thread/system
        let use_parallel = available > 2;
        
        ThreadPoolConfig {
            available_cores: available,
            use_parallel,
        }
    }
    
    #[allow(dead_code)]
    /// Get optimal parallelism level based on workload size and system resources
    fn optimal_threads(&self, workload_size: usize) -> usize {
        if !self.use_parallel || workload_size < 100 {
            // Small workloads don't benefit from parallelism
            return 1;
        }
        
        // Use cores proportional to workload, but leave 1-2 cores free
        let max_threads = (self.available_cores - 1).max(1);
        
        // Scale threads based on workload
        // Small: 1 thread, Medium: half cores, Large: max cores
        if workload_size < 1000 {
            (max_threads / 2).max(1)
        } else if workload_size < 10000 {
            (max_threads * 3 / 4).max(1)
        } else {
            max_threads
        }
    }
    
    /// Should we use parallel processing for this workload?
    fn should_parallelize(&self, workload_size: usize) -> bool {
        self.use_parallel && workload_size >= 100
    }
}

// Global thread pool config (initialized once)
static THREAD_CONFIG: once_cell::sync::Lazy<ThreadPoolConfig> = 
    once_cell::sync::Lazy::new(ThreadPoolConfig::new);

// ============================================
// DATA STRUCTURES
// ============================================

/// Legacy WAL entry (for backwards compatibility)
#[derive(Serialize, Deserialize, Debug)]
struct WalEntry {
    op: String,
    path: String,
    value: Option<Value>,
}

/// Query filter for parallel batch queries
#[derive(Serialize, Deserialize, Debug, Clone)]
#[napi(object)]
pub struct QueryFilter {
    pub field: String,
    pub op: String,   // "eq", "ne", "gt", "gte", "lt", "lte", "contains", "startswith", "endswith"
    pub value: Value,
}

/// Batch query request
#[derive(Serialize, Deserialize, Debug, Clone)]
#[napi(object)]
pub struct BatchQuery {
    pub path: String,
    pub filters: Vec<QueryFilter>,
}

/// Parallel operation result
#[derive(Debug)]
#[napi(object)]
pub struct ParallelResult {
    pub success: bool,
    pub count: u32,
    pub error: Option<String>,
}

/// System resource info
#[derive(Debug)]
#[napi(object)]
pub struct SystemInfo {
    pub available_cores: u32,
    pub parallel_enabled: bool,
    pub recommended_batch_size: u32,
}

/// Database options for v4.5
#[derive(Debug, Clone)]
pub struct DBOptions {
    pub lock_mode: LockMode,
    pub durability: DurabilityMode,
    pub wal_batch_size: usize,
    pub wal_flush_ms: u64,
}

impl Default for DBOptions {
    fn default() -> Self {
        DBOptions {
            lock_mode: LockMode::Exclusive,
            durability: DurabilityMode::Batched,
            wal_batch_size: 1000,
            wal_flush_ms: 10,
        }
    }
}

#[napi]
pub struct NativeDB {
    path: String,
    wal_path: String,
    data: Arc<PLRwLock<Value>>,
    
    // v4.5: Process-level file locking
    #[allow(dead_code)]
    process_lock: Option<ProcessLock>,
    
    // v4.5: Group commit WAL (replaces old WAL)
    wal: Option<Arc<GroupCommitWAL>>,
    
    // v5.1 Persistent Indexes
    indexes: Arc<PLRwLock<HashMap<String, BTreeIndex>>>,

    // v5.1 Schema validation
    schemas: Arc<PLRwLock<HashMap<String, Schema>>>,

    // v5.1 Transactions
    transaction_state: Arc<Mutex<Option<TransactionState>>>,

    // Options (kept for future use)
    #[allow(dead_code)]
    options: DBOptions,
}

#[napi]
impl NativeDB {
    /// Legacy constructor for backwards compatibility
    #[napi(constructor)]
    pub fn new(path: String, wal: bool) -> Result<Self> {
        let options = DBOptions {
            lock_mode: LockMode::None,  // Legacy: no locking
            durability: if wal { DurabilityMode::Batched } else { DurabilityMode::None },
            wal_batch_size: 1000,
            wal_flush_ms: 10,
        };
        
        Self::new_with_options_internal(path, options)
    }
    
    /// Internal constructor with full options
    fn new_with_options_internal(path: String, options: DBOptions) -> Result<Self> {
        // 1. Acquire process lock if requested
        let process_lock = match options.lock_mode {
            LockMode::Exclusive => {
                match ProcessLock::acquire(&path) {
                    Ok(lock) => Some(lock),
                    Err(e) => return Err(Error::from_reason(format!("Failed to acquire lock: {}", e))),
                }
            }
            LockMode::Shared => {
                // Check if locked, but don't acquire
                match ProcessLock::is_locked(&path) {
                    Ok(true) => return Err(Error::from_reason("Database is locked by another process".to_string())),
                    Ok(false) => None,
                    Err(_) => None, // If we can't check, proceed anyway
                }
            }
            LockMode::None => None,
        };
        
        // 2. Initialize WAL if durability enabled
        let wal_path = format!("{}.wal", path);
        let wal = if let Some(config) = options.durability.to_config() {
            let wal_config = WalConfig {
                batch_size: options.wal_batch_size,
                flush_interval_ms: options.wal_flush_ms,
                fsync: config.fsync,
            };
            match GroupCommitWAL::new(&wal_path, wal_config) {
                Ok(w) => Some(Arc::new(w)),
                Err(e) => return Err(Error::from_reason(format!("Failed to create WAL: {}", e))),
            }
        } else {
            None
        };
        
        // 3. Load existing data or start fresh
        let mut data = json!({});
        
        let p = PathBuf::from(&path);
        if p.exists() {
            // Load main DB
            let contents = fs::read_to_string(&p).map_err(|e| {
                Error::from_reason(format!("Failed to read database: {}", e))
            })?;
            
            data = serde_json::from_str(&contents).map_err(|e| {
                Error::from_reason(format!("Failed to parse database: {}", e))
            })?;
        }
        
        // 4. Recover from WAL
        if wal.is_some() {
            let _ = recover_from_wal(&wal_path, &mut data);
        } else {
            // Legacy WAL recovery
            let legacy_wal = format!("{}.wal", path);
            let wal_p = PathBuf::from(&legacy_wal);
            if wal_p.exists() {
                let _ = Self::recover_legacy_wal(&legacy_wal, &mut data);
            }
        }
        
        Ok(NativeDB {
            path,
            wal_path,
            data: Arc::new(PLRwLock::new(data)),
            process_lock,
            wal,
            indexes: Arc::new(PLRwLock::new(HashMap::new())),
            schemas: Arc::new(PLRwLock::new(HashMap::new())),
            transaction_state: Arc::new(Mutex::new(None)),
            options,
        })
    }
    
    /// v4.5: Create database with options from JS
    #[napi(js_name = "newWithOptions")]
    pub fn new_with_options_js(
        path: String,
        lock_mode: String,
        durability: String,
        wal_batch_size: Option<u32>,
        wal_flush_ms: Option<u32>,
    ) -> Result<Self> {
        let options = DBOptions {
            lock_mode: LockMode::from_str(&lock_mode),
            durability: DurabilityMode::from_str(&durability),
            wal_batch_size: wal_batch_size.unwrap_or(1000) as usize,
            wal_flush_ms: wal_flush_ms.unwrap_or(10) as u64,
        };
        
        Self::new_with_options_internal(path, options)
    }

    /// Get system resource information for adaptive parallelism
    #[napi]
    pub fn get_system_info(&self) -> SystemInfo {
        SystemInfo {
            available_cores: THREAD_CONFIG.available_cores as u32,
            parallel_enabled: THREAD_CONFIG.use_parallel,
            recommended_batch_size: if THREAD_CONFIG.use_parallel { 1000 } else { 100 },
        }
    }
    
    /// v4.5: Explicit sync for durability
    #[napi]
    pub fn sync(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(|e| {
                Error::from_reason(format!("Sync failed: {}", e))
            })?;
        }
        Ok(())
    }
    
    /// v4.5: Get current WAL status
    #[napi]
    pub fn wal_status(&self) -> Result<Value> {
        if let Some(ref wal) = self.wal {
            Ok(json!({
                "enabled": true,
                "committed_lsn": wal.committed_lsn(),
            }))
        } else {
            Ok(json!({
                "enabled": false,
            }))
        }
    }

    /// v4.5: Explicitly release resources (locks, WAL handles)
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        self.process_lock.take();
        if let Some(wal) = self.wal.take() {
            let _ = wal.sync();
        }
        Ok(())
    }

    /// Legacy load (maintained for compatibility)
    #[napi]
    pub fn load(&self) -> Result<()> {
        // Data is already loaded in constructor
        Ok(())
    }

    #[napi]
    pub fn save(&self) -> Result<()> {
        // Flush WAL first if enabled
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(|e| {
                Error::from_reason(format!("Failed to flush WAL: {}", e))
            })?;
        }
        
        let data_guard = self.data.read();
        let json_str = serde_json::to_string_pretty(&*data_guard).map_err(|e| Error::from_reason(e.to_string()))?;
        
        // Atomic write
        let tmp_path = format!("{}.tmp", self.path);
        let mut file = File::create(&tmp_path)?;
        file.write_all(json_str.as_bytes())?;
        file.sync_all()?;
        fs::rename(tmp_path, &self.path)?;
        
        // Clear WAL after successful save
        if self.wal.is_some() {
            // Truncate WAL file
            File::create(&self.wal_path)?;
        }
        
        // Save indexes
        let mut indexes = self.indexes.write();
        for idx in indexes.values_mut() {
            idx.save().map_err(|e| Error::from_reason(format!("Failed to save index: {:?}", e)))?;
        }
        
        Ok(())
    }
    
    /// Legacy WAL append (for internal use)
    fn append_wal(&self, op_type: WalOpType, path: &str, value: Option<Value>) -> Result<()> {
        if let Some(ref wal) = self.wal {
            let op = WalOp {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                op_type,
                path: path.to_string(),
                value,
            };
            
            wal.append(op).map_err(|e| {
                Error::from_reason(format!("WAL append failed: {}", e))
            })?;
        }
        Ok(())
    }
    
    /// Recover from legacy WAL format
    fn recover_legacy_wal(wal_path: &str, data: &mut Value) -> Result<()> {
        let file = File::open(wal_path)?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            if let Ok(l) = line {
                if l.trim().is_empty() { continue; }
                if let Ok(entry) = serde_json::from_str::<WalEntry>(&l) {
                    match entry.op.as_str() {
                        "set" => {
                            if let Some(val) = entry.value {
                                let _ = Self::set_value_at_path(data, &entry.path, val);
                            }
                        }
                        "delete" => {
                            let _ = Self::delete_value_at_path(data, &entry.path);
                        }
                        "push" => {
                            if let Some(val) = entry.value {
                                let _ = Self::push_value_at_path(data, &entry.path, val);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        
        Ok(())
    }

    // --- Logic Helpers ---

    fn set_value_at_path(root: &mut Value, path_str: &str, value: Value) -> Result<()> {
        if path_str.is_empty() {
            *root = value;
            return Ok(())
        }
        
        let parts: Vec<&str> = path_str.split('.').collect();
        if parts.is_empty() { return Ok(()) }
        
        let last_part = parts.last().unwrap();
        let parent_parts = &parts[..parts.len()-1];
        
        let mut current = root;
        
        for (i, part) in parent_parts.iter().enumerate() {
            if current.is_null() {
                 *current = Value::Object(serde_json::Map::new());
            }
            let is_array_idx = parts[i+1].parse::<usize>().is_ok(); 
            if let Value::Object(map) = current {
                if !map.contains_key(*part) {
                    map.insert(part.to_string(), if is_array_idx { json!([]) } else { json!({}) });
                }
                current = map.get_mut(*part).unwrap();
            } else if let Value::Array(arr) = current {
                 if let Ok(idx) = part.parse::<usize>() {
                     while arr.len() <= idx {
                         arr.push(Value::Null);
                     }
                     if arr[idx].is_null() {
                          let is_next_array = parts.get(i+1).map(|p| p.parse::<usize>().is_ok()).unwrap_or(false);
                          arr[idx] = if is_next_array { json!([]) } else { json!({}) };
                     }
                     current = &mut arr[idx];
                 } else {
                     return Err(Error::from_reason("Cannot index array with string".to_string()));
                 }
            } else {
                 return Err(Error::from_reason(format!("Path segment '{}' blocked by primitive", part)));
            }
        }

        if let Value::Object(map) = current {
            map.insert(last_part.to_string(), value);
        } else if let Value::Array(arr) = current {
            if let Ok(idx) = last_part.parse::<usize>() {
                while arr.len() <= idx {
                    arr.push(Value::Null);
                }
                arr[idx] = value;
            } else {
                 return Err(Error::from_reason("Cannot set non-numeric key on array".to_string()));
            }
        } else {
             if current.is_null() {
                 let is_array = last_part.parse::<usize>().is_ok();
                 if is_array {
                     let idx = last_part.parse::<usize>().unwrap();
                     let mut arr = vec![Value::Null; idx + 1];
                     arr[idx] = value;
                     *current = Value::Array(arr);
                 } else {
                     let mut map = serde_json::Map::new();
                     map.insert(last_part.to_string(), value);
                     *current = Value::Object(map);
                 }
             } else {
                  return Err(Error::from_reason(format!("Parent of '{}' is not an object/array", last_part)));
             }
        }
        Ok(())
    }

    fn delete_value_at_path(root: &mut Value, path_str: &str) -> Result<()> {
        if path_str.is_empty() {
            *root = json!({});
            return Ok(())
        }
        let parts: Vec<&str> = path_str.split('.').collect();
        if parts.is_empty() { return Ok(()) }
        
        let parent_path = parts[..parts.len()-1].join(".");
        let target_key = parts.last().unwrap();
        
        let ptr = if parent_path.is_empty() { "".to_string() } else { format!("/{}", parent_path.replace(".", "/")) };
        
        let parent = if ptr.is_empty() { Some(root) } else { root.pointer_mut(&ptr) };

        if let Some(p) = parent {
            if let Value::Object(map) = p {
                map.remove(*target_key);
            } else if let Value::Array(arr) = p {
                if let Ok(idx) = target_key.parse::<usize>() {
                    if idx < arr.len() {
                        arr.remove(idx);
                    }
                }
            }
        }
        Ok(())
    }

    fn push_value_at_path(root: &mut Value, path_str: &str, value: Value) -> Result<()> {
        let ptr = if path_str.starts_with('/') { path_str.to_string() } else { format!("/{}", path_str.replace(".", "/")) };
        
        if let Some(target) = root.pointer_mut(&ptr) {
            if let Value::Array(arr) = target {
                // Dedupe: check if value exists
                if !arr.contains(&value) {
                     arr.push(value);
                }
            } else {
                return Err(Error::from_reason("Target is not an array".to_string()));
            }
        } else {
             return Err(Error::from_reason("Path does not exist".to_string()));
        }
        Ok(())
    }

    // ============================================
    // PARALLEL OPERATIONS
    // ============================================

    /// Execute batch set operations in parallel when beneficial
    #[napi]
    pub fn batch_set_parallel(&self, operations: Vec<(String, Value)>) -> Result<ParallelResult> {
        let count = operations.len();
        
        if THREAD_CONFIG.should_parallelize(count) {
            // Pre-validate paths in parallel
            let validation_results: Vec<bool> = operations
                .par_iter()
                .map(|(path, _)| !path.is_empty())
                .collect();
            
            if validation_results.iter().any(|&v| !v) {
                return Ok(ParallelResult {
                    success: false,
                    count: 0,
                    error: Some("Invalid path in batch".to_string()),
                });
            }
            
            // Apply all operations (requires sequential write lock)
            let mut data = self.data.write();
            let mut success_count = 0u32;
            
            for (path, value) in operations {
                let _ = self.append_wal(WalOpType::Set, &path, Some(value.clone()));
                if Self::set_value_at_path(&mut data, &path, value).is_ok() {
                    success_count += 1;
                }
            }
            
            Ok(ParallelResult {
                success: true,
                count: success_count,
                error: None,
            })
        } else {
            // Sequential fallback
            let mut data = self.data.write();
            let mut success_count = 0u32;
            
            for (path, value) in operations {
                let _ = self.append_wal(WalOpType::Set, &path, Some(value.clone()));
                if Self::set_value_at_path(&mut data, &path, value).is_ok() {
                    success_count += 1;
                }
            }
            
            Ok(ParallelResult {
                success: true,
                count: success_count,
                error: None,
            })
        }
    }

    /// Parallel filter/query on a collection
    #[napi]
    pub fn parallel_query(&self, path: String, filters: Vec<QueryFilter>) -> Result<Value> {
        let data = self.data.read();
        let ptr = if path.starts_with('/') { path } else { format!("/{}", path.replace(".", "/")) };
        
        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };
        
        match collection {
            Some(Value::Object(map)) => {
                let items: Vec<&Value> = map.values().collect();
                let prepared: Vec<PreparedFilter> = filters.iter().map(PreparedFilter::from_query_filter).collect();
                let filtered = self.filter_items_parallel(&items, &prepared);
                Ok(Value::Array(filtered))
            }
            Some(Value::Array(arr)) => {
                let items: Vec<&Value> = arr.iter().collect();
                let prepared: Vec<PreparedFilter> = filters.iter().map(PreparedFilter::from_query_filter).collect();
                let filtered = self.filter_items_parallel(&items, &prepared);
                Ok(Value::Array(filtered))
            }
            _ => Ok(Value::Array(vec![])),
        }
    }
    
    /// Internal parallel filter implementation
    fn filter_items_parallel(&self, items: &[&Value], filters: &[PreparedFilter]) -> Vec<Value> {
        let count = items.len();
        
        if THREAD_CONFIG.should_parallelize(count) && !filters.is_empty() {
            items
                .par_iter()
                .filter(|item| self.matches_filters(item, filters))
                .map(|v| (*v).clone())
                .collect()
        } else {
            items
                .iter()
                .filter(|item| self.matches_filters(item, filters))
                .map(|v| (*v).clone())
                .collect()
        }
    }
    
    /// Check if an item matches all filters
    fn matches_filters(&self, item: &Value, filters: &[PreparedFilter]) -> bool {
        for filter in filters {
            if !self.matches_filter(item, filter) {
                return false;
            }
        }
        true
    }
    
    /// Check if an item matches a single filter
    fn matches_filter(&self, item: &Value, filter: &PreparedFilter) -> bool {
        let parts: Vec<&str> = filter.field.split('.').collect();
        let mut current = item;
        
        for part in &parts {
            match current {
                Value::Object(map) => {
                    if let Some(v) = map.get(*part) {
                        current = v;
                    } else {
                        return false;
                    }
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        if let Some(v) = arr.get(idx) {
                            current = v;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        
        match filter.op.as_str() {
            "eq" => current == &filter.value,
            "ne" => current != &filter.value,
            "gt" => {
                if let (Some(a), Some(b)) = (current.as_f64(), filter.value.as_f64()) {
                    a > b
                } else {
                    false
                }
            }
            "gte" => {
                if let (Some(a), Some(b)) = (current.as_f64(), filter.value.as_f64()) {
                    a >= b
                } else {
                    false
                }
            }
            "lt" => {
                if let (Some(a), Some(b)) = (current.as_f64(), filter.value.as_f64()) {
                    a < b
                } else {
                    false
                }
            }
            "lte" => {
                if let (Some(a), Some(b)) = (current.as_f64(), filter.value.as_f64()) {
                    a <= b
                } else {
                    false
                }
            }
            "contains" => {
                if let (Some(haystack), Some(needle)) = (current.as_str(), filter.value.as_str()) {
                    haystack.contains(needle)
                } else {
                    false
                }
            }
            "startswith" => {
                if let (Some(haystack), Some(needle)) = (current.as_str(), filter.value.as_str()) {
                    haystack.starts_with(needle)
                } else {
                    false
                }
            }
            "endswith" => {
                if let (Some(haystack), Some(needle)) = (current.as_str(), filter.value.as_str()) {
                    haystack.ends_with(needle)
                } else {
                    false
                }
            }
            "in" => {
                if let Value::Array(arr) = &filter.value {
                    arr.contains(current)
                } else {
                    false
                }
            }
            "notin" => {
                if let Value::Array(arr) = &filter.value {
                    !arr.contains(current)
                } else {
                    false
                }
            }
            "regex" => {
                if let (Some(s), Some(re)) = (current.as_str(), &filter.regex) {
                    re.is_match(s)
                } else {
                    false
                }
            }
            "containsAll" => {
                 if let (Value::Array(curr_arr), Value::Array(req_arr)) = (current, &filter.value) {
                     req_arr.iter().all(|req| curr_arr.contains(req))
                 } else {
                     false
                 }
            }
            "containsAny" => {
                 if let (Value::Array(curr_arr), Value::Array(req_arr)) = (current, &filter.value) {
                     req_arr.iter().any(|req| curr_arr.contains(req))
                 } else {
                     false
                 }
            }
            _ => true,
        }
    }

    /// Parallel aggregation operations
    #[napi]
    pub fn parallel_aggregate(&self, path: String, operation: String, field: Option<String>) -> Result<Value> {
        let data = self.data.read();
        let ptr = if path.starts_with('/') { path } else { format!("/{}", path.replace(".", "/")) };
        
        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };
        
        let items: Vec<&Value> = match collection {
            Some(Value::Object(map)) => map.values().collect(),
            Some(Value::Array(arr)) => arr.iter().collect(),
            _ => return Ok(Value::Null),
        };
        
        let count = items.len();
        
        match operation.as_str() {
            "count" => Ok(json!(count)),
            "sum" => {
                let field_name = field.unwrap_or_default();
                let sum: f64 = if THREAD_CONFIG.should_parallelize(count) {
                    items.par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .sum()
                } else {
                    items.iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .sum()
                };
                Ok(json!(sum))
            }
            "avg" => {
                let field_name = field.unwrap_or_default();
                let values: Vec<f64> = if THREAD_CONFIG.should_parallelize(count) {
                    items.par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .collect()
                } else {
                    items.iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .collect()
                };
                if values.is_empty() {
                    Ok(json!(0.0))
                } else {
                    let sum: f64 = values.iter().sum();
                    Ok(json!(sum / values.len() as f64))
                }
            }
            "min" => {
                let field_name = field.unwrap_or_default();
                let min: Option<f64> = if THREAD_CONFIG.should_parallelize(count) {
                    items.par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(|| f64::INFINITY, |a, b| a.min(b))
                        .into()
                } else {
                    items.iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(f64::min)
                };
                match min {
                    Some(v) if v != f64::INFINITY => Ok(json!(v)),
                    _ => Ok(Value::Null),
                }
            }
            "max" => {
                let field_name = field.unwrap_or_default();
                let max: Option<f64> = if THREAD_CONFIG.should_parallelize(count) {
                    items.par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(|| f64::NEG_INFINITY, |a, b| a.max(b))
                        .into()
                } else {
                    items.iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(f64::max)
                };
                match max {
                    Some(v) if v != f64::NEG_INFINITY => Ok(json!(v)),
                    _ => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    /// Perform a parallel left outer join between two collections (lookup)
    #[napi]
    pub fn parallel_lookup(
        &self,
        left_path: String,
        right_path: String,
        left_field: String,
        right_field: String,
        as_field: String,
    ) -> Result<Value> {
        let data = self.data.read();

        // Helper to get collection items
        let get_items = |path: &str| -> Option<Vec<&Value>> {
            let ptr = if path.starts_with('/') { path.to_string() } else { format!("/{}", path.replace(".", "/")) };
            let collection = if ptr == "/" || ptr.is_empty() {
                Some(&*data)
            } else {
                data.pointer(&ptr)
            };
            
            match collection {
                Some(Value::Object(map)) => Some(map.values().collect()),
                Some(Value::Array(arr)) => Some(arr.iter().collect()),
                _ => None,
            }
        };

        let left_items = get_items(&left_path).ok_or_else(|| Error::from_reason(format!("Left collection not found: {}", left_path)))?;
        let right_items = get_items(&right_path).ok_or_else(|| Error::from_reason(format!("Right collection not found: {}", right_path)))?;

        // Build hash table on right collection
        use std::collections::HashMap;
        let mut hash_table: HashMap<String, Vec<&Value>> = HashMap::new();
        
        for item in &right_items {
             if let Some(val) = self.get_value_at_field(item, &right_field) {
                 let key = match val {
                     Value::String(s) => s.clone(),
                     _ => val.to_string(),
                 };
                 hash_table.entry(key).or_default().push(item);
             }
        }

        // Probe with left collection
        let results: Vec<Value> = if THREAD_CONFIG.should_parallelize(left_items.len()) {
            left_items.par_iter().map(|left_item| {
                let mut joined = (*left_item).clone();
                if let Value::Object(ref mut map) = joined {
                    let mut matches_curr = Vec::new();
                    if let Some(val) = self.get_value_at_field(left_item, &left_field) {
                        let key = match val {
                            Value::String(s) => s.clone(),
                            _ => val.to_string(),
                        };
                        
                        if let Some(matches) = hash_table.get(&key) {
                            for m in matches {
                                matches_curr.push((*m).clone());
                            }
                        }
                    }
                    map.insert(as_field.clone(), Value::Array(matches_curr));
                }
                joined
            }).collect()
        } else {
             left_items.iter().map(|left_item| {
                let mut joined = (*left_item).clone();
                if let Value::Object(ref mut map) = joined {
                    let mut matches_curr = Vec::new();
                    if let Some(val) = self.get_value_at_field(left_item, &left_field) {
                        let key = match val {
                            Value::String(s) => s.clone(),
                            _ => val.to_string(),
                        };
                        
                        if let Some(matches) = hash_table.get(&key) {
                            for m in matches {
                                matches_curr.push((*m).clone());
                            }
                        }
                    }
                    map.insert(as_field.clone(), Value::Array(matches_curr));
                }
                joined
            }).collect()
        };

        Ok(Value::Array(results))
    }

    /// Helper to get arbitrary field value (supports dot notation)
    fn get_value_at_field<'a>(&self, item: &'a Value, path: &str) -> Option<&'a Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = item;
        
        for part in parts {
            match current {
                Value::Object(map) => {
                    if let Some(v) = map.get(part) {
                        current = v;
                    } else {
                        return None;
                    }
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                         if let Some(v) = arr.get(idx) {
                            current = v;
                         } else {
                             return None;
                         }
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current)
    }
    
    /// Helper to get numeric field value
    fn get_numeric_field(&self, item: &Value, field: &str) -> Option<f64> {
        if field.is_empty() {
            return item.as_f64();
        }
        
        let parts: Vec<&str> = field.split('.').collect();
        let mut current = item;
        
        for part in parts {
            match current {
                Value::Object(map) => {
                    current = map.get(part)?;
                }
                Value::Array(arr) => {
                    let idx: usize = part.parse().ok()?;
                    current = arr.get(idx)?;
                }
                _ => return None,
            }
        }
        
        current.as_f64()
    }

    // --- Exposed API ---

    #[napi]
    pub fn get(&self, path: String) -> Result<Value> {
        let data = self.data.read();
        if path.is_empty() {
            return Ok(data.clone());
        }
        let ptr = if path.starts_with('/') { path } else { format!("/{}", path.replace(".", "/")) };
        match data.pointer(&ptr) {
            Some(v) => Ok(v.clone()),
            None => Ok(Value::Null), 
        }
    }

    #[napi]
    pub fn set(&self, path: String, value: Value) -> Result<()> {
        // v5.1 Transaction support
        self.record_undo(&path);

        // Append to WAL first (durability)
        self.append_wal(WalOpType::Set, &path, Some(value.clone()))?;
        
        // Update memory
        let mut data = self.data.write();
        Self::set_value_at_path(&mut data, &path, value)?;
        Ok(())
    }
    
    #[napi]
    pub fn has(&self, path: String) -> Result<bool> {
        let data = self.data.read();
        let ptr = if path.starts_with('/') { path } else { format!("/{}", path.replace(".", "/")) };
        Ok(data.pointer(&ptr).is_some())
    }
    
    #[napi]
    pub fn delete(&self, path: String) -> Result<()> {
        // v5.1 Transaction support
        self.record_undo(&path);

        self.append_wal(WalOpType::Delete, &path, None)?;
        
        let mut data = self.data.write();
        Self::delete_value_at_path(&mut data, &path)?;
        Ok(())
    }

    #[napi]
    pub fn push(&self, path: String, value: Value) -> Result<()> {
        // v5.1 Transaction support
        self.record_undo(&path);

        let mut data = self.data.write();
        Self::push_value_at_path(&mut data, &path, value)?;
        Ok(())
    }

    // Indexing API
    
    #[napi]
    pub fn register_index(&self, name: String, field: String) -> Result<()> {
        let mut indexes = self.indexes.write();
        if !indexes.contains_key(&name) {
             let idx = BTreeIndex::load_or_create(name.clone(), field.clone(), &self.path)
                 .map_err(|e| Error::from_reason(format!("Failed to load index {}: {:?}", name, e)))?;
             indexes.insert(name, idx);
        }
        Ok(())
    }
    
    #[napi]
    pub fn update_index(&self, name: String, key: Value, path: String, is_delete: bool) -> Result<()> {
        let mut indexes = self.indexes.write();
        if let Some(idx) = indexes.get_mut(&name) {
            if is_delete {
                idx.remove(&key, &path);
            } else {
                idx.insert(&key, path);
            }
        }
        Ok(())
    }
    
    #[napi]
    pub fn find_index_paths(&self, name: String, key: Value) -> Result<Vec<String>> {
        let indexes = self.indexes.read();
        if let Some(idx) = indexes.get(&name) {
            if let Some(paths) = idx.find(&key) {
                return Ok(paths.clone());
            }
        }
        Ok(vec![])
    }
    
    #[napi]
    pub fn clear_index(&self, name: String) -> Result<()> {
         let mut indexes = self.indexes.write();
         if let Some(idx) = indexes.get_mut(&name) {
             idx.clear();
         }
         Ok(())
    }

    // Schema API

    #[napi]
    pub fn register_schema(&self, path: String, schema_json: String) -> Result<()> {
        let schema: Schema = serde_json::from_str(&schema_json)
            .map_err(|e| Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
        let mut schemas = self.schemas.write();
        schemas.insert(path, schema);
        Ok(())
    }

    #[napi]
    pub fn validate_path(&self, path: String, value: Value) -> Result<()> {
        let schemas = self.schemas.read();
        // Find best matching schema (exact or parent)
        let mut parts: Vec<&str> = path.split('.').collect();
        while !parts.is_empty() {
            let current_path = parts.join(".");
            if let Some(schema) = schemas.get(&current_path) {
                validate(&value, schema).map_err(|e| Error::from_reason(format!("Validation failed at {}: {}", current_path, e)))?;
                break;
            }
            parts.pop();
        }
        Ok(())
    }

    // Advanced Transactions
    
    #[napi]
    pub fn begin_transaction(&self) -> Result<()> {
        let mut state = self.transaction_state.lock();
        if state.is_some() {
            return Err(Error::from_reason("Transaction already active".to_string()));
        }
        *state = Some(TransactionState {
            undo_log: Vec::new(),
            savepoints: HashMap::new(),
        });
        Ok(())
    }
    
    #[napi]
    pub fn commit_transaction(&self) -> Result<()> {
        let mut state = self.transaction_state.lock();
        if state.is_none() {
            return Err(Error::from_reason("No active transaction".to_string()));
        }
        *state = None;
        Ok(())
    }
    
    #[napi]
    pub fn rollback_transaction(&self) -> Result<()> {
        let mut state_lock = self.transaction_state.lock();
        if let Some(state) = state_lock.take() {
            let mut data = self.data.write();
            self.apply_undo_log(&mut data, state.undo_log)?;
        } else {
            return Err(Error::from_reason("No active transaction".to_string()));
        }
        Ok(())
    }
    
    #[napi]
    pub fn create_savepoint(&self, name: String) -> Result<()> {
        let mut state = self.transaction_state.lock();
        if let Some(s) = state.as_mut() {
            s.savepoints.insert(name, s.undo_log.len());
            Ok(())
        } else {
            Err(Error::from_reason("No active transaction".to_string()))
        }
    }
    
    #[napi]
    pub fn rollback_to_savepoint(&self, name: String) -> Result<()> {
        let mut state_lock = self.transaction_state.lock();
        if let Some(state) = state_lock.as_mut() {
            if let Some(&index) = state.savepoints.get(&name) {
                let to_rollback = state.undo_log.split_off(index);
                let mut data = self.data.write();
                self.apply_undo_log(&mut data, to_rollback)?;
                Ok(())
            } else {
                Err(Error::from_reason(format!("Savepoint '{}' not found", name)))
            }
        } else {
            Err(Error::from_reason("No active transaction".to_string()))
        }
    }
    
    fn apply_undo_log(&self, data: &mut Value, undo_log: Vec<(String, Option<Value>)>) -> Result<()> {
        // Apply in reverse order
        for (path, old_value) in undo_log.into_iter().rev() {
            if let Some(val) = old_value {
                let _ = Self::set_value_at_path(data, &path, val);
            } else {
                let _ = Self::delete_value_at_path(data, &path);
            }
        }
        Ok(())
    }
    
    fn record_undo(&self, path: &str) {
        let mut state_lock = self.transaction_state.lock();
        if let Some(state) = state_lock.as_mut() {
            let data = self.data.read();
            let old_value = data.pointer(&format!("/{}", path.replace(".", "/"))).cloned();
            state.undo_log.push((path.to_string(), old_value));
        }
    }
}
