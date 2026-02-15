#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::RwLock as PLRwLock;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;

mod btree;
mod fs_lock;
mod schema;
mod wal;

mod memory_manager;
mod query_engine;
mod write_lock;

use btree::BTreeIndex;
use lru::LruCache;
use memory_manager::{parse_memory_limit, MemoryConfig, MemoryManager};
use parking_lot::Mutex;
use query_engine::{execute_aggregate, execute_query, parse_sort_specs, CompiledFilter};
use schema::{validate, Schema};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use write_lock::StripedLockManager;

static REGEX_CACHE: once_cell::sync::Lazy<Mutex<LruCache<String, regex::Regex>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())));

struct TransactionState {
    undo_log: Vec<(String, Option<Value>)>,
    savepoints: HashMap<String, usize>,
}

struct PreparedFilter {
    #[allow(dead_code)]
    field: String,
    op: String,
    value: Value,
    regex: Option<regex::Regex>,
    path: Vec<String>,
}

impl PreparedFilter {
    fn from_query_filter(qf: &QueryFilter) -> Self {
        let regex = if qf.op == "regex" {
            qf.value.as_str().and_then(|p| {
                let mut cache = REGEX_CACHE.lock();
                if let Some(re) = cache.get(p) {
                    Some(re.clone())
                } else {
                    match regex::Regex::new(p) {
                        Ok(re) => {
                            cache.push(p.to_string(), re.clone());
                            Some(re)
                        }
                        Err(_) => None,
                    }
                }
            })
        } else {
            None
        };

        PreparedFilter {
            field: qf.field.clone(),
            op: qf.op.clone(),
            value: qf.value.clone(),
            regex,
            path: qf.field.split('.').map(|s| s.to_string()).collect(),
        }
    }
}

use fs_lock::{LockMode, ProcessLock};
use wal::{recover_from_wal, DurabilityMode, GroupCommitWAL, WalConfig, WalOp, WalOpType};

// ============================================
// THREAD POOL CONFIGURATION
// ============================================

struct ThreadPoolConfig {
    available_cores: usize,
    use_parallel: bool,
}

impl ThreadPoolConfig {
    fn new() -> Self {
        let available = num_cpus::get();
        let use_parallel = available > 2;

        ThreadPoolConfig {
            available_cores: available,
            use_parallel,
        }
    }

    fn should_parallelize(&self, workload_size: usize) -> bool {
        self.use_parallel && workload_size >= 100
    }
}

static THREAD_CONFIG: once_cell::sync::Lazy<ThreadPoolConfig> =
    once_cell::sync::Lazy::new(ThreadPoolConfig::new);

// ============================================
// OPTIMIZATIONS
// ============================================

struct PathSegment<'a> {
    raw: &'a str,
    index: Option<usize>,
}

fn parse_path<'a>(path: &'a str) -> Vec<PathSegment<'a>> {
    path.split('.')
        .map(|part| PathSegment {
            raw: part,
            index: part.parse::<usize>().ok(),
        })
        .collect()
}

// ============================================
// DATA STRUCTURES
// ============================================

#[derive(Serialize, Deserialize, Debug)]
struct WalEntry {
    op: String,
    path: String,
    value: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[napi(object)]
pub struct QueryFilter {
    pub field: String,
    pub op: String,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[napi(object)]
pub struct BatchQuery {
    pub path: String,
    pub filters: Vec<QueryFilter>,
}

#[derive(Debug)]
#[napi(object)]
pub struct ParallelResult {
    pub success: bool,
    pub count: u32,
    pub error: Option<String>,
}

#[derive(Debug)]
#[napi(object)]
pub struct SystemInfo {
    pub available_cores: u32,
    pub parallel_enabled: bool,
    pub recommended_batch_size: u32,
}

#[derive(Debug, Clone)]
pub struct DBOptions {
    pub lock_mode: LockMode,
    pub durability: DurabilityMode,
    pub wal_batch_size: usize,
    pub wal_flush_ms: u64,
    pub lock_timeout_ms: u64,
}

impl Default for DBOptions {
    fn default() -> Self {
        DBOptions {
            lock_mode: LockMode::Exclusive,
            durability: DurabilityMode::Batched,
            wal_batch_size: 1000,
            wal_flush_ms: 10,
            lock_timeout_ms: 5000,
        }
    }
}

#[napi]
pub struct NativeDB {
    path: String,
    wal_path: String,
    data: Arc<PLRwLock<Value>>,

    #[allow(dead_code)]
    process_lock: Option<ProcessLock>,

    wal: Option<Arc<GroupCommitWAL>>,

    indexes: Arc<PLRwLock<HashMap<String, BTreeIndex>>>,

    schemas: Arc<PLRwLock<HashMap<String, Schema>>>,

    transaction_state: Arc<Mutex<Option<TransactionState>>>,

    memory_manager: Arc<Mutex<MemoryManager>>,

    write_locks: Arc<StripedLockManager>,

    #[allow(dead_code)]
    options: DBOptions,
}

#[napi]
impl NativeDB {
    #[napi(constructor)]
    pub fn new(path: String, wal: bool) -> Result<Self> {
        let options = DBOptions {
            lock_mode: LockMode::None,
            durability: if wal {
                DurabilityMode::Batched
            } else {
                DurabilityMode::None
            },
            wal_batch_size: 1000,
            wal_flush_ms: 10,
            lock_timeout_ms: 0,
        };

        Self::new_with_options_internal(path, options)
    }

    fn new_with_options_internal(path: String, options: DBOptions) -> Result<Self> {
        let process_lock = match options.lock_mode {
            LockMode::Exclusive => match ProcessLock::acquire(&path, options.lock_timeout_ms) {
                Ok(lock) => Some(lock),
                Err(e) => return Err(Error::from_reason(format!("Failed to acquire lock: {}", e))),
            },
            LockMode::Shared => {
                match ProcessLock::is_locked(&path) {
                    Ok(true) => {
                        return Err(Error::from_reason(
                            "Database is locked by another process".to_string(),
                        ))
                    }
                    Ok(false) => None,
                    Err(_) => None,
                }
            }
            LockMode::None => None,
        };

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

        let mut data = json!({});

        let p = PathBuf::from(&path);
        if p.exists() {
            let contents = fs::read_to_string(&p)
                .map_err(|e| Error::from_reason(format!("Failed to read database: {}", e)))?;

            data = serde_json::from_str(&contents)
                .map_err(|e| Error::from_reason(format!("Failed to parse database: {}", e)))?;
        }

        if wal.is_some() {
            let _ = recover_from_wal(&wal_path, &mut data);
        } else {
            let legacy_wal = format!("{}.wal", path);
            let wal_p = PathBuf::from(&legacy_wal);
            if wal_p.exists() {
                let _ = Self::recover_legacy_wal(&legacy_wal, &mut data);
            }
        }

        let memory_manager = MemoryManager::new(&path, MemoryConfig::default());
        let write_locks = StripedLockManager::new();

        Ok(NativeDB {
            path,
            wal_path,
            data: Arc::new(PLRwLock::new(data)),
            process_lock,
            wal,
            indexes: Arc::new(PLRwLock::new(HashMap::new())),
            schemas: Arc::new(PLRwLock::new(HashMap::new())),
            transaction_state: Arc::new(Mutex::new(None)),
            memory_manager: Arc::new(Mutex::new(memory_manager)),
            write_locks: Arc::new(write_locks),
            options,
        })
    }

    #[napi(js_name = "newWithOptions")]
    pub fn new_with_options_js(
        path: String,
        lock_mode: String,
        durability: String,
        wal_batch_size: Option<u32>,
        wal_flush_ms: Option<u32>,
        lock_timeout_ms: Option<u32>,
    ) -> Result<Self> {
        let options = DBOptions {
            lock_mode: LockMode::from_str(&lock_mode),
            durability: DurabilityMode::from_str(&durability),
            wal_batch_size: wal_batch_size.unwrap_or(1000) as usize,
            wal_flush_ms: wal_flush_ms.unwrap_or(10) as u64,
            lock_timeout_ms: lock_timeout_ms.unwrap_or(5000) as u64,
        };

        Self::new_with_options_internal(path, options)
    }

    #[napi]
    pub fn get_system_info(&self) -> SystemInfo {
        SystemInfo {
            available_cores: THREAD_CONFIG.available_cores as u32,
            parallel_enabled: THREAD_CONFIG.use_parallel,
            recommended_batch_size: if THREAD_CONFIG.use_parallel {
                1000
            } else {
                100
            },
        }
    }

    #[napi]
    pub fn sync(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()
                .map_err(|e| Error::from_reason(format!("Sync failed: {}", e)))?;
        }
        Ok(())
    }

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

    #[napi]
    pub fn close(&mut self) -> Result<()> {
        self.process_lock.take();
        if let Some(wal) = self.wal.take() {
            let _ = wal.sync();
        }
        Ok(())
    }

    #[napi]
    pub fn load(&self) -> Result<()> {
        let p = PathBuf::from(&self.path);

        match fs::read_to_string(&p) {
            Ok(contents) => {
                let new_data: Value = serde_json::from_str(&contents)
                    .map_err(|e| Error::from_reason(format!("Failed to parse database: {}", e)))?;

                let mut data = self.data.write();
                *data = new_data;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(e) => {
                return Err(Error::from_reason(format!(
                    "Failed to read database: {}",
                    e
                )));
            }
        }

        Ok(())
    }
    #[napi]
    pub fn save(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()
                .map_err(|e| Error::from_reason(format!("Failed to flush WAL: {}", e)))?;
        }

        let data_guard = self.data.read();
        let json_str = serde_json::to_string_pretty(&*data_guard)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let tmp_path = format!("{}.tmp", self.path);
        let mut file = File::create(&tmp_path)?;
        file.write_all(json_str.as_bytes())?;
        file.sync_all()?;
        fs::rename(tmp_path, &self.path)?;

        if self.wal.is_some() {
            File::create(&self.wal_path)?;
        }

        let mut indexes = self.indexes.write();
        for idx in indexes.values_mut() {
            idx.save()
                .map_err(|e| Error::from_reason(format!("Failed to save index: {:?}", e)))?;
        }

        Ok(())
    }

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

            wal.append(op)
                .map_err(|e| Error::from_reason(format!("WAL append failed: {}", e)))?;
        }
        Ok(())
    }

    fn recover_legacy_wal(wal_path: &str, data: &mut Value) -> Result<()> {
        let file = File::open(wal_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            if let Ok(l) = line {
                if l.trim().is_empty() {
                    continue;
                }
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



    #[allow(dead_code)]
    fn get_value_from_root<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
        if path.is_empty() {
            return Some(root);
        }
        let mut current = root;
        for part in path.split('.') {
            match current {
                Value::Object(map) => {
                    current = map.get(part)?;
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        current = arr.get(idx)?;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current)
    }

    fn normalize_path(path: &str) -> String {
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path.replace(".", "/"))
        }
    }

    fn set_value_at_path(root: &mut Value, path_str: &str, value: Value) -> Result<()> {
        if path_str.is_empty() {
            *root = value;
            return Ok(());
        }

        let mut parts = path_str.split('.');
        let mut part = match parts.next() {
            Some(p) => p,
            None => return Ok(()),
        };

        let mut current = root;

        for next_part in parts {
            let is_next_array_idx = next_part.parse::<usize>().is_ok();

            if current.is_null() {
                *current = Value::Object(serde_json::Map::new());
            }

            if let Value::Object(map) = current {
                if !map.contains_key(part) {
                    map.insert(
                        part.to_string(),
                        if is_next_array_idx {
                            json!([])
                        } else {
                            json!({})
                        },
                    );
                }
                current = map.get_mut(part).unwrap();
            } else if let Value::Array(arr) = current {
                if let Ok(idx) = part.parse::<usize>() {
                    while arr.len() <= idx {
                        arr.push(Value::Null);
                    }
                    if arr[idx].is_null() {
                        arr[idx] = if is_next_array_idx {
                            json!([])
                        } else {
                            json!({})
                        };
                    }
                    current = &mut arr[idx];
                } else {
                    return Err(Error::from_reason(
                        "Cannot index array with string".to_string(),
                    ));
                }
            } else {
                return Err(Error::from_reason(format!(
                    "Path segment '{}' blocked by primitive",
                    part
                )));
            }

            part = next_part;
        }

        if let Value::Object(map) = current {
            map.insert(part.to_string(), value);
        } else if let Value::Array(arr) = current {
            if let Ok(idx) = part.parse::<usize>() {
                while arr.len() <= idx {
                    arr.push(Value::Null);
                }
                arr[idx] = value;
            } else {
                return Err(Error::from_reason(
                    "Cannot set non-numeric key on array".to_string(),
                ));
            }
        } else {
            if current.is_null() {
                let is_array = part.parse::<usize>().is_ok();
                if is_array {
                    let idx = part.parse::<usize>().unwrap();
                    let mut arr = vec![Value::Null; idx + 1];
                    arr[idx] = value;
                    *current = Value::Array(arr);
                } else {
                    let mut map = serde_json::Map::new();
                    map.insert(part.to_string(), value);
                    *current = Value::Object(map);
                }
            } else {
                return Err(Error::from_reason(format!(
                    "Parent of '{}' is not an object/array",
                    part
                )));
            }
        }
        Ok(())
    }

    fn delete_value_at_path(root: &mut Value, path_str: &str) -> Result<()> {
        if path_str.is_empty() {
            *root = json!({});
            return Ok(());
        }
        let parts: Vec<&str> = path_str.split('.').collect();
        if parts.is_empty() {
            return Ok(());
        }

        let parent_path = parts[..parts.len() - 1].join(".");
        let target_key = parts.last().unwrap();

        let ptr = if parent_path.is_empty() {
            "".to_string()
        } else {
            format!("/{}", parent_path.replace(".", "/"))
        };

        let parent = if ptr.is_empty() {
            Some(root)
        } else {
            root.pointer_mut(&ptr)
        };

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
        let ptr = Self::normalize_path(path_str);

        if let Some(target) = root.pointer_mut(&ptr) {
            if let Value::Array(arr) = target {
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

    #[napi]
    pub fn batch_set_parallel(&self, operations: Vec<(String, Value)>) -> Result<ParallelResult> {
        let count = operations.len();
        let use_parallel = THREAD_CONFIG.should_parallelize(count);

        let has_invalid = if use_parallel {
            operations.par_iter().any(|(path, _)| path.is_empty())
        } else {
            operations.iter().any(|(path, _)| path.is_empty())
        };

        if has_invalid {
            return Ok(ParallelResult {
                success: false,
                count: 0,
                error: Some("Invalid path in batch".to_string()),
            });
        }

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

    #[napi]
    pub fn parallel_query(&self, path: String, filters: Vec<QueryFilter>) -> Result<Value> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };

        match collection {
            Some(Value::Object(map)) => {
                let items: Vec<&Value> = map.values().collect();
                let prepared: Vec<PreparedFilter> = filters
                    .iter()
                    .map(PreparedFilter::from_query_filter)
                    .collect();
                let filtered = self.filter_items_parallel(&items, &prepared);
                Ok(Value::Array(filtered))
            }
            Some(Value::Array(arr)) => {
                let items: Vec<&Value> = arr.iter().collect();
                let prepared: Vec<PreparedFilter> = filters
                    .iter()
                    .map(PreparedFilter::from_query_filter)
                    .collect();
                let filtered = self.filter_items_parallel(&items, &prepared);
                Ok(Value::Array(filtered))
            }
            _ => Ok(Value::Array(vec![])),
        }
    }

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

    fn matches_filters(&self, item: &Value, filters: &[PreparedFilter]) -> bool {
        for filter in filters {
            if !self.matches_filter(item, filter) {
                return false;
            }
        }
        true
    }

    fn matches_filter(&self, item: &Value, filter: &PreparedFilter) -> bool {
        let mut current = item;

        for part in &filter.path {
            match current {
                Value::Object(map) => {
                    if let Some(v) = map.get(part) {
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

    #[napi]
    pub fn parallel_aggregate(
        &self,
        path: String,
        operation: String,
        field: Option<String>,
    ) -> Result<Value> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

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
                    items
                        .par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .sum()
                } else {
                    items
                        .iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .sum()
                };
                Ok(json!(sum))
            }
            "avg" => {
                let field_name = field.unwrap_or_default();
                let values: Vec<f64> = if THREAD_CONFIG.should_parallelize(count) {
                    items
                        .par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .collect()
                } else {
                    items
                        .iter()
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
                    items
                        .par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(|| f64::INFINITY, |a, b| a.min(b))
                        .into()
                } else {
                    items
                        .iter()
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
                    items
                        .par_iter()
                        .filter_map(|item| self.get_numeric_field(item, &field_name))
                        .reduce(|| f64::NEG_INFINITY, |a, b| a.max(b))
                        .into()
                } else {
                    items
                        .iter()
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

        let get_items = |path: &str| -> Option<Vec<&Value>> {
            let ptr = Self::normalize_path(path);
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

        let left_items = get_items(&left_path).ok_or_else(|| {
            Error::from_reason(format!("Left collection not found: {}", left_path))
        })?;
        let right_items = get_items(&right_path).ok_or_else(|| {
            Error::from_reason(format!("Right collection not found: {}", right_path))
        })?;

        let left_segments = parse_path(&left_field);
        let right_segments = parse_path(&right_field);

        use std::collections::HashMap;
        let mut hash_table: HashMap<String, Vec<&Value>> = HashMap::new();

        for item in &right_items {
            if let Some(val) = self.get_value_at_path_segments(item, &right_segments) {
                let key = match val {
                    Value::String(s) => s.clone(),
                    _ => val.to_string(),
                };
                hash_table.entry(key).or_default().push(item);
            }
        }

        let results: Vec<Value> = if THREAD_CONFIG.should_parallelize(left_items.len()) {
            left_items
                .par_iter()
                .map(|left_item| self.join_item(left_item, &left_segments, &as_field, &hash_table))
                .collect()
        } else {
            left_items
                .iter()
                .map(|left_item| self.join_item(left_item, &left_segments, &as_field, &hash_table))
                .collect()
        };

        Ok(Value::Array(results))
    }

    fn join_item(
        &self,
        left_item: &Value,
        left_segments: &[PathSegment],
        as_field: &str,
        hash_table: &HashMap<String, Vec<&Value>>,
    ) -> Value {
        let matches_curr =
            if let Some(val) = self.get_value_at_path_segments(left_item, left_segments) {
                let matches = match val {
                    Value::String(s) => hash_table.get(s),
                    _ => {
                        let key = val.to_string();
                        hash_table.get(&key)
                    }
                };

                if let Some(m_list) = matches {
                    m_list.iter().map(|m| (*m).clone()).collect()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

        let mut joined = left_item.clone();
        if let Value::Object(ref mut map) = joined {
            map.insert(as_field.to_string(), Value::Array(matches_curr));
        }
        joined
    }

    fn get_value_at_path_segments<'a>(
        &self,
        item: &'a Value,
        segments: &[PathSegment],
    ) -> Option<&'a Value> {
        let mut current = item;

        for segment in segments {
            match current {
                Value::Object(map) => {
                    if let Some(v) = map.get(segment.raw) {
                        current = v;
                    } else {
                        return None;
                    }
                }
                Value::Array(arr) => {
                    if let Some(idx) = segment.index {
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

    #[allow(dead_code)]
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
        let ptr = Self::normalize_path(&path);
        Ok(data.pointer(&ptr).cloned().unwrap_or(Value::Null))
    }

    #[napi]
    pub fn get_many(&self, paths: Vec<String>) -> Result<Vec<Value>> {
        let data = self.data.read();
        let results = paths
            .iter()
            .map(|path| {
                if path.is_empty() {
                    data.clone()
                } else {
                    let ptr = Self::normalize_path(path);
                    data.pointer(&ptr).cloned().unwrap_or(Value::Null)
                }
            })
            .collect();
        Ok(results)
    }

    #[napi]
    pub fn set(&self, path: String, value: Value) -> Result<()> {
        self.record_undo(&path);

        self.append_wal(WalOpType::Set, &path, Some(value.clone()))?;

        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        Self::set_value_at_path(&mut data, &path, value)?;
        Ok(())
    }

    #[napi]
    pub fn has(&self, path: String) -> Result<bool> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);
        Ok(data.pointer(&ptr).is_some())
    }

    #[napi]
    pub fn delete(&self, path: String) -> Result<()> {
        self.record_undo(&path);

        self.append_wal(WalOpType::Delete, &path, None)?;

        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        Self::delete_value_at_path(&mut data, &path)?;
        Ok(())
    }

    #[napi]
    pub fn push(&self, path: String, value: Value) -> Result<()> {
        self.record_undo(&path);

        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        Self::push_value_at_path(&mut data, &path, value)?;
        Ok(())
    }

    #[napi]
    pub fn offload(&self, path: String) -> Result<String> {
        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        let ptr = Self::normalize_path(&path);

        let val_opt = data
            .pointer_mut(&ptr)
            .map(|v| std::mem::replace(v, Value::Null));

        if let Some(val) = val_opt {
            if val.is_null() {
                return Ok("".to_string());
            }

            let id = format!(
                "{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let cold_path = format!("{}.cold.{}", self.path, id);

            let json =
                serde_json::to_string(&val).map_err(|e| Error::from_reason(e.to_string()))?;
            std::fs::write(&cold_path, json)?;

            let marker = json!({
                "__cold__": true,
                "id": id
            });

            if let Some(target) = data.pointer_mut(&ptr) {
                *target = marker;
            }

            Ok(id)
        } else {
            Ok("".to_string())
        }
    }

    #[napi]
    pub fn restore(&self, path: String) -> Result<bool> {
        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        let ptr = Self::normalize_path(&path);

        let marker_opt = data.pointer(&ptr).cloned();

        if let Some(marker) = marker_opt {
            if let Some(obj) = marker.as_object() {
                if obj.contains_key("__cold__") {
                    if let Some(id_val) = obj.get("id") {
                        if let Some(id) = id_val.as_str() {
                            let cold_path = format!("{}.cold.{}", self.path, id);

                            if std::path::Path::new(&cold_path).exists() {
                                let content = std::fs::read_to_string(&cold_path)?;
                                let val: Value = serde_json::from_str(&content)
                                    .map_err(|e| Error::from_reason(e.to_string()))?;

                                if let Some(target) = data.pointer_mut(&ptr) {
                                    *target = val;
                                }

                                let _ = std::fs::remove_file(cold_path);
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    #[napi]
    pub fn execute_query(
        &self,
        path: String,
        filters_json: String,
        sort_json: Option<String>,
        limit: Option<u32>,
        skip: Option<u32>,
        select_fields: Option<Vec<String>>,
    ) -> Result<Value> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok(Value::Array(vec![])),
        };

        let compiled_filters: Vec<CompiledFilter> =
            if filters_json.is_empty() || filters_json == "[]" {
                vec![]
            } else {
                let raw_filters: Vec<QueryFilter> = serde_json::from_str(&filters_json)
                    .map_err(|e| Error::from_reason(format!("Invalid filters JSON: {}", e)))?;
                raw_filters
                    .iter()
                    .filter_map(|qf| CompiledFilter::compile(&qf.field, &qf.op, &qf.value))
                    .collect()
            };

        let sort_specs = match &sort_json {
            Some(s) if !s.is_empty() && s != "{}" => parse_sort_specs(s),
            _ => vec![],
        };

        let select_paths: Option<Vec<Vec<String>>> = select_fields.map(|fields| {
            fields
                .iter()
                .map(|f| f.split('.').map(|s| s.to_string()).collect())
                .collect()
        });

        let use_parallel = THREAD_CONFIG.use_parallel;

        Ok(execute_query(
            collection,
            &compiled_filters,
            &sort_specs,
            skip.map(|s| s as usize),
            limit.map(|l| l as usize),
            &select_paths,
            use_parallel,
        ))
    }

    #[napi]
    pub fn execute_query_fast(
        &self,
        path: String,
        filters_json: String,
        sort_json: Option<String>,
        limit: Option<u32>,
        skip: Option<u32>,
        select_fields: Option<Vec<String>>,
    ) -> Result<String> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok("[]".to_string()),
        };

        let compiled_filters: Vec<CompiledFilter> =
            if filters_json.is_empty() || filters_json == "[]" {
                vec![]
            } else {
                let raw_filters: Vec<QueryFilter> = serde_json::from_str(&filters_json)
                    .map_err(|e| Error::from_reason(format!("Invalid filters JSON: {}", e)))?;
                raw_filters
                    .iter()
                    .filter_map(|qf| CompiledFilter::compile(&qf.field, &qf.op, &qf.value))
                    .collect()
            };

        let sort_specs = match &sort_json {
            Some(s) if !s.is_empty() && s != "{}" => parse_sort_specs(s),
            _ => vec![],
        };

        let select_paths: Option<Vec<Vec<String>>> = select_fields.map(|fields| {
            fields
                .iter()
                .map(|f| f.split('.').map(|s| s.to_string()).collect())
                .collect()
        });

        let use_parallel = THREAD_CONFIG.use_parallel;

        let result = execute_query(
            collection,
            &compiled_filters,
            &sort_specs,
            skip.map(|s| s as usize),
            limit.map(|l| l as usize),
            &select_paths,
            use_parallel,
        );

        serde_json::to_string(&result)
            .map_err(|e| Error::from_reason(format!("Serialization error: {}", e)))
    }

    #[napi]
    pub fn execute_aggregate_fast(
        &self,
        path: String,
        filters_json: String,
        operation: String,
        field: Option<String>,
    ) -> Result<String> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok("null".to_string()),
        };

        let compiled_filters: Vec<CompiledFilter> =
            if filters_json.is_empty() || filters_json == "[]" {
                vec![]
            } else {
                let raw_filters: Vec<QueryFilter> = serde_json::from_str(&filters_json)
                    .map_err(|e| Error::from_reason(format!("Invalid filters JSON: {}", e)))?;
                raw_filters
                    .iter()
                    .filter_map(|qf| CompiledFilter::compile(&qf.field, &qf.op, &qf.value))
                    .collect()
            };

        let field_segments: Option<Vec<String>> =
            field.map(|f| f.split('.').map(|s| s.to_string()).collect());

        let use_parallel = THREAD_CONFIG.use_parallel;

        let result = execute_aggregate(
            collection,
            &compiled_filters,
            &operation,
            &field_segments,
            use_parallel,
        );

        serde_json::to_string(&result)
            .map_err(|e| Error::from_reason(format!("Serialization error: {}", e)))
    }

    #[napi]
    pub fn execute_aggregate(
        &self,
        path: String,
        filters_json: String,
        operation: String,
        field: Option<String>,
    ) -> Result<Value> {
        let data = self.data.read();
        let ptr = Self::normalize_path(&path);

        let collection = if ptr == "/" || ptr.is_empty() {
            Some(&*data)
        } else {
            data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok(Value::Null),
        };

        let compiled_filters: Vec<CompiledFilter> =
            if filters_json.is_empty() || filters_json == "[]" {
                vec![]
            } else {
                let raw_filters: Vec<QueryFilter> = serde_json::from_str(&filters_json)
                    .map_err(|e| Error::from_reason(format!("Invalid filters JSON: {}", e)))?;
                raw_filters
                    .iter()
                    .filter_map(|qf| CompiledFilter::compile(&qf.field, &qf.op, &qf.value))
                    .collect()
            };

        let field_segments: Option<Vec<String>> =
            field.map(|f| f.split('.').map(|s| s.to_string()).collect());

        let use_parallel = THREAD_CONFIG.use_parallel;

        Ok(execute_aggregate(
            collection,
            &compiled_filters,
            &operation,
            &field_segments,
            use_parallel,
        ))
    }

    #[napi]
    pub fn pull_items(&self, path: String, items: Vec<Value>) -> Result<u32> {
        self.record_undo(&path);
        self.append_wal(
            WalOpType::Delete,
            &path,
            Some(json!({"__pull__": items.clone()})),
        )?;

        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        let ptr = Self::normalize_path(&path);

        if let Some(target) = data.pointer_mut(&ptr) {
            if let Value::Array(arr) = target {
                let remove_set: std::collections::HashSet<String> = items
                    .iter()
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
                    .collect();

                let before = arr.len();
                arr.retain(|v| !remove_set.contains(&serde_json::to_string(v).unwrap_or_default()));
                Ok((before - arr.len()) as u32)
            } else {
                Err(Error::from_reason("Target is not an array".to_string()))
            }
        } else {
            Err(Error::from_reason("Path does not exist".to_string()))
        }
    }

    #[napi]
    pub fn push_batch(&self, path: String, items: Vec<Value>) -> Result<u32> {
        self.record_undo(&path);

        let _stripe = self.write_locks.lock_for_write(&path);

        let mut data = self.data.write();
        let ptr = Self::normalize_path(&path);

        if let Some(target) = data.pointer_mut(&ptr) {
            if let Value::Array(arr) = target {
                let existing: std::collections::HashSet<String> = arr
                    .iter()
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
                    .collect();

                let mut added = 0u32;
                for item in items {
                    let key = serde_json::to_string(&item).unwrap_or_default();
                    if !existing.contains(&key) {
                        arr.push(item);
                        added += 1;
                    }
                }
                Ok(added)
            } else {
                Err(Error::from_reason("Target is not an array".to_string()))
            }
        } else {
            Err(Error::from_reason("Path does not exist".to_string()))
        }
    }

    #[napi]
    pub fn configure_memory(
        &self,
        max_memory: String,
        cold_storage_dir: Option<String>,
        eviction_threshold_pct: Option<u32>,
        eviction_target_pct: Option<u32>,
    ) -> Result<()> {
        let max_bytes = parse_memory_limit(&max_memory);
        let config = MemoryConfig {
            max_memory_bytes: max_bytes,
            cold_storage_dir: cold_storage_dir.unwrap_or_default(),
            check_interval_ms: 5000,
            eviction_threshold_pct: eviction_threshold_pct.unwrap_or(80) as u8,
            eviction_target_pct: eviction_target_pct.unwrap_or(60) as u8,
        };

        let mut mm = self.memory_manager.lock();
        *mm = MemoryManager::new(&self.path, config);
        Ok(())
    }

    #[napi]
    pub fn check_memory_pressure(&self) -> Result<Vec<String>> {
        let mut mm = self.memory_manager.lock();
        if !mm.is_enabled() {
            return Ok(vec![]);
        }

        let mut data = self.data.write();
        let keys_to_evict = mm.check_pressure(&data);

        let mut evicted = Vec::new();
        for key in keys_to_evict {
            if mm.offload_key(&mut data, &key).is_ok() {
                evicted.push(key);
            }
        }

        Ok(evicted)
    }

    #[napi]
    pub fn memory_stats(&self) -> Result<Value> {
        let mm = self.memory_manager.lock();
        let data = self.data.read();
        drop(mm);

        let mut mm = self.memory_manager.lock();
        mm.update_size_estimates(&data);
        let stats = mm.stats();

        Ok(json!({
            "totalEstimatedBytes": stats.total_estimated_bytes,
            "maxMemoryBytes": stats.max_memory_bytes,
            "coldKeysCount": stats.cold_keys_count,
            "hotKeysCount": stats.hot_keys_count,
            "utilizationPct": stats.utilization_pct,
        }))
    }

    #[napi]
    pub fn register_index(&self, name: String, field: String) -> Result<()> {
        let mut indexes = self.indexes.write();
        if !indexes.contains_key(&name) {
            let idx = BTreeIndex::load_or_create(name.clone(), field.clone(), &self.path).map_err(
                |e| Error::from_reason(format!("Failed to load index {}: {:?}", name, e)),
            )?;
            indexes.insert(name, idx);
        }
        Ok(())
    }

    #[napi]
    pub fn update_index(
        &self,
        name: String,
        key: Value,
        path: String,
        is_delete: bool,
    ) -> Result<()> {
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
                return Ok(paths.iter().cloned().collect());
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

    #[napi]
    pub fn find_index_range(
        &self,
        name: String,
        start: Option<Value>,
        end: Option<Value>,
    ) -> Result<Vec<String>> {
        let indexes = self.indexes.read();
        if let Some(idx) = indexes.get(&name) {
            return Ok(idx.range(start.as_ref(), end.as_ref()));
        }
        Ok(vec![])
    }

    #[napi]
    pub fn register_schema(&self, path: String, schema_json: String) -> Result<()> {
        let mut schema: Schema = serde_json::from_str(&schema_json)
            .map_err(|e| Error::from_reason(format!("Invalid schema JSON: {}", e)))?;

        schema
            .compile()
            .map_err(|e| Error::from_reason(format!("Invalid regex in schema: {}", e)))?;

        let mut schemas = self.schemas.write();
        schemas.insert(path, schema);
        Ok(())
    }

    #[napi]
    pub fn validate_path(&self, path: String, value: Value) -> Result<()> {
        let schemas = self.schemas.read();
        let mut parts: Vec<&str> = path.split('.').collect();
        while !parts.is_empty() {
            let current_path = parts.join(".");
            if let Some(schema) = schemas.get(&current_path) {
                validate(&value, schema).map_err(|e| {
                    Error::from_reason(format!("Validation failed at {}: {}", current_path, e))
                })?;
                break;
            }
            parts.pop();
        }
        Ok(())
    }

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
                Err(Error::from_reason(format!(
                    "Savepoint '{}' not found",
                    name
                )))
            }
        } else {
            Err(Error::from_reason("No active transaction".to_string()))
        }
    }

    fn apply_undo_log(
        &self,
        data: &mut Value,
        undo_log: Vec<(String, Option<Value>)>,
    ) -> Result<()> {
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
            let old_value = data
                .pointer(&format!("/{}", path.replace(".", "/")))
                .cloned();
            state.undo_log.push((path.to_string(), old_value));
        }
    }
}
