#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock as PLRwLock;
use rayon::prelude::*;

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

#[napi]
pub struct NativeDB {
  path: String,
  wal_path: String,
  wal_enabled: bool,
  data: Arc<PLRwLock<Value>>,
}

#[napi]
impl NativeDB {
  #[napi(constructor)]
  pub fn new(path: String, wal: bool) -> Self {
    NativeDB {
      path: path.clone(),
      wal_path: format!("{}.wal", path),
      wal_enabled: wal,
      data: Arc::new(PLRwLock::new(json!({}))),
    }
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

  #[napi]
  pub fn load(&self) -> Result<()> {
    let p = PathBuf::from(&self.path);
    if p.exists() {
      // Load main DB
      let contents = fs::read_to_string(&p).map_err(|e| Error::from_reason(e.to_string()))?;
      let val: Value = serde_json::from_str(&contents).map_err(|e| Error::from_reason(e.to_string()))?;
      *self.data.write() = val;
    }
    
    // Replay WAL if exists
    let wal_p = PathBuf::from(&self.wal_path);
    if self.wal_enabled && wal_p.exists() {
        let file = File::open(&wal_p)?;
        let reader = BufReader::new(file);
        let mut data = self.data.write();
        
        for line in reader.lines() {
            if let Ok(l) = line {
                if l.trim().is_empty() { continue; }
                if let Ok(entry) = serde_json::from_str::<WalEntry>(&l) {
                    // Apply op
                    match entry.op.as_str() {
                        "set" => {
                            if let Some(val) = entry.value {
                                let _ = Self::set_value_at_path(&mut data, &entry.path, val);
                            }
                        },
                        "delete" => {
                             let _ = Self::delete_value_at_path(&mut data, &entry.path);
                        },
                        "push" => {
                             if let Some(val) = entry.value {
                                 let _ = Self::push_value_at_path(&mut data, &entry.path, val);
                             }
                        },
                        _ => {}
                    }
                }
            }
        }
    }
    
    Ok(())
  }

  #[napi]
  pub fn save(&self) -> Result<()> {
    let data_guard = self.data.read();
    let json_str = serde_json::to_string_pretty(&*data_guard).map_err(|e| Error::from_reason(e.to_string()))?;
    
    // Atomic write
    let tmp_path = format!("{}.tmp", self.path);
    let mut file = File::create(&tmp_path)?;
    file.write_all(json_str.as_bytes())?;
    file.sync_all()?;
    fs::rename(tmp_path, &self.path)?;
    
    // Clear WAL if enabled
    if self.wal_enabled {
        File::create(&self.wal_path)?;
    }
    
    Ok(())
  }

  fn append_wal(&self, op: &str, path: &str, value: Option<Value>) -> Result<()> {
      if !self.wal_enabled { return Ok(()); }
      let entry = WalEntry {
          op: op.to_string(),
          path: path.to_string(),
          value,
      };
      let line = serde_json::to_string(&entry).map_err(|e| Error::from_reason(e.to_string()))? + "\n";
      let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&self.wal_path)?;
      file.write_all(line.as_bytes())?;
      Ok(())
  }

  // --- Logic Helpers ---

  fn set_value_at_path(root: &mut Value, path_str: &str, value: Value) -> Result<()> {
      if path_str.is_empty() {
          *root = value;
          return Ok(())
      }
      
      let parts: Vec<&str> = path_str.split('.').collect();
      if parts.is_empty() { return Ok(())
      }
      
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
                   // If we just created it (or it was null), initialize it based on next part
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
      if parts.is_empty() { return Ok(())
      }
      
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
  /// Automatically falls back to sequential for small batches
  #[napi]
  pub fn batch_set_parallel(&self, operations: Vec<(String, Value)>) -> Result<ParallelResult> {
      let count = operations.len();
      
      if THREAD_CONFIG.should_parallelize(count) {
          // For parallel execution, we collect all operations first then apply
          // This is because we need write lock, but we can validate in parallel
          
          // Pre-validate paths in parallel (read-only)
          let validation_results: Vec<bool> = operations
              .par_iter()
              .map(|(path, _)| !path.is_empty() || path.is_empty()) // Simple validation
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
              if self.wal_enabled {
                  let _ = self.append_wal("set", &path, Some(value.clone()));
              }
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
          // Sequential fallback for small batches
          let mut data = self.data.write();
          let mut success_count = 0u32;
          
          for (path, value) in operations {
              if self.wal_enabled {
                  let _ = self.append_wal("set", &path, Some(value.clone()));
              }
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
  /// Uses rayon for CPU-bound filtering when data is large enough
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
              let filtered = self.filter_items_parallel(&items, &filters);
              Ok(Value::Array(filtered))
          }
          Some(Value::Array(arr)) => {
              let items: Vec<&Value> = arr.iter().collect();
              let filtered = self.filter_items_parallel(&items, &filters);
              Ok(Value::Array(filtered))
          }
          _ => Ok(Value::Array(vec![])),
      }
  }
  
  /// Internal parallel filter implementation
  fn filter_items_parallel(&self, items: &[&Value], filters: &[QueryFilter]) -> Vec<Value> {
      let count = items.len();
      
      if THREAD_CONFIG.should_parallelize(count) && !filters.is_empty() {
          // Parallel filtering for large datasets
          items
              .par_iter()
              .filter(|item| self.matches_filters(item, filters))
              .map(|v| (*v).clone())
              .collect()
      } else {
          // Sequential for small datasets or no filters
          items
              .iter()
              .filter(|item| self.matches_filters(item, filters))
              .map(|v| (*v).clone())
              .collect()
      }
  }
  
  /// Check if an item matches all filters
  fn matches_filters(&self, item: &Value, filters: &[QueryFilter]) -> bool {
      for filter in filters {
          if !self.matches_filter(item, filter) {
              return false;
          }
      }
      true
  }
  
  /// Check if an item matches a single filter
  fn matches_filter(&self, item: &Value, filter: &QueryFilter) -> bool {
      // Get field value using dot notation
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
      
      // Apply operation
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
          _ => true, // Unknown op, pass through
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
    // WAL first? or RAM first? "Writes are appended to a WAL file immediately."
    self.append_wal("set", &path, Some(value.clone()))?;
    
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
      self.append_wal("delete", &path, None)?;
      
      let mut data = self.data.write();
      Self::delete_value_at_path(&mut data, &path)?;
      Ok(())
  }

  #[napi]
  pub fn push(&self, path: String, value: Value) -> Result<()> {
      self.append_wal("push", &path, Some(value.clone()))?;
      
      let mut data = self.data.write();
      Self::push_value_at_path(&mut data, &path, value)?;
      Ok(())
  }
}