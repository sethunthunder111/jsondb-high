#![allow(dead_code)]

use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
// use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct MemoryConfig {
    pub max_memory_bytes: usize,
    pub cold_storage_dir: String,
    pub check_interval_ms: u64,
    pub eviction_threshold_pct: u8,
    pub eviction_target_pct: u8,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig {
            max_memory_bytes: 0,
            cold_storage_dir: String::new(),
            check_interval_ms: 5000,
            eviction_threshold_pct: 80,
            eviction_target_pct: 60,
        }
    }
}

pub struct MemoryManager {
    config: MemoryConfig,
    access_tracker: HashMap<String, u64>,
    cold_paths: HashSet<String>,
    size_estimates: HashMap<String, usize>,
    total_estimated_size: usize,
    db_path: String,
}

impl MemoryManager {
    pub fn new(db_path: &str, config: MemoryConfig) -> Self {
        let cold_dir = if config.cold_storage_dir.is_empty() {
            format!("{}.cold", db_path)
        } else {
            config.cold_storage_dir.clone()
        };
        
        if config.max_memory_bytes > 0 {
            let _ = fs::create_dir_all(&cold_dir);
        }
        
        let mut cfg = config;
        cfg.cold_storage_dir = cold_dir;
        
        MemoryManager {
            config: cfg,
            access_tracker: HashMap::new(),
            cold_paths: HashSet::new(),
            size_estimates: HashMap::new(),
            total_estimated_size: 0,
            db_path: db_path.to_string(),
        }
    }
    
    pub fn is_enabled(&self) -> bool {
        self.config.max_memory_bytes > 0
    }
    
    pub fn track_access(&mut self, path: &str) {
        let top_key = path.split('.').next().unwrap_or(path).to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.access_tracker.insert(top_key, now);
    }
    
    pub fn is_cold(&self, path: &str) -> bool {
        let top_key = path.split('.').next().unwrap_or(path);
        self.cold_paths.contains(top_key)
    }
    
    pub fn cold_file_path(&self, top_key: &str) -> String {
        format!("{}/{}.json", self.config.cold_storage_dir, top_key)
    }
    
    pub fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::Null => 8,
            Value::Bool(_) => 8,
            Value::Number(_) => 16,
            Value::String(s) => 24 + s.len(),
            Value::Array(arr) => {
                24 + arr.iter().map(Self::estimate_value_size).sum::<usize>()
            }
            Value::Object(map) => {
                48 + map.iter().map(|(k, v)| {
                    24 + k.len() + Self::estimate_value_size(v)
                }).sum::<usize>()
            }
        }
    }
    
    pub fn update_size_estimates(&mut self, data: &Value) {
        self.size_estimates.clear();
        self.total_estimated_size = 0;
        
        if let Value::Object(map) = data {
            for (key, value) in map {
                if !self.cold_paths.contains(key) {
                    let size = Self::estimate_value_size(value);
                    self.size_estimates.insert(key.clone(), size);
                    self.total_estimated_size += size;
                }
            }
        }
    }
    
    pub fn check_pressure(&mut self, data: &Value) -> Vec<String> {
        if !self.is_enabled() {
            return vec![];
        }
        
        self.update_size_estimates(data);
        
        let threshold = self.config.max_memory_bytes * self.config.eviction_threshold_pct as usize / 100;
        let target = self.config.max_memory_bytes * self.config.eviction_target_pct as usize / 100;
        
        if self.total_estimated_size <= threshold {
            return vec![];
        }
        
        let mut entries: Vec<(String, u64, usize)> = self.size_estimates.iter()
            .map(|(key, size)| {
                let access_time = self.access_tracker.get(key).copied().unwrap_or(0);
                (key.clone(), access_time, *size)
            })
            .collect();
        
        entries.sort_by_key(|(_, ts, _)| *ts);
        
        let mut to_evict = Vec::new();
        let mut projected_size = self.total_estimated_size;
        
        for (key, _, size) in entries {
            if projected_size <= target {
                break;
            }
            to_evict.push(key);
            projected_size -= size;
        }
        
        to_evict
    }
    
    pub fn offload_key(&mut self, data: &mut Value, key: &str) -> Result<(), String> {
        if let Value::Object(map) = data {
            if let Some(value) = map.get(key) {
                if is_cold_marker(value) {
                    return Ok(());
                }
                
                let cold_path = self.cold_file_path(key);
                let json = serde_json::to_string(value)
                    .map_err(|e| format!("Serialize error: {}", e))?;
                    
                fs::write(&cold_path, json)
                    .map_err(|e| format!("Write error: {}", e))?;
                
                let marker = serde_json::json!({
                    "__cold__": true,
                    "key": key,
                    "file": cold_path,
                    "size": self.size_estimates.get(key).copied().unwrap_or(0),
                });
                
                map.insert(key.to_string(), marker);
                self.cold_paths.insert(key.to_string());
                
                if let Some(size) = self.size_estimates.remove(key) {
                    self.total_estimated_size -= size;
                }
                
                Ok(())
            } else {
                Ok(())
            }
        } else {
            Err("Data root is not an object".to_string())
        }
    }
    
    pub fn restore_key(&mut self, data: &mut Value, key: &str) -> Result<bool, String> {
        if !self.cold_paths.contains(key) {
            return Ok(false);
        }
        
        let cold_path = self.cold_file_path(key);
        
        if !std::path::Path::new(&cold_path).exists() {
            self.cold_paths.remove(key);
            return Err(format!("Cold storage file missing: {}", cold_path));
        }
        
        let content = fs::read_to_string(&cold_path)
            .map_err(|e| format!("Read error: {}", e))?;
        let value: Value = serde_json::from_str(&content)
            .map_err(|e| format!("Parse error: {}", e))?;
        
        if let Value::Object(map) = data {
            let size = Self::estimate_value_size(&value);
            map.insert(key.to_string(), value);
            self.cold_paths.remove(key);
            self.size_estimates.insert(key.to_string(), size);
            self.total_estimated_size += size;
            
            self.track_access(key);
            
            let _ = fs::remove_file(&cold_path);
            
            Ok(true)
        } else {
            Err("Data root is not an object".to_string())
        }
    }
    
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            total_estimated_bytes: self.total_estimated_size,
            max_memory_bytes: self.config.max_memory_bytes,
            cold_keys_count: self.cold_paths.len(),
            hot_keys_count: self.size_estimates.len(),
            utilization_pct: if self.config.max_memory_bytes > 0 {
                (self.total_estimated_size as f64 / self.config.max_memory_bytes as f64 * 100.0) as u8
            } else {
                0
            },
        }
    }
    
    pub fn cleanup(&self) {
        if std::path::Path::new(&self.config.cold_storage_dir).exists() {
            let _ = fs::remove_dir_all(&self.config.cold_storage_dir);
        }
    }
}

pub fn is_cold_marker(value: &Value) -> bool {
    if let Value::Object(map) = value {
        map.contains_key("__cold__")
    } else {
        false
    }
}

#[derive(Debug)]
pub struct MemoryStats {
    pub total_estimated_bytes: usize,
    pub max_memory_bytes: usize,
    pub cold_keys_count: usize,
    pub hot_keys_count: usize,
    pub utilization_pct: u8,
}

pub fn parse_memory_limit(s: &str) -> usize {
    let s = s.trim().to_lowercase();
    if let Ok(n) = s.parse::<usize>() {
        return n;
    }
    
    if s.ends_with("gb") || s.ends_with("g") {
        let num_str = s.trim_end_matches(|c: char| c.is_alphabetic());
        if let Ok(n) = num_str.parse::<f64>() {
            return (n * 1024.0 * 1024.0 * 1024.0) as usize;
        }
    }
    if s.ends_with("mb") || s.ends_with("m") {
        let num_str = s.trim_end_matches(|c: char| c.is_alphabetic());
        if let Ok(n) = num_str.parse::<f64>() {
            return (n * 1024.0 * 1024.0) as usize;
        }
    }
    if s.ends_with("kb") || s.ends_with("k") {
        let num_str = s.trim_end_matches(|c: char| c.is_alphabetic());
        if let Ok(n) = num_str.parse::<f64>() {
            return (n * 1024.0) as usize;
        }
    }
    
    0
}
