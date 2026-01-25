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

#[derive(Serialize, Deserialize, Debug)]
struct WalEntry {
    op: String,
    path: String,
    value: Option<Value>,
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
      // file.sync_all()?; // Immediate flush for durability? "Writes are appended immediately" implies flush.
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