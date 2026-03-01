//! jsondb-high WASM Fallback
//!
//! A self-contained, in-memory JSON database engine compiled to WebAssembly.
//! This provides the same API surface as the native N-API module, allowing
//! jsondb-high to work on any platform without needing a Rust toolchain.
//!
//! Key differences from native:
//! - No file locking (no OS-level I/O)
//! - No mmap / buffer pool (everything is in-memory)
//! - No WAL commit thread (synchronous writes)
//! - No Rayon parallelism (WASM is single-threaded)
//! - File I/O is handled by the JS shim layer

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

// ============================================
// DATA STRUCTURES
// ============================================

#[derive(Serialize, Deserialize, Debug, Clone)]
struct QueryFilter {
    field: String,
    op: String,
    value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FilterOp {
    Eq, Ne, Gt, Gte, Lt, Lte,
    Contains, StartsWith, EndsWith,
    In, NotIn, Regex,
    Exists, IsNull, IsNotNull,
    ContainsAll, ContainsAny, Between,
}

impl FilterOp {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "eq" => Some(FilterOp::Eq),
            "ne" => Some(FilterOp::Ne),
            "gt" => Some(FilterOp::Gt),
            "gte" => Some(FilterOp::Gte),
            "lt" => Some(FilterOp::Lt),
            "lte" => Some(FilterOp::Lte),
            "contains" => Some(FilterOp::Contains),
            "startswith" | "startsWith" => Some(FilterOp::StartsWith),
            "endswith" | "endsWith" => Some(FilterOp::EndsWith),
            "in" => Some(FilterOp::In),
            "notin" | "notIn" => Some(FilterOp::NotIn),
            "regex" => Some(FilterOp::Regex),
            "exists" => Some(FilterOp::Exists),
            "isNull" | "isnull" => Some(FilterOp::IsNull),
            "isNotNull" | "isnotnull" => Some(FilterOp::IsNotNull),
            "containsAll" | "containsall" => Some(FilterOp::ContainsAll),
            "containsAny" | "containsany" => Some(FilterOp::ContainsAny),
            "between" => Some(FilterOp::Between),
            _ => None,
        }
    }
}

// ============================================
// WASM DATABASE
// ============================================

#[wasm_bindgen]
pub struct WasmDB {
    data: Value,
    schemas: HashMap<String, Value>,
}

#[wasm_bindgen]
impl WasmDB {
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmDB {
        WasmDB {
            data: json!({}),
            schemas: HashMap::new(),
        }
    }

    /// Load data from a JSON string (called by JS shim after reading file)
    #[wasm_bindgen(js_name = "loadFromString")]
    pub fn load_from_string(&mut self, json_str: &str) -> Result<(), JsValue> {
        self.data = serde_json::from_str(json_str)
            .map_err(|e| JsValue::from_str(&format!("Parse error: {}", e)))?;
        Ok(())
    }

    /// Serialize all data to JSON string (called by JS shim before writing file)
    #[wasm_bindgen(js_name = "saveToString")]
    pub fn save_to_string(&self) -> Result<String, JsValue> {
        serde_json::to_string_pretty(&self.data)
            .map_err(|e| JsValue::from_str(&format!("Serialize error: {}", e)))
    }

    /// Get a value at a dot-notation path, returns a proper JS value
    pub fn get(&self, path: &str) -> JsValue {
        let value = if path.is_empty() {
            Some(&self.data)
        } else {
            let ptr = format!("/{}", path.replace('.', "/"));
            self.data.pointer(&ptr)
        };

        match value {
            Some(v) => {
                // Use JSON round-trip to get proper plain JS objects (not Maps)
                match serde_json::to_string(v) {
                    Ok(json_str) => {
                        js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL)
                    }
                    Err(_) => JsValue::NULL,
                }
            }
            None => JsValue::NULL,
        }
    }

    /// Set a value at a dot-notation path
    pub fn set(&mut self, path: &str, value: JsValue) -> Result<(), JsValue> {
        // Use JSON round-trip for reliable conversion (avoids Map issues)
        let json_str = js_sys::JSON::stringify(&value)
            .map_err(|_| JsValue::from_str("Failed to stringify value"))?
            .as_string()
            .unwrap_or_default();
        let val: Value = serde_json::from_str(&json_str)
            .map_err(|e| JsValue::from_str(&format!("Value conversion error: {}", e)))?;

        if path.is_empty() {
            self.data = val;
            return Ok(());
        }

        Self::set_at_path(&mut self.data, path, val)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Check if a path exists
    pub fn has(&self, path: &str) -> bool {
        if path.is_empty() {
            return true;
        }
        let ptr = format!("/{}", path.replace('.', "/"));
        self.data.pointer(&ptr).is_some()
    }

    /// Delete a value at a path
    pub fn delete(&mut self, path: &str) -> Result<(), JsValue> {
        if path.is_empty() {
            self.data = json!({});
            return Ok(());
        }

        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() {
            return Ok(());
        }

        let parent_path = parts[..parts.len() - 1].join(".");
        let target_key = parts.last().unwrap();

        let parent = if parent_path.is_empty() {
            Some(&mut self.data)
        } else {
            let ptr = format!("/{}", parent_path.replace('.', "/"));
            self.data.pointer_mut(&ptr)
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

    /// Push a value onto an array at a path
    pub fn push(&mut self, path: &str, value: JsValue) -> Result<(), JsValue> {
        let json_str = js_sys::JSON::stringify(&value)
            .map_err(|_| JsValue::from_str("Failed to stringify value"))?
            .as_string()
            .unwrap_or_default();
        let val: Value = serde_json::from_str(&json_str)
            .map_err(|e| JsValue::from_str(&format!("Value conversion error: {}", e)))?;

        let ptr = format!("/{}", path.replace('.', "/"));
        match self.data.pointer_mut(&ptr) {
            Some(Value::Array(arr)) => {
                if !arr.contains(&val) {
                    arr.push(val);
                }
                Ok(())
            }
            Some(_) => Err(JsValue::from_str("Target is not an array")),
            None => Err(JsValue::from_str("Path does not exist")),
        }
    }

    /// Get multiple values at once
    #[wasm_bindgen(js_name = "getMany")]
    pub fn get_many(&self, paths: Vec<JsValue>) -> Vec<JsValue> {
        paths.iter().map(|p| {
            let path_str = p.as_string().unwrap_or_default();
            self.get(&path_str)
        }).collect()
    }

    /// Execute a query with filters, sort, skip, limit, select — returns JSON string
    #[wasm_bindgen(js_name = "executeQueryFast")]
    pub fn execute_query_fast(
        &self,
        path: String,
        filters_json: String,
        sort_json: Option<String>,
        limit: Option<u32>,
        skip: Option<u32>,
        select_fields: Option<Vec<JsValue>>,
    ) -> Result<String, JsValue> {
        let ptr = if path.is_empty() {
            String::new()
        } else {
            format!("/{}", path.replace('.', "/"))
        };

        let collection = if ptr.is_empty() {
            Some(&self.data)
        } else {
            self.data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok("[]".to_string()),
        };

        let items: Vec<&Value> = match collection {
            Value::Object(map) => map.values().collect(),
            Value::Array(arr) => arr.iter().collect(),
            _ => return Ok("[]".to_string()),
        };

        // Parse and apply filters
        let filters: Vec<QueryFilter> = if filters_json.is_empty() || filters_json == "[]" {
            vec![]
        } else {
            serde_json::from_str(&filters_json)
                .map_err(|e| JsValue::from_str(&format!("Invalid filters: {}", e)))?
        };

        let filtered: Vec<&Value> = if filters.is_empty() {
            items
        } else {
            items.into_iter()
                .filter(|item| filters.iter().all(|f| Self::matches_filter(item, f)))
                .collect()
        };

        // Sort
        let mut sorted = filtered;
        if let Some(ref sort_str) = sort_json {
            if !sort_str.is_empty() && sort_str != "{}" {
                if let Ok(Value::Object(sort_map)) = serde_json::from_str::<Value>(sort_str) {
                    let specs: Vec<(Vec<String>, bool)> = sort_map.iter()
                        .map(|(field, dir)| {
                            let desc = dir.as_f64().map(|d| d < 0.0).unwrap_or(false);
                            let segs: Vec<String> = field.split('.').map(|s| s.to_string()).collect();
                            (segs, desc)
                        })
                        .collect();

                    sorted.sort_by(|a, b| {
                        for (segs, desc) in &specs {
                            let av = Self::get_nested(a, segs);
                            let bv = Self::get_nested(b, segs);
                            let cmp = Self::compare_values(av, bv);
                            if cmp != std::cmp::Ordering::Equal {
                                return if *desc { cmp.reverse() } else { cmp };
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }
        }

        // Skip
        let skipped: &[&Value] = if let Some(s) = skip {
            let s = s as usize;
            if s < sorted.len() { &sorted[s..] } else { &[] }
        } else {
            &sorted
        };

        // Limit
        let limited: &[&Value] = if let Some(l) = limit {
            let l = l as usize;
            if l < skipped.len() { &skipped[..l] } else { skipped }
        } else {
            skipped
        };

        // Select fields
        let select_strs: Option<Vec<String>> = select_fields.map(|fields| {
            fields.iter().filter_map(|f| f.as_string()).collect()
        });

        let result: Vec<Value> = if let Some(ref fields) = select_strs {
            limited.iter().map(|item| {
                let mut obj = serde_json::Map::new();
                for f in fields {
                    let segs: Vec<String> = f.split('.').map(|s| s.to_string()).collect();
                    if let Some(v) = Self::get_nested(item, &segs) {
                        if let Some(key) = segs.last() {
                            obj.insert(key.clone(), v.clone());
                        }
                    }
                }
                Value::Object(obj)
            }).collect()
        } else {
            limited.iter().map(|v| (*v).clone()).collect()
        };

        serde_json::to_string(&Value::Array(result))
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {}", e)))
    }

    /// Explain query execution plan
    #[wasm_bindgen(js_name = "explainQueryFast")]
    pub fn explain_query_fast(
        &self,
        path: String,
        filters_json: String,
        sort_json: Option<String>,
        limit: Option<u32>,
        skip: Option<u32>,
        _select_fields: Option<Vec<JsValue>>,
    ) -> Result<String, JsValue> {
        let ptr = if path.is_empty() {
            String::new()
        } else {
            format!("/{}", path.replace('.', "/"))
        };

        let collection = if ptr.is_empty() {
            Some(&self.data)
        } else {
            self.data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok(serde_json::to_string(&json!({
                "scanType": "EMPTY",
                "collectionSize": 0,
                "engine": "wasm"
            })).unwrap_or_default()),
        };

        let items: Vec<&Value> = match collection {
            Value::Object(map) => map.values().collect(),
            Value::Array(arr) => arr.iter().collect(),
            _ => vec![],
        };

        let collection_size = items.len();

        let filters: Vec<QueryFilter> = if filters_json.is_empty() || filters_json == "[]" {
            vec![]
        } else {
            serde_json::from_str(&filters_json).unwrap_or_default()
        };

        let matched = if filters.is_empty() {
            collection_size
        } else {
            items.iter()
                .filter(|item| filters.iter().all(|f| Self::matches_filter(item, f)))
                .count()
        };

        let plan = json!({
            "scanType": if filters.is_empty() { "FULL_SCAN" } else { "FILTER_SCAN" },
            "collectionSize": collection_size,
            "matchedCount": matched,
            "skip": skip.unwrap_or(0),
            "limit": limit,
            "parallelExecution": false,
            "engine": "wasm",
        });

        serde_json::to_string(&plan)
            .map_err(|e| JsValue::from_str(&format!("Error: {}", e)))
    }

    /// Aggregate query (count, sum, avg, min, max)
    #[wasm_bindgen(js_name = "executeAggregateFast")]
    pub fn execute_aggregate_fast(
        &self,
        path: String,
        filters_json: String,
        operation: String,
        field: Option<String>,
    ) -> Result<String, JsValue> {
        let ptr = if path.is_empty() {
            String::new()
        } else {
            format!("/{}", path.replace('.', "/"))
        };

        let collection = if ptr.is_empty() {
            Some(&self.data)
        } else {
            self.data.pointer(&ptr)
        };

        let collection = match collection {
            Some(c) => c,
            None => return Ok("null".to_string()),
        };

        let items: Vec<&Value> = match collection {
            Value::Object(map) => map.values().collect(),
            Value::Array(arr) => arr.iter().collect(),
            _ => return Ok("null".to_string()),
        };

        let filters: Vec<QueryFilter> = if filters_json.is_empty() || filters_json == "[]" {
            vec![]
        } else {
            serde_json::from_str(&filters_json).unwrap_or_default()
        };

        let filtered: Vec<&Value> = if filters.is_empty() {
            items
        } else {
            items.into_iter()
                .filter(|item| filters.iter().all(|f| Self::matches_filter(item, f)))
                .collect()
        };

        let field_segs: Option<Vec<String>> = field.map(|f| f.split('.').map(|s| s.to_string()).collect());

        let result = match operation.as_str() {
            "count" => json!(filtered.len()),
            "sum" => {
                if let Some(ref segs) = field_segs {
                    let sum: f64 = filtered.iter()
                        .filter_map(|item| Self::get_nested(item, segs)?.as_f64())
                        .sum();
                    json!(sum)
                } else {
                    Value::Null
                }
            }
            "avg" => {
                if let Some(ref segs) = field_segs {
                    let vals: Vec<f64> = filtered.iter()
                        .filter_map(|item| Self::get_nested(item, segs)?.as_f64())
                        .collect();
                    if vals.is_empty() { json!(0) }
                    else { json!(vals.iter().sum::<f64>() / vals.len() as f64) }
                } else {
                    Value::Null
                }
            }
            "min" => {
                if let Some(ref segs) = field_segs {
                    let min = filtered.iter()
                        .filter_map(|item| Self::get_nested(item, segs)?.as_f64())
                        .fold(f64::INFINITY, f64::min);
                    if min.is_infinite() { Value::Null } else { json!(min) }
                } else {
                    Value::Null
                }
            }
            "max" => {
                if let Some(ref segs) = field_segs {
                    let max = filtered.iter()
                        .filter_map(|item| Self::get_nested(item, segs)?.as_f64())
                        .fold(f64::NEG_INFINITY, f64::max);
                    if max.is_infinite() { Value::Null } else { json!(max) }
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        };

        serde_json::to_string(&result)
            .map_err(|e| JsValue::from_str(&format!("Error: {}", e)))
    }

    /// Get system info (WASM mode always reports single-threaded)
    #[wasm_bindgen(js_name = "getSystemInfo")]
    pub fn get_system_info(&self) -> JsValue {
        let info = json!({
            "available_cores": 1,
            "parallel_enabled": false,
            "recommended_batch_size": 100,
            "engine": "wasm"
        });
        let s = serde_json::to_string(&info).unwrap_or_default();
        js_sys::JSON::parse(&s).unwrap_or(JsValue::NULL)
    }

    /// Register a schema (validation stored but simplified in WASM mode)
    #[wasm_bindgen(js_name = "registerSchema")]
    pub fn register_schema(&mut self, path: &str, schema_json: &str) -> Result<(), JsValue> {
        let schema: Value = serde_json::from_str(schema_json)
            .map_err(|e| JsValue::from_str(&format!("Invalid schema: {}", e)))?;
        self.schemas.insert(path.to_string(), schema);
        Ok(())
    }

    /// Sync (no-op in WASM — no WAL)
    pub fn sync(&self) -> Result<(), JsValue> {
        Ok(())
    }

    /// WAL status (always disabled in WASM)
    #[wasm_bindgen(js_name = "walStatus")]
    pub fn wal_status(&self) -> JsValue {
        let status = json!({
            "enabled": false,
            "engine": "wasm"
        });
        let s = serde_json::to_string(&status).unwrap_or_default();
        js_sys::JSON::parse(&s).unwrap_or(JsValue::NULL)
    }

    /// Close (no-op in WASM)
    pub fn close(&self) -> Result<(), JsValue> {
        Ok(())
    }

    /// Save (no-op — JS shim handles file I/O)
    pub fn save(&self) -> Result<(), JsValue> {
        Ok(())
    }

    /// Load (no-op — JS shim handles file I/O)
    pub fn load(&self) -> Result<(), JsValue> {
        Ok(())
    }
}

// ============================================
// PRIVATE HELPERS
// ============================================

impl WasmDB {
    fn set_at_path(root: &mut Value, path: &str, value: Value) -> Result<(), String> {
        if path.is_empty() {
            *root = value;
            return Ok(());
        }

        let mut parts = path.split('.');
        let mut part = match parts.next() {
            Some(p) => p,
            None => return Ok(()),
        };

        let mut current = root;

        for next_part in parts {
            let is_next_array = next_part.parse::<usize>().is_ok();

            if current.is_null() {
                *current = Value::Object(serde_json::Map::new());
            }

            if let Value::Object(map) = current {
                if !map.contains_key(part) {
                    map.insert(
                        part.to_string(),
                        if is_next_array { json!([]) } else { json!({}) },
                    );
                }
                current = map.get_mut(part).unwrap();
            } else if let Value::Array(arr) = current {
                if let Ok(idx) = part.parse::<usize>() {
                    while arr.len() <= idx {
                        arr.push(Value::Null);
                    }
                    if arr[idx].is_null() {
                        arr[idx] = if is_next_array { json!([]) } else { json!({}) };
                    }
                    current = &mut arr[idx];
                } else {
                    return Err(format!("Cannot index array with '{}'", part));
                }
            } else {
                return Err(format!("Path segment '{}' blocked by primitive", part));
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
                return Err("Cannot set non-numeric key on array".to_string());
            }
        } else if current.is_null() {
            let mut map = serde_json::Map::new();
            map.insert(part.to_string(), value);
            *current = Value::Object(map);
        } else {
            return Err(format!("Parent of '{}' is not an object/array", part));
        }
        Ok(())
    }

    fn get_nested<'a>(item: &'a Value, segments: &[String]) -> Option<&'a Value> {
        let mut current = item;
        for seg in segments {
            match current {
                Value::Object(map) => { current = map.get(seg.as_str())?; }
                Value::Array(arr) => {
                    let idx: usize = seg.parse().ok()?;
                    current = arr.get(idx)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(Value::Null), Some(Value::Null)) => Ordering::Equal,
            (Some(Value::Null), _) => Ordering::Less,
            (_, Some(Value::Null)) => Ordering::Greater,
            (Some(av), Some(bv)) => {
                if let (Some(an), Some(bn)) = (av.as_f64(), bv.as_f64()) {
                    return an.partial_cmp(&bn).unwrap_or(Ordering::Equal);
                }
                if let (Some(a_s), Some(b_s)) = (av.as_str(), bv.as_str()) {
                    return a_s.cmp(b_s);
                }
                Ordering::Equal
            }
        }
    }

    fn matches_filter(item: &Value, filter: &QueryFilter) -> bool {
        let segs: Vec<String> = filter.field.split('.').map(|s| s.to_string()).collect();
        let field_val = Self::get_nested(item, &segs);
        let op = match FilterOp::from_str(&filter.op) {
            Some(o) => o,
            None => return false,
        };

        match op {
            FilterOp::Exists => field_val.is_some(),
            FilterOp::IsNull => matches!(field_val, Some(Value::Null) | None),
            FilterOp::IsNotNull => matches!(field_val, Some(v) if !v.is_null()),
            FilterOp::Eq => field_val.map(|v| v == &filter.value).unwrap_or(filter.value.is_null()),
            FilterOp::Ne => field_val.map(|v| v != &filter.value).unwrap_or(!filter.value.is_null()),
            FilterOp::Gt => {
                field_val.and_then(|v| v.as_f64())
                    .zip(filter.value.as_f64())
                    .map(|(a, b)| a > b)
                    .unwrap_or(false)
            }
            FilterOp::Gte => {
                field_val.and_then(|v| v.as_f64())
                    .zip(filter.value.as_f64())
                    .map(|(a, b)| a >= b)
                    .unwrap_or(false)
            }
            FilterOp::Lt => {
                field_val.and_then(|v| v.as_f64())
                    .zip(filter.value.as_f64())
                    .map(|(a, b)| a < b)
                    .unwrap_or(false)
            }
            FilterOp::Lte => {
                field_val.and_then(|v| v.as_f64())
                    .zip(filter.value.as_f64())
                    .map(|(a, b)| a <= b)
                    .unwrap_or(false)
            }
            FilterOp::Between => {
                if let Some(arr) = filter.value.as_array() {
                    if arr.len() >= 2 {
                        let v = field_val.and_then(|v| v.as_f64());
                        let lo = arr[0].as_f64();
                        let hi = arr[1].as_f64();
                        match (v, lo, hi) {
                            (Some(v), Some(l), Some(h)) => v >= l && v <= h,
                            _ => false,
                        }
                    } else { false }
                } else { false }
            }
            FilterOp::Contains => {
                match (field_val.and_then(|v| v.as_str()), filter.value.as_str()) {
                    (Some(h), Some(n)) => h.contains(n),
                    _ => false,
                }
            }
            FilterOp::StartsWith => {
                match (field_val.and_then(|v| v.as_str()), filter.value.as_str()) {
                    (Some(s), Some(p)) => s.starts_with(p),
                    _ => false,
                }
            }
            FilterOp::EndsWith => {
                match (field_val.and_then(|v| v.as_str()), filter.value.as_str()) {
                    (Some(s), Some(sf)) => s.ends_with(sf),
                    _ => false,
                }
            }
            FilterOp::Regex => {
                match (field_val.and_then(|v| v.as_str()), filter.value.as_str()) {
                    (Some(s), Some(pat)) => {
                        regex::Regex::new(pat).map(|re| re.is_match(s)).unwrap_or(false)
                    }
                    _ => false,
                }
            }
            FilterOp::In => {
                match (filter.value.as_array(), field_val) {
                    (Some(set), Some(v)) => set.contains(v),
                    _ => false,
                }
            }
            FilterOp::NotIn => {
                match (filter.value.as_array(), field_val) {
                    (Some(set), Some(v)) => !set.contains(v),
                    (Some(_), None) => true,
                    _ => false,
                }
            }
            FilterOp::ContainsAll => {
                match (field_val.and_then(|v| v.as_array()), filter.value.as_array()) {
                    (Some(arr), Some(req)) => req.iter().all(|r| arr.contains(r)),
                    _ => false,
                }
            }
            FilterOp::ContainsAny => {
                match (field_val.and_then(|v| v.as_array()), filter.value.as_array()) {
                    (Some(arr), Some(req)) => req.iter().any(|r| arr.contains(r)),
                    _ => false,
                }
            }
        }
    }
}
