//! Native Query Engine for jsondb-high
//! 
//! Executes the entire query pipeline (filter → sort → skip → limit → select)
//! in Rust, avoiding costly JS↔Rust boundary crossings per item.
//! Uses Rayon for parallel filtering and sorting on large datasets.

use serde_json::{Value, json};
use rayon::prelude::*;
use std::cmp::Ordering;
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;

static QUERY_REGEX_CACHE: once_cell::sync::Lazy<Mutex<LruCache<String, regex::Regex>>> =
    once_cell::sync::Lazy::new(|| {
        Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap()))
    });

const PARALLEL_THRESHOLD: usize = 100;

pub struct CompiledFilter {
    pub segments: Vec<String>,
    pub op: FilterOp,
    pub value: Value,
    pub regex: Option<regex::Regex>,
    pub value_set: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    StartsWith,
    EndsWith,
    In,
    NotIn,
    Regex,
    Exists,
    IsNull,
    IsNotNull,
    ContainsAll,
    ContainsAny,
    Between,
}

impl FilterOp {
    pub fn from_str(s: &str) -> Option<Self> {
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

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub segments: Vec<String>,
    pub descending: bool,
}

impl CompiledFilter {
    pub fn compile(field: &str, op: &str, value: &Value) -> Option<Self> {
        let filter_op = FilterOp::from_str(op)?;
        let segments: Vec<String> = field.split('.').map(|s| s.to_string()).collect();
        
        let regex = if filter_op == FilterOp::Regex {
            value.as_str().and_then(|pattern| {
                let mut cache = QUERY_REGEX_CACHE.lock();
                if let Some(re) = cache.get(pattern) {
                    Some(re.clone())
                } else {
                    match regex::Regex::new(pattern) {
                        Ok(re) => {
                            cache.push(pattern.to_string(), re.clone());
                            Some(re)
                        }
                        Err(_) => None,
                    }
                }
            })
        } else {
            None
        };
        
        let value_set = match filter_op {
            FilterOp::In | FilterOp::NotIn | FilterOp::ContainsAll | FilterOp::ContainsAny => {
                value.as_array().map(|arr| arr.clone())
            }
            FilterOp::Between => {
                value.as_array().map(|arr| arr.clone())
            }
            _ => None,
        };
        
        Some(CompiledFilter {
            segments,
            op: filter_op,
            value: value.clone(),
            regex,
            value_set,
        })
    }
    
    #[inline]
    pub fn matches(&self, item: &Value) -> bool {
        let field_val = get_nested_value(item, &self.segments);
        
        match self.op {
            FilterOp::Exists => field_val.is_some(),
            FilterOp::IsNull => matches!(field_val, Some(Value::Null) | None),
            FilterOp::IsNotNull => matches!(field_val, Some(v) if !v.is_null()),
            
            FilterOp::Eq => {
                match field_val {
                    Some(v) => v == &self.value,
                    None => self.value.is_null(),
                }
            }
            FilterOp::Ne => {
                match field_val {
                    Some(v) => v != &self.value,
                    None => !self.value.is_null(),
                }
            }
            FilterOp::Gt => {
                numeric_cmp(field_val, &self.value)
                    .map(|o| o == Ordering::Greater)
                    .unwrap_or(false)
            }
            FilterOp::Gte => {
                numeric_cmp(field_val, &self.value)
                    .map(|o| o != Ordering::Less)
                    .unwrap_or(false)
            }
            FilterOp::Lt => {
                numeric_cmp(field_val, &self.value)
                    .map(|o| o == Ordering::Less)
                    .unwrap_or(false)
            }
            FilterOp::Lte => {
                numeric_cmp(field_val, &self.value)
                    .map(|o| o != Ordering::Greater)
                    .unwrap_or(false)
            }
            FilterOp::Between => {
                if let Some(ref arr) = self.value_set {
                    if arr.len() >= 2 {
                        let gte = numeric_cmp(field_val, &arr[0])
                            .map(|o| o != Ordering::Less)
                            .unwrap_or(false);
                        let lte = numeric_cmp(field_val, &arr[1])
                            .map(|o| o != Ordering::Greater)
                            .unwrap_or(false);
                        gte && lte
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            FilterOp::Contains => {
                match (field_val.and_then(|v| v.as_str()), self.value.as_str()) {
                    (Some(haystack), Some(needle)) => haystack.contains(needle),
                    _ => false,
                }
            }
            FilterOp::StartsWith => {
                match (field_val.and_then(|v| v.as_str()), self.value.as_str()) {
                    (Some(s), Some(prefix)) => s.starts_with(prefix),
                    _ => false,
                }
            }
            FilterOp::EndsWith => {
                match (field_val.and_then(|v| v.as_str()), self.value.as_str()) {
                    (Some(s), Some(suffix)) => s.ends_with(suffix),
                    _ => false,
                }
            }
            FilterOp::Regex => {
                match (field_val.and_then(|v| v.as_str()), &self.regex) {
                    (Some(s), Some(re)) => re.is_match(s),
                    _ => false,
                }
            }
            FilterOp::In => {
                match (&self.value_set, field_val) {
                    (Some(set), Some(v)) => set.contains(v),
                    _ => false,
                }
            }
            FilterOp::NotIn => {
                match (&self.value_set, field_val) {
                    (Some(set), Some(v)) => !set.contains(v),
                    (Some(_), None) => true,
                    _ => false,
                }
            }
            FilterOp::ContainsAll => {
                match (field_val.and_then(|v| v.as_array()), &self.value_set) {
                    (Some(arr), Some(required)) => {
                        required.iter().all(|req| arr.contains(req))
                    }
                    _ => false,
                }
            }
            FilterOp::ContainsAny => {
                match (field_val.and_then(|v| v.as_array()), &self.value_set) {
                    (Some(arr), Some(required)) => {
                        required.iter().any(|req| arr.contains(req))
                    }
                    _ => false,
                }
            }
        }
    }
}

#[inline]
fn get_nested_value<'a>(item: &'a Value, segments: &[String]) -> Option<&'a Value> {
    let mut current = item;
    for seg in segments {
        match current {
            Value::Object(map) => {
                current = map.get(seg.as_str())?;
            }
            Value::Array(arr) => {
                let idx: usize = seg.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[inline]
fn numeric_cmp(field_val: Option<&Value>, target: &Value) -> Option<Ordering> {
    let a = field_val?.as_f64()?;
    let b = target.as_f64()?;
    a.partial_cmp(&b)
}

#[inline]
fn compare_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
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
            if let (Some(as_str), Some(bs_str)) = (av.as_str(), bv.as_str()) {
                return as_str.cmp(bs_str);
            }
            if let (Some(ab), Some(bb)) = (av.as_bool(), bv.as_bool()) {
                return ab.cmp(&bb);
            }
            Ordering::Equal
        }
    }
}

pub fn execute_query(
    collection: &Value,
    filters: &[CompiledFilter],
    sort_specs: &[SortSpec],
    skip: Option<usize>,
    limit: Option<usize>,
    select_fields: &Option<Vec<Vec<String>>>,
    use_parallel: bool,
) -> Value {
    let items: Vec<&Value> = match collection {
        Value::Object(map) => map.values().collect(),
        Value::Array(arr) => arr.iter().collect(),
        _ => return Value::Array(vec![]),
    };
    
    let count = items.len();
    let should_parallel = use_parallel && count >= PARALLEL_THRESHOLD;
    
    let filtered: Vec<&Value> = if filters.is_empty() {
        items
    } else if should_parallel {
        items.par_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .copied()
            .collect()
    } else {
        items.into_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .collect()
    };
    
    let mut sorted: Vec<&Value>;
    if !sort_specs.is_empty() {
        sorted = filtered;
        let sort_fn = |a: &&Value, b: &&Value| -> Ordering {
            for spec in sort_specs {
                let av = get_nested_value(a, &spec.segments);
                let bv = get_nested_value(b, &spec.segments);
                let cmp = compare_values(av, bv);
                if cmp != Ordering::Equal {
                    return if spec.descending { cmp.reverse() } else { cmp };
                }
            }
            Ordering::Equal
        };
        
        if should_parallel && sorted.len() >= 1000 {
            sorted.par_sort_unstable_by(sort_fn);
        } else {
            sorted.sort_unstable_by(sort_fn);
        }
    } else {
        sorted = filtered;
    }
    
    let skipped: &[&Value] = if let Some(s) = skip {
        if s < sorted.len() {
            &sorted[s..]
        } else {
            &[]
        }
    } else {
        &sorted
    };
    
    let limited: &[&Value] = if let Some(l) = limit {
        if l < skipped.len() {
            &skipped[..l]
        } else {
            skipped
        }
    } else {
        skipped
    };
    
    let result: Vec<Value> = if let Some(ref fields) = select_fields {
        if should_parallel && limited.len() >= PARALLEL_THRESHOLD {
            limited.par_iter()
                .map(|item| project_fields(item, fields))
                .collect()
        } else {
            limited.iter()
                .map(|item| project_fields(item, fields))
                .collect()
        }
    } else {
        if should_parallel && limited.len() >= PARALLEL_THRESHOLD {
            limited.par_iter().map(|v| (*v).clone()).collect()
        } else {
            limited.iter().map(|v| (*v).clone()).collect()
        }
    };
    
    Value::Array(result)
}

pub fn execute_aggregate(
    collection: &Value,
    filters: &[CompiledFilter],
    operation: &str,
    field_segments: &Option<Vec<String>>,
    use_parallel: bool,
) -> Value {
    let items: Vec<&Value> = match collection {
        Value::Object(map) => map.values().collect(),
        Value::Array(arr) => arr.iter().collect(),
        _ => return Value::Null,
    };
    
    let count = items.len();
    let should_parallel = use_parallel && count >= PARALLEL_THRESHOLD;
    
    let filtered: Vec<&Value> = if filters.is_empty() {
        items
    } else if should_parallel {
        items.par_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .copied()
            .collect()
    } else {
        items.into_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .collect()
    };
    
    match operation {
        "count" => json!(filtered.len()),
        "sum" => {
            if let Some(ref segs) = field_segments {
                let sum: f64 = if should_parallel {
                    filtered.par_iter()
                        .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                        .sum()
                } else {
                    filtered.iter()
                        .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                        .sum()
                };
                json!(sum)
            } else {
                Value::Null
            }
        }
        "avg" => {
            if let Some(ref segs) = field_segments {
                let values: Vec<f64> = if should_parallel {
                    filtered.par_iter()
                        .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                        .collect()
                } else {
                    filtered.iter()
                        .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                        .collect()
                };
                if values.is_empty() {
                    json!(0)
                } else {
                    let sum: f64 = values.iter().sum();
                    json!(sum / values.len() as f64)
                }
            } else {
                Value::Null
            }
        }
        "min" => {
            if let Some(ref segs) = field_segments {
                let min = filtered.iter()
                    .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                    .fold(f64::INFINITY, f64::min);
                if min.is_infinite() { Value::Null } else { json!(min) }
            } else {
                Value::Null
            }
        }
        "max" => {
            if let Some(ref segs) = field_segments {
                let max = filtered.iter()
                    .filter_map(|item| get_nested_value(item, segs)?.as_f64())
                    .fold(f64::NEG_INFINITY, f64::max);
                if max.is_infinite() { Value::Null } else { json!(max) }
            } else {
                Value::Null
            }
        }
        "distinct" => {
            if let Some(ref segs) = field_segments {
                let mut seen = std::collections::HashSet::new();
                let mut result = Vec::new();
                for item in &filtered {
                    if let Some(v) = get_nested_value(item, segs) {
                        let key = serde_json::to_string(v).unwrap_or_default();
                        if seen.insert(key) {
                            result.push(v.clone());
                        }
                    }
                }
                Value::Array(result)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

fn project_fields(item: &Value, fields: &[Vec<String>]) -> Value {
    let mut result = serde_json::Map::new();
    for field_path in fields {
        if let Some(v) = get_nested_value(item, field_path) {
            if let Some(key) = field_path.last() {
                result.insert(key.clone(), v.clone());
            }
        }
    }
    Value::Object(result)
}

pub fn parse_sort_specs(sort_json: &str) -> Vec<SortSpec> {
    if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(sort_json) {
        map.iter().map(|(field, dir)| {
            let descending = dir.as_f64().map(|d| d < 0.0).unwrap_or(false);
            SortSpec {
                segments: field.split('.').map(|s| s.to_string()).collect(),
                descending,
            }
        }).collect()
    } else {
        vec![]
    }
}

/// v6: Explain a query execution plan without returning actual data.
/// Returns metadata about how the query would be executed.
pub fn explain_query(
    collection: &Value,
    filters: &[CompiledFilter],
    sort_specs: &[SortSpec],
    skip: Option<usize>,
    limit: Option<usize>,
    select_fields: &Option<Vec<Vec<String>>>,
    use_parallel: bool,
) -> Value {
    let start = std::time::Instant::now();

    let items: Vec<&Value> = match collection {
        Value::Object(map) => map.values().collect(),
        Value::Array(arr) => arr.iter().collect(),
        _ => return json!({
            "collectionSize": 0,
            "scanType": "EMPTY",
            "error": "Collection is not an object or array"
        }),
    };

    let collection_size = items.len();
    let should_parallel = use_parallel && collection_size >= PARALLEL_THRESHOLD;

    // Execute filters to get match count (we measure real execution time)
    let filtered: Vec<&Value> = if filters.is_empty() {
        items
    } else if should_parallel {
        items.par_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .copied()
            .collect()
    } else {
        items.into_iter()
            .filter(|item| filters.iter().all(|f| f.matches(item)))
            .collect()
    };

    let matched_count = filtered.len();

    // Determine how many results would actually be returned
    let after_skip = if let Some(s) = skip {
        if s < matched_count { matched_count - s } else { 0 }
    } else {
        matched_count
    };

    let result_count = if let Some(l) = limit {
        after_skip.min(l)
    } else {
        after_skip
    };

    let elapsed = start.elapsed();

    // Build filter descriptions
    let filter_descriptions: Vec<Value> = filters.iter().map(|f| {
        json!({
            "field": f.segments.join("."),
            "op": format!("{:?}", f.op),
        })
    }).collect();

    // Build sort descriptions
    let sort_descriptions: Vec<Value> = sort_specs.iter().map(|s| {
        json!({
            "field": s.segments.join("."),
            "direction": if s.descending { "DESC" } else { "ASC" },
        })
    }).collect();

    let scan_type = if filters.is_empty() {
        "FULL_SCAN"
    } else {
        "FILTER_SCAN"
    };

    let projected_fields: Vec<String> = select_fields
        .as_ref()
        .map(|fields| fields.iter().map(|f| f.join(".")).collect())
        .unwrap_or_default();

    json!({
        "scanType": scan_type,
        "collectionSize": collection_size,
        "filtersApplied": filter_descriptions,
        "matchedCount": matched_count,
        "sortApplied": sort_descriptions,
        "skip": skip.unwrap_or(0),
        "limit": limit,
        "projectedFields": projected_fields,
        "resultCount": result_count,
        "parallelExecution": should_parallel,
        "executionTimeMs": elapsed.as_secs_f64() * 1000.0,
    })
}
