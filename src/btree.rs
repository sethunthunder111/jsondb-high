use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use serde::{Serialize, Deserialize};
use serde_json::Value;

// Simple Persistent B-Tree Index (In-Memory BTreeMap backed by disk)
// This solves the startup time issue by loading pre-computed indexes.
// It matches the "in-memory speed" philosophy.

#[derive(Debug)]
pub enum IndexError {
    Io(io::Error),
    Serialization(serde_json::Error),
}

impl From<io::Error> for IndexError {
    fn from(e: io::Error) -> Self { IndexError::Io(e) }
}

impl From<serde_json::Error> for IndexError {
    fn from(e: serde_json::Error) -> Self { IndexError::Serialization(e) }
}

type Result<T> = std::result::Result<T, IndexError>;

// ============================================
// Index Key Types
// ============================================

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OrderedNumber(f64);

impl Eq for OrderedNumber {}

impl Ord for OrderedNumber {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IndexKey {
    Null,
    Bool(bool),
    Number(OrderedNumber),
    String(String),
}

// ============================================
// Serialization DTO
// ============================================

#[derive(Serialize, Deserialize)]
struct BTreeIndexFile {
    name: String,
    field: String,
    // Store map as list of tuples to handle non-string keys in JSON
    map: Vec<(IndexKey, Vec<String>)>,
    reverse_map: Vec<(String, IndexKey)>,
}

// ============================================
// BTree Index
// ============================================

#[derive(Debug)]
pub struct BTreeIndex {
    name: String,
    field: String,
    // Key (as string representation) -> List of Doc Paths
    map: BTreeMap<String, BTreeSet<String>>,
    // Doc Path -> Key (for O(1) updates/removals)
    reverse_map: BTreeMap<String, IndexKey>,
    path: String,
    dirty: bool,
}

impl BTreeIndex {
    pub fn new(name: String, field: String, base_path: &str) -> Self {
        let path = format!("{}.{}.idx", base_path, name);
        BTreeIndex {
            name,
            field,
            map: BTreeMap::new(),
            reverse_map: BTreeMap::new(),
            path,
            dirty: false,
        }
    }

    pub fn load_or_create(name: String, field: String, base_path: &str) -> Result<Self> {
        let path = format!("{}.{}.idx", base_path, name);
        let p = Path::new(&path);
        
        if p.exists() {
            let file = File::open(p)?;
            let reader = BufReader::new(file);

            // Try to load new format
            match serde_json::from_reader::<_, BTreeIndexFile>(reader) {
                Ok(dto) => {
                    let mut map = BTreeMap::new();
                    for (k, v) in dto.map {
                        map.insert(k, v);
                    }

                    let mut reverse_map = BTreeMap::new();
                    for (k, v) in dto.reverse_map {
                        reverse_map.insert(k, v);
                    }

                    Ok(BTreeIndex {
                        name: dto.name,
                        field: dto.field,
                        map,
                        reverse_map,
                        path,
                        dirty: false,
                    })
                },
                Err(_) => {
                    // Failed to load (likely old format or corrupt)
                    // Delete the file and return new empty index
                    // This triggers a rebuild in the upper layer (TypeScript)
                    // Note: reader consumed file, so it should be closed.
                    let _ = fs::remove_file(p);
                    Ok(Self::new(name, field, base_path))
                }
            }
        } else {
            Ok(Self::new(name, field, base_path))
        }
    }

    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        
        let path_tmp = format!("{}.tmp", self.path);
        let file = File::create(&path_tmp)?;
        let writer = BufWriter::new(file);

        // Convert to DTO
        let dto = BTreeIndexFile {
            name: self.name.clone(),
            field: self.field.clone(),
            map: self.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            reverse_map: self.reverse_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        };

        serde_json::to_writer(writer, &dto)?;
        fs::rename(path_tmp, &self.path)?;
        self.dirty = false;
        Ok(())
    }

    // Insert or Update
    pub fn insert(&mut self, key: &Value, doc_path: String) {
        let new_key = self.to_key(key);
        
        // Check if doc exists and has different key
        if let Some(old_key) = self.reverse_map.get(&doc_path) {
            if *old_key == new_key {
                return; // No change
            }
            // Remove from old key
            if let Some(list) = self.map.get_mut(old_key) {
                list.remove(&doc_path);
            }
            // Cleanup empty
            if let Some(list) = self.map.get(old_key) {
                if list.is_empty() {
                    let old_key_clone = old_key.clone(); // Split borrow
                    self.map.remove(&old_key_clone);
                }
            }
        }
        
        self.reverse_map.insert(doc_path.clone(), new_key.clone());
        self.map.entry(new_key).or_default().insert(doc_path);
        self.dirty = true;
    }

    // Remove by path (key is optional/ignored, simpler API)
    pub fn remove(&mut self, _key: &Value, doc_path: &str) {
        if let Some(old_key) = self.reverse_map.remove(doc_path) {
            if let Some(list) = self.map.get_mut(&old_key) {
                 if list.remove(doc_path) {
                    self.dirty = true;
                }
            }
            if let Some(list) = self.map.get(&old_key) {
                if list.is_empty() {
                    self.map.remove(&old_key);
                }
            }
        }
    }
    
    fn to_key(&self, key: &Value) -> IndexKey {
        match key {
            Value::String(s) => IndexKey::String(s.clone()),
            Value::Number(n) => {
                // Try to get f64, default to 0.0 or handle error?
                // serde_json::Number always works for f64 unless it's null/bool which are separate
                // But extremely large integers might lose precision.
                // Given standard JS numbers are f64, this matches environment behavior.
                IndexKey::Number(OrderedNumber(n.as_f64().unwrap_or(0.0)))
            },
            Value::Bool(b) => IndexKey::Bool(*b),
            Value::Null => IndexKey::Null,
            _ => IndexKey::String(key.to_string()), // Fallback for Arrays/Objects
        }
    }

    pub fn find(&self, key: &Value) -> Option<&Vec<String>> {
        let k = self.to_key(key);
        self.map.get(&k)
    }

    pub fn range(&self, start: Option<&Value>, end: Option<&Value>) -> Vec<String> {
        let start_k = start.map(|k| self.to_key(k));
        let end_k = end.map(|k| self.to_key(k));
        
        let mut results = Vec::new();
        
        use std::ops::Bound;
        let range = self.map.range((
            start_k.as_ref().map(|k| Bound::Included(k)).unwrap_or(Bound::Unbounded),
            end_k.as_ref().map(|k| Bound::Included(k)).unwrap_or(Bound::Unbounded)
        ));

        for (_k, v) in range {
            results.extend(v.iter().cloned());
        }
        
        results
    }
    
    pub fn clear(&mut self) {
        self.map.clear();
        self.reverse_map.clear();
        self.dirty = true;
    }
}
