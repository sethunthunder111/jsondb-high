use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
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

#[derive(Debug, Serialize, Deserialize)]
pub struct BTreeIndex {
    name: String,
    field: String,
    // Key (as string representation) -> List of Doc Paths
    map: BTreeMap<String, Vec<String>>,
    // Doc Path -> Key (for O(1) updates/removals)
    #[serde(default)] // For backward compatibility if someone had old index file
    reverse_map: BTreeMap<String, String>,
    #[serde(skip)]
    path: String,
    #[serde(skip)]
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
            let mut index: BTreeIndex = serde_json::from_reader(reader)?;
            index.path = path;
            index.dirty = false;
            // Ensure reverse_map is populated if loaded from old version (though we just added it)
            if index.reverse_map.is_empty() && !index.map.is_empty() {
                for (k, v) in &index.map {
                    for doc in v {
                        index.reverse_map.insert(doc.clone(), k.clone());
                    }
                }
            }
            Ok(index)
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
        serde_json::to_writer(writer, &self)?;
        fs::rename(path_tmp, &self.path)?;
        self.dirty = false;
        Ok(())
    }

    // Insert or Update
    pub fn insert(&mut self, key: &Value, doc_path: String) {
        let new_key = self.key_to_string(key);
        
        // Check if doc exists and has different key
        if let Some(old_key) = self.reverse_map.get(&doc_path) {
            if *old_key == new_key {
                return; // No change
            }
            // Remove from old key
            if let Some(list) = self.map.get_mut(old_key) {
                if let Some(pos) = list.iter().position(|x| x == &doc_path) {
                    list.remove(pos);
                }
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
        self.map.entry(new_key).or_default().push(doc_path);
        self.dirty = true;
    }

    // Remove by path (key is optional/ignored, simpler API)
    pub fn remove(&mut self, _key: &Value, doc_path: &str) {
        if let Some(old_key) = self.reverse_map.remove(doc_path) {
            if let Some(list) = self.map.get_mut(&old_key) {
                 if let Some(pos) = list.iter().position(|x| x == doc_path) {
                    list.remove(pos);
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
    
    fn key_to_string(&self, key: &Value) -> String {
        match key {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            _ => key.to_string(),
        }
    }

    pub fn find(&self, key: &Value) -> Option<&Vec<String>> {
        let k = self.key_to_string(key);
        self.map.get(&k)
    }

    pub fn range(&self, start: Option<&Value>, end: Option<&Value>) -> Vec<String> {
        let start_k = start.map(|k| self.key_to_string(k));
        let end_k = end.map(|k| self.key_to_string(k));
        
        let mut results = Vec::new();
        
        use std::ops::Bound;
        let range = self.map.range::<str, _>((
            start_k.as_ref().map(|k| Bound::Included(k.as_str())).unwrap_or(Bound::Unbounded),
            end_k.as_ref().map(|k| Bound::Included(k.as_str())).unwrap_or(Bound::Unbounded)
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
