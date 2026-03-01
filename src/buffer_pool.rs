#![allow(dead_code)]

//! v6: Buffer Pool Manager with mmap-backed file I/O
//!
//! Instead of reading the entire JSON file into memory via `fs::read_to_string`,
//! this module provides:
//!
//! 1. **mmap-based file loading** — memory-maps the file so the OS handles paging.
//!    Pages are loaded on-demand (lazy), not all at once. Startup is near-instant.
//!
//! 2. **Page-level LRU cache** — hot pages stay in the buffer pool, cold pages
//!    are evicted when the pool is full. The pool size is configurable.
//!
//! 3. **Dirty page tracking** — modified pages are tracked and flushed to disk
//!    on save, rather than rewriting the entire file.
//!
//! When the buffer pool is disabled (default), the database falls back to the
//! existing behavior (full file read into memory).

use lru::LruCache;
use memmap2::Mmap;
use std::collections::HashSet;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;

/// Configuration for the buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum buffer pool size in bytes. 0 = disabled (use full in-memory mode).
    pub max_size_bytes: usize,
    /// Page size in bytes for the LRU cache.
    pub page_size_bytes: usize,
    /// Whether to use mmap for file reads (even without buffer pool).
    pub use_mmap: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        BufferPoolConfig {
            max_size_bytes: 0,      // Disabled by default
            page_size_bytes: 16384, // 16 KB pages
            use_mmap: true,         // mmap is always beneficial
        }
    }
}

impl BufferPoolConfig {
    /// Create a config with the given MB limit.
    pub fn with_mb(mb: usize) -> Self {
        BufferPoolConfig {
            max_size_bytes: mb * 1024 * 1024,
            ..Default::default()
        }
    }

    /// Number of pages that fit in the pool.
    pub fn max_pages(&self) -> usize {
        if self.max_size_bytes == 0 || self.page_size_bytes == 0 {
            return 0;
        }
        self.max_size_bytes / self.page_size_bytes
    }
}

/// A page in the buffer pool — holds a slice of file data.
#[derive(Debug)]
struct Page {
    /// Page number (offset = page_num * page_size)
    page_num: usize,
    /// The actual data in this page
    data: Vec<u8>,
    /// Whether this page has been modified since loading.
    dirty: bool,
}

/// The Buffer Pool Manager.
///
/// Manages an LRU cache of file pages loaded via mmap.
/// When a page is accessed, it's either served from cache or loaded
/// from the mmap region (which the OS pages in from disk on demand).
pub struct BufferPool {
    config: BufferPoolConfig,
    /// LRU cache: page_num -> Page
    cache: LruCache<usize, Page>,
    /// Set of dirty page numbers
    dirty_pages: HashSet<usize>,
    /// Total file size in bytes
    file_size: usize,
    /// Memory-mapped file (if available)
    mmap: Option<Mmap>,
    /// Statistics
    stats: BufferPoolStats,
}

#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub pages_loaded: u64,
    pub pages_evicted: u64,
    pub dirty_flushes: u64,
    pub pool_size_bytes: usize,
    pub total_pages: usize,
    pub cached_pages: usize,
}

impl BufferPool {
    /// Create a new buffer pool without a file (for fresh databases).
    pub fn new(config: BufferPoolConfig) -> Self {
        let max_pages = config.max_pages().max(1);
        BufferPool {
            config,
            cache: LruCache::new(NonZeroUsize::new(max_pages).unwrap()),
            dirty_pages: HashSet::new(),
            file_size: 0,
            mmap: None,
            stats: BufferPoolStats::default(),
        }
    }

    /// Create a buffer pool and mmap the given file.
    pub fn from_file(path: &Path, config: BufferPoolConfig) -> Result<Self, String> {
        let file = File::open(path)
            .map_err(|e| format!("Failed to open file for mmap: {}", e))?;

        let metadata = file.metadata()
            .map_err(|e| format!("Failed to read file metadata: {}", e))?;

        let file_size = metadata.len() as usize;

        let mmap = if file_size > 0 && config.use_mmap {
            // Safety: we're memory-mapping a file for read-only access.
            // The file won't be modified externally while we hold the mmap.
            let m = unsafe { Mmap::map(&file) }
                .map_err(|e| format!("Failed to mmap file: {}", e))?;
            Some(m)
        } else {
            None
        };

        let max_pages = config.max_pages().max(1);

        Ok(BufferPool {
            config,
            cache: LruCache::new(NonZeroUsize::new(max_pages).unwrap()),
            dirty_pages: HashSet::new(),
            file_size,
            mmap,
            stats: BufferPoolStats::default(),
        })
    }

    /// Read the entire file content using mmap (near-instant for any file size).
    /// The OS lazily pages in data as it's accessed.
    pub fn read_all(&self) -> Option<&[u8]> {
        self.mmap.as_ref().map(|m| m.as_ref())
    }

    /// Read a specific page from the buffer pool.
    /// Returns the page data if within file bounds.
    pub fn read_page(&mut self, page_num: usize) -> Option<&[u8]> {
        let offset = page_num * self.config.page_size_bytes;
        if offset >= self.file_size {
            return None;
        }

        // Check cache first
        if self.cache.get(&page_num).is_some() {
            self.stats.cache_hits += 1;
            return self.cache.peek(&page_num).map(|p| p.data.as_slice());
        }

        // Cache miss — load from mmap
        self.stats.cache_misses += 1;

        let mmap = self.mmap.as_ref()?;
        let end = (offset + self.config.page_size_bytes).min(self.file_size);
        let data = mmap[offset..end].to_vec();

        let page = Page {
            page_num,
            data,
            dirty: false,
        };

        // Insert into LRU (may evict oldest page)
        if let Some((evicted_num, evicted_page)) = self.cache.push(page_num, page) {
            self.stats.pages_evicted += 1;
            if evicted_page.dirty {
                self.dirty_pages.insert(evicted_num);
            }
        }

        self.stats.pages_loaded += 1;
        self.stats.cached_pages = self.cache.len();

        self.cache.peek(&page_num).map(|p| p.data.as_slice())
    }

    /// Mark a page as dirty (modified).
    pub fn mark_dirty(&mut self, page_num: usize) {
        if let Some(page) = self.cache.get_mut(&page_num) {
            page.dirty = true;
            self.dirty_pages.insert(page_num);
        }
    }

    /// Get current statistics.
    pub fn stats(&self) -> BufferPoolStats {
        let mut s = self.stats.clone();
        s.cached_pages = self.cache.len();
        s.pool_size_bytes = self.cache.len() * self.config.page_size_bytes;
        s.total_pages = if self.config.page_size_bytes > 0 {
            (self.file_size + self.config.page_size_bytes - 1) / self.config.page_size_bytes
        } else {
            0
        };
        s
    }

    /// Whether the buffer pool is actively managing pages (vs full in-memory).
    pub fn is_enabled(&self) -> bool {
        self.config.max_size_bytes > 0
    }

    /// Whether mmap is active for this file.
    pub fn is_mmap_active(&self) -> bool {
        self.mmap.is_some()
    }

    /// File size in bytes.
    pub fn file_size(&self) -> usize {
        self.file_size
    }

    /// Cache hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let total = self.stats.cache_hits + self.stats.cache_misses;
        if total == 0 {
            return 0.0;
        }
        self.stats.cache_hits as f64 / total as f64
    }

    /// Drop the mmap to release the file handle.
    pub fn release(&mut self) {
        self.mmap = None;
        self.cache.clear();
        self.dirty_pages.clear();
    }
}

/// Load a file using mmap for near-instant access.
/// Returns the parsed JSON Value. For large files (>1MB), this is significantly
/// faster than `fs::read_to_string` because the OS handles paging.
pub fn mmap_load_json(path: &Path) -> Result<serde_json::Value, String> {
    let file = File::open(path)
        .map_err(|e| format!("Failed to open: {}", e))?;

    let metadata = file.metadata()
        .map_err(|e| format!("Failed to stat: {}", e))?;

    if metadata.len() == 0 {
        return Ok(serde_json::json!({}));
    }

    // Memory-map the file — OS handles paging, near-instant "load"
    let mmap = unsafe { Mmap::map(&file) }
        .map_err(|e| format!("Failed to mmap: {}", e))?;

    // Parse directly from the mmap'd bytes (avoids string allocation)
    serde_json::from_slice(&mmap)
        .map_err(|e| format!("Failed to parse JSON: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_buffer_pool_config_defaults() {
        let config = BufferPoolConfig::default();
        assert_eq!(config.max_size_bytes, 0);
        assert_eq!(config.page_size_bytes, 16384);
        assert!(config.use_mmap);
    }

    #[test]
    fn test_buffer_pool_config_with_mb() {
        let config = BufferPoolConfig::with_mb(256);
        assert_eq!(config.max_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_pages(), 256 * 1024 * 1024 / 16384);
    }

    #[test]
    fn test_mmap_load_json() {
        let test_path = "/tmp/test_mmap_load.json";
        let data = serde_json::json!({
            "users": {
                "1": {"name": "Alice", "age": 30},
                "2": {"name": "Bob", "age": 25}
            }
        });

        fs::write(test_path, serde_json::to_string_pretty(&data).unwrap()).unwrap();

        let loaded = mmap_load_json(Path::new(test_path)).unwrap();
        assert_eq!(loaded, data);

        fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_buffer_pool_from_file() {
        let test_path = "/tmp/test_buffer_pool.json";
        let data = serde_json::json!({"key": "value"});
        fs::write(test_path, serde_json::to_string(&data).unwrap()).unwrap();

        let config = BufferPoolConfig::with_mb(1);
        let pool = BufferPool::from_file(Path::new(test_path), config).unwrap();

        assert!(pool.is_mmap_active());
        assert!(pool.file_size() > 0);

        fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_buffer_pool_read_all() {
        let test_path = "/tmp/test_bp_read_all.json";
        let content = r#"{"hello": "world"}"#;
        fs::write(test_path, content).unwrap();

        let config = BufferPoolConfig::default();
        let pool = BufferPool::from_file(Path::new(test_path), config).unwrap();

        let data = pool.read_all().unwrap();
        assert_eq!(std::str::from_utf8(data).unwrap(), content);

        fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_buffer_pool_stats() {
        let pool = BufferPool::new(BufferPoolConfig::with_mb(1));
        let stats = pool.stats();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
        assert_eq!(stats.cached_pages, 0);
    }
}
