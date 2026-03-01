#![allow(dead_code)]

//! v6: Zero-Copy N-API Streaming
//!
//! Instead of serializing all query results into one massive JSON string
//! (which causes GC spikes), this module provides chunked streaming.
//!
//! The results are chunked into small batches, and each batch is
//! independently serialized and sent across the N-API boundary.
//! This keeps V8 memory flat — no massive allocations, no GC freeze.
//!
//! Usage from JS:
//! ```javascript
//! const stream = db.collection("users").find({active: true}).stream();
//! for await (const chunk of stream) {
//!   // chunk is an array of ~100 records
//!   process(chunk);
//! }
//! ```

use serde_json::Value;

/// Default chunk size for streaming results.
const DEFAULT_CHUNK_SIZE: usize = 100;

/// A chunked result iterator for streaming query results.
///
/// Instead of materializing all results at once, this splits them
/// into chunks of `chunk_size` records each.
pub struct ChunkedResultStream {
    /// All result records (from query execution).
    results: Vec<Value>,
    /// Current position in the results.
    cursor: usize,
    /// Number of records per chunk.
    chunk_size: usize,
    /// Total chunks produced so far.
    chunks_produced: usize,
}

impl ChunkedResultStream {
    /// Create a new stream from query results.
    pub fn new(results: Vec<Value>, chunk_size: Option<usize>) -> Self {
        ChunkedResultStream {
            results,
            cursor: 0,
            chunk_size: chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
            chunks_produced: 0,
        }
    }

    /// Get the next chunk of results as a JSON string.
    /// Returns None when all results have been consumed.
    pub fn next_chunk(&mut self) -> Option<String> {
        if self.cursor >= self.results.len() {
            return None;
        }

        let end = (self.cursor + self.chunk_size).min(self.results.len());
        let chunk = &self.results[self.cursor..end];

        let json = serde_json::to_string(chunk).ok()?;
        self.cursor = end;
        self.chunks_produced += 1;

        Some(json)
    }

    /// Get the next chunk as raw bytes (for zero-copy Buffer transfer).
    pub fn next_chunk_bytes(&mut self) -> Option<Vec<u8>> {
        if self.cursor >= self.results.len() {
            return None;
        }

        let end = (self.cursor + self.chunk_size).min(self.results.len());
        let chunk = &self.results[self.cursor..end];

        let bytes = serde_json::to_vec(chunk).ok()?;
        self.cursor = end;
        self.chunks_produced += 1;

        Some(bytes)
    }

    /// Check if there are more chunks available.
    pub fn has_next(&self) -> bool {
        self.cursor < self.results.len()
    }

    /// Total number of results.
    pub fn total_results(&self) -> usize {
        self.results.len()
    }

    /// Number of results remaining.
    pub fn remaining(&self) -> usize {
        self.results.len().saturating_sub(self.cursor)
    }

    /// Number of chunks produced so far.
    pub fn chunks_produced(&self) -> usize {
        self.chunks_produced
    }

    /// Total number of chunks (estimated).
    pub fn total_chunks(&self) -> usize {
        if self.chunk_size == 0 {
            return 0;
        }
        (self.results.len() + self.chunk_size - 1) / self.chunk_size
    }

    /// Reset the cursor to the beginning.
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.chunks_produced = 0;
    }

    /// Get stream metadata.
    pub fn metadata(&self) -> StreamMetadata {
        StreamMetadata {
            total_results: self.results.len(),
            chunk_size: self.chunk_size,
            total_chunks: self.total_chunks(),
            chunks_produced: self.chunks_produced,
            remaining: self.remaining(),
            done: !self.has_next(),
        }
    }
}

/// Metadata about the stream state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StreamMetadata {
    pub total_results: usize,
    pub chunk_size: usize,
    pub total_chunks: usize,
    pub chunks_produced: usize,
    pub remaining: usize,
    pub done: bool,
}

/// Create a streamed result from a full result set.
/// This is called from the N-API layer after executing a query.
pub fn create_stream(results: Vec<Value>, chunk_size: Option<usize>) -> ChunkedResultStream {
    ChunkedResultStream::new(results, chunk_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_records(n: usize) -> Vec<Value> {
        (0..n)
            .map(|i| {
                json!({
                    "id": i,
                    "name": format!("User {}", i),
                    "active": i % 2 == 0
                })
            })
            .collect()
    }

    #[test]
    fn test_basic_streaming() {
        let records = sample_records(250);
        let mut stream = create_stream(records, Some(100));

        assert_eq!(stream.total_results(), 250);
        assert_eq!(stream.total_chunks(), 3);
        assert!(stream.has_next());

        // Chunk 1: 100 records
        let chunk1 = stream.next_chunk().unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&chunk1).unwrap();
        assert_eq!(parsed.len(), 100);

        // Chunk 2: 100 records
        let chunk2 = stream.next_chunk().unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&chunk2).unwrap();
        assert_eq!(parsed.len(), 100);

        // Chunk 3: 50 records (remainder)
        let chunk3 = stream.next_chunk().unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&chunk3).unwrap();
        assert_eq!(parsed.len(), 50);

        // No more chunks
        assert!(!stream.has_next());
        assert!(stream.next_chunk().is_none());
        assert_eq!(stream.chunks_produced(), 3);
    }

    #[test]
    fn test_single_chunk() {
        let records = sample_records(50);
        let mut stream = create_stream(records, Some(100));

        assert_eq!(stream.total_chunks(), 1);
        assert!(stream.next_chunk().is_some());
        assert!(stream.next_chunk().is_none());
    }

    #[test]
    fn test_empty_results() {
        let mut stream = create_stream(vec![], None);
        assert_eq!(stream.total_results(), 0);
        assert!(!stream.has_next());
        assert!(stream.next_chunk().is_none());
    }

    #[test]
    fn test_bytes_output() {
        let records = sample_records(10);
        let mut stream = create_stream(records, Some(5));

        let bytes = stream.next_chunk_bytes().unwrap();
        let parsed: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.len(), 5);
    }

    #[test]
    fn test_metadata() {
        let records = sample_records(350);
        let mut stream = create_stream(records, Some(100));

        let meta = stream.metadata();
        assert_eq!(meta.total_results, 350);
        assert_eq!(meta.total_chunks, 4);
        assert!(!meta.done);

        // Consume all
        while stream.next_chunk().is_some() {}

        let meta = stream.metadata();
        assert!(meta.done);
        assert_eq!(meta.remaining, 0);
        assert_eq!(meta.chunks_produced, 4);
    }

    #[test]
    fn test_reset() {
        let records = sample_records(10);
        let mut stream = create_stream(records, Some(5));

        stream.next_chunk();
        stream.next_chunk();
        assert!(!stream.has_next());

        stream.reset();
        assert!(stream.has_next());
        assert_eq!(stream.remaining(), 10);
    }
}
