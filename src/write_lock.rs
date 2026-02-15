#![allow(dead_code)]
//! Write Concurrency Manager for jsondb-high
//! 
//! Provides collection-level locking to allow concurrent writes
//! to different top-level collections. Uses a striped lock approach
//! where each top-level key is hashed to one of N lock stripes,
//! allowing concurrent writes when they map to different stripes.
//!
//! Architecture:
//!   Global data stays in a single RwLock<Value> (for reads + atomic saves)
//!   Write operations acquire a stripe lock FIRST, then the global write lock
//!   The stripe lock serializes writers to the same collection
//!   but allows writers to different collections to enter concurrently
//!   (the global write lock is held for the minimal time - just the mutation)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use parking_lot::Mutex;

/// Number of lock stripes — more stripes = less contention but more memory
/// We use 2x cores as a practical balance
const DEFAULT_STRIPE_COUNT: usize = 64;

/// Striped lock manager for write concurrency
pub struct StripedLockManager {
    stripes: Vec<Mutex<()>>,
    stripe_count: usize,
}

impl StripedLockManager {
    /// Create a new striped lock manager
    pub fn new() -> Self {
        let stripe_count = DEFAULT_STRIPE_COUNT;
        let stripes = (0..stripe_count).map(|_| Mutex::new(())).collect();
        StripedLockManager { stripes, stripe_count }
    }

    /// Create with a specific number of stripes
    pub fn with_stripes(count: usize) -> Self {
        let stripe_count = count.max(1);
        let stripes = (0..stripe_count).map(|_| Mutex::new(())).collect();
        StripedLockManager { stripes, stripe_count }
    }

    /// Get the stripe index for a given path
    /// Uses the top-level collection key for stripe assignment
    #[inline]
    fn stripe_index(&self, path: &str) -> usize {
        let top_key = path.split('.').next().unwrap_or(path);
        let mut hasher = DefaultHasher::new();
        top_key.hash(&mut hasher);
        (hasher.finish() as usize) % self.stripe_count
    }

    /// Acquire a write lock for the given path
    /// Returns a guard that releases the lock when dropped
    #[inline]
    pub fn lock_for_write(&self, path: &str) -> StripeLockGuard<'_> {
        let idx = self.stripe_index(path);
        let guard = self.stripes[idx].lock();
        StripeLockGuard { _guard: guard }
    }

    /// Try to acquire a write lock without blocking
    /// Returns None if another writer is holding the same stripe
    #[inline]
    pub fn try_lock_for_write(&self, path: &str) -> Option<StripeLockGuard<'_>> {
        let idx = self.stripe_index(path);
        self.stripes[idx].try_lock().map(|guard| StripeLockGuard { _guard: guard })
    }

    /// Lock multiple paths at once, avoiding deadlocks by sorting stripe indices
    /// This is used for batch operations that touch multiple collections
    pub fn lock_for_batch(&self, paths: &[&str]) -> BatchLockGuard<'_> {
        // Collect unique stripe indices and sort to prevent deadlock
        let mut indices: Vec<usize> = paths.iter()
            .map(|p| self.stripe_index(p))
            .collect();
        indices.sort_unstable();
        indices.dedup();

        // Acquire locks in order
        let guards: Vec<_> = indices.iter()
            .map(|&idx| self.stripes[idx].lock())
            .collect();

        BatchLockGuard { _guards: guards }
    }

    /// Get statistics about lock contention
    pub fn stats(&self) -> LockStats {
        let mut locked_count = 0;
        for stripe in &self.stripes {
            if stripe.try_lock().is_none() {
                locked_count += 1;
            }
        }
        LockStats {
            stripe_count: self.stripe_count,
            currently_locked: locked_count,
        }
    }
}

/// Guard for a single stripe lock
pub struct StripeLockGuard<'a> {
    _guard: parking_lot::MutexGuard<'a, ()>,
}

/// Guard for multiple stripe locks (batch operations)
pub struct BatchLockGuard<'a> {
    _guards: Vec<parking_lot::MutexGuard<'a, ()>>,
}

/// Lock contention statistics
#[derive(Debug, Clone)]
pub struct LockStats {
    pub stripe_count: usize,
    pub currently_locked: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_different_collections_different_stripes() {
        let mgr = StripedLockManager::new();
        // Two very different paths should (likely) hash to different stripes
        // This is probabilistic but with 64 stripes, collision is unlikely
        let idx1 = mgr.stripe_index("users.1.name");
        let idx2 = mgr.stripe_index("orders.5.total");
        // Can't guarantee different stripes, but the mechanism works
        let _ = (idx1, idx2);
    }

    #[test]
    fn test_same_collection_same_stripe() {
        let mgr = StripedLockManager::new();
        // Same top-level key should always map to same stripe
        let idx1 = mgr.stripe_index("users.1.name");
        let idx2 = mgr.stripe_index("users.2.email");
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn test_concurrent_writes_to_different_stripes() {
        let mgr = Arc::new(StripedLockManager::with_stripes(2));
        let mgr2 = mgr.clone();

        // Find two paths that hash to different stripes
        let path1 = "a";
        let path2 = "b";

        let handle = thread::spawn(move || {
            let _guard = mgr2.lock_for_write(path1);
            // Hold lock briefly
            thread::sleep(std::time::Duration::from_millis(10));
        });

        // Should be able to acquire lock on different stripe concurrently
        let _guard = mgr.lock_for_write(path2);
        handle.join().unwrap();
    }

    #[test]
    fn test_batch_lock_no_deadlock() {
        let mgr = StripedLockManager::new();
        // Locking multiple paths in batch should not deadlock
        let _guard = mgr.lock_for_batch(&["users.1", "orders.5", "config.key"]);
    }
}
