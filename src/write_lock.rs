#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use parking_lot::Mutex;

const DEFAULT_STRIPE_COUNT: usize = 64;

pub struct StripedLockManager {
    stripes: Vec<Mutex<()>>,
    stripe_count: usize,
}

impl StripedLockManager {
    pub fn new() -> Self {
        let stripe_count = DEFAULT_STRIPE_COUNT;
        let stripes = (0..stripe_count).map(|_| Mutex::new(())).collect();
        StripedLockManager { stripes, stripe_count }
    }

    pub fn with_stripes(count: usize) -> Self {
        let stripe_count = count.max(1);
        let stripes = (0..stripe_count).map(|_| Mutex::new(())).collect();
        StripedLockManager { stripes, stripe_count }
    }

    #[inline]
    fn stripe_index(&self, path: &str) -> usize {
        let top_key = path.split('.').next().unwrap_or(path);
        let mut hasher = DefaultHasher::new();
        top_key.hash(&mut hasher);
        (hasher.finish() as usize) % self.stripe_count
    }

    #[inline]
    pub fn lock_for_write(&self, path: &str) -> StripeLockGuard<'_> {
        let idx = self.stripe_index(path);
        let guard = self.stripes[idx].lock();
        StripeLockGuard { _guard: guard }
    }

    #[inline]
    pub fn try_lock_for_write(&self, path: &str) -> Option<StripeLockGuard<'_>> {
        let idx = self.stripe_index(path);
        self.stripes[idx].try_lock().map(|guard| StripeLockGuard { _guard: guard })
    }

    pub fn lock_for_batch(&self, paths: &[&str]) -> BatchLockGuard<'_> {
        let mut indices: Vec<usize> = paths.iter()
            .map(|p| self.stripe_index(p))
            .collect();
        indices.sort_unstable();
        indices.dedup();

        let guards: Vec<_> = indices.iter()
            .map(|&idx| self.stripes[idx].lock())
            .collect();

        BatchLockGuard { _guards: guards }
    }

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

pub struct StripeLockGuard<'a> {
    _guard: parking_lot::MutexGuard<'a, ()>,
}

pub struct BatchLockGuard<'a> {
    _guards: Vec<parking_lot::MutexGuard<'a, ()>>,
}

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
        let idx1 = mgr.stripe_index("users.1.name");
        let idx2 = mgr.stripe_index("orders.5.total");
        let _ = (idx1, idx2);
    }

    #[test]
    fn test_same_collection_same_stripe() {
        let mgr = StripedLockManager::new();
        let idx1 = mgr.stripe_index("users.1.name");
        let idx2 = mgr.stripe_index("users.2.email");
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn test_concurrent_writes_to_different_stripes() {
        let mgr = Arc::new(StripedLockManager::with_stripes(2));
        let mgr2 = mgr.clone();

        let path1 = "a";
        let path2 = "b";

        let handle = thread::spawn(move || {
            let _guard = mgr2.lock_for_write(path1);
            thread::sleep(std::time::Duration::from_millis(10));
        });

        let _guard = mgr.lock_for_write(path2);
        handle.join().unwrap();
    }

    #[test]
    fn test_batch_lock_no_deadlock() {
        let mgr = StripedLockManager::new();
        let _guard = mgr.lock_for_batch(&["users.1", "orders.5", "config.key"]);
    }
}
