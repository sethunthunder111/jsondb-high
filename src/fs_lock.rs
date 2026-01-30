//! Process-level file locking for multi-process safety
//! 
//! Uses OS-level advisory locks that don't affect in-memory performance.
//! Lock is only held during file operations, not during get/set.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

#[derive(Debug)]
pub enum LockError {
    AlreadyLocked,
    Io(std::io::Error),
    #[allow(dead_code)]
    StaleLock,
}

impl From<std::io::Error> for LockError {
    fn from(e: std::io::Error) -> Self {
        LockError::Io(e)
    }
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyLocked => write!(f, "Database is already locked by another process"),
            LockError::Io(e) => write!(f, "IO error: {}", e),
            LockError::StaleLock => write!(f, "Stale lock detected"),
        }
    }
}

impl std::error::Error for LockError {}

/// Process-level advisory lock
pub struct ProcessLock {
    #[allow(dead_code)]
    lock_file: File,
    lock_path: String,
}

impl ProcessLock {
    /// Try to acquire exclusive lock on database
    pub fn acquire(db_path: &str) -> Result<Self, LockError> {
        let lock_path = format!("{}.process_lock", db_path);
        
        // Try to create/open lock file
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        
        // Try non-blocking exclusive lock
        if !Self::try_lock_exclusive(&file)? {
            // Check if it's a stale lock
            if Self::is_stale_lock(&lock_path)? {
                // Remove stale lock and retry
                let _ = std::fs::remove_file(&lock_path);
                file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&lock_path)?;
                
                if !Self::try_lock_exclusive(&file)? {
                    return Err(LockError::AlreadyLocked);
                }
            } else {
                return Err(LockError::AlreadyLocked);
            }
        }
        
        // Write our PID to help with stale lock detection
        let pid = std::process::id();
        file.set_len(0)?;
        writeln!(file, "{}", pid)?;
        file.sync_all()?;
        
        Ok(ProcessLock {
            lock_file: file,
            lock_path,
        })
    }
    
    /// Check if database is locked without acquiring
    pub fn is_locked(db_path: &str) -> Result<bool, LockError> {
        let lock_path = format!("{}.process_lock", db_path);
        
        if !Path::new(&lock_path).exists() {
            return Ok(false);
        }
        
        // Check if lock is stale
        if Self::is_stale_lock(&lock_path)? {
            let _ = std::fs::remove_file(&lock_path);
            return Ok(false);
        }
        
        // Try to acquire lock to check if it's held
        let file = OpenOptions::new()
            .write(true)
            .open(&lock_path)?;
        
        let can_lock = Self::try_lock_exclusive(&file)?;
        
        if can_lock {
            // We got the lock, release it immediately
            Self::unlock(&file)?;
            Ok(false)
        } else {
            Ok(true)
        }
    }
    
    /// Check if a lock file is stale (process no longer exists)
    fn is_stale_lock(lock_path: &str) -> Result<bool, LockError> {
        let mut file = File::open(lock_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let pid: u32 = match contents.trim().parse() {
            Ok(p) => p,
            Err(_) => return Ok(true), // Invalid PID = stale
        };
        
        // Check if process exists (signal 0)
        #[cfg(unix)]
        {
            use libc::{kill, pid_t};
            let exists = unsafe { kill(pid as pid_t, 0) == 0 };
            if !exists {
                return Ok(true);
            }
        }
        
        // On non-Unix, we can't easily check, so assume valid
        Ok(false)
    }
    
    #[cfg(unix)]
    fn try_lock_exclusive(file: &File) -> Result<bool, LockError> {
        let fd = file.as_raw_fd();
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        
        if result == 0 {
            Ok(true)
        } else {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                Ok(false)
            } else {
                Err(LockError::Io(err))
            }
        }
    }
    
    #[cfg(windows)]
    fn try_lock_exclusive(file: &File) -> Result<bool, LockError> {
        // Windows implementation using LockFile
        // For now, return true (no locking on Windows)
        Ok(true)
    }
    
    #[cfg(unix)]
    fn unlock(file: &File) -> Result<(), LockError> {
        let fd = file.as_raw_fd();
        unsafe { libc::flock(fd, libc::LOCK_UN); }
        Ok(())
    }
    
    #[cfg(windows)]
    fn unlock(_file: &File) -> Result<(), LockError> {
        Ok(())
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Lock is released when file is closed
        // Also remove the lock file
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

/// Lock mode for database
#[derive(Clone, Copy, Debug)]
pub enum LockMode {
    /// Exclusive lock - prevents other processes
    Exclusive,
    /// Shared lock - read-only, checks if exclusive exists
    Shared,
    /// No locking - fastest, single-process only
    None,
}

impl LockMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "exclusive" => LockMode::Exclusive,
            "shared" => LockMode::Shared,
            _ => LockMode::None,
        }
    }
}
