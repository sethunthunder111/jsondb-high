#![allow(dead_code)]
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

pub struct ProcessLock {
    #[allow(dead_code)]
    lock_file: File,
    lock_path: String,
}

impl ProcessLock {
    pub fn acquire(db_path: &str, timeout_ms: u64) -> Result<Self, LockError> {
        let lock_path = format!("{}.process_lock", db_path);
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        
        loop {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)?;
            
            if Self::try_lock_exclusive(&file)? {
                let pid = std::process::id();
                file.set_len(0)?;
                writeln!(file, "{}", pid)?;
                file.sync_all()?;
                
                return Ok(ProcessLock {
                    lock_file: file,
                    lock_path,
                });
            }

            if Self::is_stale_lock(&lock_path).unwrap_or(false) {
                let _ = std::fs::remove_file(&lock_path);
                continue;
            }

            if start.elapsed() >= timeout {
                return Err(LockError::AlreadyLocked);
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    
    pub fn is_locked(db_path: &str) -> Result<bool, LockError> {
        let lock_path = format!("{}.process_lock", db_path);
        
        if !Path::new(&lock_path).exists() {
            return Ok(false);
        }
        
        if Self::is_stale_lock(&lock_path)? {
            let _ = std::fs::remove_file(&lock_path);
            return Ok(false);
        }
        
        let file = OpenOptions::new()
            .write(true)
            .open(&lock_path)?;
        
        let can_lock = Self::try_lock_exclusive(&file)?;
        
        if can_lock {
            Self::unlock(&file)?;
            Ok(false)
        } else {
            Ok(true)
        }
    }
    
    fn is_stale_lock(lock_path: &str) -> Result<bool, LockError> {
        let mut file = File::open(lock_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let pid: u32 = match contents.trim().parse() {
            Ok(p) => p,
            Err(_) => return Ok(true),
        };
        
        #[cfg(unix)]
        {
            use libc::{kill, pid_t};
            let exists = unsafe { kill(pid as pid_t, 0) == 0 };
            if !exists {
                return Ok(true);
            }
        }
        
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
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LockMode {
    Exclusive,
    Shared,
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
