//! `daemon.lock`: one runtime per user. `flock(LOCK_EX|LOCK_NB)` (std `File::try_lock`) on a
//! file opened `O_CREAT|O_NOFOLLOW|O_CLOEXEC`, never deleted. The kernel releases the lock when
//! the last descriptor of that open file closes, including on `SIGKILL`; `O_CLOEXEC` keeps it out of
//! spawned VM processes, which would otherwise hold it past the runtime's death.

use std::fs::{File, TryLockError};
use std::io::{Read as _, Seek as _, Write as _};

use rustix::fs::{Mode, OFlags};
use serde::{Deserialize, Serialize};

use super::paths::RuntimePaths;

/// Held for the runtime's lifetime; dropping it releases the lock.
#[derive(Debug)]
pub struct DaemonLock {
    /// Held only to keep the lock; never read.
    _file: File,
}

/// Written into the file by the holder. Diagnostics only: never used to decide anything.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LockRecord {
    pub process_id: u32,
    pub version: String,
}

#[derive(Debug)]
pub enum Acquire {
    Acquired(DaemonLock),
    /// Another runtime holds it; `holder` is its record when readable.
    Held {
        holder: Option<LockRecord>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("can't open the daemon lock: {0}")]
    Open(std::io::Error),
    #[error("can't lock the daemon lock: {0}")]
    Lock(std::io::Error),
    #[error("can't record the holder in the daemon lock: {0}")]
    Record(std::io::Error),
}

/// # Errors
///
/// See [`LockError`]. A symlink at the lock path is `Open` (`ELOOP`).
pub fn acquire(paths: &RuntimePaths, record: &LockRecord) -> Result<Acquire, LockError> {
    let descriptor = rustix::fs::open(
        paths.lock(),
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )
    .map_err(|error| LockError::Open(error.into()))?;
    let mut file = File::from(descriptor);
    match file.try_lock() {
        Ok(()) => {
            write_record(&mut file, record).map_err(LockError::Record)?;
            Ok(Acquire::Acquired(DaemonLock { _file: file }))
        }
        Err(TryLockError::WouldBlock) => Ok(Acquire::Held {
            holder: read_record(&mut file),
        }),
        Err(TryLockError::Error(error)) => Err(LockError::Lock(error)),
    }
}

fn write_record(file: &mut File, record: &LockRecord) -> std::io::Result<()> {
    let json = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    file.set_len(0)?;
    file.rewind()?;
    file.write_all(&json)?;
    file.sync_data()
}

fn read_record(file: &mut File) -> Option<LockRecord> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    serde_json::from_slice(&bytes).ok()
}

impl crate::diagnostic::Diagnostic for LockError {
    fn code(&self) -> &'static str {
        match self {
            Self::Open(_) => "runtime.lock.open",
            Self::Lock(_) => "runtime.lock.lock",
            Self::Record(_) => "runtime.lock.record",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Open(_) | Self::Lock(_) | Self::Record(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use std::process::Command;

    use super::*;
    use crate::runtime::paths::AbsoluteDirectory;

    /// Held by every test here. A fork in a parallel test copies all open fds until its child
    /// execs (`O_CLOEXEC` acts at exec, not fork), briefly keeping a dropped lock alive.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        static FORKS: std::sync::Mutex<()> = std::sync::Mutex::new(());
        FORKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn paths(temporary: &tempfile::TempDir) -> RuntimePaths {
        let directory = AbsoluteDirectory::parse(temporary.path().to_path_buf()).unwrap();
        RuntimePaths::under(directory.clone(), directory).unwrap()
    }

    fn record() -> LockRecord {
        LockRecord {
            process_id: std::process::id(),
            version: "test".into(),
        }
    }

    #[test]
    fn a_second_acquire_sees_the_lock_held_with_the_first_holders_record() {
        let _serial = serial();
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(&directory);
        let first = acquire(&paths, &record()).unwrap();
        assert!(matches!(first, Acquire::Acquired(_)));
        let second = acquire(&paths, &record()).unwrap();
        assert!(matches!(second, Acquire::Held { holder: Some(holder) } if holder == record()));
    }

    #[test]
    fn dropping_the_lock_releases_it() {
        let _serial = serial();
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(&directory);
        drop(acquire(&paths, &record()).unwrap());
        assert!(matches!(
            acquire(&paths, &record()).unwrap(),
            Acquire::Acquired(_)
        ));
    }

    #[test]
    fn a_child_process_does_not_keep_the_lock_alive() {
        let _serial = serial();
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(&directory);
        let lock = acquire(&paths, &record()).unwrap();
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        drop(lock);
        let again = acquire(&paths, &record()).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(matches!(again, Acquire::Acquired(_)));
    }

    #[test]
    fn a_symlink_at_the_lock_path_is_refused() {
        let _serial = serial();
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(&directory);
        std::os::unix::fs::symlink(directory.path().join("elsewhere"), paths.lock()).unwrap();
        assert!(matches!(
            acquire(&paths, &record()),
            Err(LockError::Open(_))
        ));
        assert!(!directory.path().join("elsewhere").exists());
    }
}
