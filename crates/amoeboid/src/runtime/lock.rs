//! `daemon.lock`: one runtime per user. `flock(LOCK_EX|LOCK_NB)` (std `File::try_lock`) on a
//! file opened `O_CREAT|O_NOFOLLOW|O_CLOEXEC`, never deleted. The kernel releases the lock when
//! the last fd of that open file closes, including on `SIGKILL`; `O_CLOEXEC` keeps it out of
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
    pub pid: u32,
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
    let fd = rustix::fs::open(
        paths.lock(),
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )
    .map_err(|e| LockError::Open(e.into()))?;
    let mut file = File::from(fd);
    match file.try_lock() {
        Ok(()) => {
            write_record(&mut file, record).map_err(LockError::Record)?;
            Ok(Acquire::Acquired(DaemonLock { _file: file }))
        }
        Err(TryLockError::WouldBlock) => Ok(Acquire::Held {
            holder: read_record(&mut file),
        }),
        Err(TryLockError::Error(e)) => Err(LockError::Lock(e)),
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use std::process::Command;

    use super::*;
    use crate::runtime::paths::AbsDir;

    fn paths(dir: &tempfile::TempDir) -> RuntimePaths {
        let dir = AbsDir::parse(dir.path().to_path_buf()).unwrap();
        RuntimePaths::under(dir.clone(), dir).unwrap()
    }

    fn record() -> LockRecord {
        LockRecord {
            pid: std::process::id(),
            version: "test".into(),
        }
    }

    #[test]
    fn a_second_acquire_sees_the_lock_held_with_the_first_holders_record() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        let first = acquire(&paths, &record()).unwrap();
        assert!(matches!(first, Acquire::Acquired(_)));
        let second = acquire(&paths, &record()).unwrap();
        assert!(matches!(second, Acquire::Held { holder: Some(h) } if h == record()));
    }

    #[test]
    fn dropping_the_lock_releases_it() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        drop(acquire(&paths, &record()).unwrap());
        assert!(matches!(
            acquire(&paths, &record()).unwrap(),
            Acquire::Acquired(_)
        ));
    }

    #[test]
    fn a_child_process_does_not_keep_the_lock_alive() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
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
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        std::os::unix::fs::symlink(dir.path().join("elsewhere"), paths.lock()).unwrap();
        assert!(matches!(
            acquire(&paths, &record()),
            Err(LockError::Open(_))
        ));
        assert!(!dir.path().join("elsewhere").exists());
    }
}
