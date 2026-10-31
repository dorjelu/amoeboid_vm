//! Host-owned files: written atomically (temporary file, fsync, rename), never through a
//! symlink. For guest-writable directories use `adapters::guest` instead.

use std::io::Write as _;

use rustix::fs::{Mode, OFlags};

use crate::domain::path::AbsolutePath;

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("can't write {path}: {source}")]
    Write {
        path: String,
        source: std::io::Error,
    },
    #[error("can't create the directory {path}: {source}")]
    Directory {
        path: String,
        source: std::io::Error,
    },
    #[error("can't read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
}

/// Replaces `path` with `bytes` at `mode`. A reader sees the old content or the new, never a
/// mix; a symlink at `path` is replaced, not followed.
///
/// # Errors
///
/// `Write`.
pub fn write_atomically(path: &AbsolutePath, bytes: &[u8], mode: u32) -> Result<(), FileError> {
    let failed = |source: std::io::Error| FileError::Write {
        path: path.display().to_string(),
        source,
    };
    let temporary = path.with_suffix(&format!(".tmp-{}", ulid::Ulid::generate()));
    let descriptor = rustix::fs::open(
        temporary.as_path(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(mode),
    )
    .map_err(|error| failed(error.into()))?;
    let mut file = std::fs::File::from(descriptor);
    let written = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::rename(temporary.as_path(), path.as_path()));
    if let Err(error) = written {
        let _ = std::fs::remove_file(temporary.as_path());
        return Err(failed(error));
    }
    Ok(())
}

/// Creates `path` and missing parents with `mode`; an existing directory is kept as it is.
///
/// # Errors
///
/// `Directory`.
pub fn ensure_directory(path: &AbsolutePath, mode: u32) -> Result<(), FileError> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(mode)
        .create(path.as_path())
        .map_err(|source| FileError::Directory {
            path: path.display().to_string(),
            source,
        })
}

/// # Errors
///
/// `Read`.
pub fn read_to_string(path: &AbsolutePath) -> Result<String, FileError> {
    std::fs::read_to_string(path.as_path()).map_err(|source| FileError::Read {
        path: path.display().to_string(),
        source,
    })
}

impl crate::diagnostic::Diagnostic for FileError {
    fn code(&self) -> &'static str {
        match self {
            Self::Write { .. } => "file.write",
            Self::Directory { .. } => "file.directory",
            Self::Read { .. } => "file.read",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Write { .. } | Self::Directory { .. } | Self::Read { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures plant files by raw path"
    )]

    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn directory() -> (tempfile::TempDir, AbsolutePath) {
        let temporary = tempfile::tempdir().unwrap();
        let path = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        (temporary, path)
    }

    #[test]
    fn a_write_replaces_the_file_with_the_given_mode_and_leaves_no_temporary() {
        let (_temporary, root) = directory();
        let target = root.join("host.nix");
        write_atomically(&target, b"old", 0o644).unwrap();
        write_atomically(&target, b"new", 0o600).unwrap();
        assert_eq!(read_to_string(&target).unwrap(), "new");
        let mode = std::fs::metadata(target.as_path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(std::fs::read_dir(root.as_path()).unwrap().count(), 1);
    }

    #[test]
    fn a_symlink_at_the_target_is_replaced_not_followed() {
        let (_temporary, root) = directory();
        let elsewhere = root.join("elsewhere");
        let target = root.join("known_hosts");
        std::os::unix::fs::symlink(elsewhere.as_path(), target.as_path()).unwrap();
        write_atomically(&target, b"pinned", 0o644).unwrap();
        assert!(!elsewhere.as_path().exists());
        assert!(
            !std::fs::symlink_metadata(target.as_path())
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}
