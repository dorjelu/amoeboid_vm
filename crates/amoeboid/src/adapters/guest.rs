//! Writes into the guest-writable directory (`<state>/guest`, shared into the VM). The guest
//! may have replaced anything in there with a symlink, so every lookup below the root goes
//! through `openat2` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`, and files are replaced by
//! rename inside an already opened parent. There is deliberately no read function: nothing the
//! guest can write is read back to decide anything.

use std::io::Write as _;
use std::os::fd::OwnedFd;

use rustix::fs::{AtFlags, Mode, OFlags, ResolveFlags};
use rustix::io::Errno;

use crate::domain::guest_path::GuestRelative;
use crate::domain::path::AbsolutePath;

#[derive(Debug, thiserror::Error)]
pub enum GuestError {
    #[error("can't open the guest directory {path}: {source}")]
    Root {
        path: String,
        source: std::io::Error,
    },
    #[error("can't write {path} in the guest directory: {source}")]
    Write {
        path: String,
        source: std::io::Error,
    },
}

/// The guest directory, opened once; all writes resolve beneath it.
pub struct GuestTree {
    root: OwnedFd,
}

const RESOLVE: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_MAGICLINKS);

impl GuestTree {
    /// Creates the guest directory if missing (its parent is host-owned) and opens it.
    ///
    /// # Errors
    ///
    /// `Root`.
    pub fn open(root: &AbsolutePath) -> Result<Self, GuestError> {
        let failed = |source: std::io::Error| GuestError::Root {
            path: root.display().to_string(),
            source,
        };
        crate::adapters::files::ensure_directory(root, 0o755)
            .map_err(|error| failed(std::io::Error::other(error)))?;
        let root = rustix::fs::open(
            root.as_path(),
            OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::RDONLY,
            Mode::empty(),
        )
        .map_err(|error| failed(error.into()))?;
        Ok(Self { root })
    }

    /// Opens (creating with `0755` where missing) each directory on the way to `segments`.
    fn directory(&self, segments: &[&str]) -> Result<OwnedFd, Errno> {
        let mut current = rustix::io::dup(&self.root)?;
        for segment in segments {
            match rustix::fs::mkdirat(&current, *segment, Mode::from_raw_mode(0o755)) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(error) => return Err(error),
            }
            current = rustix::fs::openat2(
                &current,
                *segment,
                OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::RDONLY,
                Mode::empty(),
                RESOLVE,
            )?;
        }
        Ok(current)
    }

    fn failed(path: &GuestRelative) -> impl Fn(std::io::Error) -> GuestError + '_ {
        move |source| GuestError::Write {
            path: path.as_str().to_owned(),
            source,
        }
    }

    /// Makes sure `path` is a directory, creating it and its parents.
    ///
    /// # Errors
    ///
    /// `Write`, including when a component is a symlink.
    pub fn ensure_directory(&self, path: &GuestRelative) -> Result<(), GuestError> {
        let (parents, name) = path.split();
        let mut segments = parents;
        segments.push(name);
        self.directory(&segments)
            .map(drop)
            .map_err(|error| Self::failed(path)(error.into()))
    }

    /// Replaces `path` with `bytes` at `mode`, whatever the guest left there (a file or a
    /// symlink is replaced, never followed).
    ///
    /// # Errors
    ///
    /// `Write`, including when a parent component is a symlink or the target is a directory.
    pub fn replace_file(
        &self,
        path: &GuestRelative,
        bytes: &[u8],
        mode: u32,
    ) -> Result<(), GuestError> {
        let failed = Self::failed(path);
        let (parents, name) = path.split();
        let parent = self
            .directory(&parents)
            .map_err(|error| failed(error.into()))?;
        let temporary = format!(".{name}.tmp-{}", ulid::Ulid::generate());
        let descriptor = rustix::fs::openat(
            &parent,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(mode),
        )
        .map_err(|error| failed(error.into()))?;
        let mut file = std::fs::File::from(descriptor);
        let written = file
            .write_all(bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| {
                // The mode at creation is masked by the umask; set it exactly.
                rustix::fs::fchmod(&file, Mode::from_raw_mode(mode)).map_err(Into::into)
            })
            .and_then(|()| {
                rustix::fs::renameat(&parent, temporary.as_str(), &parent, name).map_err(Into::into)
            });
        if let Err(error) = written {
            let _ = rustix::fs::unlinkat(&parent, temporary.as_str(), AtFlags::empty());
            return Err(failed(error));
        }
        Ok(())
    }

    /// Removes the file at `path` if there is one (a symlink is removed, not followed).
    ///
    /// # Errors
    ///
    /// `Write`.
    pub fn remove_file(&self, path: &GuestRelative) -> Result<(), GuestError> {
        let (parents, name) = path.split();
        let result = self
            .directory(&parents)
            .and_then(|parent| rustix::fs::unlinkat(&parent, name, AtFlags::empty()));
        match result {
            Ok(()) | Err(Errno::NOENT) => Ok(()),
            Err(error) => Err(Self::failed(path)(error.into())),
        }
    }
}

impl crate::diagnostic::Diagnostic for GuestError {
    fn code(&self) -> &'static str {
        match self {
            Self::Root { .. } => "guest.root",
            Self::Write { .. } => "guest.write",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Root { .. } => None,
            // The guest can plant symlinks or directories; refusing them is the point.
            Self::Write { .. } => {
                Some("the guest may have replaced part of its directory; inspect it".into())
            }
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

    fn tree() -> (tempfile::TempDir, AbsolutePath, GuestTree) {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().join("guest")).unwrap();
        let tree = GuestTree::open(&root).unwrap();
        (temporary, root, tree)
    }

    fn relative(path: &str) -> GuestRelative {
        GuestRelative::parse(path).unwrap()
    }

    #[test]
    fn a_file_is_written_with_its_mode_creating_parents() {
        let (_temporary, root, tree) = tree();
        tree.replace_file(&relative("home/.config/key"), b"secret", 0o600)
            .unwrap();
        let path = root.as_path().join("home/.config/key");
        assert_eq!(std::fs::read(&path).unwrap(), b"secret");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn a_symlink_planted_at_the_target_is_replaced_and_its_target_untouched() {
        let (temporary, root, tree) = tree();
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"host data").unwrap();
        std::os::unix::fs::symlink(&outside, root.as_path().join("hostname")).unwrap();
        tree.replace_file(&relative("hostname"), b"work\n", 0o644)
            .unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"host data");
        assert_eq!(
            std::fs::read(root.as_path().join("hostname")).unwrap(),
            b"work\n"
        );
    }

    #[test]
    fn a_symlink_planted_as_a_parent_directory_is_refused() {
        let (temporary, root, tree) = tree();
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.as_path().join("home")).unwrap();
        let result = tree.replace_file(&relative("home/file"), b"x", 0o644);
        assert!(matches!(result, Err(GuestError::Write { .. })));
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
    }

    #[test]
    fn removing_a_missing_file_is_fine_and_a_symlink_is_removed_not_followed() {
        let (temporary, root, tree) = tree();
        tree.remove_file(&relative("term")).unwrap();
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"keep").unwrap();
        std::os::unix::fs::symlink(&outside, root.as_path().join("term")).unwrap();
        tree.remove_file(&relative("term")).unwrap();
        assert!(outside.exists());
        assert!(!root.as_path().join("term").exists());
    }
}
