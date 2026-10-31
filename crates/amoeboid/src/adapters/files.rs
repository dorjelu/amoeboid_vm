//! Host-owned files: written atomically (temporary file, fsync, rename), never through a
//! symlink. For guest-writable directories use `adapters::guest` instead.

use std::io::Write as _;

use rustix::fs::{Mode, OFlags};

use crate::domain::path::AbsolutePath;
use crate::domain::relative_path::RelativePath;
use crate::domain::tree::Tree;

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
pub fn read_bytes(path: &AbsolutePath) -> Result<Vec<u8>, FileError> {
    std::fs::read(path.as_path()).map_err(|source| FileError::Read {
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

/// Names never copied into or out of a tree (build results, caches, version control).
pub const IGNORED_NAMES: [&str; 3] = ["__pycache__", "result", ".git"];

/// Every regular file under `root` (host-owned), by relative path. Symlinks and other kinds are
/// skipped, never followed; so are `IGNORED_NAMES` at any depth.
///
/// # Errors
///
/// `Read`.
pub fn read_tree(root: &AbsolutePath) -> Result<Tree, FileError> {
    let mut tree = Tree::default();
    let mut pending = vec![(root.clone(), None::<RelativePath>)];
    while let Some((directory, prefix)) = pending.pop() {
        let failed = |source: std::io::Error| FileError::Read {
            path: directory.display().to_string(),
            source,
        };
        for entry in std::fs::read_dir(directory.as_path()).map_err(failed)? {
            let entry = entry.map_err(failed)?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if IGNORED_NAMES.contains(&name.as_str()) {
                continue;
            }
            let Ok(segment) = RelativePath::parse(&name) else {
                continue;
            };
            let relative = match &prefix {
                Some(prefix) => prefix.join(&segment),
                None => segment,
            };
            // `file_type` does not follow symlinks.
            let kind = entry.file_type().map_err(failed)?;
            let path = directory.join(&name);
            if kind.is_dir() {
                pending.push((path, Some(relative)));
            } else if kind.is_file() {
                let content = std::fs::read(path.as_path()).map_err(|source| FileError::Read {
                    path: path.display().to_string(),
                    source,
                })?;
                tree.insert(relative, content);
            }
        }
    }
    Ok(tree)
}

/// Replaces the host-owned directory `target` with `tree`: written aside as
/// `.<name>.new-<ulid>`, then swapped in by rename, so `target` is the old tree or the new one,
/// never a mix. Directories `0755`, files `0644`.
///
/// # Errors
///
/// `Write`, `Directory`.
pub fn replace_directory(target: &AbsolutePath, tree: &Tree) -> Result<(), FileError> {
    let suffix = ulid::Ulid::generate();
    let fresh = target.sibling(&format!(".{}.new-{suffix}", target.file_name()));
    let stale = target.sibling(&format!(".{}.old-{suffix}", target.file_name()));
    ensure_directory(&fresh, 0o755)?;
    for (relative, content) in tree.files() {
        let path = fresh.join(relative.as_str());
        if let Some(parent) = path.parent() {
            ensure_directory(&parent, 0o755)?;
        }
        write_atomically(&path, content, 0o644)?;
    }
    let swap = |from: &AbsolutePath, to: &AbsolutePath| {
        std::fs::rename(from.as_path(), to.as_path()).map_err(|source| FileError::Write {
            path: target.display().to_string(),
            source,
        })
    };
    let had_target = target.as_path().exists();
    if had_target {
        swap(target, &stale)?;
    }
    swap(&fresh, target)?;
    if had_target {
        let _ = std::fs::remove_dir_all(stale.as_path());
    }
    Ok(())
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
    fn a_tree_skips_symlinks_and_ignored_names_and_replaces_a_directory_whole() {
        let (_temporary, root) = directory();
        let source = root.join("source");
        std::fs::create_dir_all(source.as_path().join("sub")).unwrap();
        std::fs::create_dir_all(source.as_path().join(".git")).unwrap();
        std::fs::write(source.as_path().join("flake.nix"), b"flake").unwrap();
        std::fs::write(source.as_path().join("sub/recipe.nix"), b"recipe").unwrap();
        std::fs::write(source.as_path().join(".git/HEAD"), b"ref").unwrap();
        std::os::unix::fs::symlink("/etc/passwd", source.as_path().join("link")).unwrap();
        let tree = read_tree(&source).unwrap();
        let paths: Vec<&str> = tree.files().map(|(path, _)| path.as_str()).collect();
        assert_eq!(paths, ["flake.nix", "sub/recipe.nix"]);

        let target = root.join("nix");
        std::fs::create_dir_all(target.as_path()).unwrap();
        std::fs::write(target.as_path().join("stale.nix"), b"old").unwrap();
        replace_directory(&target, &tree).unwrap();
        assert_eq!(read_tree(&target).unwrap(), tree);
        let leftovers: Vec<_> = std::fs::read_dir(root.as_path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(leftovers.len(), 2, "{leftovers:?}");
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
