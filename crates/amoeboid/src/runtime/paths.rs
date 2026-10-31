//! Runtime locations, refined once at the boundary.

#![allow(clippy::disallowed_types, reason = "boundary: refines raw host paths")]

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

/// `sun_path` holds 108 bytes including the terminating NUL.
const MAX_SOCKET_PATH: usize = 107;

/// Absolute, with no `.` or `..` components.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AbsDir(PathBuf);

impl AbsDir {
    /// # Errors
    ///
    /// `NotAbsolute`, `NotNormal`.
    pub fn parse(path: PathBuf) -> Result<Self, PathError> {
        if !path.is_absolute() {
            return Err(PathError::NotAbsolute(path));
        }
        if path
            .components()
            .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
        {
            return Err(PathError::NotNormal(path));
        }
        Ok(Self(path))
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("{} is not an absolute path", .0.display())]
    NotAbsolute(PathBuf),
    #[error("{} contains . or .. components", .0.display())]
    NotNormal(PathBuf),
    #[error("{0} is not set")]
    MissingEnv(&'static str),
    #[error("socket path {} is longer than {MAX_SOCKET_PATH} bytes", .0.display())]
    SocketTooLong(PathBuf),
}

// TODO: give the runtime's own files (daemon.lock, journal.sqlite) a location separate from
// the VM data directory (decision 0002, open questions).
/// `data` holds `daemon.lock` and the journal; `run` (`$XDG_RUNTIME_DIR/amoeboid`, `0700`)
/// holds the socket.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RuntimePaths {
    data: AbsDir,
    run: AbsDir,
    socket: PathBuf,
}

impl RuntimePaths {
    /// # Errors
    ///
    /// `SocketTooLong`.
    pub fn under(data: AbsDir, run: AbsDir) -> Result<Self, PathError> {
        let socket = run.join("runtime.sock");
        if socket.as_os_str().len() > MAX_SOCKET_PATH {
            return Err(PathError::SocketTooLong(socket));
        }
        Ok(Self { data, run, socket })
    }

    /// `data` = `$XDG_DATA_HOME/amoeboid`, else `$HOME/.local/share/amoeboid`;
    /// `run` = `$XDG_RUNTIME_DIR/amoeboid`.
    ///
    /// # Errors
    ///
    /// See [`PathError`].
    pub fn from_env(env: impl Fn(&'static str) -> Option<OsString>) -> Result<Self, PathError> {
        let data = match env("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
            Some(xdg) => PathBuf::from(xdg).join("amoeboid"),
            None => PathBuf::from(env("HOME").ok_or(PathError::MissingEnv("HOME"))?)
                .join(".local/share/amoeboid"),
        };
        let run = PathBuf::from(
            env("XDG_RUNTIME_DIR")
                .filter(|v| !v.is_empty())
                .ok_or(PathError::MissingEnv("XDG_RUNTIME_DIR"))?,
        )
        .join("amoeboid");
        Self::under(AbsDir::parse(data)?, AbsDir::parse(run)?)
    }

    #[must_use]
    pub fn data(&self) -> &AbsDir {
        &self.data
    }

    #[must_use]
    pub fn run(&self) -> &AbsDir {
        &self.run
    }

    #[must_use]
    pub fn lock(&self) -> PathBuf {
        self.data.join("daemon.lock")
    }

    #[must_use]
    pub fn journal(&self) -> PathBuf {
        self.data.join("journal.sqlite")
    }

    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn env(vars: &[(&'static str, &str)]) -> impl Fn(&'static str) -> Option<OsString> {
        let vars: BTreeMap<_, _> = vars.iter().map(|(k, v)| (*k, OsString::from(v))).collect();
        move |k| vars.get(k).cloned()
    }

    #[test]
    fn xdg_data_home_wins_over_home() {
        let paths = RuntimePaths::from_env(env(&[
            ("XDG_DATA_HOME", "/x/data"),
            ("HOME", "/home/u"),
            ("XDG_RUNTIME_DIR", "/run/user/1"),
        ]))
        .unwrap();
        assert_eq!(paths.lock(), PathBuf::from("/x/data/amoeboid/daemon.lock"));
        assert_eq!(
            paths.socket(),
            Path::new("/run/user/1/amoeboid/runtime.sock")
        );
    }

    #[test]
    fn home_is_the_fallback_for_data() {
        let paths = RuntimePaths::from_env(env(&[
            ("XDG_DATA_HOME", ""),
            ("HOME", "/home/u"),
            ("XDG_RUNTIME_DIR", "/run/user/1"),
        ]))
        .unwrap();
        assert_eq!(
            paths.journal(),
            PathBuf::from("/home/u/.local/share/amoeboid/journal.sqlite")
        );
    }

    #[test]
    fn a_missing_runtime_dir_is_refused() {
        let result = RuntimePaths::from_env(env(&[("HOME", "/home/u")]));
        assert!(matches!(
            result,
            Err(PathError::MissingEnv("XDG_RUNTIME_DIR"))
        ));
    }

    #[test]
    fn relative_and_dotted_paths_are_refused() {
        assert!(matches!(
            AbsDir::parse("rel/dir".into()),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(
            AbsDir::parse("/a/../b".into()),
            Err(PathError::NotNormal(_))
        ));
    }

    #[test]
    fn a_socket_path_beyond_sun_path_is_refused() {
        let long = format!("/{}", "r".repeat(MAX_SOCKET_PATH));
        let result = RuntimePaths::under(
            AbsDir::parse("/d".into()).unwrap(),
            AbsDir::parse(long.into()).unwrap(),
        );
        assert!(matches!(result, Err(PathError::SocketTooLong(_))));
    }
}
