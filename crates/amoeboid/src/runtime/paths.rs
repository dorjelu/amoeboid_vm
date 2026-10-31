//! Runtime locations, refined once at the boundary.

#![allow(clippy::disallowed_types, reason = "boundary: refines raw host paths")]

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

/// `sun_path` holds 108 bytes including the terminating NUL.
const MAX_SOCKET_PATH: usize = 107;

/// Absolute, with no `.` or `..` components.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AbsoluteDirectory(PathBuf);

impl AbsoluteDirectory {
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
    #[error("environment variable {0} is not set")]
    MissingEnvironment(&'static str),
    #[error("socket path {} is longer than {MAX_SOCKET_PATH} bytes", .0.display())]
    SocketTooLong(PathBuf),
}

// TODO: give the runtime's own files (daemon.lock, journal.sqlite) a location separate from
// the VM data directory (decision 0002, open questions).
/// `data` holds `daemon.lock` and the journal; `run` (`$XDG_RUNTIME_DIR/amoeboid`, `0700`)
/// holds the socket.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RuntimePaths {
    data: AbsoluteDirectory,
    run: AbsoluteDirectory,
    socket: PathBuf,
}

impl RuntimePaths {
    /// # Errors
    ///
    /// `SocketTooLong`.
    pub fn under(data: AbsoluteDirectory, run: AbsoluteDirectory) -> Result<Self, PathError> {
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
    pub fn from_env(
        environment: impl Fn(&'static str) -> Option<OsString>,
    ) -> Result<Self, PathError> {
        let data = match environment("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
            Some(xdg) => PathBuf::from(xdg).join("amoeboid"),
            None => {
                PathBuf::from(environment("HOME").ok_or(PathError::MissingEnvironment("HOME"))?)
                    .join(".local/share/amoeboid")
            }
        };
        let run = PathBuf::from(
            environment("XDG_RUNTIME_DIR")
                .filter(|v| !v.is_empty())
                .ok_or(PathError::MissingEnvironment("XDG_RUNTIME_DIR"))?,
        )
        .join("amoeboid");
        Self::under(
            AbsoluteDirectory::parse(data)?,
            AbsoluteDirectory::parse(run)?,
        )
    }

    #[must_use]
    pub fn data(&self) -> &AbsoluteDirectory {
        &self.data
    }

    #[must_use]
    pub fn run(&self) -> &AbsoluteDirectory {
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

impl crate::diagnostic::Diagnostic for PathError {
    fn code(&self) -> &'static str {
        match self {
            Self::NotAbsolute(_) => "runtime.path.not_absolute",
            Self::NotNormal(_) => "runtime.path.not_normal",
            Self::MissingEnvironment(_) => "runtime.path.missing_environment",
            Self::SocketTooLong(_) => "runtime.path.socket_too_long",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::MissingEnvironment("XDG_RUNTIME_DIR") => {
                Some("run inside a systemd user session, which sets XDG_RUNTIME_DIR".into())
            }
            Self::MissingEnvironment(variable) => Some(format!("set {variable}")),
            Self::SocketTooLong(_) => Some("use a shorter XDG_RUNTIME_DIR".into()),
            Self::NotAbsolute(_) | Self::NotNormal(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn environment(vars: &[(&'static str, &str)]) -> impl Fn(&'static str) -> Option<OsString> {
        let vars: BTreeMap<_, _> = vars.iter().map(|(k, v)| (*k, OsString::from(v))).collect();
        move |k| vars.get(k).cloned()
    }

    #[test]
    fn xdg_data_home_wins_over_home() {
        let paths = RuntimePaths::from_env(environment(&[
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
        let paths = RuntimePaths::from_env(environment(&[
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
        let result = RuntimePaths::from_env(environment(&[("HOME", "/home/u")]));
        assert!(matches!(
            result,
            Err(PathError::MissingEnvironment("XDG_RUNTIME_DIR"))
        ));
    }

    #[test]
    fn relative_and_dotted_paths_are_refused() {
        assert!(matches!(
            AbsoluteDirectory::parse("rel/dir".into()),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(
            AbsoluteDirectory::parse("/a/../b".into()),
            Err(PathError::NotNormal(_))
        ));
    }

    #[test]
    fn a_socket_path_beyond_sun_path_is_refused() {
        let long = format!("/{}", "r".repeat(MAX_SOCKET_PATH));
        let result = RuntimePaths::under(
            AbsoluteDirectory::parse("/d".into()).unwrap(),
            AbsoluteDirectory::parse(long.into()).unwrap(),
        );
        assert!(matches!(result, Err(PathError::SocketTooLong(_))));
    }
}
