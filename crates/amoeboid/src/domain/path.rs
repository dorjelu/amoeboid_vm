//! Absolute host paths, refined once at the boundary. The only domain module that holds raw
//! `Path`/`PathBuf` values.

#![allow(clippy::disallowed_types, reason = "boundary: refines raw host paths")]

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

/// Absolute, with no `.` or `..` components. Ordered component by component, so a parent sorts
/// before anything inside it.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct AbsolutePath(PathBuf);

#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("{} is not an absolute path", .0.display())]
    NotAbsolute(PathBuf),
    #[error("{} contains . or .. components", .0.display())]
    NotNormal(PathBuf),
}

impl AbsolutePath {
    /// # Errors
    ///
    /// `NotAbsolute`, `NotNormal`.
    pub fn parse(path: PathBuf) -> Result<Self, PathError> {
        if !path.is_absolute() {
            return Err(PathError::NotAbsolute(path));
        }
        if path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(PathError::NotNormal(path));
        }
        Ok(Self(path))
    }

    /// `segment` is a fixed name or an already validated one (a VM name, a share tag); it never
    /// contains `/`, so the result stays absolute and normal.
    #[must_use]
    pub(crate) fn join(&self, segment: &str) -> Self {
        Self(self.0.join(segment))
    }

    /// The same path with `suffix` appended to its last segment (`a/b` + `.tmp` = `a/b.tmp`).
    #[must_use]
    pub(crate) fn with_suffix(&self, suffix: &str) -> Self {
        let mut path = self.0.clone().into_os_string();
        path.push(suffix);
        Self(PathBuf::from(path))
    }

    /// Whether `self` is `other` or lies inside it, component by component.
    #[must_use]
    pub fn is_within(&self, other: &Self) -> bool {
        self.0.starts_with(&other.0)
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    #[must_use]
    pub fn as_os_str(&self) -> &OsStr {
        self.0.as_os_str()
    }

    /// `None` when the path is not valid UTF-8.
    #[must_use]
    pub fn to_str(&self) -> Option<&str> {
        self.0.to_str()
    }

    #[must_use]
    pub fn display(&self) -> std::path::Display<'_> {
        self.0.display()
    }
}

impl AsRef<Path> for AbsolutePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl crate::diagnostic::Diagnostic for PathError {
    fn code(&self) -> &'static str {
        match self {
            Self::NotAbsolute(_) => "path.not_absolute",
            Self::NotNormal(_) => "path.not_normal",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::NotAbsolute(_) | Self::NotNormal(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_and_dotted_paths_are_refused() {
        assert!(matches!(
            AbsolutePath::parse("relative/directory".into()),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(
            AbsolutePath::parse("/a/../b".into()),
            Err(PathError::NotNormal(_))
        ));
    }

    #[test]
    fn a_path_is_within_itself_and_its_ancestors_only_by_component() {
        let parse = |text: &str| AbsolutePath::parse(text.into()).unwrap();
        assert!(parse("/srv/a/b").is_within(&parse("/srv/a")));
        assert!(parse("/srv/a").is_within(&parse("/srv/a")));
        assert!(!parse("/srv/a-b").is_within(&parse("/srv/a")));
    }
}
