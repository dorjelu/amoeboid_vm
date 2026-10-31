//! Relative paths inside a directory tree: the guest directory, a Nix config tree.

/// `/`-separated, non-empty segments, none `.` or `..`, no NUL. Resolving it can never leave
/// its root by name; symlinks are refused by the writers (`adapters::guest`, `adapters::files`).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RelativePath(String);

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("{0:?} is not a plain relative path")]
pub struct BadRelativePath(String);

impl RelativePath {
    /// # Errors
    ///
    /// Empty, absolute, an empty, `.` or `..` segment, or a NUL byte.
    pub fn parse(path: &str) -> Result<Self, BadRelativePath> {
        let valid = !path.contains('\0')
            && path
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
        if valid {
            Ok(Self(path.to_owned()))
        } else {
            Err(BadRelativePath(path.to_owned()))
        }
    }

    /// `self` followed by `other`.
    #[must_use]
    pub fn join(&self, other: &Self) -> Self {
        Self(format!("{}/{}", self.0, other.0))
    }

    /// Parent segments (possibly none) and the final name.
    #[must_use]
    pub fn split(&self) -> (Vec<&str>, &str) {
        let mut segments: Vec<&str> = self.0.split('/').collect();
        let name = segments.pop().unwrap_or_default();
        (segments, name)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl crate::diagnostic::Diagnostic for BadRelativePath {
    fn code(&self) -> &'static str {
        "relative_path.invalid"
    }
    fn hint(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_relative_paths_are_accepted() {
        for good in ["hostname", "home/.config/app", "a/b/c"] {
            assert!(RelativePath::parse(good).is_ok(), "{good}");
        }
        for bad in ["", "/etc", "a//b", "a/../b", "./a", "a/", "a\0b"] {
            assert!(RelativePath::parse(bad).is_err(), "{bad:?}");
        }
    }
}
