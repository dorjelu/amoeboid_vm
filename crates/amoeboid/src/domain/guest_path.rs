//! Paths inside the guest-writable directory, relative to its root.

/// `/`-separated, non-empty segments, none `.` or `..`, no NUL. Resolving it can never leave
/// the guest directory by name; symlinks are refused by the writer (`adapters::guest`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct GuestRelative(String);

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("{0:?} is not a relative path inside the guest directory")]
pub struct BadGuestRelative(String);

impl GuestRelative {
    /// # Errors
    ///
    /// Empty, absolute, an empty, `.` or `..` segment, or a NUL byte.
    pub fn parse(path: &str) -> Result<Self, BadGuestRelative> {
        let valid = !path.contains('\0')
            && path
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
        if valid {
            Ok(Self(path.to_owned()))
        } else {
            Err(BadGuestRelative(path.to_owned()))
        }
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

impl crate::diagnostic::Diagnostic for BadGuestRelative {
    fn code(&self) -> &'static str {
        "guest_path.invalid"
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
            assert!(GuestRelative::parse(good).is_ok(), "{good}");
        }
        for bad in ["", "/etc", "a//b", "a/../b", "./a", "a/", "a\0b"] {
            assert!(GuestRelative::parse(bad).is_err(), "{bad:?}");
        }
    }
}
