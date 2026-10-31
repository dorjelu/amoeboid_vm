//! Host directories shared into the VM, each at its own path with an access mode.
//!
//! A share is validated as a set: no duplicates, each a directory, none covering a protected
//! directory (the guest home). Valid shares are ordered parents first, because nested shares
//! mount on top of their parent.

use serde::{Deserialize, Serialize};

use super::path::AbsolutePath;
use crate::nonempty::NonEmpty;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Mode {
    #[serde(rename = "rw")]
    ReadWrite,
    #[serde(rename = "ro")]
    ReadOnly,
}

impl Mode {
    /// The word used in the guest's `mounts` table.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "rw",
            Self::ReadOnly => "ro",
        }
    }
}

/// An absolute host directory path, valid UTF-8 without control characters: it is written into
/// the tab-separated `mounts` table the guest reads line by line.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "AbsolutePath", into = "AbsolutePath")]
pub struct HostDirectory(AbsolutePath);

impl TryFrom<AbsolutePath> for HostDirectory {
    type Error = HostDirectoryError;
    fn try_from(path: AbsolutePath) -> Result<Self, HostDirectoryError> {
        Self::parse(path)
    }
}

impl From<HostDirectory> for AbsolutePath {
    fn from(directory: HostDirectory) -> Self {
        directory.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostDirectoryError {
    #[error("can't share {0}: the path is not valid UTF-8")]
    NotUtf8(String),
    #[error("can't share {0:?}: the path contains control characters")]
    ControlCharacters(String),
}

impl HostDirectory {
    /// # Errors
    ///
    /// `NotUtf8`, `ControlCharacters`.
    pub fn parse(path: AbsolutePath) -> Result<Self, HostDirectoryError> {
        let Some(text) = path.to_str() else {
            return Err(HostDirectoryError::NotUtf8(path.display().to_string()));
        };
        if text.chars().any(char::is_control) {
            return Err(HostDirectoryError::ControlCharacters(text.to_owned()));
        }
        Ok(Self(path))
    }

    #[must_use]
    pub fn path(&self) -> &AbsolutePath {
        &self.0
    }

    /// Valid UTF-8 by construction.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.to_str().unwrap_or_default()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareRequest {
    pub directory: HostDirectory,
    pub mode: Mode,
}

/// At most this many shares: each one takes a PCI slot in the guest, and the guest's bus has
/// few; it also keeps a share's index within `u16`.
pub const MAX_SHARES: usize = 24;

/// Validated shares, parents first. Index `i` (from 0) is guest tag `d{i + 1}`.
///
/// Decoding (persisted events) re-checks what the value itself proves (each path, no
/// duplicates, the order); whether each is a directory belongs to the world at start time and
/// is not re-checked.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<ShareRequest>", into = "Vec<ShareRequest>")]
pub struct Shares(Vec<ShareRequest>);

impl TryFrom<Vec<ShareRequest>> for Shares {
    type Error = SharesError;
    fn try_from(requests: Vec<ShareRequest>) -> Result<Self, SharesError> {
        let shares = Self::validate(requests.clone(), &[], |_| true)?;
        if shares.0 == requests {
            Ok(shares)
        } else {
            let first = requests.into_iter().next();
            Err(SharesError(NonEmpty::new(
                ShareProblem::NotInMountOrder {
                    directory: first.map(|share| share.directory),
                },
                vec![],
            )))
        }
    }
}

impl From<Shares> for Vec<ShareRequest> {
    fn from(shares: Shares) -> Self {
        shares.0
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ShareProblem {
    Duplicate {
        directory: HostDirectory,
    },
    NotADirectory {
        directory: HostDirectory,
    },
    HidesHome {
        directory: HostDirectory,
        home: AbsolutePath,
    },
    TooMany {
        count: usize,
    },
    /// Decoded shares that are not parents-first; never produced by `validate`.
    NotInMountOrder {
        directory: Option<HostDirectory>,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("can't share these directories:{}", list(.0))]
pub struct SharesError(pub NonEmpty<ShareProblem>);

fn list(problems: &NonEmpty<ShareProblem>) -> String {
    problems
        .iter()
        .map(|problem| match problem {
            ShareProblem::Duplicate { directory } => {
                format!("\n  {}: given more than once", directory.as_str())
            }
            ShareProblem::NotADirectory { directory } => {
                format!("\n  {}: not a directory", directory.as_str())
            }
            ShareProblem::HidesHome { directory, home } => format!(
                "\n  {}: would hide the VM's home directory {}",
                directory.as_str(),
                home.display()
            ),
            ShareProblem::TooMany { count } => {
                format!("\n  {count} shares, at most {MAX_SHARES} allowed")
            }
            ShareProblem::NotInMountOrder { .. } => "\n  shares are not in mount order".into(),
        })
        .collect()
}

impl Shares {
    /// Checks every request and reports all problems at once, in request order.
    ///
    /// # Errors
    ///
    /// [`SharesError`] with every problem found.
    pub fn validate(
        requests: Vec<ShareRequest>,
        protected: &[AbsolutePath],
        is_directory: impl Fn(&HostDirectory) -> bool,
    ) -> Result<Self, SharesError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut problems = Vec::new();
        let mut valid = Vec::new();
        for request in requests {
            let directory = &request.directory;
            if !seen.insert(directory.clone()) {
                problems.push(ShareProblem::Duplicate {
                    directory: directory.clone(),
                });
            } else if !is_directory(directory) {
                problems.push(ShareProblem::NotADirectory {
                    directory: directory.clone(),
                });
            } else if let Some(home) = protected
                .iter()
                .find(|home| home.is_within(directory.path()))
            {
                problems.push(ShareProblem::HidesHome {
                    directory: directory.clone(),
                    home: home.clone(),
                });
            } else {
                valid.push(request);
            }
        }
        if valid.len() > MAX_SHARES {
            problems.push(ShareProblem::TooMany { count: valid.len() });
        }
        if let Some(problems) = NonEmpty::from_vec(problems) {
            return Err(SharesError(problems));
        }
        valid.sort_by(|left, right| left.directory.cmp(&right.directory));
        Ok(Self(valid))
    }

    /// Each share with its index (from 0); the index fits `u16` by `MAX_SHARES`.
    pub fn indexed(&self) -> impl Iterator<Item = (u16, &ShareRequest)> {
        (0_u16..).zip(&self.0)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[ShareRequest] {
        &self.0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl crate::diagnostic::Diagnostic for HostDirectoryError {
    fn code(&self) -> &'static str {
        match self {
            Self::NotUtf8(_) => "share.directory.not_utf8",
            Self::ControlCharacters(_) => "share.directory.control_characters",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::NotUtf8(_) | Self::ControlCharacters(_) => {
                Some("rename the directory, or share a parent of it".into())
            }
        }
    }
}

impl crate::diagnostic::Diagnostic for SharesError {
    fn code(&self) -> &'static str {
        "share.invalid"
    }
    fn hint(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn directory(text: &str) -> HostDirectory {
        HostDirectory::parse(AbsolutePath::parse(text.into()).unwrap()).unwrap()
    }

    fn request(value: &Value) -> ShareRequest {
        ShareRequest {
            directory: directory(value["path"].as_str().unwrap()),
            mode: serde_json::from_value(value["mode"].clone()).unwrap(),
        }
    }

    fn as_json(problem: &ShareProblem) -> Value {
        match problem {
            ShareProblem::Duplicate { directory } => {
                json!({"path": directory.as_str(), "kind": "duplicate"})
            }
            ShareProblem::NotADirectory { directory } => {
                json!({"path": directory.as_str(), "kind": "not_a_directory"})
            }
            ShareProblem::HidesHome { directory, home } => {
                json!({"path": directory.as_str(), "kind": "hides_home", "home": home.to_str()})
            }
            ShareProblem::TooMany { .. } => json!({"kind": "too_many"}),
            ShareProblem::NotInMountOrder { .. } => json!({"kind": "not_in_mount_order"}),
        }
    }

    #[test]
    fn validation_and_order_match_the_golden_cases() {
        let cases = crate::testing::golden_json("mount_order.json");
        for case in cases.as_array().unwrap() {
            let input = &case["input"];
            let requests = input["mounts"]
                .as_array()
                .unwrap()
                .iter()
                .map(request)
                .collect();
            let protected: Vec<AbsolutePath> = input["protected"]
                .as_array()
                .unwrap()
                .iter()
                .map(|home| AbsolutePath::parse(home.as_str().unwrap().into()).unwrap())
                .collect();
            let not_directories: Vec<&str> = input["not_dirs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|path| path.as_str().unwrap())
                .collect();
            let result = Shares::validate(requests, &protected, |candidate| {
                !not_directories.contains(&candidate.as_str())
            });
            let actual = match result {
                Ok(shares) => json!({"ok": shares.as_slice().iter().map(|share| json!({
                    "path": share.directory.as_str(), "mode": share.mode.as_str()})).collect::<Vec<_>>()}),
                Err(SharesError(problems)) => {
                    json!({"errors": problems.iter().map(as_json).collect::<Vec<_>>()})
                }
            };
            assert_eq!(actual, case["output"], "case {}", case["name"]);
        }
    }

    #[test]
    fn paths_with_control_characters_are_refused() {
        let path = AbsolutePath::parse("/srv/a\tb".into()).unwrap();
        assert!(matches!(
            HostDirectory::parse(path),
            Err(HostDirectoryError::ControlCharacters(_))
        ));
    }
}
