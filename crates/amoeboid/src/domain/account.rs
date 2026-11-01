//! The guest account: user name, ids and home. They mirror the host user, so paths and file
//! ownership carry across the shares unchanged.

use serde::{Deserialize, Serialize};

/// `[a-z_][a-z0-9_-]{0,31}`: the portable Linux user name grammar.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct UserName(String);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserId(u32);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupId(u32);

/// The guest user's home: the host user's home path, so paths paste across. Absolute, not `/`,
/// outside the directories the guest system uses, no control characters (it is written into
/// `host.nix` and read by shell code in the guest).
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GuestHome(String);

/// Directories the guest system owns; a home at or under one would collide with it.
pub const RESERVED_HOMES: [&str; 15] = [
    "/nix",
    "/mnt/amoeboid",
    "/etc",
    "/proc",
    "/sys",
    "/dev",
    "/run",
    "/boot",
    "/root",
    "/tmp",
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib64",
];

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum AccountError {
    #[error(
        "bad user name {0:?}: use lowercase letters, digits, '_' and '-', starting with a letter or '_', at most 32"
    )]
    BadUserName(String),
    #[error("can't use {0:?} as the VM's home directory: not an absolute path below /")]
    HomeNotAbsolute(String),
    #[error("can't use {home:?} as the VM's home directory: the guest system uses {reserved}")]
    HomeReserved {
        home: String,
        reserved: &'static str,
    },
    #[error("can't use {0:?} as the VM's home directory: it contains control characters")]
    HomeControlCharacters(String),
}

impl UserName {
    /// # Errors
    ///
    /// `BadUserName`.
    pub fn parse(name: String) -> Result<Self, AccountError> {
        let mut bytes = name.bytes();
        let valid = bytes
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first == b'_')
            && name.len() <= 32
            && bytes.all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
            });
        if valid {
            Ok(Self(name))
        } else {
            Err(AccountError::BadUserName(name))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl UserId {
    #[must_use]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl GroupId {
    #[must_use]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl GuestHome {
    /// # Errors
    ///
    /// `HomeNotAbsolute`, `HomeReserved`, `HomeControlCharacters`.
    pub fn parse(home: String) -> Result<Self, AccountError> {
        let segments: Vec<&str> = home.split('/').skip(1).collect();
        let normal = home.starts_with('/')
            && segments
                .iter()
                .all(|segment| !segment.is_empty() && *segment != "." && *segment != "..");
        if !normal {
            return Err(AccountError::HomeNotAbsolute(home));
        }
        if let Some(reserved) = RESERVED_HOMES.iter().copied().find(|reserved| {
            home == *reserved
                || home
                    .strip_prefix(reserved)
                    .is_some_and(|rest| rest.starts_with('/'))
        }) {
            return Err(AccountError::HomeReserved { home, reserved });
        }
        if home
            .chars()
            .any(|character| character < ' ' || character == '\u{7f}')
        {
            return Err(AccountError::HomeControlCharacters(home));
        }
        Ok(Self(home))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! string_conversions {
    ($type:ident) => {
        impl TryFrom<String> for $type {
            type Error = AccountError;
            fn try_from(value: String) -> Result<Self, AccountError> {
                Self::parse(value)
            }
        }
        impl From<$type> for String {
            fn from(value: $type) -> String {
                value.0
            }
        }
    };
}
string_conversions!(UserName);
string_conversions!(GuestHome);

impl crate::diagnostic::Diagnostic for AccountError {
    fn code(&self) -> &'static str {
        match self {
            Self::BadUserName(_) => "account.user_name.invalid",
            Self::HomeNotAbsolute(_) => "account.home.not_absolute",
            Self::HomeReserved { .. } => "account.home.reserved",
            Self::HomeControlCharacters(_) => "account.home.control_characters",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::BadUserName(_) => None,
            Self::HomeNotAbsolute(_)
            | Self::HomeReserved { .. }
            | Self::HomeControlCharacters(_) => {
                Some("run the runtime with HOME set to your home directory".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn guest_homes_match_the_golden_verdicts() {
        let cases: Value = crate::testing::golden_json("guest_home.json");
        for case in cases.as_array().unwrap() {
            let home = case["input"]["home"].as_str().unwrap().to_owned();
            let kind = match GuestHome::parse(home) {
                Ok(_) => "ok",
                Err(AccountError::HomeNotAbsolute(_)) => "not_absolute_or_root",
                Err(AccountError::HomeReserved { .. }) => "reserved",
                Err(AccountError::HomeControlCharacters(_)) => "control_characters",
                Err(AccountError::BadUserName(_)) => "user_name",
            };
            let expected = case["output"]["kind"].as_str().unwrap_or("ok");
            assert_eq!(kind, expected, "case {}", case["name"]);
        }
    }

    #[test]
    fn user_names_follow_the_portable_grammar() {
        for good in ["alice", "_svc-1", "a", "u_2-x"] {
            assert!(UserName::parse(good.into()).is_ok(), "{good}");
        }
        for bad in ["", "Alice", "1user", "-x", "a b", &"a".repeat(33)] {
            assert!(UserName::parse(bad.into()).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_guest_home_with_dot_segments_is_refused() {
        for home in ["/home/./alice", "/home/../alice", "/home//alice"] {
            assert!(GuestHome::parse(home.into()).is_err(), "{home}");
        }
    }
}
