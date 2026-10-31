//! `VmName`: `[A-Za-z0-9][A-Za-z0-9-]{0,62}`. Also the guest's hostname, so it follows hostname
//! label rules; it names directories and the scope unit, so it never contains `/` or `.`.

use serde::{Deserialize, Serialize};

/// Hostname label limit.
pub const MAX_LENGTH: usize = 63;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct VmName(String);

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum VmNameError {
    #[error("bad VM name {name:?}: it is empty")]
    Empty { name: String },
    #[error("bad VM name {name:?}: longer than {MAX_LENGTH} characters")]
    TooLong { name: String },
    #[error("bad VM name {name:?}: it must start with a letter or digit")]
    BadStart { name: String },
    #[error("bad VM name {name:?}: {character:?} isn't a letter, digit or '-'")]
    BadCharacter { name: String, character: char },
}

impl VmName {
    /// # Errors
    ///
    /// See [`VmNameError`].
    pub fn parse(name: String) -> Result<Self, VmNameError> {
        let Some(first) = name.chars().next() else {
            return Err(VmNameError::Empty { name });
        };
        if name.len() > MAX_LENGTH {
            return Err(VmNameError::TooLong { name });
        }
        if !first.is_ascii_alphanumeric() {
            return Err(VmNameError::BadStart { name });
        }
        if let Some(character) = name
            .chars()
            .find(|character| !(character.is_ascii_alphanumeric() || *character == '-'))
        {
            return Err(VmNameError::BadCharacter { name, character });
        }
        Ok(Self(name))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for VmName {
    type Error = VmNameError;
    fn try_from(name: String) -> Result<Self, VmNameError> {
        Self::parse(name)
    }
}

impl From<VmName> for String {
    fn from(name: VmName) -> String {
        name.0
    }
}

impl std::fmt::Display for VmName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl crate::diagnostic::Diagnostic for VmNameError {
    fn code(&self) -> &'static str {
        match self {
            Self::Empty { .. } => "vm_name.empty",
            Self::TooLong { .. } => "vm_name.too_long",
            Self::BadStart { .. } => "vm_name.bad_start",
            Self::BadCharacter { .. } => "vm_name.bad_character",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Empty { .. }
            | Self::TooLong { .. }
            | Self::BadStart { .. }
            | Self::BadCharacter { .. } => Some(format!(
                "use letters, digits and '-', starting with a letter or digit, at most {MAX_LENGTH}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn every_name_of_the_grammar_is_accepted(name in "[A-Za-z0-9][A-Za-z0-9-]{0,62}") {
            prop_assert_eq!(VmName::parse(name.clone()).map(String::from), Ok(name));
        }

        #[test]
        fn anything_with_a_character_outside_the_grammar_is_refused(
            prefix in "[a-z0-9]{1,10}", bad in "[^A-Za-z0-9-]", suffix in "[a-z0-9]{0,10}"
        ) {
            let name = [prefix, bad, suffix].concat();
            prop_assert!(VmName::parse(name).is_err());
        }
    }

    #[test]
    fn empty_too_long_and_dash_first_are_refused() {
        assert!(matches!(
            VmName::parse(String::new()),
            Err(VmNameError::Empty { .. })
        ));
        assert!(matches!(
            VmName::parse("a".repeat(MAX_LENGTH + 1)),
            Err(VmNameError::TooLong { .. })
        ));
        assert!(matches!(
            VmName::parse("-a".into()),
            Err(VmNameError::BadStart { .. })
        ));
    }
}
