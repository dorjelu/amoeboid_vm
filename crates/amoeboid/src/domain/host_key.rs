//! The guest's ssh host public key, as read from the `.pub` file `ssh-keygen` wrote on the host.

/// `ssh-ed25519 <base64>`; the comment is dropped.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct HostPublicKey {
    key: String,
}

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("the VM's public host key is malformed: expected `ssh-ed25519 <base64> [comment]`")]
pub struct HostPublicKeyError;

impl HostPublicKey {
    pub const KIND: &'static str = "ssh-ed25519";

    /// # Errors
    ///
    /// Not `ssh-ed25519`, or a key that is not base64.
    pub fn parse(text: &str) -> Result<Self, HostPublicKeyError> {
        let mut fields = text.split_ascii_whitespace();
        let (Some(kind), Some(key)) = (fields.next(), fields.next()) else {
            return Err(HostPublicKeyError);
        };
        let base64 = key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='));
        if kind == Self::KIND && base64 {
            Ok(Self {
                key: key.to_owned(),
            })
        } else {
            Err(HostPublicKeyError)
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl crate::diagnostic::Diagnostic for HostPublicKeyError {
    fn code(&self) -> &'static str {
        "host_key.malformed"
    }
    fn hint(&self) -> Option<String> {
        Some("remove the VM's ssh directory so the key is generated again".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ed25519_line_parses_and_drops_the_comment() {
        let key = HostPublicKey::parse("ssh-ed25519 AAAAC3Nza+/= amoeboid-work\n").unwrap();
        assert_eq!(key.key(), "AAAAC3Nza+/=");
    }

    #[test]
    fn other_kinds_and_non_base64_keys_are_refused() {
        assert!(HostPublicKey::parse("ssh-rsa AAAA").is_err());
        assert!(HostPublicKey::parse("ssh-ed25519 AA\u{7}A").is_err());
        assert!(HostPublicKey::parse("ssh-ed25519").is_err());
    }
}
