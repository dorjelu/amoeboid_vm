//! How errors present themselves to the operator (0002 §Diagnostics): a stable code, a message
//! and an optional remedy. Each error enum implements [`Diagnostic`] with an exhaustive `match`,
//! so a new variant does not compile without its code and hint.

use serde::{Deserialize, Serialize};

pub trait Diagnostic: std::fmt::Display {
    /// Stable dotted id, unique across the library: `runtime.lock.held`.
    fn code(&self) -> &'static str;

    /// Lowercase, no trailing period, action + object: `can't write {path}: {reason}`.
    fn message(&self) -> String {
        self.to_string()
    }

    /// An imperative remedy, when there is one.
    fn hint(&self) -> Option<String>;
}

/// A diagnostic as data: what crosses the socket and what the CLI renders.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
}

impl Report {
    pub fn of(diagnostic: &(impl Diagnostic + ?Sized)) -> Self {
        Self {
            code: diagnostic.code().to_owned(),
            message: diagnostic.message(),
            hint: diagnostic.hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::client::ClientError;
    use crate::journal::{PersistFailure, PersistenceId};
    use crate::persistent::Unrecoverable;
    use crate::protocol::codec::FrameError;
    use crate::protocol::{NotVersion7, Refusal};
    use crate::runtime::StartError;
    use crate::runtime::lock::LockError;
    use crate::runtime::paths::PathError;

    fn io() -> std::io::Error {
        std::io::Error::from(std::io::ErrorKind::PermissionDenied)
    }

    fn json_error() -> serde_json::Error {
        serde_json::from_str::<u8>("x").unwrap_err()
    }

    /// One instance of every variant of every library diagnostic.
    fn catalogue() -> Vec<Box<dyn Diagnostic>> {
        let sequence = crate::journal::Sequence::FIRST;
        vec![
            Box::new(NotVersion7),
            Box::new(FrameError::TooLarge { length: 1 }),
            Box::new(FrameError::Decode(json_error())),
            Box::new(FrameError::Encode(json_error())),
            Box::new(FrameError::Io(io())),
            Box::new(Refusal::HandshakeRequired),
            Box::new(Refusal::HandshakeRepeated),
            Box::new(Refusal::MalformedFrame),
            Box::new(Refusal::FrameTooLarge { max: 1 }),
            Box::new(PersistenceId::parse("X".into()).unwrap_err()),
            Box::new(PersistFailure::Conflict { first: 1, next: 2 }),
            Box::new(PersistFailure::Store(
                crate::journal::tests_support::store_error("x"),
            )),
            Box::new(Unrecoverable::UndecodableEvent {
                at: sequence,
                error: "x".into(),
            }),
            Box::new(Unrecoverable::UnknownEventVersion {
                at: sequence,
                found: 9,
            }),
            Box::new(PathError::NotAbsolute("rel".into())),
            Box::new(PathError::NotNormal("/a/..".into())),
            Box::new(PathError::MissingEnvironment("HOME")),
            Box::new(PathError::SocketTooLong("/s".into())),
            Box::new(LockError::Open(io())),
            Box::new(LockError::Lock(io())),
            Box::new(LockError::Record(io())),
            Box::new(StartError::AlreadyRunning { holder: None }),
            Box::new(StartError::Directory {
                directory: "/d".into(),
                source: io(),
            }),
            Box::new(StartError::Lock(LockError::Open(io()))),
            Box::new(StartError::Listen {
                socket: "/s".into(),
                source: io(),
            }),
            Box::new(StartError::Spawn("x".into())),
            Box::new(ClientError::Unreachable {
                socket: "/s".into(),
                source: io(),
            }),
            Box::new(ClientError::Rejected {
                runtime: crate::protocol::ProtocolVersion::CURRENT,
                client: crate::protocol::ProtocolVersion::CURRENT,
            }),
            Box::new(ClientError::Refused(Refusal::MalformedFrame)),
            Box::new(ClientError::Frame(FrameError::Io(io()))),
            Box::new(ClientError::Closed),
        ]
    }

    #[test]
    fn codes_are_unique_dotted_lowercase_ids() {
        let mut seen = BTreeMap::new();
        for diagnostic in catalogue() {
            let code = diagnostic.code();
            assert!(
                code.split('.').count() >= 2
                    && code.split('.').all(|segment| !segment.is_empty()
                        && segment.bytes().all(|byte| byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || byte == b'_')),
                "{code:?} is not a dotted lowercase id"
            );
            // Wrapping variants (StartError::Lock) reuse the inner code; the message must match.
            if let Some(previous) = seen.insert(code, diagnostic.message()) {
                assert_eq!(
                    previous,
                    diagnostic.message(),
                    "{code} used by two diagnostics"
                );
            }
        }
    }

    #[test]
    fn messages_are_lowercase_without_a_trailing_period() {
        for diagnostic in catalogue() {
            let message = diagnostic.message();
            assert!(
                !message.starts_with(|c: char| c.is_uppercase()) && !message.ends_with('.'),
                "{}: {message:?}",
                diagnostic.code()
            );
        }
    }

    #[test]
    fn a_report_carries_code_message_and_hint() {
        let error = ClientError::Closed;
        let report = Report::of(&error);
        assert_eq!(report.code, "client.closed");
        assert_eq!(report.message, error.to_string());
    }
}
