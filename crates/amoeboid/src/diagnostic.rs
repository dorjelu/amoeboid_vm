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
    use crate::adapters::process::ProcessError;
    use crate::client::ClientError;
    use crate::domain::account::AccountError;
    use crate::domain::host_key::HostPublicKeyError;
    use crate::domain::share::{HostDirectory, HostDirectoryError, ShareProblem, SharesError};
    use crate::domain::vm_name::VmNameError;
    use crate::journal::{PersistFailure, PersistenceId};
    use crate::persistent::Unrecoverable;
    use crate::protocol::codec::FrameError;
    use crate::protocol::{NotVersion7, Refusal};
    use crate::runtime::StartError;
    use crate::runtime::lock::LockError;
    use crate::runtime::paths::LocationError;

    fn io() -> std::io::Error {
        std::io::Error::from(std::io::ErrorKind::PermissionDenied)
    }

    fn process() -> crate::domain::process::ProcessId {
        crate::domain::process::ProcessId::new(std::num::NonZeroU32::MIN)
    }

    fn json_error() -> serde_json::Error {
        serde_json::from_str::<u8>("x").unwrap_err()
    }

    /// One instance of every variant of every library diagnostic.
    fn protocol_journal_runtime_client() -> Vec<Box<dyn Diagnostic>> {
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
            Box::new(crate::domain::path::PathError::NotAbsolute(
                "relative".into(),
            )),
            Box::new(crate::domain::path::PathError::NotNormal("/a/..".into())),
            Box::new(LocationError::MissingEnvironment("HOME")),
            Box::new(LocationError::SocketTooLong(
                crate::domain::path::AbsolutePath::parse("/s".into()).unwrap(),
            )),
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

    fn domain() -> Vec<Box<dyn Diagnostic>> {
        vec![
            Box::new(LocationError::Path(
                crate::domain::path::PathError::NotNormal("/a/..".into()),
            )),
            Box::new(VmNameError::Empty {
                name: String::new(),
            }),
            Box::new(VmNameError::TooLong { name: "x".into() }),
            Box::new(VmNameError::BadStart { name: "-".into() }),
            Box::new(VmNameError::BadCharacter {
                name: "a.b".into(),
                character: '.',
            }),
            Box::new(AccountError::BadUserName("X".into())),
            Box::new(AccountError::HomeNotAbsolute("/".into())),
            Box::new(AccountError::HomeReserved {
                home: "/etc".into(),
                reserved: "/etc",
            }),
            Box::new(AccountError::HomeControlCharacters("/a\tb".into())),
            Box::new(HostDirectoryError::NotUtf8("/x".into())),
            Box::new(HostDirectoryError::ControlCharacters("/x".into())),
            Box::new(SharesError(crate::nonempty::NonEmpty::new(
                ShareProblem::Duplicate {
                    directory: HostDirectory::parse(
                        crate::domain::path::AbsolutePath::parse("/s".into()).unwrap(),
                    )
                    .unwrap(),
                },
                vec![],
            ))),
            Box::new(HostPublicKeyError),
        ]
    }

    fn adapters() -> Vec<Box<dyn Diagnostic>> {
        vec![
            Box::new(crate::domain::process::BootId::parse("x".into()).unwrap_err()),
            Box::new(crate::domain::guest_path::GuestRelative::parse("..").unwrap_err()),
            Box::new(crate::adapters::files::FileError::Write {
                path: "/f".into(),
                source: io(),
            }),
            Box::new(crate::adapters::files::FileError::Directory {
                path: "/f".into(),
                source: io(),
            }),
            Box::new(crate::adapters::files::FileError::Read {
                path: "/f".into(),
                source: io(),
            }),
            Box::new(crate::adapters::guest::GuestError::Root {
                path: "/g".into(),
                source: io(),
            }),
            Box::new(crate::adapters::guest::GuestError::Write {
                path: "x".into(),
                source: io(),
            }),
            Box::new(ProcessError::BootId(io())),
            Box::new(ProcessError::Read {
                process: process(),
                file: "stat",
                source: io(),
            }),
            Box::new(ProcessError::Parse {
                process: process(),
                file: "stat",
            }),
            Box::new(ProcessError::Spawn {
                program: "qemu".into(),
                source: io(),
            }),
            Box::new(ProcessError::Output {
                path: "/l".into(),
                source: io(),
            }),
            Box::new(ProcessError::Signal {
                process: process(),
                source: io(),
            }),
            Box::new(ProcessError::List(io())),
        ]
    }

    fn catalogue() -> Vec<Box<dyn Diagnostic>> {
        [protocol_journal_runtime_client(), domain(), adapters()]
            .into_iter()
            .flatten()
            .collect()
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
