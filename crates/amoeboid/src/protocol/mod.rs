//! Client <-> runtime frames (0002 §Protocol). One enum per direction; bodies split per domain.
//! Variants are added with the slice that handles them.

pub mod codec;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Exact-match protocol version; any difference is refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    pub const CURRENT: Self = Self(1);
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Client-minted operation id, a ULID; the idempotency key of a request.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(Ulid);

/// Runtime instance id, a ULID minted once per runtime start.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeId(Ulid);

/// ULIDs on the wire are their 26-character Crockford base32 text, checked on decode.
macro_rules! ulid_identifier {
    ($type:ident) => {
        impl $type {
            /// A fresh id from the current time.
            #[must_use]
            pub fn mint() -> Self {
                Self(Ulid::generate())
            }
        }
        impl std::fmt::Display for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}
ulid_identifier!(OperationId);
ulid_identifier!(RuntimeId);

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "body",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ClientFrame {
    /// First frame on every connection; anything else first is `Refused { HandshakeRequired }`.
    HandshakeStart { protocol: ProtocolVersion },
    Request {
        operation: OperationId,
        body: RequestBody,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "body",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ServerFrame {
    HandshakeAccepted {
        protocol: ProtocolVersion,
        runtime: RuntimeId,
    },
    Reply {
        operation: OperationId,
        body: ReplyBody,
    },
    /// Answer to a `HandshakeStart` with another protocol version; ends the connection.
    /// Hint: `amoeboid daemon restart`.
    HandshakeRejected {
        runtime: ProtocolVersion,
        client: ProtocolVersion,
    },
    /// Ends the connection.
    Refused {
        operation: Option<OperationId>,
        reason: Refusal,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(
    tag = "domain",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RequestBody {
    Runtime(RuntimeRequest),
    Vm(VmRequest),
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(
    tag = "domain",
    content = "reply",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ReplyBody {
    Runtime(RuntimeReply),
    Vm(VmReply),
    /// The operation failed; rendered by the client like a local error.
    Failed(crate::diagnostic::Report),
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeRequest {
    Ping,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeReply {
    Pong,
}

/// Requests about one VM, routed by Root to its twin.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case", deny_unknown_fields)]
pub enum VmRequest {
    /// Start (creating the VM if new). Shares arrive syntactically valid; the runtime checks
    /// them against the world (each a directory, none hiding the home) before the twin sees
    /// them.
    Start {
        name: crate::domain::vm_name::VmName,
        shares: Vec<crate::domain::share::ShareRequest>,
        /// Create or rebase from this config directory; `None` keeps the VM's own.
        nix_config: Option<crate::domain::path::AbsolutePath>,
    },
    /// Power the VM off (gracefully, then by force), or take down what a failed start left.
    Stop {
        name: crate::domain::vm_name::VmName,
    },
    /// One VM, or every VM when `name` is `None`.
    Status {
        name: Option<crate::domain::vm_name::VmName>,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case", deny_unknown_fields)]
pub enum VmReply {
    Started {
        name: crate::domain::vm_name::VmName,
        ssh: crate::vm::SshEndpoint,
    },
    /// Also when it was already stopped.
    Stopped {
        name: crate::domain::vm_name::VmName,
    },
    Status {
        vms: Vec<crate::vm::VmStatus>,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Refusal {
    #[error("the first frame must be a handshake")]
    HandshakeRequired,
    #[error("the connection already completed its handshake")]
    HandshakeRepeated,
    #[error("the frame doesn't decode")]
    MalformedFrame,
    #[error("the frame exceeds {max} bytes")]
    FrameTooLarge { max: u32 },
}

#[cfg(test)]
mod tests;

impl crate::diagnostic::Diagnostic for Refusal {
    fn code(&self) -> &'static str {
        match self {
            Self::HandshakeRequired => "protocol.refusal.handshake_required",
            Self::HandshakeRepeated => "protocol.refusal.handshake_repeated",
            Self::MalformedFrame => "protocol.refusal.malformed_frame",
            Self::FrameTooLarge { .. } => "protocol.refusal.frame_too_large",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            // A client built from the same tree never sends these; another client is wrong.
            Self::HandshakeRequired
            | Self::HandshakeRepeated
            | Self::MalformedFrame
            | Self::FrameTooLarge { .. } => None,
        }
    }
}
