//! Client <-> runtime frames (0002 §Protocol). One enum per direction; bodies split per domain.
//! Variants are added with the slice that handles them.

pub mod codec;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// Client-minted operation id, `UUIDv7`; the idempotency key of a request.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct OperationId(Uuid);

/// Runtime instance id, `UUIDv7`, minted once per runtime start.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct RuntimeId(Uuid);

#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("expected a version 7 uuid")]
pub struct NotV7;

macro_rules! uuid_v7 {
    ($t:ident) => {
        impl $t {
            /// A fresh id from the current time.
            #[must_use]
            pub fn mint() -> Self {
                Self(Uuid::now_v7())
            }
        }
        impl TryFrom<Uuid> for $t {
            type Error = NotV7;
            fn try_from(id: Uuid) -> Result<Self, NotV7> {
                if id.get_version() == Some(uuid::Version::SortRand) {
                    Ok(Self(id))
                } else {
                    Err(NotV7)
                }
            }
        }
        impl From<$t> for Uuid {
            fn from(id: $t) -> Uuid {
                id.0
            }
        }
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
uuid_v7!(OperationId);
uuid_v7!(RuntimeId);

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
