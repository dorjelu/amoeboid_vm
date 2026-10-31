//! Client side of the protocol: connect, handshake, one request at a time.

use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::protocol::codec::{self, FrameError};
use crate::protocol::{
    ClientFrame, OperationId, ProtocolVersion, Refusal, ReplyBody, RequestBody, RuntimeId,
    ServerFrame,
};
use crate::runtime::paths::RuntimePaths;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no runtime answers at {socket}: {source}")]
    Unreachable {
        socket: String,
        source: std::io::Error,
    },
    #[error("the runtime speaks protocol {runtime}, this client {client}")]
    Rejected {
        runtime: ProtocolVersion,
        client: ProtocolVersion,
    },
    #[error("the runtime refused: {0}")]
    Refused(Refusal),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("the runtime closed the connection")]
    Closed,
    #[error("unexpected frame from the runtime: {0:?}")]
    Unexpected(Box<ServerFrame>),
}

impl ClientError {
    /// An imperative remedy, when there is one.
    #[must_use]
    pub fn hint(&self) -> Option<&'static str> {
        match self {
            Self::Unreachable { .. } => Some("start it: systemctl --user start amoeboid.socket"),
            Self::Rejected { .. } => Some("restart it: amoeboid daemon restart"),
            Self::Refused(_) | Self::Frame(_) | Self::Closed | Self::Unexpected(_) => None,
        }
    }
}

pub struct Client {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    runtime: RuntimeId,
}

impl Client {
    /// Connects and completes the handshake.
    ///
    /// # Errors
    ///
    /// See [`ClientError`].
    pub async fn connect(paths: &RuntimePaths) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(paths.socket())
            .await
            .map_err(|source| ClientError::Unreachable {
                socket: paths.socket().display().to_string(),
                source,
            })?;
        let (mut reader, mut writer) = stream.into_split();
        codec::write(
            &mut writer,
            &ClientFrame::HandshakeStart {
                protocol: ProtocolVersion::CURRENT,
            },
        )
        .await?;
        match next(&mut reader).await? {
            ServerFrame::HandshakeAccepted { runtime, .. } => Ok(Self {
                reader,
                writer,
                runtime,
            }),
            ServerFrame::HandshakeRejected { runtime, client } => {
                Err(ClientError::Rejected { runtime, client })
            }
            ServerFrame::Refused { reason, .. } => Err(ClientError::Refused(reason)),
            other @ ServerFrame::Reply { .. } => Err(ClientError::Unexpected(Box::new(other))),
        }
    }

    #[must_use]
    pub fn runtime(&self) -> RuntimeId {
        self.runtime
    }

    /// Sends one request under a fresh [`OperationId`] and waits for its reply.
    ///
    /// # Errors
    ///
    /// See [`ClientError`].
    pub async fn request(&mut self, body: RequestBody) -> Result<ReplyBody, ClientError> {
        let operation = OperationId::mint();
        codec::write(&mut self.writer, &ClientFrame::Request { operation, body }).await?;
        match next(&mut self.reader).await? {
            ServerFrame::Reply {
                operation: answered,
                body,
            } if answered == operation => Ok(body),
            ServerFrame::Refused { reason, .. } => Err(ClientError::Refused(reason)),
            other @ (ServerFrame::Reply { .. }
            | ServerFrame::HandshakeAccepted { .. }
            | ServerFrame::HandshakeRejected { .. }) => {
                Err(ClientError::Unexpected(Box::new(other)))
            }
        }
    }
}

async fn next(reader: &mut OwnedReadHalf) -> Result<ServerFrame, ClientError> {
    codec::read(reader).await?.ok_or(ClientError::Closed)
}
