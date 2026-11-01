//! Client side of the protocol: connect, handshake, one operation at a time (its progress, then
//! its reply).

use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::protocol::codec::{self, FrameError};
use crate::protocol::{
    ClientFrame, EventSequence, OperationId, ProgressEvent, ProtocolVersion, Refusal, ReplyBody,
    RequestBody, RuntimeId, ServerFrame,
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

impl crate::diagnostic::Diagnostic for ClientError {
    fn code(&self) -> &'static str {
        match self {
            Self::Unreachable { .. } => "client.unreachable",
            Self::Rejected { .. } => "client.rejected",
            Self::Refused(_) => "client.refused",
            Self::Frame(_) => "client.frame",
            Self::Closed => "client.closed",
            Self::Unexpected(_) => "client.unexpected",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Unreachable { .. } => {
                Some("start it: systemctl --user start amoeboid.socket".into())
            }
            Self::Rejected { .. } => {
                Some("restart it: systemctl --user restart amoeboid.service".into())
            }
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
            other @ (ServerFrame::Reply { .. } | ServerFrame::Event { .. }) => {
                Err(ClientError::Unexpected(Box::new(other)))
            }
        }
    }

    #[must_use]
    pub fn runtime(&self) -> RuntimeId {
        self.runtime
    }

    /// Sends one request under a fresh [`OperationId`] and waits for its reply, ignoring its
    /// progress.
    ///
    /// # Errors
    ///
    /// See [`ClientError`].
    pub async fn request(&mut self, body: RequestBody) -> Result<ReplyBody, ClientError> {
        self.request_as(OperationId::mint(), body, |_, _| {}).await
    }

    /// Sends one request under `operation` (a retry repeats it) and waits for its reply,
    /// handing each progress event to `on_event` as it arrives.
    ///
    /// # Errors
    ///
    /// See [`ClientError`].
    pub async fn request_as(
        &mut self,
        operation: OperationId,
        body: RequestBody,
        on_event: impl FnMut(EventSequence, ProgressEvent),
    ) -> Result<ReplyBody, ClientError> {
        codec::write(&mut self.writer, &ClientFrame::Request { operation, body }).await?;
        self.until_reply(operation, on_event).await
    }

    /// Re-attaches to `operation`: its progress from `from`, then its reply.
    ///
    /// # Errors
    ///
    /// See [`ClientError`].
    pub async fn subscribe(
        &mut self,
        operation: OperationId,
        from: Option<EventSequence>,
        on_event: impl FnMut(EventSequence, ProgressEvent),
    ) -> Result<ReplyBody, ClientError> {
        codec::write(
            &mut self.writer,
            &ClientFrame::Subscribe { operation, from },
        )
        .await?;
        self.until_reply(operation, on_event).await
    }

    async fn until_reply(
        &mut self,
        operation: OperationId,
        mut on_event: impl FnMut(EventSequence, ProgressEvent),
    ) -> Result<ReplyBody, ClientError> {
        loop {
            match next(&mut self.reader).await? {
                ServerFrame::Event {
                    operation: about,
                    sequence,
                    event,
                } if about == operation => on_event(sequence, event),
                ServerFrame::Reply {
                    operation: answered,
                    body,
                } if answered == operation => return Ok(body),
                ServerFrame::Refused { reason, .. } => return Err(ClientError::Refused(reason)),
                other @ (ServerFrame::Reply { .. }
                | ServerFrame::Event { .. }
                | ServerFrame::HandshakeAccepted { .. }
                | ServerFrame::HandshakeRejected { .. }) => {
                    return Err(ClientError::Unexpected(Box::new(other)));
                }
            }
        }
    }
}

async fn next(reader: &mut OwnedReadHalf) -> Result<ServerFrame, ClientError> {
    codec::read(reader).await?.ok_or(ClientError::Closed)
}
