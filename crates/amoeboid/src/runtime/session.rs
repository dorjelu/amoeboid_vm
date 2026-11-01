//! One actor per client connection: handshake, frame codec, routing to the root.
//!
//! ```text
//! AwaitingHandshake ─HandshakeStart(current)─▶ Open ─Request─▶ Root ─ServerFrame─▶ client
//!        │ other version ─▶ HandshakeRejected, stop
//!        │ anything else ─▶ Refused{HandshakeRequired}, stop
//! ```

use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinHandle;

use super::root::RootMessage;
use crate::protocol::codec::{self, FrameError, MAX_FRAME};
use crate::protocol::{ClientFrame, ProtocolVersion, Refusal, RuntimeId, ServerFrame};

#[derive(Debug)]
pub enum SessionMessage {
    /// Decoded by the connection's reader task.
    Inbound(ClientFrame),
    /// From the root, written to the client.
    Outbound(ServerFrame),
    /// The reader got a frame it can't decode; answered with this refusal.
    ReadFailed(Refusal),
    /// EOF or a broken connection.
    Closed,
}

impl From<ServerFrame> for SessionMessage {
    fn from(frame: ServerFrame) -> Self {
        Self::Outbound(frame)
    }
}

impl TryFrom<SessionMessage> for ServerFrame {
    type Error = SessionMessage;
    fn try_from(message: SessionMessage) -> Result<Self, Self::Error> {
        match message {
            SessionMessage::Outbound(frame) => Ok(frame),
            other @ (SessionMessage::Inbound(_)
            | SessionMessage::ReadFailed(_)
            | SessionMessage::Closed) => Err(other),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    AwaitingHandshake,
    Open,
}

pub struct SessionArgs {
    pub stream: UnixStream,
    pub root: ActorRef<RootMessage>,
    pub runtime: RuntimeId,
}

pub struct Session;

pub struct SessionState {
    writer: OwnedWriteHalf,
    reader: JoinHandle<()>,
    phase: Phase,
    root: ActorRef<RootMessage>,
    runtime: RuntimeId,
}

impl Actor for Session {
    type Msg = SessionMessage;
    type State = SessionState;
    type Arguments = SessionArgs;

    fn pre_start(
        &self,
        myself: ActorRef<SessionMessage>,
        args: SessionArgs,
    ) -> impl Future<Output = Result<SessionState, ActorProcessingErr>> + Send {
        let (read, writer) = args.stream.into_split();
        let reader = tokio::spawn(read_loop(read, myself));
        std::future::ready(Ok(SessionState {
            writer,
            reader,
            phase: Phase::AwaitingHandshake,
            root: args.root,
            runtime: args.runtime,
        }))
    }

    async fn handle(
        &self,
        myself: ActorRef<SessionMessage>,
        message: SessionMessage,
        state: &mut SessionState,
    ) -> Result<(), ActorProcessingErr> {
        let (reply, end) = match (state.phase, message) {
            (
                Phase::AwaitingHandshake,
                SessionMessage::Inbound(ClientFrame::HandshakeStart { protocol }),
            ) if protocol == ProtocolVersion::CURRENT => {
                state.phase = Phase::Open;
                let ack = ServerFrame::HandshakeAccepted {
                    protocol,
                    runtime: state.runtime,
                };
                (Some(ack), false)
            }
            (
                Phase::AwaitingHandshake,
                SessionMessage::Inbound(ClientFrame::HandshakeStart { protocol }),
            ) => {
                let rejected = ServerFrame::HandshakeRejected {
                    runtime: ProtocolVersion::CURRENT,
                    client: protocol,
                };
                (Some(rejected), true)
            }
            (
                Phase::AwaitingHandshake,
                SessionMessage::Inbound(
                    ClientFrame::Request { operation, .. }
                    | ClientFrame::Subscribe { operation, .. },
                ),
            ) => (
                Some(refused(Some(operation), Refusal::HandshakeRequired)),
                true,
            ),
            (Phase::Open, SessionMessage::Inbound(ClientFrame::HandshakeStart { .. })) => {
                (Some(refused(None, Refusal::HandshakeRepeated)), true)
            }
            (Phase::Open, SessionMessage::Inbound(ClientFrame::Request { operation, body })) => {
                let request = RootMessage::Request {
                    operation,
                    body,
                    reply_to: myself.get_derived(),
                };
                if state.root.cast(request).is_err() {
                    tracing::warn!(%operation, "request dropped: root stopped");
                }
                (None, false)
            }
            (Phase::Open, SessionMessage::Inbound(ClientFrame::Subscribe { operation, from })) => {
                let subscribe = RootMessage::Subscribe {
                    operation,
                    from,
                    reply_to: myself.get_derived(),
                };
                if state.root.cast(subscribe).is_err() {
                    tracing::warn!(%operation, "subscription dropped: root stopped");
                }
                (None, false)
            }
            (_, SessionMessage::Outbound(frame)) => (Some(frame), false),
            (_, SessionMessage::ReadFailed(reason)) => (Some(refused(None, reason)), true),
            (_, SessionMessage::Closed) => (None, true),
        };
        if let Some(frame) = reply
            && let Err(error) = codec::write(&mut state.writer, &frame).await
        {
            tracing::debug!(error = %error, "can't write to client; closing");
            myself.stop(None);
            return Ok(());
        }
        if end {
            myself.stop(None);
        }
        Ok(())
    }

    fn post_stop(
        &self,
        _myself: ActorRef<SessionMessage>,
        state: &mut SessionState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        state.reader.abort();
        std::future::ready(Ok(()))
    }
}

fn refused(operation: Option<crate::protocol::OperationId>, reason: Refusal) -> ServerFrame {
    ServerFrame::Refused { operation, reason }
}

async fn read_loop(mut read: OwnedReadHalf, session: ActorRef<SessionMessage>) {
    loop {
        let message = match codec::read::<ClientFrame, _>(&mut read).await {
            Ok(Some(frame)) => SessionMessage::Inbound(frame),
            Ok(None) | Err(FrameError::Io(_)) => SessionMessage::Closed,
            Err(FrameError::TooLarge { .. }) => {
                SessionMessage::ReadFailed(Refusal::FrameTooLarge { max: MAX_FRAME })
            }
            Err(FrameError::Decode(_) | FrameError::Encode(_)) => {
                SessionMessage::ReadFailed(Refusal::MalformedFrame)
            }
        };
        let last = matches!(
            message,
            SessionMessage::Closed | SessionMessage::ReadFailed(_)
        );
        if session.cast(message).is_err() || last {
            return;
        }
    }
}
