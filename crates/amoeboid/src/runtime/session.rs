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

use super::root::RootMsg;
use crate::protocol::codec::{self, FrameError, MAX_FRAME};
use crate::protocol::{ClientFrame, ProtocolVersion, Refusal, RuntimeId, ServerFrame};

#[derive(Debug)]
pub enum SessionMsg {
    /// Decoded by the connection's reader task.
    Inbound(ClientFrame),
    /// From the root, written to the client.
    Outbound(ServerFrame),
    /// The reader got a frame it can't decode; answered with this refusal.
    ReadFailed(Refusal),
    /// EOF or a broken connection.
    Closed,
}

impl From<ServerFrame> for SessionMsg {
    fn from(frame: ServerFrame) -> Self {
        Self::Outbound(frame)
    }
}

impl TryFrom<SessionMsg> for ServerFrame {
    type Error = SessionMsg;
    fn try_from(msg: SessionMsg) -> Result<Self, Self::Error> {
        match msg {
            SessionMsg::Outbound(frame) => Ok(frame),
            other @ (SessionMsg::Inbound(_) | SessionMsg::ReadFailed(_) | SessionMsg::Closed) => {
                Err(other)
            }
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
    pub root: ActorRef<RootMsg>,
    pub runtime: RuntimeId,
}

pub struct Session;

pub struct SessionState {
    writer: OwnedWriteHalf,
    reader: JoinHandle<()>,
    phase: Phase,
    root: ActorRef<RootMsg>,
    runtime: RuntimeId,
}

impl Actor for Session {
    type Msg = SessionMsg;
    type State = SessionState;
    type Arguments = SessionArgs;

    fn pre_start(
        &self,
        myself: ActorRef<SessionMsg>,
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
        myself: ActorRef<SessionMsg>,
        msg: SessionMsg,
        state: &mut SessionState,
    ) -> Result<(), ActorProcessingErr> {
        let (reply, end) = match (state.phase, msg) {
            (
                Phase::AwaitingHandshake,
                SessionMsg::Inbound(ClientFrame::HandshakeStart { protocol }),
            ) if protocol == ProtocolVersion::CURRENT => {
                state.phase = Phase::Open;
                let ack = ServerFrame::HandshakeAck {
                    protocol,
                    runtime: state.runtime,
                };
                (Some(ack), false)
            }
            (
                Phase::AwaitingHandshake,
                SessionMsg::Inbound(ClientFrame::HandshakeStart { protocol }),
            ) => {
                let rejected = ServerFrame::HandshakeRejected {
                    runtime: ProtocolVersion::CURRENT,
                    client: protocol,
                };
                (Some(rejected), true)
            }
            (Phase::AwaitingHandshake, SessionMsg::Inbound(ClientFrame::Request { op, .. })) => {
                (Some(refused(Some(op), Refusal::HandshakeRequired)), true)
            }
            (Phase::Open, SessionMsg::Inbound(ClientFrame::HandshakeStart { .. })) => {
                (Some(refused(None, Refusal::HandshakeRepeated)), true)
            }
            (Phase::Open, SessionMsg::Inbound(ClientFrame::Request { op, body })) => {
                let request = RootMsg::Request {
                    op,
                    body,
                    reply_to: myself.get_derived(),
                };
                if state.root.cast(request).is_err() {
                    tracing::warn!(%op, "request dropped: root stopped");
                }
                (None, false)
            }
            (_, SessionMsg::Outbound(frame)) => (Some(frame), false),
            (_, SessionMsg::ReadFailed(reason)) => (Some(refused(None, reason)), true),
            (_, SessionMsg::Closed) => (None, true),
        };
        if let Some(frame) = reply
            && let Err(e) = codec::write(&mut state.writer, &frame).await
        {
            tracing::debug!(error = %e, "can't write to client; closing");
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
        _myself: ActorRef<SessionMsg>,
        state: &mut SessionState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        state.reader.abort();
        std::future::ready(Ok(()))
    }
}

fn refused(op: Option<crate::protocol::OpId>, reason: Refusal) -> ServerFrame {
    ServerFrame::Refused { op, reason }
}

async fn read_loop(mut read: OwnedReadHalf, session: ActorRef<SessionMsg>) {
    loop {
        let msg = match codec::read::<ClientFrame, _>(&mut read).await {
            Ok(Some(frame)) => SessionMsg::Inbound(frame),
            Ok(None) | Err(FrameError::Io(_)) => SessionMsg::Closed,
            Err(FrameError::TooLarge { .. }) => {
                SessionMsg::ReadFailed(Refusal::FrameTooLarge { max: MAX_FRAME })
            }
            Err(FrameError::Decode(_) | FrameError::Encode(_)) => {
                SessionMsg::ReadFailed(Refusal::MalformedFrame)
            }
        };
        let last = matches!(msg, SessionMsg::Closed | SessionMsg::ReadFailed(_));
        if session.cast(msg).is_err() || last {
            return;
        }
    }
}
