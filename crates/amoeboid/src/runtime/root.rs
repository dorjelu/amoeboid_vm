//! Root actor: spawns the journal and listener, answers runtime requests. Root-level strategy:
//! the journal and listener are essential, so the death of either ends the runtime (systemd
//! restarts it; VMs outlive it by design).

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef, SupervisionEvent};

use super::listener::{Listener, ListenerArgs};
use super::paths::RuntimePaths;
use crate::journal::{Journal, JournalArgs, Persistence};
use crate::protocol::{
    OperationId, ReplyBody, RequestBody, RuntimeId, RuntimeReply, RuntimeRequest, ServerFrame,
};

#[derive(Debug)]
pub enum RootMessage {
    /// A request from a session past its handshake; the reply goes to `reply_to`.
    Request {
        operation: OperationId,
        body: RequestBody,
        reply_to: DerivedActorRef<ServerFrame>,
    },
}

pub struct RootArgs {
    pub id: RuntimeId,
    pub persistence: Persistence,
    pub paths: RuntimePaths,
    pub listener: tokio::net::UnixListener,
}

pub struct Root;

pub struct RootState {
    id: RuntimeId,
}

#[derive(Debug, thiserror::Error)]
#[error("essential actor {name} ended: {reason}")]
struct EssentialEnded {
    name: &'static str,
    reason: String,
}

impl Actor for Root {
    type Msg = RootMessage;
    type State = RootState;
    type Arguments = RootArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<RootMessage>,
        args: RootArgs,
    ) -> Result<RootState, ActorProcessingErr> {
        Actor::spawn_linked(
            Some(format!("{}/journal", args.id)),
            Journal,
            JournalArgs {
                persistence: args.persistence,
                paths: args.paths,
            },
            myself.get_cell(),
        )
        .await?;
        Actor::spawn_linked(
            Some(format!("{}/listener", args.id)),
            Listener,
            ListenerArgs {
                listener: args.listener,
                root: myself.clone(),
                runtime: args.id,
            },
            myself.get_cell(),
        )
        .await?;
        Ok(RootState { id: args.id })
    }

    fn handle(
        &self,
        _myself: ActorRef<RootMessage>,
        message: RootMessage,
        _state: &mut RootState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        match message {
            RootMessage::Request {
                operation,
                body,
                reply_to,
            } => {
                let body = match body {
                    RequestBody::Runtime(RuntimeRequest::Ping) => {
                        ReplyBody::Runtime(RuntimeReply::Pong)
                    }
                };
                if reply_to
                    .send_message(ServerFrame::Reply { operation, body })
                    .is_err()
                {
                    tracing::debug!(%operation, "reply undeliverable: session ended");
                }
            }
        }
        std::future::ready(Ok(()))
    }

    fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<RootMessage>,
        event: SupervisionEvent,
        state: &mut RootState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let ended = match event {
            SupervisionEvent::ActorTerminated(cell, _, reason) => Some(EssentialEnded {
                name: name_of(&cell, state.id),
                reason: reason.unwrap_or_else(|| "stopped".into()),
            }),
            SupervisionEvent::ActorFailed(cell, error) => Some(EssentialEnded {
                name: name_of(&cell, state.id),
                reason: error.to_string(),
            }),
            SupervisionEvent::ActorStarted(_) | SupervisionEvent::ProcessGroupChanged(_) => None,
        };
        std::future::ready(match ended {
            Some(ended) => {
                tracing::error!(%ended, "runtime ending");
                Err(ended.into())
            }
            None => Ok(()),
        })
    }
}

fn name_of(cell: &ractor::ActorCell, runtime: RuntimeId) -> &'static str {
    match cell.get_name() {
        Some(name) if name == format!("{runtime}/journal") => "journal",
        Some(name) if name == format!("{runtime}/listener") => "listener",
        Some(_) | None => "unknown child",
    }
}
