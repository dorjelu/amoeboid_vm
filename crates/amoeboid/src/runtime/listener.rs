//! Accepts client connections and spawns one [`Session`] per connection. Sessions are not
//! linked: a client's failure never reaches the root.

use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::JoinHandle;

use super::root::RootMessage;
use super::session::{Session, SessionArgs};
use crate::protocol::RuntimeId;

#[derive(Debug)]
pub enum ListenerMessage {
    /// `accept` failed; the listener ends and the runtime with it.
    AcceptFailed(String),
}

pub struct ListenerArgs {
    pub listener: tokio::net::UnixListener,
    pub root: ActorRef<RootMessage>,
    pub runtime: RuntimeId,
}

pub struct Listener;

pub struct ListenerState {
    accept: JoinHandle<()>,
}

impl Actor for Listener {
    type Msg = ListenerMessage;
    type State = ListenerState;
    type Arguments = ListenerArgs;

    fn pre_start(
        &self,
        myself: ActorRef<ListenerMessage>,
        args: ListenerArgs,
    ) -> impl Future<Output = Result<ListenerState, ActorProcessingErr>> + Send {
        let accept = tokio::spawn(accept_loop(myself, args));
        std::future::ready(Ok(ListenerState { accept }))
    }

    fn handle(
        &self,
        _myself: ActorRef<ListenerMessage>,
        message: ListenerMessage,
        _state: &mut ListenerState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let ListenerMessage::AcceptFailed(error) = message;
        std::future::ready(Err(format!("can't accept connections: {error}").into()))
    }

    fn post_stop(
        &self,
        _myself: ActorRef<ListenerMessage>,
        state: &mut ListenerState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        state.accept.abort();
        std::future::ready(Ok(()))
    }
}

async fn accept_loop(myself: ActorRef<ListenerMessage>, args: ListenerArgs) {
    let user_id = rustix::process::getuid().as_raw();
    loop {
        let stream = match args.listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                let _ = myself.cast(ListenerMessage::AcceptFailed(error.to_string()));
                return;
            }
        };
        // The run directory is 0700; this is the second line: same user_id only.
        match stream.peer_cred() {
            Ok(credentials) if credentials.uid() == user_id => {}
            Ok(credentials) => {
                tracing::warn!(
                    peer_user_id = credentials.uid(),
                    "refused a connection from another user"
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(error = %error, "refused a connection without peer credentials");
                continue;
            }
        }
        let session = SessionArgs {
            stream,
            root: args.root.clone(),
            runtime: args.runtime,
        };
        if let Err(error) = Actor::spawn(None, Session, session).await {
            tracing::warn!(error = %error, "can't start a session");
        }
    }
}
