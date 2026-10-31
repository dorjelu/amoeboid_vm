//! Accepts client connections and spawns one [`Session`] per connection. Sessions are not
//! linked: a client's failure never reaches the root.

use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::JoinHandle;

use super::root::RootMsg;
use super::session::{Session, SessionArgs};
use crate::protocol::RuntimeId;

#[derive(Debug)]
pub enum ListenerMsg {
    /// `accept` failed; the listener ends and the runtime with it.
    AcceptFailed(String),
}

pub struct ListenerArgs {
    pub listener: tokio::net::UnixListener,
    pub root: ActorRef<RootMsg>,
    pub runtime: RuntimeId,
}

pub struct Listener;

pub struct ListenerState {
    accept: JoinHandle<()>,
}

impl Actor for Listener {
    type Msg = ListenerMsg;
    type State = ListenerState;
    type Arguments = ListenerArgs;

    fn pre_start(
        &self,
        myself: ActorRef<ListenerMsg>,
        args: ListenerArgs,
    ) -> impl Future<Output = Result<ListenerState, ActorProcessingErr>> + Send {
        let accept = tokio::spawn(accept_loop(myself, args));
        std::future::ready(Ok(ListenerState { accept }))
    }

    fn handle(
        &self,
        _myself: ActorRef<ListenerMsg>,
        msg: ListenerMsg,
        _state: &mut ListenerState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let ListenerMsg::AcceptFailed(error) = msg;
        std::future::ready(Err(format!("can't accept connections: {error}").into()))
    }

    fn post_stop(
        &self,
        _myself: ActorRef<ListenerMsg>,
        state: &mut ListenerState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        state.accept.abort();
        std::future::ready(Ok(()))
    }
}

async fn accept_loop(myself: ActorRef<ListenerMsg>, args: ListenerArgs) {
    let uid = rustix::process::getuid().as_raw();
    loop {
        let stream = match args.listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                let _ = myself.cast(ListenerMsg::AcceptFailed(e.to_string()));
                return;
            }
        };
        // The run directory is 0700; this is the second line: same uid only.
        match stream.peer_cred() {
            Ok(cred) if cred.uid() == uid => {}
            Ok(cred) => {
                tracing::warn!(
                    peer_uid = cred.uid(),
                    "refused a connection from another uid"
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(error = %e, "refused a connection without peer credentials");
                continue;
            }
        }
        let session = SessionArgs {
            stream,
            root: args.root.clone(),
            runtime: args.runtime,
        };
        if let Err(e) = Actor::spawn(None, Session, session).await {
            tracing::warn!(error = %e, "can't start a session");
        }
    }
}
