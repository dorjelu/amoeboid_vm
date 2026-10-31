//! The runtime (0002 §Runtime start and ownership): `daemon.lock`, root actor, listener and
//! client sessions.
//!
//! ```text
//! Runtime::start ─ lock ─ bind/adopt socket ─▶ Root
//!                                               ├─ Journal    (linked: its death ends the runtime)
//!                                               └─ Listener   (linked) ─ accept ─▶ Session × client
//! ```

pub mod listener;
pub mod lock;
pub mod paths;
pub mod root;
pub mod session;
#[cfg(test)]
mod tests;

use std::os::unix::fs::DirBuilderExt as _;

use ractor::{Actor, ActorRef};
use tokio::task::JoinHandle;

use crate::journal::Persistence;
use crate::protocol::RuntimeId;
use lock::{Acquire, DaemonLock, LockError, LockRecord};
use paths::RuntimePaths;
use root::{Root, RootArgs, RootMsg};

/// Where the listening socket comes from and who restarts the process.
#[derive(Debug)]
pub enum Hosting {
    /// Socket activation; the unit has `Restart=on-failure`.
    SystemdUser { listener: ActivatedSocket },
    /// The embedding process binds `RuntimePaths::socket` and owns restarts; tests use it.
    InProcess,
}

/// The listener passed by systemd (`LISTEN_FDS`).
#[derive(Debug)]
pub struct ActivatedSocket(std::os::unix::net::UnixListener);

impl ActivatedSocket {
    /// The first socket passed by systemd; `None` when not socket-activated.
    ///
    /// # Errors
    ///
    /// The passed fd is not a Unix stream socket.
    pub fn from_env() -> std::io::Result<Option<Self>> {
        listenfd::ListenFd::from_env()
            .take_unix_listener(0)
            .map(|listener| listener.map(Self))
    }
}

#[derive(Debug)]
pub struct RuntimeConfig {
    pub paths: RuntimePaths,
    pub hosting: Hosting,
    pub persistence: Persistence,
    /// Recorded in `daemon.lock` for diagnostics.
    pub version: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("a runtime is already running{}", holder.as_ref().map_or(String::new(), |h| format!(" (pid {}, version {})", h.pid, h.version)))]
    AlreadyRunning { holder: Option<LockRecord> },
    #[error("can't create {dir}: {source}")]
    Dir { dir: String, source: std::io::Error },
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error("can't listen on {socket}: {source}")]
    Listen {
        socket: String,
        source: std::io::Error,
    },
    #[error("can't start the root actor: {0}")]
    Spawn(String),
}

/// A running runtime. Dropping it does not stop it; call [`Runtime::stop`].
pub struct Runtime {
    id: RuntimeId,
    root: ActorRef<RootMsg>,
    join: JoinHandle<()>,
    _lock: DaemonLock,
}

impl Runtime {
    /// Takes `daemon.lock`, gets the listening socket, then starts the actor tree.
    ///
    /// # Errors
    ///
    /// See [`StartError`].
    pub async fn start(config: RuntimeConfig) -> Result<Self, StartError> {
        let RuntimeConfig {
            paths,
            hosting,
            persistence,
            version,
        } = config;
        for dir in [paths.data(), paths.run()] {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir.as_path())
                .map_err(|source| StartError::Dir {
                    dir: dir.as_path().display().to_string(),
                    source,
                })?;
        }
        let record = LockRecord {
            pid: std::process::id(),
            version,
        };
        let lock = match lock::acquire(&paths, &record)? {
            Acquire::Acquired(lock) => lock,
            Acquire::Held { holder } => return Err(StartError::AlreadyRunning { holder }),
        };
        let listener = listen(&paths, hosting).map_err(|source| StartError::Listen {
            socket: paths.socket().display().to_string(),
            source,
        })?;
        let id = RuntimeId::mint();
        tracing::info!(%id, "runtime starting");
        let (root, join) = Actor::spawn(
            None,
            Root,
            RootArgs {
                id,
                persistence,
                paths,
                listener,
            },
        )
        .await
        .map_err(|e| StartError::Spawn(e.to_string()))?;
        Ok(Self {
            id,
            root,
            join,
            _lock: lock,
        })
    }

    #[must_use]
    pub fn id(&self) -> RuntimeId {
        self.id
    }

    /// Asks the actor tree to stop; [`Runtime::wait`] returns once it has.
    pub fn stop(&self) {
        self.root.stop(None);
    }

    /// A handle that can stop the runtime while [`Runtime::wait`] owns it.
    #[must_use]
    pub fn stopper(&self) -> Stopper {
        Stopper(self.root.clone())
    }

    /// Waits for the actor tree to end, then releases `daemon.lock`.
    pub async fn wait(self) {
        if let Err(e) = self.join.await {
            tracing::error!(error = %e, "root actor task failed");
        }
    }
}

#[derive(Clone)]
pub struct Stopper(ActorRef<RootMsg>);

impl Stopper {
    pub fn stop(&self) {
        self.0.stop(None);
    }
}

/// The lock is held, so a socket file left by a dead runtime is ours to replace.
fn listen(paths: &RuntimePaths, hosting: Hosting) -> std::io::Result<tokio::net::UnixListener> {
    let std_listener = match hosting {
        Hosting::SystemdUser { listener } => listener.0,
        Hosting::InProcess => {
            match std::fs::remove_file(paths.socket()) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            std::os::unix::net::UnixListener::bind(paths.socket())?
        }
    };
    std_listener.set_nonblocking(true)?;
    tokio::net::UnixListener::from_std(std_listener)
}
