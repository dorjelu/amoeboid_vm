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
pub mod machine_supervisor;
pub mod paths;
pub mod quarantine;
pub mod root;
pub mod session;
#[cfg(test)]
mod tests;
pub mod timing;

use std::os::unix::fs::DirBuilderExt as _;

use ractor::{Actor, ActorRef};
use tokio::task::JoinHandle;

use crate::config::LoadError;
use crate::config::daemon::DaemonConfig;
use crate::domain::path::AbsolutePath;
use crate::journal::JournalStore;
use crate::protocol::RuntimeId;
use lock::{Acquire, DaemonLock, LockError, LockRecord};
use paths::{ConfigPaths, RuntimePaths};
use root::{Root, RootArgs, RootMessage};

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
    /// The passed descriptor is not a Unix stream socket.
    pub fn from_env() -> std::io::Result<Option<Self>> {
        listenfd::ListenFd::from_env()
            .take_unix_listener(0)
            .map(|listener| listener.map(Self))
    }
}

/// Where the runtime keeps its journal.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Persistence {
    Sqlite(JournalLocation),
    /// Nothing outlives the runtime; tests.
    Memory,
}

/// The journal file: named by whoever builds the runtime, else `daemon_config.toml`'s
/// `journal.path`, else [`RuntimePaths::journal`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum JournalLocation {
    Configured,
    At(AbsolutePath),
}

/// Where the runtime-wide settings come from.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DaemonConfigSource {
    /// `daemon_config.toml`, read now; the defaults when there is none.
    File,
    /// Already read (the daemon reads it first, for linger), or built by the caller.
    Given(DaemonConfig),
}

#[derive(Debug)]
pub struct RuntimeConfig {
    pub paths: RuntimePaths,
    pub config: ConfigPaths,
    pub hosting: Hosting,
    pub persistence: Persistence,
    pub daemon: DaemonConfigSource,
    /// Recorded in `daemon.lock` for diagnostics.
    pub version: String,
    /// The guest account of every VM: the user the runtime runs as.
    pub account: crate::vm::Account,
    pub vm_defaults: root::VmDefaults,
    /// Wall time for the journal's stamps.
    pub clock: crate::clock::Clock,
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("a runtime is already running{}", holder.as_ref().map_or(String::new(), |holder| format!(" (process {}, version {})", holder.process_id, holder.version)))]
    AlreadyRunning { holder: Option<LockRecord> },
    #[error("can't create {directory}: {source}")]
    Directory {
        directory: String,
        source: std::io::Error,
    },
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error("can't listen on {socket}: {source}")]
    Listen {
        socket: String,
        source: std::io::Error,
    },
    #[error("can't start the root actor: {0}")]
    Spawn(String),
    #[error(transparent)]
    Config(#[from] LoadError),
    #[error("the journal can't live at {0}: it is inside a VM's directory")]
    JournalInsideVm(String),
}

impl crate::diagnostic::Diagnostic for StartError {
    fn code(&self) -> &'static str {
        match self {
            Self::AlreadyRunning { .. } => "runtime.start.already_running",
            Self::Directory { .. } => "runtime.start.directory",
            Self::Lock(inner) => inner.code(),
            Self::Listen { .. } => "runtime.start.listen",
            Self::Spawn(_) => "runtime.start.spawn",
            Self::Config(inner) => inner.code(),
            Self::JournalInsideVm(_) => "runtime.start.journal_inside_vm",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::AlreadyRunning { .. } => Some(
                "use the running one, or stop it: systemctl --user stop amoeboid.service".into(),
            ),
            Self::Lock(inner) => inner.hint(),
            Self::Config(inner) => inner.hint(),
            Self::JournalInsideVm(_) => Some(
                "put the journal directly in the data directory or outside it (journal.path in \
                 daemon_config.toml, or --journal)"
                    .into(),
            ),
            Self::Directory { .. } | Self::Listen { .. } | Self::Spawn(_) => None,
        }
    }
}

/// A running runtime. Dropping it does not stop it; call [`Runtime::stop`].
pub struct Runtime {
    id: RuntimeId,
    root: ActorRef<RootMessage>,
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
            config,
            hosting,
            persistence,
            daemon,
            version,
            account,
            vm_defaults,
            clock,
        } = config;
        let daemon = match daemon {
            DaemonConfigSource::File => crate::config::daemon::load(&config)?,
            DaemonConfigSource::Given(daemon) => daemon,
        };
        let journal = journal_store(&paths, persistence, daemon.journal.as_ref())?;
        for directory in [paths.data(), paths.run()] {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(directory.as_path())
                .map_err(|source| StartError::Directory {
                    directory: directory.as_path().display().to_string(),
                    source,
                })?;
        }
        let record = LockRecord {
            process_id: std::process::id(),
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
                journal,
                config,
                daemon,
                paths,
                listener,
                account,
                vm_defaults,
                clock,
            },
        )
        .await
        .map_err(|error| StartError::Spawn(error.to_string()))?;
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
        if let Err(error) = self.join.await {
            tracing::error!(error = %error, "root actor task failed");
        }
    }
}

#[derive(Clone)]
pub struct Stopper(ActorRef<RootMessage>);

impl Stopper {
    pub fn stop(&self) {
        self.0.stop(None);
    }
}

/// The journal's store: the builder's location, else the configured one, else the default;
/// never inside a VM's directory (guest-writable trees live there).
fn journal_store(
    paths: &RuntimePaths,
    persistence: Persistence,
    configured: Option<&AbsolutePath>,
) -> Result<JournalStore, StartError> {
    let path = match persistence {
        Persistence::Memory => return Ok(JournalStore::Memory),
        Persistence::Sqlite(JournalLocation::At(path)) => path,
        Persistence::Sqlite(JournalLocation::Configured) => {
            configured.cloned().unwrap_or_else(|| paths.journal())
        }
    };
    let inside_vm = path.is_within(paths.data()) && path.parent().as_ref() != Some(paths.data());
    if inside_vm {
        return Err(StartError::JournalInsideVm(path.display().to_string()));
    }
    Ok(JournalStore::Sqlite(path))
}

/// The lock is held, so a socket file left by a dead runtime is ours to replace.
fn listen(paths: &RuntimePaths, hosting: Hosting) -> std::io::Result<tokio::net::UnixListener> {
    let std_listener = match hosting {
        Hosting::SystemdUser { listener } => listener.0,
        Hosting::InProcess => {
            match std::fs::remove_file(paths.socket()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            std::os::unix::net::UnixListener::bind(paths.socket())?
        }
    };
    std_listener.set_nonblocking(true)?;
    tokio::net::UnixListener::from_std(std_listener)
}
