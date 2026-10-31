//! The VM subtree: the twin (`twin`) and the children only it can reach (`builder`,
//! `guest_vm`, `hypervisor`). Children report to the twin as observations, every one carrying
//! the generation it belongs to; the twin drops observations from older generations.

pub mod builder;
pub mod bundled;
pub mod guest_vm;
pub mod hypervisor;
pub mod local_qemu;
pub mod twin;
pub mod vacuum;

use serde::{Deserialize, Serialize};

use crate::domain::account::{GroupId, GuestHome, UserId, UserName};
use crate::domain::path::AbsolutePath;
use crate::domain::port::LoopbackPort;
use crate::domain::share::Shares;

pub use crate::domain::lifecycle::{Generation, Incarnation};

/// Which hypervisor backend runs a VM; persisted at creation, so a restarted twin spawns the
/// same kind to adopt from.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Local,
}

/// The guest account, mirroring the host user the runtime runs as.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Account {
    pub user: UserName,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub home: GuestHome,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountSourceError {
    #[error("environment variable {0} is not set")]
    Missing(&'static str),
    #[error(transparent)]
    Invalid(#[from] crate::domain::account::AccountError),
}

impl Account {
    /// The user this process runs as: `USER`, `HOME`, and the process's own ids. Read once, at
    /// the boundary (the daemon's start).
    ///
    /// # Errors
    ///
    /// A variable is missing or not valid for a guest account.
    #[allow(
        clippy::disallowed_types,
        reason = "boundary: reads the process environment"
    )]
    pub fn of_this_process(
        environment: impl Fn(&'static str) -> Option<std::ffi::OsString>,
    ) -> Result<Self, AccountSourceError> {
        let text = |name: &'static str| {
            environment(name)
                .and_then(|value| value.into_string().ok())
                .filter(|value| !value.is_empty())
                .ok_or(AccountSourceError::Missing(name))
        };
        Ok(Self {
            user: UserName::parse(text("USER")?)?,
            user_id: UserId::new(rustix::process::getuid().as_raw()),
            group_id: GroupId::new(rustix::process::getgid().as_raw()),
            home: GuestHome::parse(text("HOME")?)?,
        })
    }
}

impl crate::diagnostic::Diagnostic for AccountSourceError {
    fn code(&self) -> &'static str {
        match self {
            Self::Missing(_) => "account.missing_environment",
            Self::Invalid(inner) => inner.code(),
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Missing(_) => Some("run the runtime in a login session".into()),
            Self::Invalid(inner) => inner.hint(),
        }
    }
}

/// Where the VM's NixOS configuration comes from on this start.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NixConfig {
    /// The VM's own copy; a VM without one gets the bundled configuration.
    Existing,
    /// Create the VM's copy from this directory, or replace it (rebase).
    From(AbsolutePath),
}

/// What a start asks for; persisted in the twin's `StartRequested`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct VmSpec {
    pub shares: Shares,
    pub account: Account,
    pub nix_config: NixConfig,
    pub boot_timeout_seconds: u32,
    /// How long the guest gets to power off when stopped; absent in starts recorded before it
    /// existed.
    #[serde(default = "default_powerdown_grace_seconds")]
    pub powerdown_grace_seconds: u32,
}

/// A stop's powerdown grace when no start recorded one.
pub const DEFAULT_POWERDOWN_GRACE_SECONDS: u32 = 30;

fn default_powerdown_grace_seconds() -> u32 {
    DEFAULT_POWERDOWN_GRACE_SECONDS
}

/// Where a VM is, as the operator sees it: the twin's state without its internals.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum VmCondition {
    Stopped {
        last_failure: Option<crate::diagnostic::Report>,
    },
    Starting {
        step: StartStep,
    },
    Running {
        user: UserName,
        port: LoopbackPort,
        generation: Generation,
    },
    Stopping,
    Failed {
        reason: crate::diagnostic::Report,
        /// Processes of the failed start still recorded; `stop` takes them down.
        leftovers: usize,
    },
    /// Its twin didn't answer in time (Root's view, never the twin's).
    Unknown,
    /// Its twin failed and is restarted after a backoff (Root's view).
    Restarting {
        reason: crate::diagnostic::Report,
        attempt: u32,
    },
    /// No twin runs for it until the quarantine is lifted (Root's view, from the VM's
    /// quarantine record).
    Quarantined {
        cause: crate::runtime::quarantine::QuarantineCause,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartStep {
    Building,
    PreparingGuest,
    Booting,
    WaitingForSsh,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VmStatus {
    pub name: crate::domain::vm_name::VmName,
    pub condition: VmCondition,
}

/// How the operator reaches a running VM; the CLI renders it with `plan::argv::ssh`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SshEndpoint {
    pub user: UserName,
    pub port: LoopbackPort,
    pub known_hosts: AbsolutePath,
}
