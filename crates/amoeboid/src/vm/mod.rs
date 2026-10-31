//! The VM subtree: the twin (`twin`) and the children only it can reach (`builder`,
//! `guest_vm`, `hypervisor`). Children report to the twin as observations, every one carrying
//! the generation it belongs to; the twin drops observations from older generations.

pub mod builder;
pub mod bundled;
pub mod guest_vm;
pub mod hypervisor;
pub mod local_qemu;
pub mod twin;

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
}

/// How the operator reaches a running VM; the CLI renders it with `plan::argv::ssh`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SshEndpoint {
    pub user: UserName,
    pub port: LoopbackPort,
    pub known_hosts: AbsolutePath,
}
