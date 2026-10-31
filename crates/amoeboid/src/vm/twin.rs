//! The VM's twin: the only persistent actor in a VM subtree and the only owner of its history.
//!
//! ```text
//! Stopped ─Start─▶ Starting{Building} ─Built─▶ Starting{PreparingGuest} ─Prepared─▶
//!   Starting{Booting} ─MachineRunning─▶ Starting{WaitingForSsh} ─SshReady─▶ Running
//!   any failure ─▶ Failed{reason}            (OperationDone goes to Root at Running or Failed)
//! ```

use std::collections::BTreeMap;

use ractor::{ActorRef, DerivedActorRef};
use serde::{Deserialize, Serialize};

use super::builder::{BuilderCommand, BuilderObservation};
use super::guest_vm::{GuestVmCommand, GuestVmObservation};
use super::hypervisor::{HypervisorCommand, HypervisorObservation};
use super::{Backend, Generation, Incarnation, SshEndpoint, VmSpec};
use crate::adapters::process::Exit;
use crate::diagnostic::Report;
use crate::domain::port::LoopbackPort;
use crate::domain::process::{ProcessIdentity, ProcessRole};
use crate::protocol::OperationId;
use crate::runtime::paths::VmPaths;

/// Marker type implementing `persistent::Behaviour` for one VM.
pub struct VmTwin;

#[derive(Debug)]
pub enum VmCommand {
    Start {
        operation: OperationId,
        spec: VmSpec,
        /// Used only if this VM has no history yet (its first start ever).
        incarnation: Incarnation,
    },
}

/// Everything the twin's children report, tagged by child.
#[derive(Debug)]
pub enum VmObservation {
    Builder(BuilderObservation),
    GuestVm(GuestVmObservation),
    Hypervisor(HypervisorObservation),
}

/// The twin's history; persisted, so only facts and decisions, never derived paths.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum VmEvent {
    /// First event of every stream.
    Created {
        incarnation: Incarnation,
        backend: Backend,
    },
    StartRequested {
        operation: OperationId,
        generation: Generation,
        spec: VmSpec,
    },
    Built {
        generation: Generation,
    },
    GuestPrepared {
        generation: Generation,
        port: LoopbackPort,
    },
    ProcessStarted {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
    },
    MachineRunning {
        generation: Generation,
    },
    MachineStopped {
        generation: Generation,
        reason: Option<Report>,
    },
    Ready {
        generation: Generation,
    },
    ProcessExited {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
        exit: Exit,
    },
    Failed {
        generation: Generation,
        reason: Report,
    },
}

/// The twin's state: what it was created as, and where its VM is.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VmTwinState {
    /// `None` until `Created`: a twin with no history.
    pub created: Option<(Incarnation, Backend)>,
    pub vm: VmState,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum VmState {
    /// Never started, or not running.
    Stopped { last_failure: Option<Report> },
    Starting {
        operation: OperationId,
        generation: Generation,
        spec: VmSpec,
        progress: StartProgress,
    },
    Running {
        generation: Generation,
        spec: VmSpec,
        port: LoopbackPort,
        processes: BTreeMap<ProcessRole, ProcessIdentity>,
    },
    Failed {
        generation: Generation,
        reason: Report,
        /// Started before the failure; to be stopped or reaped.
        leftovers: BTreeMap<ProcessRole, ProcessIdentity>,
    },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StartProgress {
    Building,
    PreparingGuest,
    Booting {
        port: LoopbackPort,
        processes: BTreeMap<ProcessRole, ProcessIdentity>,
    },
    WaitingForSsh {
        port: LoopbackPort,
        processes: BTreeMap<ProcessRole, ProcessIdentity>,
    },
}

/// What the twin sends, chosen by `Behaviour::send` with one `match`.
#[derive(Debug)]
pub enum VmOut {
    Builder(BuilderCommand),
    GuestVm(GuestVmCommand),
    Hypervisor(HypervisorCommand),
    Root(VmReport),
}

/// To Root: results of operations, and the twin's own condition.
#[derive(Debug)]
pub enum VmReport {
    OperationDone {
        operation: OperationId,
        result: Result<SshEndpoint, Report>,
    },
    Unrecoverable {
        reason: Report,
    },
}

/// What the spawner (Root) gives the twin; its `start` hook spawns the children from it.
pub struct VmSetup {
    pub vm: VmPaths,
    pub root: DerivedActorRef<VmReport>,
}

/// Refs to the twin's children (spawned by the twin itself when it starts) and to Root.
pub struct VmPeers {
    pub vm: VmPaths,
    pub builder: ActorRef<BuilderCommand>,
    pub guest_vm: ActorRef<GuestVmCommand>,
    /// Whichever backend runs this VM: anything that accepts `HypervisorCommand`.
    pub hypervisor: DerivedActorRef<HypervisorCommand>,
    pub root: DerivedActorRef<VmReport>,
}
