//! Hypervisor: the backend actor that runs the VM. `LocalQemu` realises a `BootPlan` as
//! virtiofsd per share plus qemu, each in its own scope, and watches them; argv, sockets and
//! qemu arguments are its private business. Keeps no state: on recovery the twin tells it what
//! to adopt.

use std::collections::BTreeMap;

use super::Generation;
use crate::adapters::process::Exit;
use crate::diagnostic::Report;
use crate::domain::account::{GroupId, UserId};
use crate::domain::lifecycle::Incarnation;
use crate::domain::port::LoopbackPort;
use crate::domain::process::{Nonce, ProcessIdentity, ProcessRole};
use crate::domain::share::Shares;
use crate::runtime::paths::VmPaths;

/// What to run, stated as what the VM needs, not how a backend provides it.
#[derive(Debug)]
pub struct BootPlan {
    pub vm: VmPaths,
    /// With the generation, makes every spawn's nonce.
    pub incarnation: Incarnation,
    pub shares: Shares,
    pub ssh_port: LoopbackPort,
    pub guest_user: UserId,
    pub guest_group: GroupId,
}

#[derive(Debug)]
pub enum HypervisorCommand {
    /// Start the VM's processes. Each spawn carries `Nonce::of(vm, incarnation, generation,
    /// role)`, so a spawn whose `ProcessStarted` was lost is found again without anything
    /// persisted before it.
    Boot {
        generation: Generation,
        plan: BootPlan,
    },
    /// Stop the machine of this generation and whatever of it still runs; answered by
    /// `MachineStopped`. Carries what the twin knows, so a backend that didn't start these
    /// processes (after a runtime restart) can still stop them.
    Stop {
        generation: Generation,
        vm: VmPaths,
        processes: BTreeMap<ProcessRole, ProcessIdentity>,
        /// How long the guest gets to power off before it is stopped by force.
        grace: std::time::Duration,
    },
    /// After a runtime restart: watch these if they are still the same processes.
    Adopt {
        generation: Generation,
        vm: VmPaths,
        processes: BTreeMap<ProcessRole, Adoptee>,
    },
}

/// What the twin knows about a process it asks the backend to adopt.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Adoptee {
    /// Its identity was persisted: check it (pidfd, start time).
    Known(ProcessIdentity),
    /// Only its spawn was decided: search for the nonce (topmost carrier).
    Expected(Nonce),
}

#[derive(Debug)]
pub enum HypervisorObservation {
    ProcessStarted {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
    },
    /// The machine runs: every process of the plan started (or was adopted).
    MachineRunning { generation: Generation },
    /// The machine no longer runs; `reason` when it stopped other than by request. How a
    /// helper's failure turns into this (virtiofsd dies, qemu is powered off) is the
    /// backend's knowledge.
    MachineStopped {
        generation: Generation,
        reason: Option<Report>,
    },
    BootFailed {
        generation: Generation,
        reason: Report,
    },
    Adopted {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
    },
    Lost {
        generation: Generation,
        role: ProcessRole,
    },
    ProcessExited {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
        exit: Exit,
    },
}
