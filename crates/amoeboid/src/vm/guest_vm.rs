//! `GuestVm`: everything about the guest side of a VM. Prepares what the guest reads at boot
//! (host key copy, `mounts`, `hostname`, Nix config copies) through guest delivery, pins the
//! host key, picks the forwarded port, and later waits for sshd. Keeps no state.

use super::{Generation, VmSpec};
use crate::diagnostic::Report;
use crate::domain::host_key::HostPublicKey;
use crate::domain::port::LoopbackPort;
use crate::runtime::paths::VmPaths;

#[derive(Debug)]
pub enum GuestVmCommand {
    Prepare {
        generation: Generation,
        vm: VmPaths,
        spec: VmSpec,
    },
    /// Probe for the sshd banner until it answers or `timeout_seconds` pass.
    AwaitSsh {
        generation: Generation,
        port: LoopbackPort,
        timeout_seconds: u32,
    },
}

#[derive(Debug)]
pub enum GuestVmObservation {
    Prepared {
        generation: Generation,
        port: LoopbackPort,
        host_key: HostPublicKey,
    },
    PrepareFailed {
        generation: Generation,
        reason: Report,
    },
    SshReady {
        generation: Generation,
    },
    SshTimedOut {
        generation: Generation,
    },
}
