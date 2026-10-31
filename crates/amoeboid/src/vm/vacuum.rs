//! `vacuum_vm`'s effects on the host (0003 §Vacuum): end every process carrying the VM's nonce,
//! then remove its runtime and state directories. Needs no history, so it works on a
//! quarantined VM whose stream can't be replayed. Run by Root after the twin (if any) has
//! stopped the VM gracefully and been stopped; whatever is left is ended by force.
//!
//! ```text
//! find_by_vm ─▶ TERM each ─▶ wait ≤ terminate grace ─▶ KILL survivors ─▶ wait ≤ kill grace
//!            ─▶ any survivor: Survived (directories kept)   else: remove runtime, state
//! ```

use std::time::Duration;

use crate::adapters::files::{self, FileError};
use crate::adapters::process::{self, Liveness, ProcessError, Signal};
use crate::domain::process::ProcessIdentity;
use crate::runtime::paths::VmPaths;
use crate::runtime::timing::ProcessTiming;

const POLL: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub enum VacuumError {
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error("{0} of the VM's processes outlived SIGKILL")]
    Survived(usize),
    #[error(transparent)]
    File(#[from] FileError),
}

impl crate::diagnostic::Diagnostic for VacuumError {
    fn code(&self) -> &'static str {
        match self {
            Self::Process(_) => "vm.vacuum.process",
            Self::Survived(_) => "vm.vacuum.survived",
            Self::File(_) => "vm.vacuum.remove",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Process(_) | Self::File(_) => None,
            Self::Survived(_) => Some(
                "check for processes in uninterruptible sleep, then run vacuum_vm again".into(),
            ),
        }
    }
}

/// Ends the VM's processes and removes its directories. Idempotent: a VM with nothing left
/// is vacuumed again without error.
///
/// # Errors
///
/// `/proc` can't be read, a process survives `SIGKILL`, or a directory can't be removed.
pub async fn vacuum(vm: &VmPaths, timing: &ProcessTiming) -> Result<(), VacuumError> {
    let found = process::find_by_vm(vm.name())?;
    if !found.is_empty() {
        tracing::info!(vm = %vm.name(), processes = found.len(), "ending the VM's processes");
    }
    let survivors = end(found, Signal::Terminate, timing.vacuum_terminate_grace).await?;
    let survivors = end(survivors, Signal::Kill, timing.vacuum_kill_grace).await?;
    if !survivors.is_empty() {
        return Err(VacuumError::Survived(survivors.len()));
    }
    files::remove_directory(vm.runtime())?;
    files::remove_directory(vm.state())?;
    Ok(())
}

/// Signals each process, then waits up to `grace` for all to be gone; returns those alive.
async fn end(
    processes: Vec<ProcessIdentity>,
    signal: Signal,
    grace: Duration,
) -> Result<Vec<ProcessIdentity>, ProcessError> {
    for identity in &processes {
        process::signal(identity, signal)?;
    }
    let deadline = tokio::time::Instant::now() + grace;
    let mut alive = processes;
    loop {
        let mut still = Vec::new();
        for identity in alive {
            if process::liveness(&identity)? == Liveness::Alive {
                still.push(identity);
            }
        }
        alive = still;
        if alive.is_empty() || tokio::time::Instant::now() >= deadline {
            return Ok(alive);
        }
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use super::*;
    use crate::adapters::process::{Containment, Output, SpawnRequest};
    use crate::domain::lifecycle::{Generation, Incarnation};
    use crate::domain::path::AbsolutePath;
    use crate::domain::process::{LocalRole, Nonce, ProcessRole};
    use crate::domain::vm_name::VmName;
    use crate::plan::argv::Argv;
    use crate::runtime::paths::RuntimePaths;

    fn paths(root: &std::path::Path, name: &str) -> VmPaths {
        let data = AbsolutePath::parse(root.join("data")).unwrap();
        let run = AbsolutePath::parse(root.join("run")).unwrap();
        RuntimePaths::under(data, run)
            .unwrap()
            .vm(&VmName::parse(name.into()).unwrap())
            .unwrap()
    }

    /// A process marked as `vm`'s, ignoring SIGTERM when `stubborn`.
    fn marked(vm: &VmPaths, stubborn: bool) -> crate::adapters::process::Spawned {
        let nonce = Nonce::of(
            vm.name(),
            Incarnation::mint(),
            Generation::FIRST,
            &ProcessRole::Local(LocalRole::Qemu),
        );
        let script = if stubborn {
            "trap '' TERM; while :; do sleep 0.1; done"
        } else {
            "exec sleep 60"
        };
        process::spawn(&SpawnRequest {
            argv: Argv::for_tests(&["sh", "-c", script]),
            containment: Containment::Direct,
            nonce: Some(nonce),
            directory: AbsolutePath::parse("/".into()).unwrap(),
            output: Output::Discard,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn vacuum_ends_the_vms_processes_and_removes_its_directories_only() {
        let temporary = tempfile::tempdir().unwrap();
        let vm = paths(temporary.path(), "vacuumed");
        // A neighbour whose name extends this one's: its processes and files stay.
        let neighbour = paths(temporary.path(), "vacuumed-2");
        for directory in [vm.state(), vm.runtime(), neighbour.state()] {
            std::fs::create_dir_all(directory.as_path().join("inner")).unwrap();
        }
        let mut gentle = marked(&vm, false);
        let mut stubborn = marked(&vm, true);
        let mut other = marked(&neighbour, false);
        // Give the stubborn shell time to install its trap.
        tokio::time::sleep(Duration::from_millis(300)).await;

        vacuum(&vm, &ProcessTiming::quick()).await.unwrap();

        for spawned in [&mut gentle, &mut stubborn] {
            assert!(spawned.child.try_wait().unwrap().is_some());
        }
        assert!(other.child.try_wait().unwrap().is_none());
        assert!(!vm.state().as_path().exists());
        assert!(!vm.runtime().as_path().exists());
        assert!(neighbour.state().as_path().exists());
        process::signal(&other.identity, Signal::Kill).unwrap();
        other.child.wait().await.unwrap();
    }

    #[tokio::test]
    async fn vacuuming_a_vm_with_nothing_left_succeeds() {
        let temporary = tempfile::tempdir().unwrap();
        vacuum(&paths(temporary.path(), "gone"), &ProcessTiming::quick())
            .await
            .unwrap();
    }
}
