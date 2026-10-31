//! `LocalQemu`: the hypervisor backend that runs a VM on this host. It realises a `BootPlan`
//! as one virtiofsd per shared directory, then qemu through the build's runner, each process in
//! its own scope, and watches them. How processes depend on each other is its knowledge: a
//! virtiofsd that dies takes qemu down with it, qemu exiting stops the shares; the twin only
//! hears `MachineRunning` and `MachineStopped { reason }`, plus per-process facts to persist.
//!
//! ```text
//! Boot ─▶ clear runtime dir ─▶ qemu args ─▶ virtiofsd × shares ─▶ sockets present ─▶ qemu
//!      ─▶ MachineRunning                       (any failure: started processes terminated,
//!                                                BootFailed)
//! Exited(virtiofsd) while running ─▶ terminate qemu; its exit reports MachineStopped(share)
//! Exited(qemu) ─▶ terminate the shares ─▶ MachineStopped
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef};
use tokio::task::JoinHandle;

use super::Generation;
use super::hypervisor::{Adoptee, BootPlan, HypervisorCommand, HypervisorObservation};
use crate::adapters::files::{self, FileError};
use crate::adapters::process::{
    self, Cancel, Containment, Exit, Output, ProcessError, Signal, SpawnRequest, Watched,
};
use crate::adapters::qmp;
use crate::diagnostic::Report;
use crate::domain::account::{GroupId, UserId};
use crate::domain::lifecycle::Incarnation;
use crate::domain::process::{LocalRole, Nonce, ProcessIdentity, ProcessRole, ScopeName};
use crate::plan;
use crate::plan::argv::IdMap;
use crate::runtime::paths::VmPaths;
use crate::runtime::timing::Timing;

const SOCKET_POLL: Duration = Duration::from_millis(100);

/// The backend's mailbox: commands from the twin (through a `DerivedActorRef`) and its own
/// watchers' reports. One wrapper variant per derived type keeps `TryFrom` total.
#[derive(Debug)]
pub enum LocalQemuMessage {
    Command(HypervisorCommand),
    Exited {
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
        exit: Exit,
    },
    /// A stop's next step, if qemu is still there: the powerdown grace ran out (terminate), or
    /// the terminate grace did (kill).
    Escalate {
        generation: Generation,
        signal: Signal,
    },
}

impl From<HypervisorCommand> for LocalQemuMessage {
    fn from(command: HypervisorCommand) -> Self {
        Self::Command(command)
    }
}

impl TryFrom<LocalQemuMessage> for HypervisorCommand {
    type Error = LocalQemuMessage;
    fn try_from(message: LocalQemuMessage) -> Result<Self, LocalQemuMessage> {
        match message {
            LocalQemuMessage::Command(command) => Ok(command),
            other @ (LocalQemuMessage::Exited { .. } | LocalQemuMessage::Escalate { .. }) => {
                Err(other)
            }
        }
    }
}

/// Where VM processes live.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Placement {
    /// Each in its own user scope: they outlive the runtime.
    Scoped,
    /// Plain children; tests only.
    #[cfg(test)]
    Direct,
}

pub struct LocalQemuSetup {
    pub report: DerivedActorRef<HypervisorObservation>,
    pub placement: Placement,
    /// Powerdown and terminate graces, socket wait.
    pub timing: Timing,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalQemuError {
    #[error(transparent)]
    File(#[from] FileError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error("virtiofsd didn't create {sockets} within {waited:?}")]
    SocketsMissing { sockets: String, waited: Duration },
    #[error("qemu exited ({0:?})")]
    QemuExited(Exit),
    #[error("{role} exited ({exit:?}), so the VM lost a shared directory")]
    ShareExited { role: String, exit: Exit },
    #[error("{0} is gone, so the VM can't be adopted whole")]
    Lost(String),
}

/// Whether the machine is up, and if it is going down, why.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Machine {
    Down,
    Running,
    Stopping { reason: Option<Report> },
}

pub struct LocalQemu;

pub struct LocalQemuState {
    report: DerivedActorRef<HypervisorObservation>,
    placement: Placement,
    timing: Timing,
    generation: Option<Generation>,
    processes: BTreeMap<ProcessRole, ProcessIdentity>,
    machine: Machine,
    watchers: Vec<JoinHandle<()>>,
    cancel: Cancel,
}

impl Actor for LocalQemu {
    type Msg = LocalQemuMessage;
    type State = LocalQemuState;
    type Arguments = LocalQemuSetup;

    fn pre_start(
        &self,
        _myself: ActorRef<LocalQemuMessage>,
        setup: LocalQemuSetup,
    ) -> impl Future<Output = Result<LocalQemuState, ActorProcessingErr>> + Send {
        std::future::ready(Ok(LocalQemuState {
            report: setup.report,
            placement: setup.placement,
            timing: setup.timing,
            generation: None,
            processes: BTreeMap::new(),
            machine: Machine::Down,
            watchers: Vec::new(),
            cancel: Cancel::default(),
        }))
    }

    async fn handle(
        &self,
        myself: ActorRef<LocalQemuMessage>,
        message: LocalQemuMessage,
        state: &mut LocalQemuState,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            LocalQemuMessage::Command(HypervisorCommand::Boot { generation, plan }) => {
                state.begin(generation);
                if let Err(error) = boot(&myself, state, generation, &plan).await {
                    state.terminate_all();
                    state.tell(HypervisorObservation::BootFailed {
                        generation,
                        reason: Report::of(&error),
                    });
                }
            }
            LocalQemuMessage::Command(HypervisorCommand::Stop {
                generation,
                vm,
                processes,
            }) => stop(&myself, state, generation, &vm, processes).await,
            LocalQemuMessage::Escalate { generation, signal } => {
                let qemu = ProcessRole::Local(LocalRole::Qemu);
                let stopping = matches!(state.machine, Machine::Stopping { .. });
                if state.generation == Some(generation) && stopping {
                    if let Some(identity) = state.processes.get(&qemu) {
                        tracing::warn!(?signal, "qemu is still running; escalating");
                        let _ = process::signal(identity, signal);
                    }
                    if signal == Signal::Terminate {
                        let grace = state.timing.terminate_grace;
                        myself.send_after(grace, move || LocalQemuMessage::Escalate {
                            generation,
                            signal: Signal::Kill,
                        });
                    }
                }
            }
            LocalQemuMessage::Command(HypervisorCommand::Adopt {
                generation,
                vm: _,
                processes,
            }) => {
                state.begin(generation);
                adopt(&myself, state, generation, processes);
            }
            LocalQemuMessage::Exited {
                generation,
                role,
                identity,
                exit,
            } => state.exited(generation, &role, identity, exit),
        }
        Ok(())
    }

    fn post_stop(
        &self,
        _myself: ActorRef<LocalQemuMessage>,
        state: &mut LocalQemuState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        // Stop watching; the processes themselves keep running (they outlive the runtime).
        state
            .cancel
            .store(true, std::sync::atomic::Ordering::Relaxed);
        for watcher in state.watchers.drain(..) {
            watcher.abort();
        }
        std::future::ready(Ok(()))
    }
}

impl LocalQemuState {
    fn tell(&self, observation: HypervisorObservation) {
        if self.report.send_message(observation).is_err() {
            tracing::debug!("hypervisor report undeliverable: the twin stopped");
        }
    }

    /// A new generation: forget the previous one's processes (the twin tracks them).
    fn begin(&mut self, generation: Generation) {
        self.generation = Some(generation);
        self.processes.clear();
        self.machine = Machine::Down;
    }

    fn record(&mut self, generation: Generation, role: ProcessRole, identity: ProcessIdentity) {
        self.tell(HypervisorObservation::ProcessStarted {
            generation,
            role: role.clone(),
            identity: identity.clone(),
        });
        self.processes.insert(role, identity);
    }

    fn watch(
        &mut self,
        myself: &ActorRef<LocalQemuMessage>,
        generation: Generation,
        role: ProcessRole,
        identity: ProcessIdentity,
        watched: Watched,
    ) {
        let myself = myself.clone();
        let cancel = self.cancel.clone();
        self.watchers.push(tokio::spawn(async move {
            if let Some(exit) = process::exited(watched, cancel).await {
                let _ = myself.cast(LocalQemuMessage::Exited {
                    generation,
                    role,
                    identity,
                    exit,
                });
            }
        }));
    }

    fn terminate(&self, role: &ProcessRole) {
        if let Some(identity) = self.processes.get(role)
            && let Err(error) = process::signal(identity, Signal::Terminate)
        {
            tracing::warn!(%error, role = role.name(), "can't terminate");
        }
    }

    fn terminate_all(&self) {
        for role in self.processes.keys() {
            self.terminate(role);
        }
    }

    fn exited(
        &mut self,
        generation: Generation,
        role: &ProcessRole,
        identity: ProcessIdentity,
        exit: Exit,
    ) {
        if self.generation != Some(generation) {
            return;
        }
        self.tell(HypervisorObservation::ProcessExited {
            generation,
            role: role.clone(),
            identity,
            exit,
        });
        self.processes.remove(role);
        let qemu = ProcessRole::Local(LocalRole::Qemu);
        match (role, &self.machine) {
            (ProcessRole::Local(LocalRole::Qemu), Machine::Running) => {
                let reason = match exit {
                    Exit::Code(0) => None,
                    Exit::Code(_) | Exit::Signal(_) | Exit::Unknown => {
                        Some(Report::of(&LocalQemuError::QemuExited(exit)))
                    }
                };
                self.stopped(generation, reason);
            }
            (ProcessRole::Local(LocalRole::Qemu), Machine::Stopping { reason }) => {
                let reason = reason.clone();
                self.stopped(generation, reason);
            }
            (ProcessRole::Local(LocalRole::Virtiofsd(_)), Machine::Running) => {
                // Exits are observed in no particular order: virtiofsd also exits when qemu,
                // its only client, goes away. Only a share that dies under a living qemu is
                // the cause.
                let qemu_alive = self.processes.get(&qemu).is_some_and(|identity| {
                    matches!(process::liveness(identity), Ok(process::Liveness::Alive))
                });
                let reason = if qemu_alive {
                    LocalQemuError::ShareExited {
                        role: role.name(),
                        exit,
                    }
                } else {
                    LocalQemuError::QemuExited(Exit::Unknown)
                };
                self.machine = Machine::Stopping {
                    reason: Some(Report::of(&reason)),
                };
                self.terminate(&qemu);
            }
            (ProcessRole::Local(LocalRole::Virtiofsd(_)), Machine::Stopping { reason }) => {
                // A stop of leftovers without qemu ends when the last of them has gone.
                if self.processes.is_empty() {
                    let reason = reason.clone();
                    self.stopped(generation, reason);
                }
            }
            (ProcessRole::Local(LocalRole::Qemu | LocalRole::Virtiofsd(_)), Machine::Down) => {}
        }
    }

    /// qemu is gone: stop what is left and report the machine down once.
    fn stopped(&mut self, generation: Generation, reason: Option<Report>) {
        self.machine = Machine::Down;
        self.terminate_all();
        self.tell(HypervisorObservation::MachineStopped { generation, reason });
    }
}

fn containment(placement: Placement, vm: &VmPaths, role: &ProcessRole) -> Containment {
    match placement {
        Placement::Scoped => Containment::Scope(ScopeName::of(vm.name(), role)),
        #[cfg(test)]
        Placement::Direct => Containment::Direct,
    }
}

fn nonce(
    vm: &VmPaths,
    incarnation: Incarnation,
    generation: Generation,
    role: &ProcessRole,
) -> Nonce {
    Nonce::of(vm.name(), incarnation, generation, role)
}

async fn boot(
    myself: &ActorRef<LocalQemuMessage>,
    state: &mut LocalQemuState,
    generation: Generation,
    plan: &BootPlan,
) -> Result<(), LocalQemuError> {
    let vm = &plan.vm;
    let runtime = vm.runtime();
    files::ensure_directory(runtime, 0o700)?;
    // Sockets left by an earlier run would satisfy the wait below; the twin boots only a
    // stopped VM, so nothing in here is in use.
    files::clear_directory(runtime)?;
    files::write_atomically(
        &vm.qemu_arguments(),
        plan::qemu::arguments(&plan.shares, plan.ssh_port).as_bytes(),
        0o600,
    )?;
    let ids = IdMap {
        guest_user: plan.guest_user,
        guest_group: plan.guest_group,
        host_user: UserId::new(rustix::process::getuid().as_raw()),
        host_group: GroupId::new(rustix::process::getgid().as_raw()),
    };
    let shares = plan::share::virtiofs_shares(&vm.guest(), &plan.shares);
    for share in &shares {
        let role = share.role();
        let spawned = process::spawn(&SpawnRequest {
            argv: plan::argv::virtiofsd(&vm.virtiofsd(), share, runtime, ids),
            containment: containment(state.placement, vm, &role),
            nonce: Some(nonce(vm, plan.incarnation, generation, &role)),
            directory: runtime.clone(),
            output: Output::Append(vm.helper_log()),
        })?;
        state.record(generation, role.clone(), spawned.identity.clone());
        state.watch(
            myself,
            generation,
            role,
            spawned.identity,
            Watched::Child(spawned.child),
        );
    }
    let deadline = tokio::time::Instant::now() + state.timing.socket_wait;
    loop {
        let missing: Vec<String> = shares
            .iter()
            .map(plan::share::VirtiofsShare::socket)
            .filter(|socket| !runtime.join(socket).as_path().exists())
            .collect();
        if missing.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(LocalQemuError::SocketsMissing {
                sockets: missing.join(", "),
                waited: state.timing.socket_wait,
            });
        }
        tokio::time::sleep(SOCKET_POLL).await;
    }
    let qemu = ProcessRole::Local(LocalRole::Qemu);
    let spawned = process::spawn(&SpawnRequest {
        argv: plan::argv::runner(&vm.runner()),
        containment: containment(state.placement, vm, &qemu),
        nonce: Some(nonce(vm, plan.incarnation, generation, &qemu)),
        directory: runtime.clone(),
        output: Output::Append(vm.console_log()),
    })?;
    state.record(generation, qemu.clone(), spawned.identity.clone());
    state.watch(
        myself,
        generation,
        qemu,
        spawned.identity,
        Watched::Child(spawned.child),
    );
    state.machine = Machine::Running;
    state.tell(HypervisorObservation::MachineRunning { generation });
    Ok(())
}

/// Powers the machine off: ACPI request over QMP, then terminate and kill on timers; qemu's
/// exit stops the shares and reports `MachineStopped`. Processes the twin knows but this
/// backend doesn't watch yet are adopted first.
async fn stop(
    myself: &ActorRef<LocalQemuMessage>,
    state: &mut LocalQemuState,
    generation: Generation,
    vm: &VmPaths,
    processes: BTreeMap<ProcessRole, ProcessIdentity>,
) {
    if state.generation != Some(generation) {
        state.begin(generation);
    }
    for (role, identity) in processes {
        if state.processes.contains_key(&role) {
            continue;
        }
        if let Ok(Some(watched)) = process::adopt(&identity) {
            state.processes.insert(role.clone(), identity.clone());
            state.watch(myself, generation, role, identity, watched);
        }
    }
    if state.processes.is_empty() {
        state.stopped(generation, None);
        return;
    }
    state.machine = Machine::Stopping { reason: None };
    let qemu = ProcessRole::Local(LocalRole::Qemu);
    if !state.processes.contains_key(&qemu) {
        state.terminate_all();
        return;
    }
    let next = match qmp::powerdown(&vm.control_socket()).await {
        Ok(()) => state.timing.powerdown_grace,
        Err(error) => {
            tracing::warn!(%error, "can't ask the guest to power off; terminating qemu");
            Duration::ZERO
        }
    };
    myself.send_after(next, move || LocalQemuMessage::Escalate {
        generation,
        signal: Signal::Terminate,
    });
}

/// Re-watches what an earlier runtime started. All present: the machine runs. Anything lost:
/// the rest is stopped and the machine reported down.
fn adopt(
    myself: &ActorRef<LocalQemuMessage>,
    state: &mut LocalQemuState,
    generation: Generation,
    processes: BTreeMap<ProcessRole, Adoptee>,
) {
    let mut lost = Vec::new();
    for (role, adoptee) in processes {
        let identity = match adoptee {
            Adoptee::Known(identity) => Ok(Some(identity)),
            Adoptee::Expected(nonce) => process::find_by_nonce(&nonce).inspect(|found| {
                if let Some(identity) = found {
                    state.record(generation, role.clone(), identity.clone());
                }
            }),
        };
        let watched = identity.and_then(|identity| match identity {
            Some(identity) => Ok(process::adopt(&identity)?.map(|watched| (identity, watched))),
            None => Ok(None),
        });
        if let Ok(Some((identity, watched))) = watched {
            state.processes.insert(role.clone(), identity.clone());
            state.tell(HypervisorObservation::Adopted {
                generation,
                role: role.clone(),
                identity: identity.clone(),
            });
            state.watch(myself, generation, role, identity, watched);
        } else {
            state.tell(HypervisorObservation::Lost {
                generation,
                role: role.clone(),
            });
            lost.push(role.name());
        }
    }
    if lost.is_empty() {
        state.machine = Machine::Running;
        state.tell(HypervisorObservation::MachineRunning { generation });
    } else {
        let reason = Report::of(&LocalQemuError::Lost(lost.join(", ")));
        state.stopped(generation, Some(reason));
    }
}

impl crate::diagnostic::Diagnostic for LocalQemuError {
    fn code(&self) -> &'static str {
        match self {
            Self::File(inner) => inner.code(),
            Self::Process(inner) => inner.code(),
            Self::SocketsMissing { .. } => "hypervisor.sockets_missing",
            Self::QemuExited(_) => "hypervisor.qemu_exited",
            Self::ShareExited { .. } => "hypervisor.share_exited",
            Self::Lost(_) => "hypervisor.lost",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::File(inner) => inner.hint(),
            Self::Process(inner) => inner.hint(),
            Self::SocketsMissing { .. } | Self::ShareExited { .. } => {
                Some("see the VM's virtiofsd.log".into())
            }
            Self::QemuExited(_) => Some("see the VM's console.log".into()),
            Self::Lost(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;
    use crate::domain::path::AbsolutePath;
    use crate::domain::port::LoopbackPort;
    use crate::domain::process::SharedDirectory;
    use crate::domain::share::{HostDirectory, Mode, ShareRequest, Shares};
    use crate::domain::vm_name::VmName;
    use crate::runtime::paths::RuntimePaths;
    use crate::testing::{executable, next, probe};

    /// Creates its `--socket-path=` file, then waits like virtiofsd.
    const FAKE_VIRTIOFSD: &str = "#!/bin/sh\nfor a; do case $a in --socket-path=*) : > \"${a#--socket-path=}\";; esac; done\nexec sleep 30\n";
    const SILENT_VIRTIOFSD: &str = "#!/bin/sh\nexec sleep 30\n";
    const FAKE_RUNNER: &str = "#!/bin/sh\nexec sleep 30\n";

    struct Fixture {
        _temporary: tempfile::TempDir,
        vm: VmPaths,
        shared: AbsolutePath,
        observations: UnboundedReceiver<HypervisorObservation>,
        report: DerivedActorRef<HypervisorObservation>,
    }

    async fn fixture(virtiofsd: &str) -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let paths = RuntimePaths::under(root.join("data"), root.join("run")).unwrap();
        let vm = paths.vm(&VmName::parse("work".into()).unwrap()).unwrap();
        executable(vm.virtiofsd().as_path(), virtiofsd);
        executable(vm.runner().as_path(), FAKE_RUNNER);
        let shared = root.join("project");
        files::ensure_directory(&shared, 0o755).unwrap();
        let (twin, observations) = probe::<HypervisorObservation>().await;
        Fixture {
            _temporary: temporary,
            vm,
            shared,
            observations,
            report: twin.get_derived(),
        }
    }

    async fn spawn(fixture: &Fixture) -> DerivedActorRef<HypervisorCommand> {
        let (actor, _) = Actor::spawn(
            None,
            LocalQemu,
            LocalQemuSetup {
                report: fixture.report.clone(),
                placement: Placement::Direct,
                timing: crate::runtime::timing::Timing::quick(),
            },
        )
        .await
        .unwrap();
        actor.get_derived()
    }

    fn plan(fixture: &Fixture, incarnation: Incarnation) -> BootPlan {
        let share = ShareRequest {
            directory: HostDirectory::parse(fixture.shared.clone()).unwrap(),
            mode: Mode::ReadWrite,
        };
        BootPlan {
            vm: fixture.vm.clone(),
            incarnation,
            shares: Shares::validate(vec![share], &[], |_| true).unwrap(),
            ssh_port: LoopbackPort::new(std::num::NonZeroU16::new(40022).unwrap()),
            guest_user: UserId::new(1000),
            guest_group: GroupId::new(100),
        }
    }

    /// Observations until `MachineRunning`, collecting started processes.
    async fn until_running(
        observations: &mut UnboundedReceiver<HypervisorObservation>,
    ) -> BTreeMap<ProcessRole, ProcessIdentity> {
        let mut started = BTreeMap::new();
        loop {
            match next(observations).await {
                HypervisorObservation::ProcessStarted { role, identity, .. }
                | HypervisorObservation::Adopted { role, identity, .. } => {
                    started.insert(role, identity);
                }
                HypervisorObservation::MachineRunning { .. } => return started,
                other @ (HypervisorObservation::MachineStopped { .. }
                | HypervisorObservation::BootFailed { .. }
                | HypervisorObservation::Lost { .. }
                | HypervisorObservation::ProcessExited { .. }) => unexpected(&format!("{other:?}")),
            }
        }
    }

    /// Fails the test (the lints forbid `panic!`, `assert_eq!` reports the observation).
    fn unexpected(observation: &str) {
        assert_eq!(observation, "", "unexpected observation");
    }

    fn kill_all(processes: &BTreeMap<ProcessRole, ProcessIdentity>) {
        for identity in processes.values() {
            let _ = process::signal(identity, Signal::Kill);
        }
    }

    fn virtiofsd(directory: SharedDirectory) -> ProcessRole {
        ProcessRole::Local(LocalRole::Virtiofsd(directory))
    }

    #[tokio::test]
    async fn boot_starts_every_share_then_qemu_and_a_lost_share_stops_the_machine() {
        let mut fixture = fixture(FAKE_VIRTIOFSD).await;
        let hypervisor = spawn(&fixture).await;
        let generation = Generation::FIRST;
        hypervisor
            .send_message(HypervisorCommand::Boot {
                generation,
                plan: plan(&fixture, Incarnation::mint()),
            })
            .unwrap();
        let started = until_running(&mut fixture.observations).await;
        let roles: Vec<ProcessRole> = started.keys().cloned().collect();
        assert_eq!(
            roles,
            [
                ProcessRole::Local(LocalRole::Qemu),
                virtiofsd(SharedDirectory::NixStore),
                virtiofsd(SharedDirectory::GuestState),
                virtiofsd(SharedDirectory::Requested(0)),
            ]
        );
        let arguments = files::read_to_string(&fixture.vm.qemu_arguments()).unwrap();
        assert!(arguments.contains("hostfwd=tcp:127.0.0.1:40022-:22"));

        let share = virtiofsd(SharedDirectory::Requested(0));
        process::signal(&started[&share], Signal::Kill).unwrap();
        let mut exited = Vec::new();
        let reason = loop {
            match next(&mut fixture.observations).await {
                HypervisorObservation::ProcessExited { role, .. } => exited.push(role),
                HypervisorObservation::MachineStopped { reason, .. } => break reason,
                other @ (HypervisorObservation::ProcessStarted { .. }
                | HypervisorObservation::MachineRunning { .. }
                | HypervisorObservation::BootFailed { .. }
                | HypervisorObservation::Adopted { .. }
                | HypervisorObservation::Lost { .. }) => unexpected(&format!("{other:?}")),
            }
        };
        assert_eq!(exited.first(), Some(&share));
        assert!(exited.contains(&ProcessRole::Local(LocalRole::Qemu)));
        assert_eq!(
            reason.map(|reason| reason.code),
            Some("hypervisor.share_exited".into())
        );
        kill_all(&started);
    }

    #[tokio::test]
    async fn qemu_going_first_is_blamed_even_when_a_share_exit_is_seen_first() {
        let mut fixture = fixture(FAKE_VIRTIOFSD).await;
        let hypervisor = spawn(&fixture).await;
        hypervisor
            .send_message(HypervisorCommand::Boot {
                generation: Generation::FIRST,
                plan: plan(&fixture, Incarnation::mint()),
            })
            .unwrap();
        let started = until_running(&mut fixture.observations).await;
        // Kill qemu and a share at once, as when qemu stops and its shares follow.
        process::signal(&started[&ProcessRole::Local(LocalRole::Qemu)], Signal::Kill).unwrap();
        process::signal(
            &started[&virtiofsd(SharedDirectory::Requested(0))],
            Signal::Kill,
        )
        .unwrap();
        let reason = loop {
            match next(&mut fixture.observations).await {
                HypervisorObservation::ProcessExited { .. } => {}
                HypervisorObservation::MachineStopped { reason, .. } => break reason,
                other @ (HypervisorObservation::ProcessStarted { .. }
                | HypervisorObservation::MachineRunning { .. }
                | HypervisorObservation::BootFailed { .. }
                | HypervisorObservation::Adopted { .. }
                | HypervisorObservation::Lost { .. }) => unexpected(&format!("{other:?}")),
            }
        };
        assert_eq!(
            reason.map(|reason| reason.code),
            Some("hypervisor.qemu_exited".into())
        );
        kill_all(&started);
    }

    async fn until_stopped(
        observations: &mut UnboundedReceiver<HypervisorObservation>,
    ) -> Option<Report> {
        loop {
            match next(observations).await {
                HypervisorObservation::ProcessExited { .. } => {}
                HypervisorObservation::MachineStopped { reason, .. } => return reason,
                other @ (HypervisorObservation::ProcessStarted { .. }
                | HypervisorObservation::MachineRunning { .. }
                | HypervisorObservation::BootFailed { .. }
                | HypervisorObservation::Adopted { .. }
                | HypervisorObservation::Lost { .. }) => unexpected(&format!("{other:?}")),
            }
        }
    }

    #[tokio::test]
    async fn stop_asks_the_guest_first_then_escalates_and_stops_the_shares() {
        let mut fixture = fixture(FAKE_VIRTIOFSD).await;
        let hypervisor = spawn(&fixture).await;
        let generation = Generation::FIRST;
        hypervisor
            .send_message(HypervisorCommand::Boot {
                generation,
                plan: plan(&fixture, Incarnation::mint()),
            })
            .unwrap();
        let started = until_running(&mut fixture.observations).await;
        // The fake qemu ignores the ACPI request, so the stop has to escalate.
        let mut commands =
            crate::adapters::qmp::tests::fake_qmp(fixture.vm.control_socket().as_path());
        hypervisor
            .send_message(HypervisorCommand::Stop {
                generation,
                vm: fixture.vm.clone(),
                processes: started.clone(),
            })
            .unwrap();
        assert_eq!(until_stopped(&mut fixture.observations).await, None);
        assert_eq!(commands.recv().await.as_deref(), Some("qmp_capabilities"));
        assert_eq!(commands.recv().await.as_deref(), Some("system_powerdown"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        for identity in started.values() {
            assert_eq!(
                process::liveness(identity).unwrap(),
                process::Liveness::Gone
            );
        }
    }

    #[tokio::test]
    async fn a_fresh_backend_stops_processes_it_never_started() {
        let mut fixture = fixture(FAKE_VIRTIOFSD).await;
        let generation = Generation::FIRST;
        let (first, _) = Actor::spawn(
            None,
            LocalQemu,
            LocalQemuSetup {
                report: fixture.report.clone(),
                placement: Placement::Direct,
                timing: crate::runtime::timing::Timing::quick(),
            },
        )
        .await
        .unwrap();
        first
            .cast(LocalQemuMessage::Command(HypervisorCommand::Boot {
                generation,
                plan: plan(&fixture, Incarnation::mint()),
            }))
            .unwrap();
        let started = until_running(&mut fixture.observations).await;
        first.stop_and_wait(None, None).await.unwrap();
        let second = spawn(&fixture).await;
        second
            .send_message(HypervisorCommand::Stop {
                generation,
                vm: fixture.vm.clone(),
                processes: started.clone(),
            })
            .unwrap();
        assert_eq!(until_stopped(&mut fixture.observations).await, None);
        tokio::time::sleep(Duration::from_millis(300)).await;
        for identity in started.values() {
            assert_eq!(
                process::liveness(identity).unwrap(),
                process::Liveness::Gone
            );
        }
    }

    #[tokio::test]
    async fn missing_sockets_fail_the_boot_and_terminate_what_started() {
        let mut fixture = fixture(SILENT_VIRTIOFSD).await;
        let hypervisor = spawn(&fixture).await;
        hypervisor
            .send_message(HypervisorCommand::Boot {
                generation: Generation::FIRST,
                plan: plan(&fixture, Incarnation::mint()),
            })
            .unwrap();
        let mut started = BTreeMap::new();
        let reason = loop {
            match next(&mut fixture.observations).await {
                HypervisorObservation::ProcessStarted { role, identity, .. } => {
                    started.insert(role, identity);
                }
                HypervisorObservation::BootFailed { reason, .. } => break reason,
                other @ (HypervisorObservation::MachineRunning { .. }
                | HypervisorObservation::MachineStopped { .. }
                | HypervisorObservation::Adopted { .. }
                | HypervisorObservation::Lost { .. }
                | HypervisorObservation::ProcessExited { .. }) => unexpected(&format!("{other:?}")),
            }
        };
        assert_eq!(reason.code, "hypervisor.sockets_missing");
        assert!(!started.contains_key(&ProcessRole::Local(LocalRole::Qemu)));
        tokio::time::sleep(Duration::from_millis(300)).await;
        for identity in started.values() {
            assert_eq!(
                process::liveness(identity).unwrap(),
                process::Liveness::Gone
            );
        }
    }

    #[tokio::test]
    async fn a_new_backend_adopts_a_running_machine_by_identity_or_nonce() {
        let mut fixture = fixture(FAKE_VIRTIOFSD).await;
        let (first, _) = Actor::spawn(
            None,
            LocalQemu,
            LocalQemuSetup {
                report: fixture.report.clone(),
                placement: Placement::Direct,
                timing: crate::runtime::timing::Timing::quick(),
            },
        )
        .await
        .unwrap();
        let incarnation = Incarnation::mint();
        let generation = Generation::FIRST;
        first
            .cast(LocalQemuMessage::Command(HypervisorCommand::Boot {
                generation,
                plan: plan(&fixture, incarnation),
            }))
            .unwrap();
        let started = until_running(&mut fixture.observations).await;
        // The runtime restarts: the old backend goes, its processes stay.
        first.stop_and_wait(None, None).await.unwrap();
        let qemu = ProcessRole::Local(LocalRole::Qemu);
        let processes: BTreeMap<ProcessRole, Adoptee> = started
            .iter()
            .map(|(role, identity)| {
                let adoptee = if *role == qemu {
                    // As if the runtime died before persisting qemu's identity.
                    Adoptee::Expected(Nonce::of(fixture.vm.name(), incarnation, generation, role))
                } else {
                    Adoptee::Known(identity.clone())
                };
                (role.clone(), adoptee)
            })
            .collect();
        let second = spawn(&fixture).await;
        second
            .send_message(HypervisorCommand::Adopt {
                generation,
                vm: fixture.vm.clone(),
                processes,
            })
            .unwrap();
        let adopted = until_running(&mut fixture.observations).await;
        assert_eq!(adopted, started);
        kill_all(&started);
    }
}
