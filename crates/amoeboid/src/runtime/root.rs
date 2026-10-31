//! Root actor: spawns the journal, the listener and the machine supervisor, answers runtime
//! requests, and routes VM requests to each VM's twin.
//!
//! Root-level strategy: the journal, the listener and the machine supervisor are essential, so
//! the end of any of them ends the runtime (systemd restarts it; VMs outlive it by design).
//! Twins are not Root's children: the machine supervisor runs and restarts them (0003
//! §Supervision), and Root routes by what it reports.
//!
//! Root knows clients only through the in-memory `waiters` table: which session waits on which
//! operation. A twin answers by operation id; a client reference never enters persisted state.
//!
//! ```text
//! pre_start ─▶ Streams{quarantine} ─▶ Replay quarantine/<name> × listed ─┐
//!           └▶ Streams{vm} ──────────────────────────────────────────────┴▶ loaded: Ensure every
//!              requests held until loaded                                  vm/* not quarantined
//! Ensure ─▶ route Spawning (requests held) ─▶ Up{twin} ─▶ route Up (held requests handled)
//! Restarting ─▶ route Restarting, waiters failed    Exhausted ─▶ quarantine SuspiciousHalt
//! twin Unrecoverable ─▶ quarantine Unreplayable
//! quarantine ─▶ Replay quarantine/<name> ─▶ no live event: Persist at last + 1 (conflict: recorded)
//! Release (SuspiciousHalt) ─▶ Retire quarantine/<name> ─▶ Ensure (fresh budget) ─▶ Released
//! Vacuum ─▶ [twin up, not quarantined] Stop ─▶ Stopped ─┐
//!        └▶ [otherwise] ────────────────────────────────┴▶ Terminate ─▶ Terminated ─▶ vacuum task
//!           (end processes, remove directories) ─▶ Vacuumed ─▶ Retire vm/<name>
//!           ─▶ Retire quarantine/<name> ─▶ reply Vacuumed
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef, SupervisionEvent};

use super::listener::{Listener, ListenerArgs};
use super::machine_supervisor::{
    MachineReport, MachineSupervisor, MachineSupervisorArgs, MachineSupervisorMessage,
};
use super::paths::{ConfigPaths, RuntimePaths};
use super::quarantine::{self, QuarantineCause, QuarantineEvent};
use super::timing::ProcessTiming;
use crate::clock::Clock;
use crate::config::daemon::DaemonConfig;
use crate::diagnostic::Report;
use crate::domain::lifecycle::Incarnation;
use crate::domain::path::AbsolutePath;
use crate::domain::share::{ShareRequest, Shares};
use crate::domain::vm_name::VmName;
use crate::journal::{
    BatchId, Journal, JournalArgs, JournalMessage, JournalReply, JournalStore, PersistFailure,
    PersistenceId, Replay, Sequence, StoreError,
};
use crate::nonempty::NonEmpty;
use crate::persistent::PersistentMessage;
use crate::protocol::{
    OperationId, ReplyBody, RequestBody, RuntimeId, RuntimeReply, RuntimeRequest, ServerFrame,
    VmReply, VmRequest,
};
use crate::vm::builder::BuildCommand;
use crate::vm::local_qemu::Placement;
use crate::vm::twin::{FromVm, TwinMessage, VmCommand, VmOutcome, VmReport};
use crate::vm::vacuum;
use crate::vm::{Account, NixConfig, SshEndpoint, VmCondition, VmSpec, VmStatus};

#[derive(Debug)]
pub enum RootMessage {
    /// A request from a session past its handshake; the reply goes to `reply_to`.
    Request {
        operation: OperationId,
        body: RequestBody,
        reply_to: DerivedActorRef<ServerFrame>,
    },
    /// A twin's report.
    FromVm(FromVm),
    /// The journal's answer to Root's own questions: stream listings and quarantine records.
    Journal(JournalReply),
    /// The machine supervisor's report: a twin up, restarting, given up or terminated.
    Machine(MachineReport),
    /// A status collection's time is up: answer with what has arrived.
    StatusTimeout { operation: OperationId },
    /// A vacuum task finished ending the VM's processes and removing its directories.
    Vacuumed {
        name: VmName,
        result: Result<(), Report>,
    },
}

/// One wrapper variant per derived type keeps `TryFrom` total (ractor's `get_derived`).
/// `$others` are every other variant, so a new variant fails to compile until listed.
macro_rules! derived {
    ($variant:ident, $type:ty, $($others:pat_param)|+) => {
        impl From<$type> for RootMessage {
            fn from(message: $type) -> Self {
                Self::$variant(message)
            }
        }
        impl TryFrom<RootMessage> for $type {
            type Error = RootMessage;
            fn try_from(message: RootMessage) -> Result<Self, RootMessage> {
                match message {
                    RootMessage::$variant(inner) => Ok(inner),
                    other @ ($($others)|+) => Err(other),
                }
            }
        }
    };
}
derived!(
    Journal,
    JournalReply,
    RootMessage::Request { .. }
        | RootMessage::FromVm(_)
        | RootMessage::Machine(_)
        | RootMessage::StatusTimeout { .. }
        | RootMessage::Vacuumed { .. }
);
derived!(
    FromVm,
    FromVm,
    RootMessage::Request { .. }
        | RootMessage::Journal(_)
        | RootMessage::Machine(_)
        | RootMessage::StatusTimeout { .. }
        | RootMessage::Vacuumed { .. }
);
derived!(
    Machine,
    MachineReport,
    RootMessage::Request { .. }
        | RootMessage::FromVm(_)
        | RootMessage::Journal(_)
        | RootMessage::StatusTimeout { .. }
        | RootMessage::Vacuumed { .. }
);

/// How twins run their children (`nix build` and scoped processes in production) and how long
/// processes get; set by whoever builds the runtime, never by a configuration file.
#[derive(Clone, Debug)]
pub struct VmDefaults {
    pub build: BuildCommand,
    pub placement: Placement,
    pub process: ProcessTiming,
}

impl VmDefaults {
    #[must_use]
    pub fn production() -> Self {
        Self {
            build: BuildCommand::Nix,
            placement: Placement::Scoped,
            process: ProcessTiming::production(),
        }
    }
}

pub struct RootArgs {
    pub id: RuntimeId,
    pub journal: JournalStore,
    pub config: ConfigPaths,
    /// Runtime-wide settings, read when the runtime started.
    pub daemon: DaemonConfig,
    pub paths: RuntimePaths,
    pub listener: tokio::net::UnixListener,
    /// The guest account of every VM: the user the runtime runs as.
    pub account: Account,
    pub vm_defaults: VmDefaults,
    pub clock: Clock,
}

pub struct Root;

pub struct RootState {
    /// `vm`: the parent of every twin's stream, `vm/<name>`.
    vm_streams: PersistenceId,
    /// `quarantine`: the parent of every quarantine record, `quarantine/<name>`.
    quarantine_streams: PersistenceId,
    paths: RuntimePaths,
    config: ConfigPaths,
    account: Account,
    process: ProcessTiming,
    status_timeout: Duration,
    journal: ActorRef<JournalMessage>,
    machines: ActorRef<MachineSupervisorMessage>,
    /// Where each VM's requests go, as the machine supervisor last reported.
    routes: BTreeMap<VmName, Route>,
    waiters: BTreeMap<OperationId, (VmName, DerivedActorRef<ServerFrame>)>,
    statuses: BTreeMap<OperationId, StatusCollection>,
    /// Quarantined VMs and why: recorded in the journal, or decided since start. No request
    /// reaches their twin.
    quarantined: BTreeMap<VmName, QuarantineCause>,
    /// Quarantines decided since start whose record is being looked up before the append.
    recording: BTreeSet<VmName>,
    /// Quarantine appends in flight.
    appending: BTreeMap<BatchId, VmName>,
    next_batch: BatchId,
    /// `Some` until the registry is known.
    loading: Option<Loading>,
    /// Vacuums in progress; other requests that would change the VM are refused meanwhile.
    vacuums: BTreeMap<VmName, Vacuum>,
    /// Releases in progress: the quarantine record is being retired.
    releases: BTreeMap<VmName, (OperationId, DerivedActorRef<ServerFrame>)>,
}

enum Route {
    /// `Ensure` sent; requests for the VM wait here, in order.
    Spawning {
        held: Vec<RootMessage>,
    },
    Up {
        twin: ActorRef<TwinMessage>,
    },
    /// The twin failed; the supervisor restarts it after a backoff.
    Restarting {
        reason: Report,
        attempt: u32,
    },
}

struct Vacuum {
    operation: OperationId,
    reply_to: DerivedActorRef<ServerFrame>,
    step: VacuumStep,
}

enum VacuumStep {
    /// The twin is stopping the VM gracefully, as operation `stop`.
    Stopping { stop: OperationId },
    /// The supervisor is stopping the twin.
    Terminating,
    /// The vacuum task is ending what is left and removing the directories.
    Removing,
    /// Retiring streams in order; the first is in flight.
    Retiring { remaining: Vec<PersistenceId> },
}

/// Start-up: the registry is known once both listings and every quarantine cause are in.
/// Requests that arrive meanwhile are held, then handled in order, so no request sees a
/// registry that is still being filled.
#[derive(Default)]
struct Loading {
    held: Vec<RootMessage>,
    /// `None` until the `vm` listing arrives.
    vms: Option<Vec<VmName>>,
    /// `None` until the `quarantine` listing arrives; then the names whose cause is pending.
    quarantines: Option<BTreeSet<VmName>>,
}

impl Loading {
    fn complete(&self) -> bool {
        self.vms.is_some() && self.quarantines.as_ref().is_some_and(BTreeSet::is_empty)
    }
}

/// Requests Root refuses or fails before or instead of a twin.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("there is no VM named {0}")]
    Unknown(VmName),
    #[error("the VM {name} is quarantined: {reason}")]
    Quarantined {
        name: VmName,
        reason: String,
        releasable: bool,
    },
    #[error("the VM {0} is being vacuumed")]
    Vacuuming(VmName),
    #[error("the twin of the VM {name} failed and is restarting (attempt {attempt})")]
    Restarting { name: VmName, attempt: u32 },
    #[error("the twin of the VM {name} failed: {reason}")]
    TwinFailed { name: VmName, reason: String },
    #[error("the VM {0} is quarantined because its history can't be replayed")]
    NotReleasable(VmName),
    #[error("the VM {0} is not quarantined")]
    NotQuarantined(VmName),
}

impl crate::diagnostic::Diagnostic for RegistryError {
    fn code(&self) -> &'static str {
        match self {
            Self::Unknown(_) => "runtime.vm.unknown",
            Self::Quarantined { .. } => "runtime.vm.quarantined",
            Self::Vacuuming(_) => "runtime.vm.vacuuming",
            Self::Restarting { .. } => "runtime.vm.restarting",
            Self::TwinFailed { .. } => "runtime.vm.twin_failed",
            Self::NotReleasable(_) => "runtime.vm.not_releasable",
            Self::NotQuarantined(_) => "runtime.vm.not_quarantined",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Unknown(_) | Self::NotQuarantined(_) => {
                Some("see the VMs with amoeboid status".into())
            }
            Self::Quarantined {
                name,
                releasable: true,
                ..
            } => Some(format!(
                "its logs and history are kept for inspection; retry it with amoeboid release_vm \
                 {name}, or remove it with amoeboid vacuum_vm {name}"
            )),
            Self::Quarantined {
                name,
                releasable: false,
                ..
            }
            | Self::NotReleasable(name) => Some(format!(
                "its history is kept for inspection; remove the VM with amoeboid vacuum_vm {name}"
            )),
            Self::Vacuuming(_) => Some("wait for the vacuum to finish".into()),
            Self::Restarting { .. } => Some("retry shortly; see amoeboid status".into()),
            Self::TwinFailed { .. } => Some("see amoeboid status, then retry".into()),
        }
    }
}

/// A status question to several twins, answered when all have replied or time is up.
struct StatusCollection {
    collected: BTreeMap<VmName, VmCondition>,
    /// VMs whose twin is being spawned: asked once it is up.
    spawning: BTreeSet<VmName>,
    reply_to: DerivedActorRef<ServerFrame>,
}

impl StatusCollection {
    fn complete(&self) -> bool {
        self.collected
            .values()
            .all(|condition| *condition != VmCondition::Unknown)
    }

    fn reply(self, operation: OperationId) {
        let vms = self
            .collected
            .into_iter()
            .map(|(name, condition)| VmStatus { name, condition })
            .collect();
        reply(
            &self.reply_to,
            operation,
            ReplyBody::Vm(VmReply::Status { vms }),
        );
    }
}

#[derive(Debug, thiserror::Error)]
#[error("essential actor {name} ended: {reason}")]
struct EssentialEnded {
    name: String,
    reason: String,
}

impl Actor for Root {
    type Msg = RootMessage;
    type State = RootState;
    type Arguments = RootArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<RootMessage>,
        args: RootArgs,
    ) -> Result<RootState, ActorProcessingErr> {
        let (journal, _) = Actor::spawn_linked(
            Some(format!("{}/journal", args.id)),
            Journal,
            JournalArgs {
                store: args.journal,
                clock: args.clock,
            },
            myself.get_cell(),
        )
        .await?;
        let process = args.vm_defaults.process;
        let (machines, _) = Actor::spawn_linked(
            Some(format!("{}/machines", args.id)),
            MachineSupervisor,
            MachineSupervisorArgs {
                runtime: args.id,
                report: myself.get_derived(),
                from_vm: myself.get_derived(),
                journal: journal.clone(),
                paths: args.paths.clone(),
                vm_defaults: args.vm_defaults,
                restart: args.daemon.restart.policy(),
                persist_timeout: args.daemon.timeouts.persist.duration(),
            },
            myself.get_cell(),
        )
        .await?;
        Actor::spawn_linked(
            Some(format!("{}/listener", args.id)),
            Listener,
            ListenerArgs {
                listener: args.listener,
                root: myself.clone(),
                runtime: args.id,
            },
            myself.get_cell(),
        )
        .await?;
        // Every VM the journal knows gets its twin once loaded, so adoption starts with the
        // runtime; quarantined ones don't.
        let vm_streams = PersistenceId::parse("vm".into())?;
        let quarantine_streams = PersistenceId::parse("quarantine".into())?;
        for under in [&quarantine_streams, &vm_streams] {
            journal.cast(JournalMessage::Streams {
                under: under.clone(),
                reply_to: myself.get_derived(),
            })?;
        }
        Ok(RootState {
            vm_streams,
            quarantine_streams,
            paths: args.paths,
            config: args.config,
            account: args.account,
            process,
            status_timeout: args.daemon.timeouts.status.duration(),
            journal,
            machines,
            routes: BTreeMap::new(),
            waiters: BTreeMap::new(),
            statuses: BTreeMap::new(),
            quarantined: BTreeMap::new(),
            recording: BTreeSet::new(),
            appending: BTreeMap::new(),
            next_batch: BatchId::FIRST,
            loading: Some(Loading::default()),
            vacuums: BTreeMap::new(),
            releases: BTreeMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<RootMessage>,
        message: RootMessage,
        state: &mut RootState,
    ) -> Result<(), ActorProcessingErr> {
        if let (Some(loading), RootMessage::Request { .. }) = (&mut state.loading, &message) {
            loading.held.push(message);
            return Ok(());
        }
        handle(&myself, message, state).await;
        if state.loading.as_ref().is_some_and(Loading::complete)
            && let Some(loading) = state.loading.take()
        {
            loaded(state, loading.vms.unwrap_or_default());
            for message in loading.held {
                handle(&myself, message, state).await;
            }
        }
        Ok(())
    }

    /// Every child of Root is essential: whichever ends, the runtime ends.
    fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<RootMessage>,
        event: SupervisionEvent,
        _state: &mut RootState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let ended = match event {
            SupervisionEvent::ActorTerminated(cell, _, reason) => {
                Some((cell, reason.unwrap_or_else(|| "stopped".into())))
            }
            SupervisionEvent::ActorFailed(cell, error) => Some((cell, error.to_string())),
            SupervisionEvent::ActorStarted(_) | SupervisionEvent::ProcessGroupChanged(_) => None,
        };
        std::future::ready(match ended {
            None => Ok(()),
            Some((cell, reason)) => {
                let ended = EssentialEnded {
                    name: cell.get_name().unwrap_or_else(|| "an unnamed child".into()),
                    reason,
                };
                tracing::error!(%ended, "runtime ending");
                Err(ended.into())
            }
        })
    }
}

/// One message, with the registry known (or known to be unavailable).
async fn handle(myself: &ActorRef<RootMessage>, message: RootMessage, state: &mut RootState) {
    match message {
        RootMessage::Request {
            operation,
            body: RequestBody::Runtime(RuntimeRequest::Ping),
            reply_to,
        } => reply(&reply_to, operation, ReplyBody::Runtime(RuntimeReply::Pong)),
        RootMessage::Request {
            operation,
            body: RequestBody::Vm(request),
            reply_to,
        } => vm_request(myself, state, operation, request, reply_to),
        RootMessage::Vacuumed { name, result } => match result {
            Ok(()) => retire_next(myself, state, &name, None),
            Err(failure) => vacuum_failed(state, &name, failure),
        },
        RootMessage::StatusTimeout { operation } => {
            if let Some(collection) = state.statuses.remove(&operation) {
                collection.reply(operation);
            }
        }
        RootMessage::FromVm(FromVm { vm, report }) => from_vm(myself, state, &vm, report),
        RootMessage::Journal(reply) => journal(myself, state, reply),
        RootMessage::Machine(report) => machine(myself, state, report).await,
    }
}

fn vm_request(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    request: VmRequest,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    let name = match &request {
        VmRequest::Status { name } => {
            return status(myself, state, operation, name.clone(), reply_to);
        }
        VmRequest::Vacuum { name } => {
            return begin_vacuum(state, operation, name.clone(), reply_to);
        }
        VmRequest::Release { name } => {
            return release(myself, state, operation, name.clone(), reply_to);
        }
        VmRequest::Start { name, .. } | VmRequest::Stop { name } => name.clone(),
    };
    if let Some(refusal) = refusal(state, &name) {
        return reply(&reply_to, operation, ReplyBody::Failed(refusal));
    }
    let twin = match state.routes.get_mut(&name) {
        Some(Route::Up { twin }) => twin.clone(),
        Some(Route::Spawning { held }) => {
            return held.push(RootMessage::Request {
                operation,
                body: RequestBody::Vm(request),
                reply_to,
            });
        }
        Some(Route::Restarting { attempt, .. }) => {
            let failure = Report::of(&RegistryError::Restarting {
                name,
                attempt: *attempt,
            });
            return reply(&reply_to, operation, ReplyBody::Failed(failure));
        }
        // A first start creates the VM: its twin is spawned for it.
        None if matches!(request, VmRequest::Start { .. }) => {
            let held = RootMessage::Request {
                operation,
                body: RequestBody::Vm(request),
                reply_to,
            };
            return ensure(state, name, vec![held]);
        }
        None => {
            let failure = Report::of(&RegistryError::Unknown(name));
            return reply(&reply_to, operation, ReplyBody::Failed(failure));
        }
    };
    let sent = match request {
        VmRequest::Start {
            name,
            shares,
            nix_config,
        } => start(state, &twin, operation, name, shares, nix_config, &reply_to),
        VmRequest::Stop { name } => stop(state, &twin, operation, name, &reply_to),
        VmRequest::Status { .. } | VmRequest::Vacuum { .. } | VmRequest::Release { .. } => Ok(()),
    };
    if let Err(failure) = sent {
        reply(&reply_to, operation, ReplyBody::Failed(failure));
    }
}

/// Asks the supervisor for the VM's twin; `held` waits for it.
fn ensure(state: &mut RootState, name: VmName, held: Vec<RootMessage>) {
    if let Err(error) = state
        .machines
        .cast(MachineSupervisorMessage::Ensure { name: name.clone() })
    {
        tracing::error!(vm = %name, %error, "can't reach the machine supervisor");
    }
    state.routes.insert(name, Route::Spawning { held });
}

/// The registry is known: a twin for every VM that isn't quarantined.
fn loaded(state: &mut RootState, vms: Vec<VmName>) {
    for name in vms {
        if state.quarantined.contains_key(&name) {
            tracing::warn!(vm = %name, "quarantined; its twin is not started");
            continue;
        }
        ensure(state, name, Vec::new());
    }
}

/// The machine supervisor's report.
async fn machine(myself: &ActorRef<RootMessage>, state: &mut RootState, report: MachineReport) {
    match report {
        MachineReport::Up { name, twin } => {
            let previous = state
                .routes
                .insert(name.clone(), Route::Up { twin: twin.clone() });
            for (operation, collection) in &mut state.statuses {
                if collection.spawning.remove(&name) {
                    let _ = twin.cast(PersistentMessage::Command(VmCommand::Status {
                        operation: *operation,
                    }));
                }
            }
            if let Some(Route::Spawning { held }) = previous {
                for message in held {
                    Box::pin(handle(myself, message, state)).await;
                }
            }
        }
        MachineReport::Restarting {
            name,
            failure,
            attempt,
            ..
        } => {
            twin_failed(state, &name, &failure.report);
            state.routes.insert(
                name.clone(),
                Route::Restarting {
                    reason: failure.report.clone(),
                    attempt,
                },
            );
            settle_statuses(state, &name, || VmCondition::Restarting {
                reason: failure.report.clone(),
                attempt,
            });
        }
        MachineReport::Exhausted {
            name,
            restarts,
            failures,
        } => {
            let last = failures.last().map(|failure| failure.report.clone());
            if let Some(last) = &last {
                twin_failed(state, &name, last);
            }
            state.routes.remove(&name);
            let cause = QuarantineCause::SuspiciousHalt {
                restarts,
                failures: failures.into_iter().map(|failure| failure.report).collect(),
            };
            settle_statuses(state, &name, || VmCondition::Quarantined {
                cause: cause.clone(),
            });
            quarantine(myself, state, &name, cause);
        }
        MachineReport::Terminated { name } => {
            state.routes.remove(&name);
            if let Some(vacuum) = state.vacuums.get_mut(&name)
                && matches!(vacuum.step, VacuumStep::Terminating)
            {
                remove(myself, state, &name);
            }
        }
    }
}

/// A twin failed: every operation waiting on it is answered, and a vacuum waiting for its stop
/// goes on to terminate it.
fn twin_failed(state: &mut RootState, name: &VmName, reason: &Report) {
    let failed: Vec<OperationId> = state
        .waiters
        .iter()
        .filter(|(_, (vm, _))| vm == name)
        .map(|(operation, _)| *operation)
        .collect();
    for operation in failed {
        if let Some((_, reply_to)) = state.waiters.remove(&operation) {
            let failure = Report::of(&RegistryError::TwinFailed {
                name: name.clone(),
                reason: reason.message.clone(),
            });
            reply(&reply_to, operation, ReplyBody::Failed(failure));
        }
    }
    if let Some(vacuum) = state.vacuums.get_mut(name)
        && matches!(vacuum.step, VacuumStep::Stopping { .. })
    {
        vacuum.step = VacuumStep::Terminating;
        terminate(state, name);
    }
}

/// Status collections still waiting on `name` get `condition` for it.
fn settle_statuses(state: &mut RootState, name: &VmName, condition: impl Fn() -> VmCondition) {
    let mut complete = Vec::new();
    for (operation, collection) in &mut state.statuses {
        let waiting = collection.spawning.remove(name)
            || collection.collected.get(name) == Some(&VmCondition::Unknown);
        if waiting {
            collection.collected.insert(name.clone(), condition());
            if collection.complete() && collection.spawning.is_empty() {
                complete.push(*operation);
            }
        }
    }
    for operation in complete {
        if let Some(collection) = state.statuses.remove(&operation) {
            collection.reply(operation);
        }
    }
}

/// The VM names among `ids` below `parent`; anything else is logged and skipped.
fn names_below(ids: Vec<PersistenceId>, parent: &PersistenceId) -> Vec<VmName> {
    ids.into_iter()
        .filter_map(|id| {
            let name = id
                .below(parent)
                .and_then(|name| VmName::parse(name.to_owned()).ok());
            if name.is_none() {
                tracing::warn!(id = id.as_str(), "a stream that names no VM");
            }
            name
        })
        .collect()
}

fn journal(myself: &ActorRef<RootMessage>, state: &mut RootState, answer: JournalReply) {
    match answer {
        JournalReply::Streams { under, ids } => listed(myself, state, &under, ids),
        JournalReply::StreamsFailed { under, reason } => {
            tracing::error!(under = under.as_str(), %reason, "can't list streams; treated as none");
            listed(myself, state, &under, Vec::new());
        }
        JournalReply::Replayed { id, replay } => quarantine_replayed(myself, state, &id, &replay),
        JournalReply::ReplayFailed { id, reason } => quarantine_unreadable(state, &id, &reason),
        JournalReply::Persisted { batch, .. } => {
            if let Some(vm) = state.appending.remove(&batch) {
                tracing::info!(%vm, "quarantine recorded");
            }
        }
        JournalReply::PersistFailed { batch, reason } => {
            if let Some(vm) = state.appending.remove(&batch) {
                match reason {
                    // Root is the only writer of quarantine streams: someone was first.
                    PersistFailure::Conflict { .. } => {
                        tracing::debug!(%vm, "quarantine already recorded");
                    }
                    PersistFailure::Store(reason) => {
                        tracing::error!(%vm, %reason, "can't record the quarantine; held in memory");
                    }
                }
            }
        }
        JournalReply::Retired { id } => {
            if let Some(name) = releasing(state, &id) {
                released(state, &name);
            } else if let Some(name) = retiring(state, &id) {
                retire_next(myself, state, &name, Some(&id));
            }
        }
        JournalReply::RetireFailed { id, reason } => {
            let failure = Report {
                code: "runtime.vm.retire".into(),
                message: format!("can't retire {}: {reason}", id.as_str()),
                hint: None,
            };
            if let Some(name) = releasing(state, &id) {
                if let Some((operation, reply_to)) = state.releases.remove(&name) {
                    reply(&reply_to, operation, ReplyBody::Failed(failure));
                }
            } else if let Some(name) = retiring(state, &id) {
                vacuum_failed(state, &name, failure);
            }
        }
    }
}

fn listed(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    under: &PersistenceId,
    ids: Vec<PersistenceId>,
) {
    let Some(loading) = &mut state.loading else {
        return tracing::debug!(under = under.as_str(), "listing after start; ignored");
    };
    if *under == state.vm_streams {
        loading.vms = Some(names_below(ids, &state.vm_streams));
    } else if *under == state.quarantine_streams {
        let names = names_below(ids, &state.quarantine_streams);
        for name in &names {
            ask_record(myself, state, name);
        }
        if let Some(loading) = &mut state.loading {
            loading.quarantines = Some(names.into_iter().collect());
        }
    }
}

/// Marks the VM quarantined and records it, unless it already is. Nothing reaches its twin
/// from now on; a live twin (an unrecoverable one) stays alive refusing commands.
fn quarantine(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    name: &VmName,
    cause: QuarantineCause,
) {
    tracing::error!(vm = %name, cause = cause.summary(), "VM quarantined");
    if state.quarantined.insert(name.clone(), cause).is_none() {
        state.recording.insert(name.clone());
        ask_record(myself, state, name);
    }
}

/// Asks the journal for a VM's quarantine record.
fn ask_record(myself: &ActorRef<RootMessage>, state: &RootState, name: &VmName) {
    let asked = state
        .quarantine_streams
        .child(name.as_str())
        .map_err(|error| error.to_string())
        .and_then(|id| {
            state
                .journal
                .cast(JournalMessage::Replay {
                    id,
                    reply_to: myself.get_derived(),
                })
                .map_err(|error| error.to_string())
        });
    if let Err(error) = asked {
        tracing::error!(vm = %name, %error, "can't ask for the quarantine record");
    }
}

/// A quarantine record: its cause at start, or, for a quarantine decided since, whether it
/// still has to be appended.
fn quarantine_replayed(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    id: &PersistenceId,
    replay: &Replay,
) {
    let Some(name) = id
        .below(&state.quarantine_streams)
        .and_then(|name| VmName::parse(name.to_owned()).ok())
    else {
        return tracing::debug!(id = id.as_str(), "replay Root didn't ask for; ignored");
    };
    if let Some(pending) = state
        .loading
        .as_mut()
        .and_then(|loading| loading.quarantines.as_mut())
        && pending.remove(&name)
    {
        if let Some(cause) = quarantine::cause(&replay.events) {
            state.quarantined.insert(name, cause);
        }
        return;
    }
    if !state.recording.remove(&name) {
        return;
    }
    if !replay.events.is_empty() {
        return tracing::debug!(vm = %name, "quarantine already recorded");
    }
    let Some(cause) = state.quarantined.get(&name).cloned() else {
        return;
    };
    let appended = Sequence::after(replay.last)
        .ok_or_else(|| "stream position overflow".to_owned())
        .and_then(|first| {
            let event = quarantine::encode(&QuarantineEvent::Quarantined { cause })
                .map_err(|error| error.to_string())?;
            let batch = state.next_batch;
            state
                .journal
                .cast(JournalMessage::Persist {
                    batch,
                    id: id.clone(),
                    first,
                    events: NonEmpty::new(event, Vec::new()),
                    reply_to: myself.get_derived(),
                })
                .map_err(|error| error.to_string())?;
            state.next_batch = batch.next();
            state.appending.insert(batch, name.clone());
            Ok(())
        });
    if let Err(error) = appended {
        tracing::error!(vm = %name, %error, "can't record the quarantine; held in memory");
    }
}

/// A quarantine record that can't be read: at start the VM counts as quarantined (its twin
/// would otherwise replay a stream that may be broken).
fn quarantine_unreadable(state: &mut RootState, id: &PersistenceId, reason: &StoreError) {
    let Some(name) = id
        .below(&state.quarantine_streams)
        .and_then(|name| VmName::parse(name.to_owned()).ok())
    else {
        return tracing::debug!(id = id.as_str(), "replay Root didn't ask for; ignored");
    };
    tracing::error!(vm = %name, %reason, "can't read the quarantine record");
    if let Some(pending) = state
        .loading
        .as_mut()
        .and_then(|loading| loading.quarantines.as_mut())
        && pending.remove(&name)
    {
        state.quarantined.insert(
            name,
            QuarantineCause::Unreplayable {
                reason: Report {
                    code: "runtime.quarantine.unreadable".into(),
                    message: format!("can't read the quarantine record: {reason}"),
                    hint: None,
                },
            },
        );
        return;
    }
    state.recording.remove(&name);
}

fn reply(reply_to: &DerivedActorRef<ServerFrame>, operation: OperationId, body: ReplyBody) {
    if reply_to
        .send_message(ServerFrame::Reply { operation, body })
        .is_err()
    {
        tracing::debug!(%operation, "reply undeliverable: session ended");
    }
}

/// Checks the request against the world (shares: each a directory, none hiding the home), then
/// hands it to the VM's twin.
fn start(
    state: &mut RootState,
    twin: &ActorRef<TwinMessage>,
    operation: OperationId,
    name: VmName,
    shares: Vec<ShareRequest>,
    nix_config: Option<AbsolutePath>,
    reply_to: &DerivedActorRef<ServerFrame>,
) -> Result<(), Report> {
    // Read at every start: a change to the file applies to the next start.
    let run = crate::config::run::load(&state.config).map_err(|error| Report::of(&error))?;
    let home = AbsolutePath::parse(state.account.home.as_str().into())
        .map_err(|error| Report::of(&error))?;
    let shares = Shares::validate(shares, &[home], |directory| {
        directory.path().as_path().is_dir()
    })
    .map_err(|error| Report::of(&error))?;
    let spec = VmSpec {
        shares,
        account: state.account.clone(),
        nix_config: nix_config.map_or(NixConfig::Existing, NixConfig::From),
        boot_timeout_seconds: run.timeouts.boot.get(),
        powerdown_grace_seconds: run.timeouts.powerdown.get(),
    };
    state.waiters.insert(operation, (name, reply_to.clone()));
    twin.cast(PersistentMessage::Command(VmCommand::Start {
        operation,
        spec,
        incarnation: Incarnation::mint(),
    }))
    .map_err(|error| unreachable_twin(&error))
}

fn stop(
    state: &mut RootState,
    twin: &ActorRef<TwinMessage>,
    operation: OperationId,
    name: VmName,
    reply_to: &DerivedActorRef<ServerFrame>,
) -> Result<(), Report> {
    twin.cast(PersistentMessage::Command(VmCommand::Stop { operation }))
        .map_err(|error| unreachable_twin(&error))?;
    state.waiters.insert(operation, (name, reply_to.clone()));
    Ok(())
}

fn from_vm(myself: &ActorRef<RootMessage>, state: &mut RootState, vm: &VmName, report: VmReport) {
    match report {
        VmReport::OperationDone { operation, result } => {
            if is_vacuum_stop(state, vm, operation) {
                return vacuum_stopped(state, vm, result);
            }
            if let Some(collection) = state.statuses.get_mut(&operation) {
                // A twin asked while still replaying reports itself unrecoverable before it
                // refuses what it stashed, so its refusal finds the VM quarantined.
                let condition = match (result, state.quarantined.get(vm)) {
                    (Ok(VmOutcome::Status(condition)), _) => condition,
                    (Err(_), Some(cause)) => VmCondition::Quarantined {
                        cause: cause.clone(),
                    },
                    (Ok(VmOutcome::Started(_) | VmOutcome::Stopped) | Err(_), _) => {
                        VmCondition::Unknown
                    }
                };
                collection.collected.insert(vm.clone(), condition);
                if collection.complete()
                    && collection.spawning.is_empty()
                    && let Some(collection) = state.statuses.remove(&operation)
                {
                    collection.reply(operation);
                }
                return;
            }
            let Some((name, reply_to)) = state.waiters.remove(&operation) else {
                tracing::debug!(%operation, "no session waits on this operation");
                return;
            };
            let body = match (result, state.paths.vm(&name)) {
                (Ok(VmOutcome::Started(started)), Ok(paths)) => ReplyBody::Vm(VmReply::Started {
                    name,
                    ssh: SshEndpoint {
                        user: started.user,
                        port: started.port,
                        known_hosts: paths.known_hosts(),
                    },
                }),
                (Ok(VmOutcome::Started(_)), Err(error)) => ReplyBody::Failed(Report::of(&error)),
                (Ok(VmOutcome::Stopped), _) => ReplyBody::Vm(VmReply::Stopped { name }),
                (Ok(VmOutcome::Status(condition)), _) => ReplyBody::Vm(VmReply::Status {
                    vms: vec![VmStatus { name, condition }],
                }),
                (Err(failure), _) => ReplyBody::Failed(failure),
            };
            reply(&reply_to, operation, body);
        }
        VmReport::Unrecoverable { reason } => {
            quarantine(myself, state, vm, QuarantineCause::Unreplayable { reason });
        }
    }
}

/// The refusal for a VM being vacuumed or quarantined, if it is one.
fn refusal(state: &RootState, name: &VmName) -> Option<Report> {
    if state.vacuums.contains_key(name) || state.releases.contains_key(name) {
        return Some(Report::of(&RegistryError::Vacuuming(name.clone())));
    }
    state.quarantined.get(name).map(|cause| {
        Report::of(&RegistryError::Quarantined {
            name: name.clone(),
            reason: cause.summary(),
            releasable: matches!(cause, QuarantineCause::SuspiciousHalt { .. }),
        })
    })
}

fn unreachable_twin(error: &dyn std::fmt::Display) -> Report {
    Report {
        code: "runtime.vm.unreachable".into(),
        message: format!("can't reach the VM's twin: {error}"),
        hint: None,
    }
}

/// Lifts a `SuspiciousHalt` quarantine: its record is retired, then the twin is spawned with a
/// fresh budget.
fn release(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    name: VmName,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    let refused = match state.quarantined.get(&name) {
        _ if state.vacuums.contains_key(&name) || state.releases.contains_key(&name) => {
            Some(RegistryError::Vacuuming(name.clone()))
        }
        Some(QuarantineCause::SuspiciousHalt { .. }) => None,
        Some(QuarantineCause::Unreplayable { .. }) => {
            Some(RegistryError::NotReleasable(name.clone()))
        }
        None => Some(RegistryError::NotQuarantined(name.clone())),
    };
    if let Some(refused) = refused {
        return reply(
            &reply_to,
            operation,
            ReplyBody::Failed(Report::of(&refused)),
        );
    }
    let asked = state
        .quarantine_streams
        .child(name.as_str())
        .map_err(|error| Report::of(&error))
        .and_then(|id| {
            state
                .journal
                .cast(JournalMessage::Retire {
                    id,
                    reply_to: myself.get_derived(),
                })
                .map_err(|error| Report {
                    code: "runtime.vm.retire".into(),
                    message: format!("can't ask the journal to retire the record: {error}"),
                    hint: None,
                })
        });
    match asked {
        Ok(()) => {
            state.releases.insert(name, (operation, reply_to));
        }
        Err(failure) => reply(&reply_to, operation, ReplyBody::Failed(failure)),
    }
}

/// The VM whose release is retiring `id`, if any.
fn releasing(state: &RootState, id: &PersistenceId) -> Option<VmName> {
    let name = id
        .below(&state.quarantine_streams)
        .and_then(|name| VmName::parse(name.to_owned()).ok())?;
    state.releases.contains_key(&name).then_some(name)
}

fn released(state: &mut RootState, name: &VmName) {
    state.quarantined.remove(name);
    ensure(state, name.clone(), Vec::new());
    if let Some((operation, reply_to)) = state.releases.remove(name) {
        tracing::info!(vm = %name, "released from quarantine");
        reply(
            &reply_to,
            operation,
            ReplyBody::Vm(VmReply::Released { name: name.clone() }),
        );
    }
}

/// Starts a vacuum: a twin that is up stops the VM gracefully first; otherwise (quarantined,
/// restarting, spawning) the twin is terminated and the vacuum task ends what is left.
fn begin_vacuum(
    state: &mut RootState,
    operation: OperationId,
    name: VmName,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    if state.vacuums.contains_key(&name) || state.releases.contains_key(&name) {
        let failure = Report::of(&RegistryError::Vacuuming(name));
        return reply(&reply_to, operation, ReplyBody::Failed(failure));
    }
    let quarantined = state.quarantined.contains_key(&name);
    let step = match (quarantined, state.routes.get(&name)) {
        (false, Some(Route::Up { twin })) => {
            let stop = OperationId::mint();
            if let Err(error) = twin.cast(PersistentMessage::Command(VmCommand::Stop {
                operation: stop,
            })) {
                let failure = unreachable_twin(&error);
                return reply(&reply_to, operation, ReplyBody::Failed(failure));
            }
            VacuumStep::Stopping { stop }
        }
        (true, _) | (false, Some(Route::Spawning { .. } | Route::Restarting { .. })) => {
            VacuumStep::Terminating
        }
        (false, None) => {
            let failure = Report::of(&RegistryError::Unknown(name));
            return reply(&reply_to, operation, ReplyBody::Failed(failure));
        }
    };
    let terminating = matches!(step, VacuumStep::Terminating);
    state.vacuums.insert(
        name.clone(),
        Vacuum {
            operation,
            reply_to,
            step,
        },
    );
    if terminating {
        terminate(state, &name);
    }
}

fn terminate(state: &RootState, name: &VmName) {
    if let Err(error) = state
        .machines
        .cast(MachineSupervisorMessage::Terminate { name: name.clone() })
    {
        tracing::error!(vm = %name, %error, "can't reach the machine supervisor");
    }
}

/// Whether `operation` is the stop a vacuum of `vm` asked its twin for.
fn is_vacuum_stop(state: &RootState, vm: &VmName, operation: OperationId) -> bool {
    state.vacuums.get(vm).is_some_and(
        |vacuum| matches!(vacuum.step, VacuumStep::Stopping { stop } if stop == operation),
    )
}

/// The twin's answer to a vacuum's stop.
fn vacuum_stopped(state: &mut RootState, vm: &VmName, result: Result<VmOutcome, Report>) {
    match result {
        Ok(_) => {
            if let Some(vacuum) = state.vacuums.get_mut(vm) {
                vacuum.step = VacuumStep::Terminating;
            }
            terminate(state, vm);
        }
        Err(failure) => {
            // The twin still runs: nothing to respawn.
            if let Some(vacuum) = state.vacuums.remove(vm) {
                reply(
                    &vacuum.reply_to,
                    vacuum.operation,
                    ReplyBody::Failed(failure),
                );
            }
        }
    }
}

/// The twin is gone: the vacuum task ends what is left and removes the directories.
fn remove(myself: &ActorRef<RootMessage>, state: &mut RootState, name: &VmName) {
    if let Some(vacuum) = state.vacuums.get_mut(name) {
        vacuum.step = VacuumStep::Removing;
    }
    let paths = state.paths.vm(name);
    let process = state.process;
    let root = myself.clone();
    let name = name.clone();
    tokio::spawn(async move {
        let result = match paths {
            Ok(paths) => vacuum::vacuum(&paths, &process)
                .await
                .map_err(|error| Report::of(&error)),
            Err(error) => Err(Report::of(&error)),
        };
        if root.cast(RootMessage::Vacuumed { name, result }).is_err() {
            tracing::debug!("vacuum result undeliverable: runtime stopping");
        }
    });
}

/// The VM whose vacuum is retiring `id`, if any.
fn retiring(state: &RootState, id: &PersistenceId) -> Option<VmName> {
    state
        .vacuums
        .iter()
        .find_map(|(name, vacuum)| match &vacuum.step {
            VacuumStep::Retiring { remaining } if remaining.first() == Some(id) => {
                Some(name.clone())
            }
            VacuumStep::Stopping { .. }
            | VacuumStep::Terminating
            | VacuumStep::Removing
            | VacuumStep::Retiring { .. } => None,
        })
}

/// Retires the vacuum's next stream: `vm/<name>`, then `quarantine/<name>` (last, so a VM
/// interrupted halfway stays quarantined until done). `retired` is the one just answered.
fn retire_next(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    name: &VmName,
    retired: Option<&PersistenceId>,
) {
    let streams = [&state.vm_streams, &state.quarantine_streams]
        .into_iter()
        .map(|parent| parent.child(name.as_str()))
        .collect::<Result<Vec<_>, _>>();
    let Some(vacuum) = state.vacuums.get_mut(name) else {
        return;
    };
    let remaining = match (&mut vacuum.step, retired, streams) {
        (VacuumStep::Removing, None, Ok(streams)) => streams,
        (VacuumStep::Retiring { remaining }, Some(_), _) => {
            remaining.remove(0);
            std::mem::take(remaining)
        }
        (_, _, Err(error)) => {
            tracing::error!(vm = %name, %error, "can't name the VM's streams");
            Vec::new()
        }
        (
            VacuumStep::Stopping { .. }
            | VacuumStep::Terminating
            | VacuumStep::Removing
            | VacuumStep::Retiring { .. },
            _,
            _,
        ) => {
            return tracing::debug!(vm = %name, "retire reply out of step; ignored");
        }
    };
    let Some(next) = remaining.first().cloned() else {
        state.quarantined.remove(name);
        if let Some(vacuum) = state.vacuums.remove(name) {
            tracing::info!(vm = %name, "vacuumed");
            reply(
                &vacuum.reply_to,
                vacuum.operation,
                ReplyBody::Vm(VmReply::Vacuumed { name: name.clone() }),
            );
        }
        return;
    };
    vacuum.step = VacuumStep::Retiring { remaining };
    if let Err(error) = state.journal.cast(JournalMessage::Retire {
        id: next,
        reply_to: myself.get_derived(),
    }) {
        tracing::error!(vm = %name, %error, "can't ask the journal to retire");
    }
}

/// A vacuum that failed after its twin was terminated: the VM must stay addressable, so a VM
/// that isn't quarantined gets its twin back (it replays what is left).
fn vacuum_failed(state: &mut RootState, name: &VmName, failure: Report) {
    let Some(vacuum) = state.vacuums.remove(name) else {
        return;
    };
    tracing::error!(vm = %name, message = failure.message, "vacuum failed");
    if !state.quarantined.contains_key(name) && !state.routes.contains_key(name) {
        ensure(state, name.clone(), Vec::new());
    }
    reply(
        &vacuum.reply_to,
        vacuum.operation,
        ReplyBody::Failed(failure),
    );
}

/// Answers for one VM or every VM. Quarantined and restarting VMs are answered by Root; twins
/// that are up are asked (tell-only); twins being spawned are asked once up. The reply goes out
/// when all are in or the status timeout passes.
fn status(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    name: Option<VmName>,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    let names: Vec<VmName> = match name {
        Some(name) => {
            if !state.quarantined.contains_key(&name) && !state.routes.contains_key(&name) {
                let failure = Report::of(&RegistryError::Unknown(name));
                return reply(&reply_to, operation, ReplyBody::Failed(failure));
            }
            vec![name]
        }
        None => state
            .routes
            .keys()
            .chain(state.quarantined.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    let mut collection = StatusCollection {
        collected: BTreeMap::new(),
        spawning: BTreeSet::new(),
        reply_to,
    };
    let mut asked = Vec::new();
    for name in names {
        let condition = match (state.quarantined.get(&name), state.routes.get(&name)) {
            (Some(cause), _) => VmCondition::Quarantined {
                cause: cause.clone(),
            },
            (None, Some(Route::Restarting { reason, attempt })) => VmCondition::Restarting {
                reason: reason.clone(),
                attempt: *attempt,
            },
            (None, Some(Route::Up { twin })) => {
                asked.push(twin.clone());
                VmCondition::Unknown
            }
            (None, Some(Route::Spawning { .. }) | None) => {
                collection.spawning.insert(name.clone());
                VmCondition::Unknown
            }
        };
        collection.collected.insert(name, condition);
    }
    if asked.is_empty() && collection.spawning.is_empty() {
        return collection.reply(operation);
    }
    state.statuses.insert(operation, collection);
    for twin in asked {
        let _ = twin.cast(PersistentMessage::Command(VmCommand::Status { operation }));
    }
    myself.send_after(state.status_timeout, move || RootMessage::StatusTimeout {
        operation,
    });
}
