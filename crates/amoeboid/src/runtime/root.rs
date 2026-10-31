//! Root actor: spawns the journal and listener, answers runtime requests, and routes VM
//! requests to each VM's twin, which it spawns on first use.
//!
//! Root-level strategy: the journal and listener are essential, so the death of either ends
//! the runtime (systemd restarts it; VMs outlive it by design). A twin that ends is dropped from
//! the registry and spawned again by the next request for its VM.
//!
//! Root knows clients only through the in-memory `waiters` table: which session waits on which
//! operation. A twin answers by operation id; a client reference never enters persisted state.
//!
//! ```text
//! pre_start ─▶ Streams{quarantine} ─▶ Replay quarantine/<name> × listed ─┐
//!           └▶ Streams{vm} ──────────────────────────────────────────────┴▶ loaded: twins for
//!              requests held until loaded                                  vm/* not quarantined
//! twin Unrecoverable ─▶ quarantined (in memory) ─▶ Replay quarantine/<name>
//!                       ─▶ no live event: Persist Quarantined at last + 1 (conflict: recorded)
//! Vacuum ─▶ [twin, not quarantined] Stop ─▶ Stopped ─┐
//!        └▶ [quarantined] ───────────────────────────┴▶ stop the twin actor ─▶ vacuum task
//!           (end processes, remove directories) ─▶ Vacuumed ─▶ Retire vm/<name>
//!           ─▶ Retire quarantine/<name> ─▶ reply Vacuumed
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef, SupervisionEvent};

use super::listener::{Listener, ListenerArgs};
use super::paths::RuntimePaths;
use super::quarantine::{self, QuarantineEvent};
use crate::diagnostic::Report;
use crate::domain::lifecycle::Incarnation;
use crate::domain::path::AbsolutePath;
use crate::domain::share::{ShareRequest, Shares};
use crate::domain::vm_name::VmName;
use crate::journal::{
    BatchId, Journal, JournalArgs, JournalMessage, JournalReply, PersistFailure, Persistence,
    PersistenceId, Replay, Sequence, StoreError,
};
use crate::nonempty::NonEmpty;
use crate::persistent::{Persistent, PersistentArgs, PersistentMessage};
use crate::protocol::{
    OperationId, ReplyBody, RequestBody, RuntimeId, RuntimeReply, RuntimeRequest, ServerFrame,
    VmReply, VmRequest,
};
use crate::vm::builder::BuildCommand;
use crate::vm::local_qemu::Placement;
use crate::vm::twin::{FromVm, TwinMessage, VmCommand, VmOutcome, VmReport, VmSetup, VmTwin};
use crate::vm::vacuum;
use crate::vm::{Account, NixConfig, SshEndpoint, VmCondition, VmSpec, VmStatus};

/// How long a status question waits for the twins; later answers are dropped.
const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a VM may take from qemu starting to sshd answering.
const BOOT_TIMEOUT_SECONDS: u32 = 120;

/// No `Persisted` within this: the persistent actor stops and is restarted.
const PERSIST_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// A status collection's time is up: answer with what has arrived.
    StatusTimeout { operation: OperationId },
    /// A vacuum task finished ending the VM's processes and removing its directories.
    Vacuumed {
        name: VmName,
        result: Result<(), Report>,
    },
}

impl From<JournalReply> for RootMessage {
    fn from(reply: JournalReply) -> Self {
        Self::Journal(reply)
    }
}

impl TryFrom<RootMessage> for JournalReply {
    type Error = RootMessage;
    fn try_from(message: RootMessage) -> Result<Self, RootMessage> {
        match message {
            RootMessage::Journal(reply) => Ok(reply),
            other @ (RootMessage::Request { .. }
            | RootMessage::FromVm(_)
            | RootMessage::StatusTimeout { .. }
            | RootMessage::Vacuumed { .. }) => Err(other),
        }
    }
}

impl From<FromVm> for RootMessage {
    fn from(report: FromVm) -> Self {
        Self::FromVm(report)
    }
}

impl TryFrom<RootMessage> for FromVm {
    type Error = RootMessage;
    fn try_from(message: RootMessage) -> Result<Self, RootMessage> {
        match message {
            RootMessage::FromVm(report) => Ok(report),
            other @ (RootMessage::Request { .. }
            | RootMessage::Journal(_)
            | RootMessage::StatusTimeout { .. }
            | RootMessage::Vacuumed { .. }) => Err(other),
        }
    }
}

/// How twins run their children: `nix build` and scoped processes in production.
#[derive(Clone, Debug)]
pub struct VmDefaults {
    pub build: BuildCommand,
    pub placement: Placement,
}

impl VmDefaults {
    #[must_use]
    pub fn production() -> Self {
        Self {
            build: BuildCommand::Nix,
            placement: Placement::Scoped,
        }
    }
}

pub struct RootArgs {
    pub id: RuntimeId,
    pub persistence: Persistence,
    pub paths: RuntimePaths,
    pub listener: tokio::net::UnixListener,
    /// The guest account of every VM: the user the runtime runs as.
    pub account: Account,
    pub vm_defaults: VmDefaults,
}

pub struct Root;

pub struct RootState {
    id: RuntimeId,
    /// `vm`: the parent of every twin's stream, `vm/<name>`.
    vm_streams: PersistenceId,
    /// `quarantine`: the parent of every quarantine record, `quarantine/<name>`.
    quarantine_streams: PersistenceId,
    paths: RuntimePaths,
    account: Account,
    vm_defaults: VmDefaults,
    journal: ActorRef<JournalMessage>,
    twins: BTreeMap<VmName, ActorRef<TwinMessage>>,
    waiters: BTreeMap<OperationId, (VmName, DerivedActorRef<ServerFrame>)>,
    statuses: BTreeMap<OperationId, StatusCollection>,
    /// Quarantined VMs and why: recorded in the journal, or reported since start. No twin is
    /// spawned for them and no request reaches one.
    quarantined: BTreeMap<VmName, Report>,
    /// Quarantines reported since start whose record is being looked up before the append.
    recording: BTreeSet<VmName>,
    /// Quarantine appends in flight.
    appending: BTreeMap<BatchId, VmName>,
    next_batch: BatchId,
    /// `Some` until the registry is known.
    loading: Option<Loading>,
    /// Vacuums in progress; other requests that would change the VM are refused meanwhile.
    vacuums: BTreeMap<VmName, Vacuum>,
    /// Twins stopped by a vacuum: their end is expected.
    vacuumed_twins: BTreeMap<VmName, ActorRef<TwinMessage>>,
}

struct Vacuum {
    operation: OperationId,
    reply_to: DerivedActorRef<ServerFrame>,
    step: VacuumStep,
}

enum VacuumStep {
    /// The twin is stopping the VM gracefully, as operation `stop`.
    Stopping { stop: OperationId },
    /// The vacuum task is ending what is left and removing the directories.
    Removing,
    /// Retiring streams in order; the first is in flight.
    Retiring { remaining: Vec<PersistenceId> },
}

/// Start-up: the registry is known once both listings and every quarantine reason are in.
/// Requests that arrive meanwhile are held, then handled in order, so no request sees a
/// registry that is still being filled.
#[derive(Default)]
struct Loading {
    held: Vec<RootMessage>,
    /// `None` until the `vm` listing arrives.
    vms: Option<Vec<VmName>>,
    /// `None` until the `quarantine` listing arrives; then the names whose reason is pending.
    quarantines: Option<BTreeSet<VmName>>,
}

impl Loading {
    fn complete(&self) -> bool {
        self.vms.is_some() && self.quarantines.as_ref().is_some_and(BTreeSet::is_empty)
    }
}

/// Requests Root refuses before any twin sees them.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("there is no VM named {0}")]
    Unknown(VmName),
    #[error("the VM {name} is quarantined: {reason}")]
    Quarantined { name: VmName, reason: String },
    #[error("the VM {0} is being vacuumed")]
    Vacuuming(VmName),
}

impl crate::diagnostic::Diagnostic for RegistryError {
    fn code(&self) -> &'static str {
        match self {
            Self::Unknown(_) => "runtime.vm.unknown",
            Self::Quarantined { .. } => "runtime.vm.quarantined",
            Self::Vacuuming(_) => "runtime.vm.vacuuming",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Unknown(_) => Some("see the VMs with amoeboid status".into()),
            Self::Quarantined { name, .. } => Some(format!(
                "its history is kept for inspection; remove the VM with amoeboid vacuum_vm {name}"
            )),
            Self::Vacuuming(_) => Some("wait for the vacuum to finish".into()),
        }
    }
}

/// A status question to several twins, answered when all have replied or time is up.
struct StatusCollection {
    collected: BTreeMap<VmName, VmCondition>,
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
    name: &'static str,
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
                persistence: args.persistence,
                paths: args.paths.clone(),
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
            id: args.id,
            vm_streams,
            quarantine_streams,
            paths: args.paths,
            account: args.account,
            vm_defaults: args.vm_defaults,
            journal,
            twins: BTreeMap::new(),
            waiters: BTreeMap::new(),
            statuses: BTreeMap::new(),
            quarantined: BTreeMap::new(),
            recording: BTreeSet::new(),
            appending: BTreeMap::new(),
            next_batch: BatchId::FIRST,
            loading: Some(Loading::default()),
            vacuums: BTreeMap::new(),
            vacuumed_twins: BTreeMap::new(),
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
            loaded(&myself, state, loading.vms.unwrap_or_default()).await;
            for message in loading.held {
                handle(&myself, message, state).await;
            }
        }
        Ok(())
    }

    fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<RootMessage>,
        event: SupervisionEvent,
        state: &mut RootState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let ended = match event {
            SupervisionEvent::ActorTerminated(cell, _, reason) => {
                Some((cell, reason.unwrap_or_else(|| "stopped".into())))
            }
            SupervisionEvent::ActorFailed(cell, error) => Some((cell, error.to_string())),
            SupervisionEvent::ActorStarted(_) | SupervisionEvent::ProcessGroupChanged(_) => None,
        };
        let result = match ended {
            None => Ok(()),
            Some((cell, reason)) => {
                let vacuumed = state
                    .vacuumed_twins
                    .iter()
                    .find(|(_, twin)| twin.get_id() == cell.get_id())
                    .map(|(name, _)| name.clone());
                if let Some(name) = vacuumed {
                    tracing::debug!(vm = %name, "twin stopped for a vacuum");
                    state.vacuumed_twins.remove(&name);
                    return std::future::ready(Ok(()));
                }
                let twin = state
                    .twins
                    .iter()
                    .find(|(_, twin)| twin.get_id() == cell.get_id())
                    .map(|(name, _)| name.clone());
                if let Some(name) = twin {
                    tracing::warn!(vm = %name, %reason, "twin ended; spawned again on the next request");
                    state.twins.remove(&name);
                    Ok(())
                } else {
                    let ended = EssentialEnded {
                        name: essential_name(&cell, state.id),
                        reason,
                    };
                    tracing::error!(%ended, "runtime ending");
                    Err(ended.into())
                }
            }
        };
        std::future::ready(result)
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
            body:
                RequestBody::Vm(VmRequest::Start {
                    name,
                    shares,
                    nix_config,
                }),
            reply_to,
        } => {
            let started = start(
                myself, state, operation, name, shares, nix_config, &reply_to,
            )
            .await;
            if let Err(failure) = started {
                reply(&reply_to, operation, ReplyBody::Failed(failure));
            }
        }
        RootMessage::Request {
            operation,
            body: RequestBody::Vm(VmRequest::Stop { name }),
            reply_to,
        } => {
            let sent = known_twin(state, &name).and_then(|twin| {
                twin.cast(PersistentMessage::Command(VmCommand::Stop { operation }))
                    .map_err(|error| unreachable_twin(&error))
            });
            match sent {
                Ok(()) => {
                    state.waiters.insert(operation, (name, reply_to));
                }
                Err(failure) => reply(&reply_to, operation, ReplyBody::Failed(failure)),
            }
        }
        RootMessage::Request {
            operation,
            body: RequestBody::Vm(VmRequest::Status { name }),
            reply_to,
        } => status(myself, state, operation, name, reply_to),
        RootMessage::Request {
            operation,
            body: RequestBody::Vm(VmRequest::Vacuum { name }),
            reply_to,
        } => begin_vacuum(myself, state, operation, name, reply_to),
        RootMessage::Vacuumed { name, result } => match result {
            Ok(()) => retire_next(myself, state, &name, None),
            Err(failure) => vacuum_failed(myself, state, &name, failure).await,
        },
        RootMessage::StatusTimeout { operation } => {
            if let Some(collection) = state.statuses.remove(&operation) {
                collection.reply(operation);
            }
        }
        RootMessage::FromVm(FromVm { vm, report }) => from_vm(myself, state, &vm, report),
        RootMessage::Journal(reply) => journal(myself, state, reply).await,
    }
}

/// The registry is known: a twin for every VM that isn't quarantined.
async fn loaded(myself: &ActorRef<RootMessage>, state: &mut RootState, vms: Vec<VmName>) {
    for name in vms {
        if state.quarantined.contains_key(&name) {
            tracing::warn!(vm = %name, "quarantined; its twin is not started");
            continue;
        }
        if let Err(failure) = twin(myself, state, &name).await {
            tracing::error!(vm = %name, message = failure.message, "can't start its twin");
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

async fn journal(myself: &ActorRef<RootMessage>, state: &mut RootState, reply: JournalReply) {
    match reply {
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
            if let Some(name) = retiring(state, &id) {
                retire_next(myself, state, &name, Some(&id));
            }
        }
        JournalReply::RetireFailed { id, reason } => {
            if let Some(name) = retiring(state, &id) {
                let failure = Report {
                    code: "runtime.vm.retire".into(),
                    message: format!("can't retire {}: {reason}", id.as_str()),
                    hint: None,
                };
                vacuum_failed(myself, state, &name, failure).await;
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

/// A quarantine record: its reason at start, or, for a quarantine reported since, whether it
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
        if let Some(reason) = quarantine::reason(&replay.events) {
            state.quarantined.insert(name, reason);
        }
        return;
    }
    if !state.recording.remove(&name) {
        return;
    }
    if !replay.events.is_empty() {
        return tracing::debug!(vm = %name, "quarantine already recorded");
    }
    let Some(reason) = state.quarantined.get(&name).cloned() else {
        return;
    };
    let appended = Sequence::after(replay.last)
        .ok_or_else(|| "stream position overflow".to_owned())
        .and_then(|first| {
            let event = quarantine::encode(QuarantineEvent::Quarantined { reason })
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
            Report {
                code: "runtime.quarantine.unreadable".into(),
                message: format!("can't read the quarantine record: {reason}"),
                hint: None,
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
async fn start(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    name: VmName,
    shares: Vec<ShareRequest>,
    nix_config: Option<AbsolutePath>,
    reply_to: &DerivedActorRef<ServerFrame>,
) -> Result<(), Report> {
    if let Some(refusal) = refusal(state, &name) {
        return Err(refusal);
    }
    let home = AbsolutePath::parse(state.account.home.as_str().into())
        .map_err(|error| Report::of(&error))?;
    let shares = Shares::validate(shares, &[home], |directory| {
        directory.path().as_path().is_dir()
    })
    .map_err(|error| Report::of(&error))?;
    let twin = twin(myself, state, &name).await?;
    let spec = VmSpec {
        shares,
        account: state.account.clone(),
        nix_config: nix_config.map_or(NixConfig::Existing, NixConfig::From),
        boot_timeout_seconds: BOOT_TIMEOUT_SECONDS,
    };
    state.waiters.insert(operation, (name, reply_to.clone()));
    twin.cast(PersistentMessage::Command(VmCommand::Start {
        operation,
        spec,
        incarnation: Incarnation::mint(),
    }))
    .map_err(|error| unreachable_twin(&error))
}

/// The VM's twin, spawned (linked to Root) on first use.
async fn twin(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    name: &VmName,
) -> Result<ActorRef<TwinMessage>, Report> {
    if let Some(twin) = state.twins.get(name) {
        return Ok(twin.clone());
    }
    let vm = state.paths.vm(name).map_err(|error| Report::of(&error))?;
    let id = state
        .vm_streams
        .child(name.as_str())
        .map_err(|error| Report::of(&error))?;
    let (twin, _) = Actor::spawn_linked(
        Some(format!("{}/vm/{name}", state.id)),
        Persistent::<VmTwin>::new(),
        PersistentArgs {
            id,
            journal: state.journal.clone(),
            setup: VmSetup {
                vm,
                root: myself.get_derived(),
                build: state.vm_defaults.build.clone(),
                placement: state.vm_defaults.placement,
            },
            persist_timeout: PERSIST_TIMEOUT,
        },
        myself.get_cell(),
    )
    .await
    .map_err(|error| Report {
        code: "runtime.vm.spawn".into(),
        message: format!("can't start the VM's twin: {error}"),
        hint: None,
    })?;
    state.twins.insert(name.clone(), twin.clone());
    Ok(twin)
}

fn from_vm(myself: &ActorRef<RootMessage>, state: &mut RootState, vm: &VmName, report: VmReport) {
    match report {
        VmReport::OperationDone { operation, result } => {
            if is_vacuum_stop(state, vm, operation) {
                return vacuum_stopped(myself, state, vm, result);
            }
            if let Some(collection) = state.statuses.get_mut(&operation) {
                let condition = match result {
                    Ok(VmOutcome::Status(condition)) => condition,
                    Ok(VmOutcome::Started(_) | VmOutcome::Stopped) | Err(_) => VmCondition::Unknown,
                };
                collection.collected.insert(vm.clone(), condition);
                if collection.complete()
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
            tracing::error!(%vm, code = reason.code, message = reason.message, "VM quarantined");
            // The twin stays alive refusing commands; Root routes nothing to it from now on.
            if state.quarantined.insert(vm.clone(), reason).is_none() {
                state.recording.insert(vm.clone());
                ask_record(myself, state, vm);
            }
        }
    }
}

/// The refusal for a VM being vacuumed or quarantined, if it is one.
fn refusal(state: &RootState, name: &VmName) -> Option<Report> {
    if state.vacuums.contains_key(name) {
        return Some(Report::of(&RegistryError::Vacuuming(name.clone())));
    }
    state.quarantined.get(name).map(|reason| {
        Report::of(&RegistryError::Quarantined {
            name: name.clone(),
            reason: reason.message.clone(),
        })
    })
}

/// The twin of a VM the runtime knows (from the journal, or started since) and that isn't
/// quarantined.
fn known_twin(state: &RootState, name: &VmName) -> Result<ActorRef<TwinMessage>, Report> {
    if let Some(refusal) = refusal(state, name) {
        return Err(refusal);
    }
    state
        .twins
        .get(name)
        .cloned()
        .ok_or_else(|| Report::of(&RegistryError::Unknown(name.clone())))
}

fn unreachable_twin(error: &dyn std::fmt::Display) -> Report {
    Report {
        code: "runtime.vm.unreachable".into(),
        message: format!("can't reach the VM's twin: {error}"),
        hint: None,
    }
}

/// Starts a vacuum: a twin that can still run stops the VM gracefully first; a quarantined
/// VM goes straight to removal.
fn begin_vacuum(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    name: VmName,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    if state.vacuums.contains_key(&name) {
        let failure = Report::of(&RegistryError::Vacuuming(name));
        return reply(&reply_to, operation, ReplyBody::Failed(failure));
    }
    let step = match (
        state.quarantined.contains_key(&name),
        state.twins.get(&name),
    ) {
        (true, _) => VacuumStep::Removing,
        (false, Some(twin)) => {
            let stop = OperationId::mint();
            if let Err(error) = twin.cast(PersistentMessage::Command(VmCommand::Stop {
                operation: stop,
            })) {
                let failure = unreachable_twin(&error);
                return reply(&reply_to, operation, ReplyBody::Failed(failure));
            }
            VacuumStep::Stopping { stop }
        }
        (false, None) => {
            let failure = Report::of(&RegistryError::Unknown(name));
            return reply(&reply_to, operation, ReplyBody::Failed(failure));
        }
    };
    let removing = matches!(step, VacuumStep::Removing);
    state.vacuums.insert(
        name.clone(),
        Vacuum {
            operation,
            reply_to,
            step,
        },
    );
    if removing {
        remove(myself, state, &name);
    }
}

/// Whether `operation` is the stop a vacuum of `vm` asked its twin for.
fn is_vacuum_stop(state: &RootState, vm: &VmName, operation: OperationId) -> bool {
    state.vacuums.get(vm).is_some_and(
        |vacuum| matches!(vacuum.step, VacuumStep::Stopping { stop } if stop == operation),
    )
}

/// The twin's answer to a vacuum's stop.
fn vacuum_stopped(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    vm: &VmName,
    result: Result<VmOutcome, Report>,
) {
    match result {
        Ok(_) => remove(myself, state, vm),
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

/// Stops the VM's twin (if any) and starts the vacuum task.
fn remove(myself: &ActorRef<RootMessage>, state: &mut RootState, name: &VmName) {
    if let Some(vacuum) = state.vacuums.get_mut(name) {
        vacuum.step = VacuumStep::Removing;
    }
    if let Some(twin) = state.twins.remove(name) {
        twin.stop(Some("vacuumed".into()));
        state.vacuumed_twins.insert(name.clone(), twin);
    }
    let paths = state.paths.vm(name);
    let root = myself.clone();
    let name = name.clone();
    tokio::spawn(async move {
        let result = match paths {
            Ok(paths) => vacuum::vacuum(&paths)
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
            VacuumStep::Stopping { .. } | VacuumStep::Removing | VacuumStep::Retiring { .. } => {
                None
            }
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
            VacuumStep::Stopping { .. } | VacuumStep::Removing | VacuumStep::Retiring { .. },
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

/// A vacuum that failed after its twin was stopped: the VM must stay addressable, so a VM
/// that isn't quarantined gets its twin back (it replays what is left).
async fn vacuum_failed(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    name: &VmName,
    failure: Report,
) {
    let Some(vacuum) = state.vacuums.remove(name) else {
        return;
    };
    tracing::error!(vm = %name, message = failure.message, "vacuum failed");
    if !state.quarantined.contains_key(name)
        && let Err(respawn) = twin(myself, state, name).await
    {
        tracing::error!(vm = %name, message = respawn.message, "can't start its twin again");
    }
    reply(
        &vacuum.reply_to,
        vacuum.operation,
        ReplyBody::Failed(failure),
    );
}

/// Asks one twin, or every twin, for its condition; tell-only: answers are collected as they
/// arrive and the reply goes out when all are in or `STATUS_TIMEOUT` passes.
fn status(
    myself: &ActorRef<RootMessage>,
    state: &mut RootState,
    operation: OperationId,
    name: Option<VmName>,
    reply_to: DerivedActorRef<ServerFrame>,
) {
    let quarantined = |name: &VmName, reason: &Report| {
        (
            name.clone(),
            VmCondition::Quarantined {
                reason: reason.clone(),
            },
        )
    };
    let (asked, known): (Vec<(VmName, ActorRef<TwinMessage>)>, Vec<_>) = match name {
        Some(name) => match (state.quarantined.get(&name), state.twins.get(&name)) {
            (Some(reason), _) => (Vec::new(), vec![quarantined(&name, reason)]),
            (None, Some(twin)) => (vec![(name, twin.clone())], Vec::new()),
            (None, None) => {
                let failure = Report::of(&RegistryError::Unknown(name));
                return reply(&reply_to, operation, ReplyBody::Failed(failure));
            }
        },
        None => (
            state
                .twins
                .iter()
                .filter(|(name, _)| !state.quarantined.contains_key(*name))
                .map(|(name, twin)| (name.clone(), twin.clone()))
                .collect(),
            state
                .quarantined
                .iter()
                .map(|(name, reason)| quarantined(name, reason))
                .collect(),
        ),
    };
    let collection = StatusCollection {
        collected: asked
            .iter()
            .map(|(name, _)| (name.clone(), VmCondition::Unknown))
            .chain(known)
            .collect(),
        reply_to,
    };
    if asked.is_empty() {
        return collection.reply(operation);
    }
    state.statuses.insert(operation, collection);
    for (_, twin) in asked {
        let _ = twin.cast(PersistentMessage::Command(VmCommand::Status { operation }));
    }
    myself.send_after(STATUS_TIMEOUT, move || RootMessage::StatusTimeout {
        operation,
    });
}

fn essential_name(cell: &ractor::ActorCell, runtime: RuntimeId) -> &'static str {
    match cell.get_name() {
        Some(name) if name == format!("{runtime}/journal") => "journal",
        Some(name) if name == format!("{runtime}/listener") => "listener",
        Some(_) | None => "unknown child",
    }
}
