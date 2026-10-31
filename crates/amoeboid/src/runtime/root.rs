//! Root actor: spawns the journal and listener, answers runtime requests, and routes VM
//! requests to each VM's twin, which it spawns on first use.
//!
//! Root-level strategy: the journal and listener are essential, so the death of either ends
//! the runtime (systemd restarts it; VMs outlive it by design). A twin that ends is dropped from
//! the registry and spawned again by the next request for its VM.
//!
//! Root knows clients only through the in-memory `waiters` table: which session waits on which
//! operation. A twin answers by operation id; a client reference never enters persisted state.

use std::collections::BTreeMap;
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef, SupervisionEvent};

use super::listener::{Listener, ListenerArgs};
use super::paths::RuntimePaths;
use crate::diagnostic::Report;
use crate::domain::lifecycle::Incarnation;
use crate::domain::path::AbsolutePath;
use crate::domain::share::{ShareRequest, Shares};
use crate::domain::vm_name::VmName;
use crate::journal::{
    Journal, JournalArgs, JournalMessage, JournalReply, Persistence, PersistenceId,
};
use crate::persistent::{Persistent, PersistentArgs, PersistentMessage};
use crate::protocol::{
    OperationId, ReplyBody, RequestBody, RuntimeId, RuntimeReply, RuntimeRequest, ServerFrame,
    VmReply, VmRequest,
};
use crate::vm::builder::BuildCommand;
use crate::vm::local_qemu::Placement;
use crate::vm::twin::{FromVm, TwinMessage, VmCommand, VmOutcome, VmReport, VmSetup, VmTwin};
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
    /// The journal's answer to Root's own questions (the stream listing at start).
    Journal(JournalReply),
    /// A status collection's time is up: answer with what has arrived.
    StatusTimeout { operation: OperationId },
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
            | RootMessage::StatusTimeout { .. }) => Err(other),
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
            | RootMessage::StatusTimeout { .. }) => Err(other),
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
    paths: RuntimePaths,
    account: Account,
    vm_defaults: VmDefaults,
    journal: ActorRef<JournalMessage>,
    twins: BTreeMap<VmName, ActorRef<TwinMessage>>,
    waiters: BTreeMap<OperationId, (VmName, DerivedActorRef<ServerFrame>)>,
    statuses: BTreeMap<OperationId, StatusCollection>,
    /// Requests that arrived before the journal listed the known VMs: held, then handled in
    /// order, so no request sees a registry that is still being filled.
    held: Option<Vec<RootMessage>>,
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
        // Every VM the journal knows gets its twin now, so adoption starts with the runtime.
        let vm_streams = PersistenceId::parse("vm".into())?;
        journal.cast(JournalMessage::Streams {
            under: vm_streams.clone(),
            reply_to: myself.get_derived(),
        })?;
        Ok(RootState {
            id: args.id,
            vm_streams,
            paths: args.paths,
            account: args.account,
            vm_defaults: args.vm_defaults,
            journal,
            twins: BTreeMap::new(),
            waiters: BTreeMap::new(),
            statuses: BTreeMap::new(),
            held: Some(Vec::new()),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<RootMessage>,
        message: RootMessage,
        state: &mut RootState,
    ) -> Result<(), ActorProcessingErr> {
        if let (Some(held), RootMessage::Request { .. }) = (&mut state.held, &message) {
            held.push(message);
            return Ok(());
        }
        let listed = matches!(
            message,
            RootMessage::Journal(JournalReply::Streams { .. } | JournalReply::StreamsFailed { .. })
        );
        handle(&myself, message, state).await;
        if listed && let Some(held) = state.held.take() {
            for message in held {
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
        RootMessage::StatusTimeout { operation } => {
            if let Some(collection) = state.statuses.remove(&operation) {
                collection.reply(operation);
            }
        }
        RootMessage::FromVm(FromVm { vm, report }) => from_vm(state, &vm, report),
        RootMessage::Journal(JournalReply::Streams { ids, .. }) => {
            for id in ids {
                match id
                    .below(&state.vm_streams)
                    .map(|name| VmName::parse(name.to_owned()))
                {
                    Some(Ok(name)) => {
                        if let Err(failure) = twin(myself, state, &name).await {
                            tracing::error!(vm = %name, message = failure.message, "can't start its twin");
                        }
                    }
                    Some(Err(_)) | None => {
                        tracing::warn!(id = id.as_str(), "a stream under vm/ that names no VM");
                    }
                }
            }
        }
        RootMessage::Journal(JournalReply::StreamsFailed { reason, .. }) => {
            tracing::error!(%reason, "can't list the VMs; their twins start on first request");
        }
        RootMessage::Journal(
            JournalReply::Replayed { .. }
            | JournalReply::ReplayFailed { .. }
            | JournalReply::Persisted { .. }
            | JournalReply::PersistFailed { .. }
            | JournalReply::Retired { .. }
            | JournalReply::RetireFailed { .. },
        ) => tracing::debug!("journal reply Root didn't ask for; ignored"),
    }
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

fn from_vm(state: &mut RootState, vm: &VmName, report: VmReport) {
    match report {
        VmReport::OperationDone { operation, result } => {
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
        }
    }
}

/// The twin of a VM the runtime knows (from the journal, or started since).
fn known_twin(state: &RootState, name: &VmName) -> Result<ActorRef<TwinMessage>, Report> {
    state.twins.get(name).cloned().ok_or_else(|| Report {
        code: "runtime.vm.unknown".into(),
        message: format!("there is no VM named {name}"),
        hint: Some("see the VMs with amoeboid status".into()),
    })
}

fn unreachable_twin(error: &dyn std::fmt::Display) -> Report {
    Report {
        code: "runtime.vm.unreachable".into(),
        message: format!("can't reach the VM's twin: {error}"),
        hint: None,
    }
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
    let asked: Vec<(VmName, ActorRef<TwinMessage>)> = match name {
        Some(name) => match known_twin(state, &name) {
            Ok(twin) => vec![(name, twin)],
            Err(failure) => return reply(&reply_to, operation, ReplyBody::Failed(failure)),
        },
        None => state
            .twins
            .iter()
            .map(|(name, twin)| (name.clone(), twin.clone()))
            .collect(),
    };
    let collection = StatusCollection {
        collected: asked
            .iter()
            .map(|(name, _)| (name.clone(), VmCondition::Unknown))
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
