//! The VM's twin: the only persistent actor in a VM subtree and the only owner of its history.
//! Its handlers are pure; `send` turns intents (`VmOut`) into child commands, adding the VM's
//! paths, which are derived and never persisted.
//!
//! ```text
//! Stopped ─Start─▶ Starting{Building} ─Built─▶ Starting{PreparingGuest} ─Prepared─▶
//!   Starting{Booting} ─MachineRunning─▶ Starting{WaitingForSsh} ─SshReady─▶ Running
//!   any failure ─▶ Failed{reason, leftovers}   Running ─MachineStopped─▶ Stopped
//!   (OperationDone goes to Root at Running or Failed)
//! ```

use std::collections::BTreeMap;

use ractor::{Actor, ActorCell, ActorProcessingErr, DerivedActorRef};
use serde::{Deserialize, Serialize};

use super::builder::{BuildCommand, Builder, BuilderCommand, BuilderObservation, BuilderSetup};
use super::guest_vm::{GuestVm, GuestVmCommand, GuestVmObservation};
use super::hypervisor::{Adoptee, BootPlan, HypervisorCommand, HypervisorObservation};
use super::local_qemu::{LocalQemu, LocalQemuSetup, Placement};
use super::{Account, Backend, Generation, Incarnation, NixConfig, VmSpec};
use crate::adapters::process::Exit;
use crate::diagnostic::Report;
use crate::domain::account::UserName;
use crate::domain::port::LoopbackPort;
use crate::domain::process::{ProcessIdentity, ProcessRole};
use crate::domain::share::Shares;
use crate::nonempty::NonEmpty;
use crate::persistent::{
    Behaviour, ChildEndReason, ChildEnded, Effect, PersistentMessage, Unrecoverable,
};
use crate::protocol::OperationId;
use crate::runtime::paths::VmPaths;

/// Marker type implementing [`Behaviour`] for one VM.
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
    /// A child actor ended without being asked to (a bug: children don't fail by design).
    ChildEnded(ChildEnded),
}

/// The twin's history; persisted, so only facts and decisions, never derived paths.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
        /// Seed the guest's own config copy: a VM's first start, or a rebase.
        seed_own_config: bool,
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
    ProcessLost {
        generation: Generation,
        role: ProcessRole,
    },
    Failed {
        generation: Generation,
        reason: Report,
    },
}

/// The twin's state: what it was created as, its latest generation, and where its VM is.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VmTwinState {
    /// `None` until `Created`: a twin with no history.
    pub created: Option<(Incarnation, Backend)>,
    /// The latest start's generation; `None` before the first.
    pub generation: Option<Generation>,
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
        seed_own_config: bool,
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
        /// Started before the failure; to be stopped or reaped (slice 3).
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

/// The twin's mailbox type.
pub type TwinMessage = PersistentMessage<VmCommand, VmObservation>;

/// Each child reports through a `DerivedActorRef` of its own observation type into the twin's
/// mailbox: `Observation(VmObservation::<Child>(..))`, exactly one wrapper per child type, so
/// the conversion back is total.
macro_rules! child_observation {
    ($variant:ident, $observation:ty) => {
        impl From<$observation> for TwinMessage {
            fn from(observation: $observation) -> Self {
                PersistentMessage::Observation(VmObservation::$variant(observation))
            }
        }
        impl TryFrom<TwinMessage> for $observation {
            type Error = TwinMessage;
            fn try_from(message: TwinMessage) -> Result<Self, TwinMessage> {
                match message {
                    PersistentMessage::Observation(VmObservation::$variant(observation)) => {
                        Ok(observation)
                    }
                    other @ (PersistentMessage::Command(_)
                    | PersistentMessage::Observation(_)
                    | PersistentMessage::Journal(_)
                    | PersistentMessage::PersistTimeout(_)) => Err(other),
                }
            }
        }
    };
}
child_observation!(Builder, BuilderObservation);
child_observation!(GuestVm, GuestVmObservation);
child_observation!(Hypervisor, HypervisorObservation);

/// What the twin wants sent: intents without derived paths; `send` adds them.
#[derive(Debug)]
pub enum VmOut {
    Build {
        generation: Generation,
        account: Account,
        nix_config: NixConfig,
    },
    PrepareGuest {
        generation: Generation,
        spec: VmSpec,
        seed_own_config: bool,
    },
    Boot {
        generation: Generation,
        incarnation: Incarnation,
        shares: Shares,
        port: LoopbackPort,
        account: Account,
    },
    AwaitSsh {
        generation: Generation,
        port: LoopbackPort,
        timeout_seconds: u32,
    },
    StopMachine {
        generation: Generation,
    },
    Adopt {
        generation: Generation,
        processes: BTreeMap<ProcessRole, Adoptee>,
    },
    Root(VmReport),
}

/// To Root: results of operations, and the twin's own condition.
#[derive(Debug)]
pub enum VmReport {
    OperationDone {
        operation: OperationId,
        result: Result<VmStarted, Report>,
    },
    Unrecoverable {
        reason: Report,
    },
}

/// A report and the VM it is about; `send` adds the name.
#[derive(Debug)]
pub struct FromVm {
    pub vm: crate::domain::vm_name::VmName,
    pub report: VmReport,
}

/// A started VM, as the twin knows it; Root adds the paths to make the reply.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VmStarted {
    pub user: UserName,
    pub port: LoopbackPort,
}

/// What Root gives the twin; its `start` hook spawns the children from it.
pub struct VmSetup {
    pub vm: VmPaths,
    pub root: DerivedActorRef<FromVm>,
    pub build: BuildCommand,
    pub placement: Placement,
}

/// Refs to the twin's children (spawned by the twin itself when it starts) and to Root.
pub struct VmPeers {
    pub vm: VmPaths,
    pub builder: DerivedActorRef<BuilderCommand>,
    pub guest_vm: DerivedActorRef<GuestVmCommand>,
    /// Whichever backend runs this VM: anything that accepts `HypervisorCommand`.
    pub hypervisor: DerivedActorRef<HypervisorCommand>,
    pub root: DerivedActorRef<FromVm>,
}

/// Why a twin refuses or fails something; becomes the `Report` in an `OperationDone`.
#[derive(Debug, thiserror::Error)]
pub enum TwinError {
    #[error("the VM is already starting")]
    AlreadyStarting,
    #[error("the VM is already running")]
    AlreadyRunning,
    #[error("processes of an earlier start are still recorded")]
    Leftovers,
    #[error("the VM's sshd didn't answer within {0} seconds")]
    SshTimedOut(u32),
    #[error("the machine stopped while starting")]
    StoppedWhileStarting,
    #[error("the runtime restarted while the VM was starting")]
    RestartedWhileStarting,
    #[error("a part of the VM's runtime ended unexpectedly: {0}")]
    ChildEnded(String),
    #[error("no generation is left for this VM")]
    GenerationsExhausted,
}

impl crate::diagnostic::Diagnostic for TwinError {
    fn code(&self) -> &'static str {
        match self {
            Self::AlreadyStarting => "vm.already_starting",
            Self::AlreadyRunning => "vm.already_running",
            Self::Leftovers => "vm.leftovers",
            Self::SshTimedOut(_) => "vm.ssh_timed_out",
            Self::StoppedWhileStarting => "vm.stopped_while_starting",
            Self::RestartedWhileStarting => "vm.restarted_while_starting",
            Self::ChildEnded(_) => "vm.child_ended",
            Self::GenerationsExhausted => "vm.generations_exhausted",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::AlreadyStarting | Self::AlreadyRunning => {
                Some("connect to it, or stop it first".into())
            }
            Self::Leftovers => Some("stop the VM first".into()),
            Self::SshTimedOut(_) | Self::StoppedWhileStarting => {
                Some("see the VM's console.log".into())
            }
            Self::RestartedWhileStarting | Self::ChildEnded(_) => Some("start it again".into()),
            Self::GenerationsExhausted => None,
        }
    }
}

fn one<T>(item: T) -> NonEmpty<T> {
    NonEmpty::new(item, Vec::new())
}

fn done(operation: OperationId, result: Result<VmStarted, Report>) -> VmOut {
    VmOut::Root(VmReport::OperationDone { operation, result })
}

/// Persist `Failed`, then tell Root the operation failed.
fn fail(operation: OperationId, generation: Generation, reason: Report) -> Effect<VmEvent, VmOut> {
    Effect::Persist {
        events: one(VmEvent::Failed {
            generation,
            reason: reason.clone(),
        }),
        then: vec![done(operation, Err(reason))],
    }
}

fn refuse(operation: OperationId, error: &TwinError) -> Effect<VmEvent, VmOut> {
    Effect::Send(one(done(operation, Err(Report::of(error)))))
}

impl Behaviour for VmTwin {
    type Command = VmCommand;
    type Observation = VmObservation;
    type Event = VmEvent;
    type State = VmTwinState;
    type Out = VmOut;
    type Setup = VmSetup;
    type Peers = VmPeers;

    const EVENT_VERSION: u16 = 1;

    fn initial() -> VmTwinState {
        VmTwinState {
            created: None,
            generation: None,
            vm: VmState::Stopped { last_failure: None },
        }
    }

    fn on_command(state: &VmTwinState, command: VmCommand) -> Effect<VmEvent, VmOut> {
        let VmCommand::Start {
            operation,
            spec,
            incarnation,
        } = command;
        match &state.vm {
            VmState::Starting { .. } => return refuse(operation, &TwinError::AlreadyStarting),
            VmState::Running { .. } => return refuse(operation, &TwinError::AlreadyRunning),
            VmState::Failed { leftovers, .. } if !leftovers.is_empty() => {
                return refuse(operation, &TwinError::Leftovers);
            }
            VmState::Stopped { .. } | VmState::Failed { .. } => {}
        }
        let Some(generation) = state
            .generation
            .map_or(Some(Generation::FIRST), Generation::next)
        else {
            return refuse(operation, &TwinError::GenerationsExhausted);
        };
        let seed_own_config =
            state.created.is_none() || matches!(spec.nix_config, NixConfig::From(_));
        let build = VmOut::Build {
            generation,
            account: spec.account.clone(),
            nix_config: spec.nix_config.clone(),
        };
        let requested = VmEvent::StartRequested {
            operation,
            generation,
            spec,
            seed_own_config,
        };
        let events = match state.created {
            Some(_) => one(requested),
            None => NonEmpty::new(
                VmEvent::Created {
                    incarnation,
                    backend: Backend::Local,
                },
                vec![requested],
            ),
        };
        Effect::Persist {
            events,
            then: vec![build],
        }
    }

    fn on_observation(state: &VmTwinState, observation: VmObservation) -> Effect<VmEvent, VmOut> {
        match observation {
            VmObservation::Builder(observation) => builder(state, observation),
            VmObservation::GuestVm(observation) => guest_vm(state, observation),
            VmObservation::Hypervisor(observation) => hypervisor(state, observation),
            VmObservation::ChildEnded(child) => match &state.vm {
                VmState::Starting {
                    operation,
                    generation,
                    ..
                } => {
                    let reason = match child.reason {
                        ChildEndReason::Stopped(reason) => reason.unwrap_or_default(),
                        ChildEndReason::Failed(reason) => reason,
                    };
                    let name = child.name.unwrap_or_else(|| "a child".into());
                    fail(
                        *operation,
                        *generation,
                        Report::of(&TwinError::ChildEnded(format!("{name}: {reason}"))),
                    )
                }
                VmState::Stopped { .. } | VmState::Running { .. } | VmState::Failed { .. } => {
                    Effect::None
                }
            },
        }
    }

    fn on_event(state: VmTwinState, event: &VmEvent) -> VmTwinState {
        evolve(state, event)
    }

    fn on_recovered(state: &VmTwinState) -> Effect<VmEvent, VmOut> {
        match &state.vm {
            VmState::Running {
                generation,
                processes,
                ..
            } => Effect::Send(one(VmOut::Adopt {
                generation: *generation,
                processes: processes
                    .iter()
                    .map(|(role, identity)| (role.clone(), Adoptee::Known(identity.clone())))
                    .collect(),
            })),
            VmState::Starting { generation, .. } => Effect::Persist {
                events: one(VmEvent::Failed {
                    generation: *generation,
                    reason: Report::of(&TwinError::RestartedWhileStarting),
                }),
                then: Vec::new(),
            },
            VmState::Stopped { .. } | VmState::Failed { .. } => Effect::None,
        }
    }

    fn reconciled(_state: &VmTwinState) -> bool {
        // Adoption answers arrive as observations, which run in Ready too.
        true
    }

    fn refuse(command: VmCommand, reason: &Unrecoverable) -> Option<VmOut> {
        let VmCommand::Start { operation, .. } = command;
        Some(done(operation, Err(Report::of(reason))))
    }

    fn on_unrecoverable(reason: &Unrecoverable) -> VmOut {
        VmOut::Root(VmReport::Unrecoverable {
            reason: Report::of(reason),
        })
    }

    fn send(peers: &VmPeers, out: VmOut) {
        let sent = match out {
            VmOut::Build {
                generation,
                account,
                nix_config,
            } => peers
                .builder
                .send_message(BuilderCommand::Build {
                    generation,
                    vm: peers.vm.clone(),
                    account,
                    nix_config,
                })
                .is_ok(),
            VmOut::PrepareGuest {
                generation,
                spec,
                seed_own_config,
            } => peers
                .guest_vm
                .send_message(GuestVmCommand::Prepare {
                    generation,
                    vm: peers.vm.clone(),
                    spec,
                    seed_own_config,
                })
                .is_ok(),
            VmOut::AwaitSsh {
                generation,
                port,
                timeout_seconds,
            } => peers
                .guest_vm
                .send_message(GuestVmCommand::AwaitSsh {
                    generation,
                    port,
                    timeout_seconds,
                })
                .is_ok(),
            VmOut::Boot {
                generation,
                incarnation,
                shares,
                port,
                account,
            } => peers
                .hypervisor
                .send_message(HypervisorCommand::Boot {
                    generation,
                    plan: BootPlan {
                        vm: peers.vm.clone(),
                        incarnation,
                        shares,
                        ssh_port: port,
                        guest_user: account.user_id,
                        guest_group: account.group_id,
                    },
                })
                .is_ok(),
            VmOut::StopMachine { generation } => peers
                .hypervisor
                .send_message(HypervisorCommand::Stop { generation })
                .is_ok(),
            VmOut::Adopt {
                generation,
                processes,
            } => peers
                .hypervisor
                .send_message(HypervisorCommand::Adopt {
                    generation,
                    vm: peers.vm.clone(),
                    processes,
                })
                .is_ok(),
            VmOut::Root(report) => peers
                .root
                .send_message(FromVm {
                    vm: peers.vm.name().clone(),
                    report,
                })
                .is_ok(),
        };
        if !sent {
            tracing::warn!(vm = %peers.vm.name(), "a message from the twin was undeliverable");
        }
    }

    async fn start(setup: VmSetup, myself: ActorCell) -> Result<VmPeers, ActorProcessingErr> {
        let twin: ractor::ActorRef<crate::persistent::PersistentMessage<VmCommand, VmObservation>> =
            myself.into();
        let (builder, _) = Actor::spawn_linked(
            None,
            Builder,
            BuilderSetup {
                report: twin.get_derived(),
                command: setup.build,
            },
            twin.get_cell(),
        )
        .await?;
        let (guest_vm, _) =
            Actor::spawn_linked(None, GuestVm, twin.get_derived(), twin.get_cell()).await?;
        let (hypervisor, _) = Actor::spawn_linked(
            None,
            LocalQemu,
            LocalQemuSetup {
                report: twin.get_derived(),
                placement: setup.placement,
            },
            twin.get_cell(),
        )
        .await?;
        Ok(VmPeers {
            vm: setup.vm,
            builder: builder.get_derived(),
            guest_vm: guest_vm.get_derived(),
            hypervisor: hypervisor.get_derived(),
            root: setup.root,
        })
    }

    fn child_ended(child: ChildEnded) -> Option<VmObservation> {
        Some(VmObservation::ChildEnded(child))
    }
}

/// The current start, if this observation belongs to it.
fn current(state: &VmTwinState, generation: Generation) -> Option<&VmState> {
    (state.generation == Some(generation)).then_some(&state.vm)
}

fn builder(state: &VmTwinState, observation: BuilderObservation) -> Effect<VmEvent, VmOut> {
    let generation = match &observation {
        BuilderObservation::Built { generation }
        | BuilderObservation::BuildFailed { generation, .. } => *generation,
    };
    let Some(VmState::Starting {
        operation,
        spec,
        seed_own_config,
        progress: StartProgress::Building,
        ..
    }) = current(state, generation)
    else {
        return Effect::None;
    };
    match observation {
        BuilderObservation::Built { generation } => Effect::Persist {
            events: one(VmEvent::Built { generation }),
            then: vec![VmOut::PrepareGuest {
                generation,
                spec: spec.clone(),
                seed_own_config: *seed_own_config,
            }],
        },
        BuilderObservation::BuildFailed { generation, reason } => {
            fail(*operation, generation, reason)
        }
    }
}

fn guest_vm(state: &VmTwinState, observation: GuestVmObservation) -> Effect<VmEvent, VmOut> {
    let generation = match &observation {
        GuestVmObservation::Prepared { generation, .. }
        | GuestVmObservation::PrepareFailed { generation, .. }
        | GuestVmObservation::SshReady { generation }
        | GuestVmObservation::SshTimedOut { generation } => *generation,
    };
    let Some(VmState::Starting {
        operation,
        spec,
        progress,
        ..
    }) = current(state, generation)
    else {
        return Effect::None;
    };
    let incarnation = state.created.map(|(incarnation, _)| incarnation);
    match (observation, progress, incarnation) {
        (
            GuestVmObservation::Prepared { port, .. },
            StartProgress::PreparingGuest,
            Some(incarnation),
        ) => Effect::Persist {
            events: one(VmEvent::GuestPrepared { generation, port }),
            then: vec![VmOut::Boot {
                generation,
                incarnation,
                shares: spec.shares.clone(),
                port,
                account: spec.account.clone(),
            }],
        },
        (GuestVmObservation::PrepareFailed { reason, .. }, StartProgress::PreparingGuest, _) => {
            fail(*operation, generation, reason)
        }
        (GuestVmObservation::SshReady { .. }, StartProgress::WaitingForSsh { port, .. }, _) => {
            Effect::Persist {
                events: one(VmEvent::Ready { generation }),
                then: vec![done(
                    *operation,
                    Ok(VmStarted {
                        user: spec.account.user.clone(),
                        port: *port,
                    }),
                )],
            }
        }
        (GuestVmObservation::SshTimedOut { .. }, StartProgress::WaitingForSsh { .. }, _) => {
            let reason = Report::of(&TwinError::SshTimedOut(spec.boot_timeout_seconds));
            Effect::Persist {
                events: one(VmEvent::Failed {
                    generation,
                    reason: reason.clone(),
                }),
                then: vec![
                    VmOut::StopMachine { generation },
                    done(*operation, Err(reason)),
                ],
            }
        }
        (
            GuestVmObservation::Prepared { .. }
            | GuestVmObservation::PrepareFailed { .. }
            | GuestVmObservation::SshReady { .. }
            | GuestVmObservation::SshTimedOut { .. },
            StartProgress::Building
            | StartProgress::PreparingGuest
            | StartProgress::Booting { .. }
            | StartProgress::WaitingForSsh { .. },
            _,
        ) => Effect::None,
    }
}

fn hypervisor(state: &VmTwinState, observation: HypervisorObservation) -> Effect<VmEvent, VmOut> {
    let generation = match &observation {
        HypervisorObservation::ProcessStarted { generation, .. }
        | HypervisorObservation::MachineRunning { generation }
        | HypervisorObservation::MachineStopped { generation, .. }
        | HypervisorObservation::BootFailed { generation, .. }
        | HypervisorObservation::Adopted { generation, .. }
        | HypervisorObservation::Lost { generation, .. }
        | HypervisorObservation::ProcessExited { generation, .. } => *generation,
    };
    let Some(vm) = current(state, generation) else {
        return Effect::None;
    };
    let persist = |event| Effect::Persist {
        events: one(event),
        then: Vec::new(),
    };
    match (observation, vm) {
        (HypervisorObservation::ProcessStarted { role, identity, .. }, _) => {
            persist(VmEvent::ProcessStarted {
                generation,
                role,
                identity,
            })
        }
        (
            HypervisorObservation::ProcessExited {
                role,
                identity,
                exit,
                ..
            },
            _,
        ) => persist(VmEvent::ProcessExited {
            generation,
            role,
            identity,
            exit,
        }),
        (HypervisorObservation::Lost { role, .. }, _) => {
            persist(VmEvent::ProcessLost { generation, role })
        }
        (
            HypervisorObservation::MachineRunning { .. },
            VmState::Starting {
                spec,
                progress: StartProgress::Booting { port, .. },
                ..
            },
        ) => Effect::Persist {
            events: one(VmEvent::MachineRunning { generation }),
            then: vec![VmOut::AwaitSsh {
                generation,
                port: *port,
                timeout_seconds: spec.boot_timeout_seconds,
            }],
        },
        (HypervisorObservation::BootFailed { reason, .. }, VmState::Starting { operation, .. }) => {
            fail(*operation, generation, reason)
        }
        (
            HypervisorObservation::MachineStopped { reason, .. },
            VmState::Starting { operation, .. },
        ) => fail(
            *operation,
            generation,
            reason.unwrap_or_else(|| Report::of(&TwinError::StoppedWhileStarting)),
        ),
        (
            HypervisorObservation::MachineStopped { reason, .. },
            VmState::Running { .. } | VmState::Failed { .. },
        ) => persist(VmEvent::MachineStopped { generation, reason }),
        (
            HypervisorObservation::MachineRunning { .. }
            | HypervisorObservation::BootFailed { .. }
            | HypervisorObservation::Adopted { .. }
            | HypervisorObservation::MachineStopped { .. },
            VmState::Stopped { .. }
            | VmState::Starting { .. }
            | VmState::Running { .. }
            | VmState::Failed { .. },
        ) => Effect::None,
    }
}

/// The fold: total, so a replayed event never fails. Events of another generation than the
/// state's leave it unchanged (they are never persisted that way, but the fold doesn't rely
/// on it).
fn evolve(state: VmTwinState, event: &VmEvent) -> VmTwinState {
    let VmTwinState {
        created,
        generation: latest,
        vm,
    } = state;
    let rebuild = |vm| VmTwinState {
        created,
        generation: latest,
        vm,
    };
    match event {
        VmEvent::Created {
            incarnation,
            backend,
        } => VmTwinState {
            created: Some((*incarnation, *backend)),
            generation: latest,
            vm,
        },
        VmEvent::StartRequested {
            operation,
            generation,
            spec,
            seed_own_config,
        } => VmTwinState {
            created,
            generation: Some(*generation),
            vm: VmState::Starting {
                operation: *operation,
                generation: *generation,
                spec: spec.clone(),
                seed_own_config: *seed_own_config,
                progress: StartProgress::Building,
            },
        },
        event if event_generation(event) != latest => rebuild(vm),
        VmEvent::Built { .. } | VmEvent::GuestPrepared { .. } | VmEvent::MachineRunning { .. } => {
            rebuild(advance(vm, |progress| next_progress(progress, event)))
        }
        VmEvent::ProcessStarted { role, identity, .. } => {
            rebuild(with_processes(vm, |processes| {
                processes.insert(role.clone(), identity.clone());
            }))
        }
        VmEvent::ProcessExited { role, .. } | VmEvent::ProcessLost { role, .. } => {
            rebuild(with_processes(vm, |processes| {
                processes.remove(role);
            }))
        }
        VmEvent::Ready { generation } => rebuild(match vm {
            VmState::Starting {
                spec,
                progress: StartProgress::WaitingForSsh { port, processes },
                ..
            } => VmState::Running {
                generation: *generation,
                spec,
                port,
                processes,
            },
            other @ (VmState::Stopped { .. }
            | VmState::Starting { .. }
            | VmState::Running { .. }
            | VmState::Failed { .. }) => other,
        }),
        VmEvent::MachineStopped { reason, .. } => rebuild(match vm {
            VmState::Running { .. } => VmState::Stopped {
                last_failure: reason.clone(),
            },
            VmState::Failed {
                generation,
                reason: failure,
                ..
            } => VmState::Failed {
                generation,
                reason: failure,
                leftovers: BTreeMap::new(),
            },
            other @ (VmState::Stopped { .. } | VmState::Starting { .. }) => other,
        }),
        VmEvent::Failed { generation, reason } => rebuild(VmState::Failed {
            generation: *generation,
            reason: reason.clone(),
            leftovers: processes_of(&vm),
        }),
    }
}

/// The start progress after `event`; unchanged unless the event is the next step.
fn next_progress(progress: StartProgress, event: &VmEvent) -> StartProgress {
    match (progress, event) {
        (StartProgress::Building, VmEvent::Built { .. }) => StartProgress::PreparingGuest,
        (StartProgress::PreparingGuest, VmEvent::GuestPrepared { port, .. }) => {
            StartProgress::Booting {
                port: *port,
                processes: BTreeMap::new(),
            }
        }
        (StartProgress::Booting { port, processes }, VmEvent::MachineRunning { .. }) => {
            StartProgress::WaitingForSsh { port, processes }
        }
        (progress, _) => progress,
    }
}

fn event_generation(event: &VmEvent) -> Option<Generation> {
    match event {
        VmEvent::Created { .. } => None,
        VmEvent::StartRequested { generation, .. }
        | VmEvent::Built { generation }
        | VmEvent::GuestPrepared { generation, .. }
        | VmEvent::ProcessStarted { generation, .. }
        | VmEvent::MachineRunning { generation }
        | VmEvent::MachineStopped { generation, .. }
        | VmEvent::Ready { generation }
        | VmEvent::ProcessExited { generation, .. }
        | VmEvent::ProcessLost { generation, .. }
        | VmEvent::Failed { generation, .. } => Some(*generation),
    }
}

fn advance(vm: VmState, step: impl FnOnce(StartProgress) -> StartProgress) -> VmState {
    match vm {
        VmState::Starting {
            operation,
            generation,
            spec,
            seed_own_config,
            progress,
        } => VmState::Starting {
            operation,
            generation,
            spec,
            seed_own_config,
            progress: step(progress),
        },
        other @ (VmState::Stopped { .. } | VmState::Running { .. } | VmState::Failed { .. }) => {
            other
        }
    }
}

fn with_processes(
    mut vm: VmState,
    change: impl FnOnce(&mut BTreeMap<ProcessRole, ProcessIdentity>),
) -> VmState {
    match &mut vm {
        VmState::Starting {
            progress:
                StartProgress::Booting { processes, .. }
                | StartProgress::WaitingForSsh { processes, .. },
            ..
        }
        | VmState::Running { processes, .. }
        | VmState::Failed {
            leftovers: processes,
            ..
        } => change(processes),
        VmState::Starting {
            progress: StartProgress::Building | StartProgress::PreparingGuest,
            ..
        }
        | VmState::Stopped { .. } => {}
    }
    vm
}

fn processes_of(vm: &VmState) -> BTreeMap<ProcessRole, ProcessIdentity> {
    match vm {
        VmState::Starting {
            progress:
                StartProgress::Booting { processes, .. }
                | StartProgress::WaitingForSsh { processes, .. },
            ..
        }
        | VmState::Running { processes, .. }
        | VmState::Failed {
            leftovers: processes,
            ..
        } => processes.clone(),
        VmState::Starting {
            progress: StartProgress::Building | StartProgress::PreparingGuest,
            ..
        }
        | VmState::Stopped { .. } => BTreeMap::new(),
    }
}

#[cfg(test)]
#[path = "twin_tests.rs"]
mod tests;
