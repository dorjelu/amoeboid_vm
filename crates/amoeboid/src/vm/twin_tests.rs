//! The twin's handlers, driven directly: no actors, no journal. Each step applies an effect the
//! way the persistent actor does (fold the events, then the messages).

use std::num::{NonZeroU16, NonZeroU32};

use super::*;
use crate::domain::account::{GroupId, GuestHome, UserId};
use crate::domain::path::AbsolutePath;
use crate::domain::process::{BootId, LocalRole, ProcessId};

/// Applies the effect; progress reports are left out of the messages (see [`progress_of`]).
fn apply(
    state: VmTwinState,
    effect: Effect<VmEvent, VmOut>,
) -> (VmTwinState, Vec<VmEvent>, Vec<VmOut>) {
    let (state, events, outs) = match effect {
        Effect::None => (state, Vec::new(), Vec::new()),
        Effect::Send(outs) => (state, Vec::new(), outs.into_iter().collect()),
        Effect::Persist { events, then } => {
            let events: Vec<VmEvent> = events.into_iter().collect();
            let state = events.iter().fold(state, VmTwin::on_event);
            (state, events, then)
        }
    };
    let outs = outs
        .into_iter()
        .filter(|out| !matches!(out, VmOut::Root(VmReport::Progress { .. })))
        .collect();
    (state, events, outs)
}

/// The progress an effect reports, and whether it rides with persisted events.
fn progress_of(effect: &Effect<VmEvent, VmOut>) -> (Vec<crate::protocol::ProgressEvent>, bool) {
    let (outs, persisted): (Vec<&VmOut>, bool) = match effect {
        Effect::None => (Vec::new(), false),
        Effect::Send(outs) => (outs.iter().collect(), false),
        Effect::Persist { then, .. } => (then.iter().collect(), true),
    };
    let events = outs
        .into_iter()
        .filter_map(|out| match out {
            VmOut::Root(VmReport::Progress { event, .. }) => Some(*event),
            VmOut::Build { .. }
            | VmOut::PrepareGuest { .. }
            | VmOut::Boot { .. }
            | VmOut::AwaitSsh { .. }
            | VmOut::StopMachine { .. }
            | VmOut::Adopt { .. }
            | VmOut::Root(_) => None,
        })
        .collect();
    (events, persisted)
}

fn command(state: VmTwinState, command: VmCommand) -> (VmTwinState, Vec<VmEvent>, Vec<VmOut>) {
    let effect = VmTwin::on_command(&state, command);
    apply(state, effect)
}

fn observe(
    state: VmTwinState,
    observation: VmObservation,
) -> (VmTwinState, Vec<VmEvent>, Vec<VmOut>) {
    let effect = VmTwin::on_observation(&state, observation);
    apply(state, effect)
}

fn spec(nix_config: NixConfig) -> VmSpec {
    VmSpec {
        shares: Shares::default(),
        account: Account {
            user: UserName::parse("alice".into()).unwrap(),
            user_id: UserId::new(1000),
            group_id: GroupId::new(100),
            home: GuestHome::parse("/home/alice".into()).unwrap(),
        },
        nix_config,
        boot_timeout_seconds: 90,
        powerdown_grace_seconds: 30,
        home_files: Vec::new(),
        ssh_port: None,
    }
}

/// A request that resolves to `spec(nix_config)`: a given `From` (a rebase), else the VM's
/// own with `nix_config` for a new VM.
fn request(nix_config: NixConfig) -> StartRequest {
    let spec = spec(nix_config.clone());
    StartRequest {
        shares: SharesRequest::OwnElse(Ok(spec.shares)),
        nix_config: match nix_config {
            NixConfig::From(_) => NixConfigRequest::Given(nix_config),
            NixConfig::Existing | NixConfig::Preset(_) => NixConfigRequest::OwnElse(nix_config),
        },
        account: spec.account,
        home_files: spec.home_files,
        ssh_port: spec.ssh_port,
        boot_timeout_seconds: spec.boot_timeout_seconds,
        powerdown_grace_seconds: spec.powerdown_grace_seconds,
    }
}

fn start_command(nix_config: NixConfig) -> (VmCommand, OperationId) {
    let operation = OperationId::mint();
    let command = VmCommand::Start {
        operation,
        request: Box::new(request(nix_config)),
        incarnation: Incarnation::mint(),
    };
    (command, operation)
}

fn port() -> LoopbackPort {
    LoopbackPort::new(NonZeroU16::new(40022).unwrap())
}

fn identity(process: u32) -> ProcessIdentity {
    ProcessIdentity {
        process: ProcessId::new(NonZeroU32::new(process).unwrap()),
        start_time: 7,
        boot: BootId::parse("0f3c2e1a-9b8d-4c7e-a6f5-0123456789ab".into()).unwrap(),
    }
}

fn qemu() -> ProcessRole {
    ProcessRole::Local(LocalRole::Qemu)
}

fn hostkey() -> crate::domain::host_key::HostPublicKey {
    crate::domain::host_key::HostPublicKey::parse("ssh-ed25519 AAAA").unwrap()
}

/// From a fresh twin through every step of a start to `Running`.
fn running() -> (VmTwinState, OperationId) {
    let (first_start, operation) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let generation = Generation::FIRST;
    let (state, ..) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built { generation }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::Prepared {
            generation,
            port: port(),
            host_key: hostkey(),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::ProcessStarted {
            generation,
            role: qemu(),
            identity: identity(812),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineRunning { generation }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::SshReady { generation }),
    );
    (state, operation)
}

#[test]
fn a_first_start_creates_the_twin_and_builds_seeding_the_guest_config() {
    let (first_start, _) = start_command(NixConfig::Existing);
    let (state, events, outs) = command(VmTwin::initial(), first_start);
    assert!(matches!(events.as_slice(), [
        VmEvent::Created { backend: Backend::Local, .. },
        VmEvent::StartRequested { generation, seed_own_config: true, .. },
    ] if *generation == Generation::FIRST));
    assert!(
        matches!(outs.as_slice(), [VmOut::Build { generation, .. }] if *generation == Generation::FIRST)
    );
    assert!(matches!(
        state.vm,
        VmState::Starting {
            progress: StartProgress::Building,
            ..
        }
    ));
}

#[test]
fn every_step_of_a_start_persists_its_fact_then_asks_the_next_child() {
    let (first_start, operation) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let generation = Generation::FIRST;

    let (state, events, outs) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built { generation }),
    );
    assert!(matches!(events.as_slice(), [VmEvent::Built { .. }]));
    assert!(matches!(
        outs.as_slice(),
        [VmOut::PrepareGuest {
            seed_own_config: true,
            ..
        }]
    ));

    let (state, events, outs) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::Prepared {
            generation,
            port: port(),
            host_key: hostkey(),
        }),
    );
    assert!(matches!(events.as_slice(), [VmEvent::GuestPrepared { .. }]));
    assert!(matches!(outs.as_slice(), [VmOut::Boot { port: p, .. }] if *p == port()));

    let (state, events, outs) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::ProcessStarted {
            generation,
            role: qemu(),
            identity: identity(812),
        }),
    );
    assert!(matches!(
        events.as_slice(),
        [VmEvent::ProcessStarted { .. }]
    ));
    assert!(outs.is_empty());

    let (state, events, outs) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineRunning { generation }),
    );
    assert!(matches!(
        events.as_slice(),
        [VmEvent::MachineRunning { .. }]
    ));
    assert!(matches!(
        outs.as_slice(),
        [VmOut::AwaitSsh {
            timeout_seconds: 90,
            ..
        }]
    ));

    let (state, events, outs) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::SshReady { generation }),
    );
    assert!(matches!(events.as_slice(), [VmEvent::Ready { .. }]));
    assert!(
        matches!(outs.as_slice(), [VmOut::Root(VmReport::OperationDone { operation: done, result: Ok(VmOutcome::Started(started)) })]
        if *done == operation && started.port == port() && started.user.as_str() == "alice")
    );
    assert!(
        matches!(&state.vm, VmState::Running { processes, .. } if processes.contains_key(&qemu()))
    );
}

#[test]
fn a_later_start_seeds_only_on_rebase_and_takes_the_next_generation() {
    let (state, _) = running();
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation: Generation::FIRST,
            reason: None,
        }),
    );
    assert!(matches!(state.vm, VmState::Stopped { last_failure: None }));
    let second = Generation::FIRST.next().unwrap();
    let (start_again, _) = start_command(NixConfig::Existing);
    let (_, events, _) = command(state.clone(), start_again);
    assert!(
        matches!(events.as_slice(), [VmEvent::StartRequested { generation, seed_own_config: false, .. }] if *generation == second)
    );
    let (rebase, _) = start_command(NixConfig::From(
        AbsolutePath::parse("/presets/x".into()).unwrap(),
    ));
    let (_, events, _) = command(state, rebase);
    assert!(matches!(
        events.as_slice(),
        [VmEvent::StartRequested {
            seed_own_config: true,
            ..
        }]
    ));
}

#[test]
fn a_start_while_starting_or_running_is_refused_without_persisting() {
    let (first, _) = start_command(NixConfig::Existing);
    let (starting, ..) = command(VmTwin::initial(), first);
    let (running, _) = running();
    for (state, code) in [
        (starting, "vm.already_starting"),
        (running, "vm.already_running"),
    ] {
        let (again, operation) = start_command(NixConfig::Existing);
        let (_, events, outs) = command(state, again);
        assert_eq!(events, Vec::new());
        assert!(
            matches!(outs.as_slice(), [VmOut::Root(VmReport::OperationDone { operation: done, result: Err(reason) })]
            if *done == operation && reason.code == code)
        );
    }
}

#[test]
fn observations_of_an_older_generation_are_ignored() {
    let (state, _) = running();
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation: Generation::FIRST,
            reason: None,
        }),
    );
    let (again, _) = start_command(NixConfig::Existing);
    let (state, ..) = command(state, again);
    let (_, events, outs) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built {
            generation: Generation::FIRST,
        }),
    );
    assert!(events.is_empty() && outs.is_empty());
}

#[test]
fn a_failed_build_fails_the_start_and_answers_the_operation() {
    let (first_start, operation) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let reason = Report {
        code: "nix.build_failed".into(),
        message: "nix build failed".into(),
        hint: None,
    };
    let (state, events, outs) = observe(
        state,
        VmObservation::Builder(BuilderObservation::BuildFailed {
            generation: Generation::FIRST,
            reason: reason.clone(),
        }),
    );
    assert!(matches!(events.as_slice(), [VmEvent::Failed { reason: r, .. }] if *r == reason));
    assert!(
        matches!(outs.as_slice(), [VmOut::Root(VmReport::OperationDone { operation: done, result: Err(r) })]
        if *done == operation && *r == reason)
    );
    assert!(matches!(state.vm, VmState::Failed { .. }));
}

#[test]
fn an_ssh_timeout_fails_the_start_stops_the_machine_and_keeps_leftovers_until_it_stops() {
    let (first_start, _) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let generation = Generation::FIRST;
    let (state, ..) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built { generation }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::Prepared {
            generation,
            port: port(),
            host_key: hostkey(),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::ProcessStarted {
            generation,
            role: qemu(),
            identity: identity(812),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineRunning { generation }),
    );
    let (state, events, outs) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::SshTimedOut { generation }),
    );
    assert!(
        matches!(events.as_slice(), [VmEvent::Failed { reason, .. }] if reason.code == "vm.ssh_timed_out")
    );
    assert!(matches!(
        outs.as_slice(),
        [
            VmOut::StopMachine { .. },
            VmOut::Root(VmReport::OperationDone { result: Err(_), .. })
        ]
    ));
    assert!(
        matches!(&state.vm, VmState::Failed { leftovers, .. } if leftovers.contains_key(&qemu()))
    );

    // Leftovers block a new start until the machine is down.
    let (again, _) = start_command(NixConfig::Existing);
    let (_, events, outs) = command(state.clone(), again);
    assert_eq!(events, Vec::new());
    assert!(
        matches!(outs.as_slice(), [VmOut::Root(VmReport::OperationDone { result: Err(reason), .. })] if reason.code == "vm.leftovers")
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation,
            reason: None,
        }),
    );
    assert!(matches!(&state.vm, VmState::Failed { leftovers, .. } if leftovers.is_empty()));
}

#[test]
fn a_running_machine_that_stops_records_why() {
    let (state, _) = running();
    let reason = Report {
        code: "hypervisor.qemu_exited".into(),
        message: "qemu exited".into(),
        hint: None,
    };
    let (state, events, _) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation: Generation::FIRST,
            reason: Some(reason.clone()),
        }),
    );
    assert!(matches!(
        events.as_slice(),
        [VmEvent::MachineStopped { .. }]
    ));
    assert_eq!(
        state.vm,
        VmState::Stopped {
            last_failure: Some(reason)
        }
    );
}

#[test]
fn recovery_adopts_a_running_vm_and_fails_an_interrupted_start() {
    let (state, _) = running();
    let (_, events, outs) = apply(state.clone(), VmTwin::on_recovered(&state));
    assert_eq!(events, Vec::new());
    assert!(matches!(outs.as_slice(), [VmOut::Adopt { processes, .. }]
        if processes.get(&qemu()) == Some(&Adoptee::Known(identity(812)))));

    let (first_start, _) = start_command(NixConfig::Existing);
    let (starting, ..) = command(VmTwin::initial(), first_start);
    let (state, events, _) = apply(starting.clone(), VmTwin::on_recovered(&starting));
    assert!(
        matches!(events.as_slice(), [VmEvent::Failed { reason, .. }] if reason.code == "vm.restarted_while_starting")
    );
    assert!(matches!(state.vm, VmState::Failed { .. }));
}

#[test]
fn events_survive_their_persisted_form() {
    let (first_start, _) = start_command(NixConfig::Existing);
    let (state, mut events, _) = command(VmTwin::initial(), first_start);
    let (_, more, _) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built {
            generation: Generation::FIRST,
        }),
    );
    events.extend(more);
    events.push(VmEvent::ProcessExited {
        generation: Generation::FIRST,
        role: qemu(),
        identity: identity(812),
        exit: Exit::Signal(15),
    });
    for event in events {
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(
            serde_json::from_str::<VmEvent>(&json).unwrap(),
            event,
            "{json}"
        );
    }
}

fn stopped_answers(outs: &[VmOut]) -> Vec<OperationId> {
    outs.iter()
        .filter_map(|out| match out {
            VmOut::Root(VmReport::OperationDone {
                operation,
                result: Ok(VmOutcome::Stopped),
            }) => Some(*operation),
            VmOut::Build { .. }
            | VmOut::PrepareGuest { .. }
            | VmOut::Boot { .. }
            | VmOut::AwaitSsh { .. }
            | VmOut::StopMachine { .. }
            | VmOut::Adopt { .. }
            | VmOut::Root(_) => None,
        })
        .collect()
}

#[test]
fn stopping_a_running_vm_asks_the_backend_and_answers_every_waiter_once_it_is_down() {
    let (state, _) = running();
    let first = OperationId::mint();
    let (state, events, outs) = command(state, VmCommand::Stop { operation: first });
    assert!(matches!(events.as_slice(), [VmEvent::StopRequested { .. }]));
    assert!(
        matches!(outs.as_slice(), [VmOut::StopMachine { processes, .. }] if processes.contains_key(&qemu()))
    );
    assert!(matches!(state.vm, VmState::Stopping { .. }));

    // A second stop waits for the same one; a start is refused meanwhile.
    let second = OperationId::mint();
    let (state, _, outs) = command(state, VmCommand::Stop { operation: second });
    assert!(outs.is_empty());
    let (again, _) = start_command(NixConfig::Existing);
    let (_, _, outs) = command(state.clone(), again);
    assert!(
        matches!(outs.as_slice(), [VmOut::Root(VmReport::OperationDone { result: Err(reason), .. })] if reason.code == "vm.stopping")
    );

    let (state, events, outs) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation: Generation::FIRST,
            reason: None,
        }),
    );
    assert!(matches!(
        events.as_slice(),
        [VmEvent::MachineStopped { .. }]
    ));
    assert_eq!(stopped_answers(&outs), [first, second]);
    assert_eq!(state.vm, VmState::Stopped { last_failure: None });
}

#[test]
fn stopping_a_start_in_progress_cancels_it_and_stops_what_it_started() {
    let (first_start, start_operation) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let generation = Generation::FIRST;
    let (state, ..) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built { generation }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::Prepared {
            generation,
            port: port(),
            host_key: hostkey(),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::ProcessStarted {
            generation,
            role: qemu(),
            identity: identity(812),
        }),
    );
    let stop = OperationId::mint();
    let (state, events, outs) = command(state, VmCommand::Stop { operation: stop });
    assert!(
        matches!(events.as_slice(), [VmEvent::Failed { reason, .. }, VmEvent::StopRequested { .. }] if reason.code == "vm.cancelled")
    );
    assert!(matches!(outs.as_slice(), [
        VmOut::Root(VmReport::OperationDone { operation, result: Err(_) }),
        VmOut::StopMachine { processes, .. },
    ] if *operation == start_operation && processes.contains_key(&qemu())));
    // The build or boot finishing late changes nothing.
    let (state, events, _) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineRunning { generation }),
    );
    assert_eq!(events, Vec::new());
    let (state, _, outs) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation,
            reason: None,
        }),
    );
    assert_eq!(stopped_answers(&outs), [stop]);
    assert!(matches!(state.vm, VmState::Stopped { .. }));
}

#[test]
fn stopping_a_failed_start_takes_down_its_leftovers_and_then_allows_a_new_start() {
    let (first_start, _) = start_command(NixConfig::Existing);
    let (state, ..) = command(VmTwin::initial(), first_start);
    let generation = Generation::FIRST;
    let (state, ..) = observe(
        state,
        VmObservation::Builder(BuilderObservation::Built { generation }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::GuestVm(GuestVmObservation::Prepared {
            generation,
            port: port(),
            host_key: hostkey(),
        }),
    );
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::ProcessStarted {
            generation,
            role: qemu(),
            identity: identity(812),
        }),
    );
    let reason = Report {
        code: "x".into(),
        message: "x".into(),
        hint: None,
    };
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::BootFailed { generation, reason }),
    );
    let (state, _, outs) = command(
        state,
        VmCommand::Stop {
            operation: OperationId::mint(),
        },
    );
    assert!(matches!(outs.as_slice(), [VmOut::StopMachine { .. }]));
    let (state, ..) = observe(
        state,
        VmObservation::Hypervisor(HypervisorObservation::MachineStopped {
            generation,
            reason: None,
        }),
    );
    let (again, _) = start_command(NixConfig::Existing);
    let (_, events, _) = command(state, again);
    assert!(matches!(
        events.as_slice(),
        [VmEvent::StartRequested { .. }]
    ));
}

#[test]
fn stopping_a_stopped_vm_answers_at_once_without_persisting() {
    let operation = OperationId::mint();
    let (_, events, outs) = command(VmTwin::initial(), VmCommand::Stop { operation });
    assert_eq!(events, Vec::new());
    assert_eq!(stopped_answers(&outs), [operation]);
}

#[test]
fn status_is_answered_in_every_state_without_persisting() {
    let (running_state, _) = running();
    let (first_start, _) = start_command(NixConfig::Existing);
    let (starting, ..) = command(VmTwin::initial(), first_start);
    for (state, expected) in [
        (VmTwin::initial(), "stopped"),
        (starting, "starting"),
        (running_state, "running"),
    ] {
        let operation = OperationId::mint();
        let (_, events, outs) = command(state, VmCommand::Status { operation });
        assert_eq!(events, Vec::new());
        let condition = match outs.as_slice() {
            [
                VmOut::Root(VmReport::OperationDone {
                    result: Ok(VmOutcome::Status(condition)),
                    ..
                }),
            ] => condition.clone(),
            _ => crate::vm::VmCondition::Unknown,
        };
        let name = serde_json::to_value(&condition).unwrap();
        assert!(
            name.get(expected).is_some() || name == serde_json::json!(expected),
            "{name}"
        );
    }
}

#[test]
fn recovery_carries_on_an_interrupted_stop() {
    let (state, _) = running();
    let (state, ..) = command(
        state,
        VmCommand::Stop {
            operation: OperationId::mint(),
        },
    );
    let (_, events, outs) = apply(state.clone(), VmTwin::on_recovered(&state));
    assert_eq!(events, Vec::new());
    assert!(
        matches!(outs.as_slice(), [VmOut::StopMachine { processes, .. }] if processes.contains_key(&qemu()))
    );
}

#[test]
fn a_stop_uses_the_powerdown_grace_of_the_start_it_stops() {
    let mut patient = spec(NixConfig::Existing);
    patient.powerdown_grace_seconds = 7;
    let started = VmTwin::on_event(
        VmTwin::initial(),
        &VmEvent::StartRequested {
            operation: OperationId::mint(),
            generation: Generation::FIRST,
            spec: patient,
            seed_own_config: true,
        },
    );
    assert_eq!(
        started.last_spec.map(|spec| spec.powerdown_grace_seconds),
        Some(7)
    );
    let (mut state, _) = running();
    if let Some(last) = &mut state.last_spec {
        last.powerdown_grace_seconds = 7;
    }
    let (_, _, outs) = command(
        state,
        VmCommand::Stop {
            operation: OperationId::mint(),
        },
    );
    assert!(
        matches!(outs.as_slice(), [VmOut::StopMachine { grace, .. }] if *grace == std::time::Duration::from_secs(7))
    );
}

fn shares_of(directories: &[&str]) -> Shares {
    let requests = directories
        .iter()
        .map(|directory| crate::domain::share::ShareRequest {
            directory: crate::domain::share::HostDirectory::parse(
                AbsolutePath::parse((*directory).into()).unwrap(),
            )
            .unwrap(),
            mode: crate::domain::share::Mode::ReadWrite,
        })
        .collect();
    Shares::validate(requests, &[], |_| true).unwrap()
}

fn spec_of(events: &[VmEvent]) -> Option<VmSpec> {
    events.iter().find_map(|event| match event {
        VmEvent::StartRequested { spec, .. } => Some(spec.clone()),
        VmEvent::Created { .. }
        | VmEvent::StopRequested { .. }
        | VmEvent::Built { .. }
        | VmEvent::GuestPrepared { .. }
        | VmEvent::ProcessStarted { .. }
        | VmEvent::MachineRunning { .. }
        | VmEvent::MachineStopped { .. }
        | VmEvent::Ready { .. }
        | VmEvent::ProcessExited { .. }
        | VmEvent::ProcessLost { .. }
        | VmEvent::Failed { .. } => None,
    })
}

#[test]
fn without_shares_a_new_vm_gets_the_default_and_an_existing_one_its_last_shares() {
    let mut first = request(NixConfig::Existing);
    first.shares = SharesRequest::OwnElse(Ok(shares_of(&["/srv/default"])));
    let (state, events, _) = command(
        VmTwin::initial(),
        VmCommand::Start {
            operation: OperationId::mint(),
            request: Box::new(first),
            incarnation: Incarnation::mint(),
        },
    );
    assert_eq!(
        spec_of(&events).map(|spec| spec.shares),
        Some(shares_of(&["/srv/default"]))
    );
    // Stopped again (the start failed), then started without shares: its own, not the default.
    let (state, ..) = observe(
        state,
        VmObservation::Builder(BuilderObservation::BuildFailed {
            generation: Generation::FIRST,
            reason: Report {
                code: "x".into(),
                message: "x".into(),
                hint: None,
            },
        }),
    );
    let mut again = request(NixConfig::Existing);
    again.shares = SharesRequest::OwnElse(Ok(shares_of(&["/srv/other"])));
    let (_, events, _) = command(
        state,
        VmCommand::Start {
            operation: OperationId::mint(),
            request: Box::new(again),
            incarnation: Incarnation::mint(),
        },
    );
    assert_eq!(
        spec_of(&events).map(|spec| spec.shares),
        Some(shares_of(&["/srv/default"]))
    );
}

#[test]
fn an_unusable_default_fails_only_a_new_vm() {
    let unusable = Report {
        code: "shares.invalid".into(),
        message: "x".into(),
        hint: None,
    };
    let mut first = request(NixConfig::Existing);
    first.shares = SharesRequest::OwnElse(Err(unusable.clone()));
    let operation = OperationId::mint();
    let (_, events, outs) = command(
        VmTwin::initial(),
        VmCommand::Start {
            operation,
            request: Box::new(first),
            incarnation: Incarnation::mint(),
        },
    );
    assert_eq!(events, Vec::new());
    assert!(matches!(
        outs.as_slice(),
        [VmOut::Root(VmReport::OperationDone { result: Err(report), .. })] if *report == unusable
    ));
}

#[test]
fn a_new_vm_is_created_from_the_base_and_an_existing_one_keeps_its_own_unless_given_one() {
    let base = NixConfig::Preset(crate::config::run::PresetName::default_preset());
    let resolve_on = |state: &VmTwinState, nix_config| {
        let mut request = request(NixConfig::Existing);
        request.nix_config = nix_config;
        resolve(state, request).map(|(spec, seed)| (spec.nix_config, seed))
    };
    let new = VmTwin::initial();
    assert_eq!(
        resolve_on(&new, NixConfigRequest::OwnElse(base.clone())),
        Ok((base.clone(), true))
    );
    let (existing, _) = running();
    assert_eq!(
        resolve_on(&existing, NixConfigRequest::OwnElse(base.clone())),
        Ok((NixConfig::Existing, false))
    );
    assert_eq!(
        resolve_on(&existing, NixConfigRequest::Given(base.clone())),
        Ok((base, true))
    );
}

#[test]
fn each_step_of_a_start_and_a_stop_is_reported_with_the_event_that_records_it() {
    use crate::protocol::{ProgressEvent, StopStep};
    let (start, _) = start_command(NixConfig::Existing);
    let state = VmTwin::initial();
    let effect = VmTwin::on_command(&state, start);
    assert_eq!(
        progress_of(&effect),
        (vec![ProgressEvent::Start(StartStep::Building)], true)
    );
    let (state, ..) = apply(state, effect);
    let generation = Generation::FIRST;
    let steps = [
        (
            VmObservation::Builder(BuilderObservation::Built { generation }),
            StartStep::PreparingGuest,
        ),
        (
            VmObservation::GuestVm(GuestVmObservation::Prepared {
                generation,
                port: port(),
                host_key: hostkey(),
            }),
            StartStep::Booting,
        ),
        (
            VmObservation::Hypervisor(HypervisorObservation::MachineRunning { generation }),
            StartStep::WaitingForSsh,
        ),
    ];
    let mut state = state;
    for (observation, step) in steps {
        let effect = VmTwin::on_observation(&state, observation);
        assert_eq!(
            progress_of(&effect),
            (vec![ProgressEvent::Start(step)], true)
        );
        state = apply(state, effect).0;
    }
    let (running, _) = running();
    let stop = VmTwin::on_command(
        &running,
        VmCommand::Stop {
            operation: OperationId::mint(),
        },
    );
    assert_eq!(
        progress_of(&stop),
        (vec![ProgressEvent::Stop(StopStep::PoweringOff)], true)
    );
}
