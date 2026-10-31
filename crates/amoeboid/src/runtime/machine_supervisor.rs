//! `MachineSupervisor` (0003 §Supervision): runs one twin per VM, linked to itself, and restarts
//! a failed twin under a per-VM budget. One-for-one: a VM's failures never touch another VM.
//! When a VM's budget is spent it is given up and reported, for Root to quarantine; the runtime
//! never ends because of a VM. The decision is the pure [`decide`]; the actor only executes it.
//!
//! ```text
//!  Spawning ──spawned──▶ Up ──failed, budget left──▶ Restarting{attempt} ──backoff──▶ Spawning
//!     │ spawn error counts as a failure                │ budget spent
//!     └────────────────────────────────────────────────┴──▶ Exhausted (entry dropped)
//!  Up ──Terminate──▶ Terminating ──ended──▶ Terminated (entry dropped)
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef, SupervisionEvent};

use super::paths::RuntimePaths;
use super::root::VmDefaults;
use crate::diagnostic::Report;
use crate::domain::vm_name::VmName;
use crate::journal::{JournalMessage, PersistFailure};
use crate::persistent::machine::Fatal;
use crate::persistent::{Persistent, PersistentArgs};
use crate::protocol::RuntimeId;
use crate::vm::twin::{FromVm, TwinMessage, VmSetup, VmTwin};

/// How often and how fast a VM's twin is restarted before the VM is given up. Comes from
/// `daemon_config.toml` (`config::daemon::RestartConfig::policy`), which holds the defaults.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RestartPolicy {
    /// Restarts allowed within `window`; the next failure gives the VM up.
    pub max_restarts: u32,
    /// Failures older than this no longer count.
    pub window: Duration,
    /// The first restart's delay; each further one doubles it.
    pub backoff_first: Duration,
    pub backoff_max: Duration,
}

/// What kind of failure ended a twin; the restart reaction depends on it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FailureKind {
    /// Another incarnation wrote to the stream: this one was stale. Restarted at once.
    Conflict,
    /// The journal timed out or failed to read or write.
    Journal,
    /// One of the twin's children ended, and the twin escalated it.
    ChildEscalated,
    /// The twin could not be spawned.
    SpawnFailed,
    /// A panic, an unknown error, or a failure the code should make impossible.
    Bug,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwinFailure {
    pub kind: FailureKind,
    pub report: Report,
}

/// A VM's failures within the policy's window, oldest first.
#[derive(Clone, Default, Debug)]
pub struct RestartHistory {
    failures: VecDeque<(Instant, TwinFailure)>,
}

impl RestartHistory {
    /// The failures counted now, oldest first.
    #[must_use]
    pub fn failures(&self) -> Vec<TwinFailure> {
        self.failures
            .iter()
            .map(|(_, failure)| failure.clone())
            .collect()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestartDecision {
    /// Restart number `attempt` (from 1) within the window, after `after`.
    Restart { attempt: u32, after: Duration },
    /// The budget is spent after `restarts` restarts: give the VM up.
    Exhausted { restarts: u32 },
}

/// Records `failure` at `now` (tokio's monotonic clock, so a paused test runtime controls the
/// window) and decides. Pure.
#[must_use]
pub fn decide(
    policy: &RestartPolicy,
    mut history: RestartHistory,
    failure: TwinFailure,
    now: Instant,
) -> (RestartHistory, RestartDecision) {
    history
        .failures
        .retain(|(at, _)| now.saturating_duration_since(*at) < policy.window);
    let kind = failure.kind;
    history.failures.push_back((now, failure));
    let failures = u32::try_from(history.failures.len()).unwrap_or(u32::MAX);
    if failures > policy.max_restarts {
        return (
            history,
            RestartDecision::Exhausted {
                restarts: failures.saturating_sub(1),
            },
        );
    }
    let after = match kind {
        FailureKind::Conflict => Duration::ZERO,
        FailureKind::Journal
        | FailureKind::ChildEscalated
        | FailureKind::SpawnFailed
        | FailureKind::Bug => policy
            .backoff_first
            .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1)))
            .min(policy.backoff_max),
    };
    (
        history,
        RestartDecision::Restart {
            attempt: failures,
            after,
        },
    )
}

/// Reads a twin's failure: the persistent actor fails with a typed [`Fatal`]; anything else (a
/// panic, an unknown error) is a bug.
#[must_use]
pub fn classify(error: &ActorProcessingErr) -> TwinFailure {
    match error.downcast_ref::<Fatal>() {
        Some(fatal) => TwinFailure {
            kind: match fatal {
                Fatal::Persist(PersistFailure::Conflict { .. }) => FailureKind::Conflict,
                Fatal::Persist(PersistFailure::Store(_))
                | Fatal::PersistTimeout
                | Fatal::Replay(_) => FailureKind::Journal,
                Fatal::ChildEnded { .. } => FailureKind::ChildEscalated,
                Fatal::Encode(_) | Fatal::SeqOverflow => FailureKind::Bug,
            },
            report: Report::of(fatal),
        },
        None => bug(format!("internal error: {error}")),
    }
}

fn bug(message: String) -> TwinFailure {
    TwinFailure {
        kind: FailureKind::Bug,
        report: Report {
            code: "runtime.machine.bug".into(),
            message,
            hint: Some("this is a bug in amoeboid; the VM's log has the details".into()),
        },
    }
}

#[derive(Debug)]
pub enum MachineSupervisorMessage {
    /// Run a twin for `name`; an existing one is reported `Up` again, a restarting one is left
    /// to its timer. Also how a quarantine is lifted: a fresh budget.
    Ensure { name: VmName },
    /// Stop the twin and forget the VM; answered `Terminated`.
    Terminate { name: VmName },
    /// Own timer: restart number `attempt` is due.
    Retry { name: VmName, attempt: u32 },
}

/// To Root, by tell.
#[derive(Debug)]
pub enum MachineReport {
    Up {
        name: VmName,
        twin: ActorRef<TwinMessage>,
    },
    Restarting {
        name: VmName,
        failure: TwinFailure,
        attempt: u32,
        after: Duration,
    },
    /// The budget is spent; the VM has no twin until it is ensured again.
    Exhausted {
        name: VmName,
        restarts: u32,
        failures: Vec<TwinFailure>,
    },
    Terminated {
        name: VmName,
    },
}

pub struct MachineSupervisorArgs {
    /// Names twins `<runtime>/vm/<name>`.
    pub runtime: RuntimeId,
    pub report: DerivedActorRef<MachineReport>,
    /// Where twins send their reports (Root).
    pub from_vm: DerivedActorRef<FromVm>,
    pub journal: ActorRef<JournalMessage>,
    pub paths: RuntimePaths,
    /// How twins run their children.
    pub vm_defaults: VmDefaults,
    pub restart: RestartPolicy,
    /// How long a twin waits for the journal to persist.
    pub persist_timeout: Duration,
}

pub struct MachineSupervisor;

pub struct MachineSupervisorState {
    args: MachineSupervisorArgs,
    entries: BTreeMap<VmName, Entry>,
}

enum Entry {
    Up {
        twin: ActorRef<TwinMessage>,
        history: RestartHistory,
    },
    /// A `Retry { attempt }` timer is armed.
    Restarting {
        attempt: u32,
        history: RestartHistory,
    },
    Terminating {
        twin: ActorRef<TwinMessage>,
    },
}

impl Entry {
    fn twin(&self) -> Option<&ActorRef<TwinMessage>> {
        match self {
            Self::Up { twin, .. } | Self::Terminating { twin } => Some(twin),
            Self::Restarting { .. } => None,
        }
    }
}

impl Actor for MachineSupervisor {
    type Msg = MachineSupervisorMessage;
    type State = MachineSupervisorState;
    type Arguments = MachineSupervisorArgs;

    fn pre_start(
        &self,
        _myself: ActorRef<MachineSupervisorMessage>,
        args: MachineSupervisorArgs,
    ) -> impl Future<Output = Result<MachineSupervisorState, ActorProcessingErr>> + Send {
        std::future::ready(Ok(MachineSupervisorState {
            args,
            entries: BTreeMap::new(),
        }))
    }

    async fn handle(
        &self,
        myself: ActorRef<MachineSupervisorMessage>,
        message: MachineSupervisorMessage,
        state: &mut MachineSupervisorState,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            MachineSupervisorMessage::Ensure { name } => match state.entries.get(&name) {
                Some(Entry::Up { twin, .. }) => {
                    let twin = twin.clone();
                    state.report(MachineReport::Up { name, twin });
                }
                Some(Entry::Restarting { .. } | Entry::Terminating { .. }) => {
                    tracing::debug!(vm = %name, "ensure while restarting or terminating; ignored");
                }
                None => spawn(&myself, state, name, RestartHistory::default()).await,
            },
            MachineSupervisorMessage::Terminate { name } => match state.entries.remove(&name) {
                Some(Entry::Up { twin, .. } | Entry::Terminating { twin }) => {
                    twin.stop(Some("terminated".into()));
                    state.entries.insert(name, Entry::Terminating { twin });
                }
                // A pending retry finds no entry and is dropped.
                Some(Entry::Restarting { .. }) | None => {
                    state.report(MachineReport::Terminated { name });
                }
            },
            MachineSupervisorMessage::Retry { name, attempt } => {
                if let Some(Entry::Restarting {
                    attempt: due,
                    history,
                }) = state.entries.remove(&name)
                {
                    if due == attempt {
                        spawn(&myself, state, name, history).await;
                    } else {
                        state.entries.insert(
                            name,
                            Entry::Restarting {
                                attempt: due,
                                history,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_supervisor_evt(
        &self,
        myself: ActorRef<MachineSupervisorMessage>,
        event: SupervisionEvent,
        state: &mut MachineSupervisorState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        supervision_event(&myself, event, state);
        std::future::ready(Ok(()))
    }
}

/// A twin ended: expected when terminating, otherwise a failure to decide on.
fn supervision_event(
    myself: &ActorRef<MachineSupervisorMessage>,
    event: SupervisionEvent,
    state: &mut MachineSupervisorState,
) {
    let (cell, failure) = match event {
        SupervisionEvent::ActorFailed(cell, error) => (cell, classify(&error)),
        SupervisionEvent::ActorTerminated(cell, _, reason) => (
            cell,
            // Twins stop normally only when terminated; anything else is unexpected.
            bug(format!(
                "the VM's twin stopped unexpectedly ({})",
                reason.unwrap_or_else(|| "no reason".into())
            )),
        ),
        SupervisionEvent::ActorStarted(_) | SupervisionEvent::ProcessGroupChanged(_) => {
            return;
        }
    };
    let Some(name) = state
        .entries
        .iter()
        .find(|(_, entry)| {
            entry
                .twin()
                .is_some_and(|twin| twin.get_id() == cell.get_id())
        })
        .map(|(name, _)| name.clone())
    else {
        return;
    };
    match state.entries.remove(&name) {
        Some(Entry::Terminating { .. }) => {
            tracing::info!(vm = %name, "twin terminated");
            state.report(MachineReport::Terminated { name });
        }
        Some(Entry::Up { history, .. }) => failed(myself, state, name, history, failure),
        Some(Entry::Restarting { .. }) | None => {}
    }
}

impl MachineSupervisorState {
    fn report(&self, report: MachineReport) {
        if self.args.report.send_message(report).is_err() {
            tracing::debug!("machine report undeliverable: Root stopping");
        }
    }
}

/// Spawns the VM's twin linked to the supervisor; a spawn error is a failure like any other.
async fn spawn(
    myself: &ActorRef<MachineSupervisorMessage>,
    state: &mut MachineSupervisorState,
    name: VmName,
    history: RestartHistory,
) {
    let args = &state.args;
    let spawned = match (args.paths.vm(&name), args.journal.clone()) {
        (Err(error), _) => Err(Report::of(&error)),
        (Ok(vm), journal) => {
            let id = crate::journal::PersistenceId::parse("vm".into())
                .and_then(|parent| parent.child(name.as_str()));
            match id {
                Err(error) => Err(Report::of(&error)),
                Ok(id) => Actor::spawn_linked(
                    Some(format!("{}/vm/{name}", args.runtime)),
                    Persistent::<VmTwin>::new(),
                    PersistentArgs {
                        id,
                        journal,
                        setup: VmSetup {
                            vm,
                            root: args.from_vm.clone(),
                            build: args.vm_defaults.build.clone(),
                            placement: args.vm_defaults.placement,
                            timing: args.vm_defaults.process,
                        },
                        persist_timeout: args.persist_timeout,
                    },
                    myself.get_cell(),
                )
                .await
                .map(|(twin, _)| twin)
                .map_err(|error| Report {
                    code: "runtime.machine.spawn".into(),
                    message: format!("can't start the VM's twin: {error}"),
                    hint: None,
                }),
            }
        }
    };
    match spawned {
        Ok(twin) => {
            state.entries.insert(
                name.clone(),
                Entry::Up {
                    twin: twin.clone(),
                    history,
                },
            );
            state.report(MachineReport::Up { name, twin });
        }
        Err(report) => failed(
            myself,
            state,
            name,
            history,
            TwinFailure {
                kind: FailureKind::SpawnFailed,
                report,
            },
        ),
    }
}

/// Decides on a twin's failure: arm the restart, or give the VM up.
fn failed(
    myself: &ActorRef<MachineSupervisorMessage>,
    state: &mut MachineSupervisorState,
    name: VmName,
    history: RestartHistory,
    failure: TwinFailure,
) {
    let (history, decision) = decide(
        &state.args.restart,
        history,
        failure.clone(),
        Instant::now(),
    );
    match decision {
        RestartDecision::Restart { attempt, after } => {
            tracing::warn!(vm = %name, attempt, ?after, code = failure.report.code, message = failure.report.message, "twin failed; restarting");
            state
                .entries
                .insert(name.clone(), Entry::Restarting { attempt, history });
            let retry = name.clone();
            myself.send_after(after, move || MachineSupervisorMessage::Retry {
                name: retry,
                attempt,
            });
            state.report(MachineReport::Restarting {
                name,
                failure,
                attempt,
                after,
            });
        }
        RestartDecision::Exhausted { restarts } => {
            tracing::error!(vm = %name, restarts, code = failure.report.code, message = failure.report.message, "twin keeps failing; giving the VM up");
            state.report(MachineReport::Exhausted {
                name,
                restarts,
                failures: history.failures(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use super::*;
    use crate::domain::path::AbsolutePath;
    use crate::journal::{Journal, JournalArgs};
    use crate::testing::{next, probe};
    use crate::vm::builder::BuildCommand;
    use crate::vm::local_qemu::Placement;

    fn policy() -> RestartPolicy {
        RestartPolicy {
            max_restarts: 3,
            window: Duration::from_secs(60),
            backoff_first: Duration::from_secs(1),
            backoff_max: Duration::from_secs(5),
        }
    }

    fn failure(kind: FailureKind) -> TwinFailure {
        TwinFailure {
            kind,
            report: Report {
                code: "test".into(),
                message: "x".into(),
                hint: None,
            },
        }
    }

    /// Feeds failures `seconds` after a start; returns each decision.
    fn run(failures: &[(u64, FailureKind)]) -> Vec<RestartDecision> {
        let start = Instant::now();
        let mut history = RestartHistory::default();
        failures
            .iter()
            .map(|(seconds, kind)| {
                let (next, decision) = decide(
                    &policy(),
                    history.clone(),
                    failure(*kind),
                    start + Duration::from_secs(*seconds),
                );
                history = next;
                decision
            })
            .collect()
    }

    fn restart(attempt: u32, seconds: u64) -> RestartDecision {
        RestartDecision::Restart {
            attempt,
            after: Duration::from_secs(seconds),
        }
    }

    #[test]
    fn backoff_doubles_up_to_the_maximum_then_the_budget_is_spent() {
        use FailureKind::Journal;
        assert_eq!(
            run(&[(0, Journal), (1, Journal), (2, Journal), (3, Journal)]),
            [
                restart(1, 1),
                restart(2, 2),
                restart(3, 4),
                RestartDecision::Exhausted { restarts: 3 }
            ]
        );
    }

    #[test]
    fn the_delay_is_capped_at_the_maximum() {
        let policy = RestartPolicy {
            max_restarts: 10,
            ..policy()
        };
        let start = Instant::now();
        let mut history = RestartHistory::default();
        let mut last = RestartDecision::Exhausted { restarts: 0 };
        for second in 0..5 {
            let (next, decision) = decide(
                &policy,
                history,
                failure(FailureKind::Bug),
                start + Duration::from_secs(second),
            );
            history = next;
            last = decision;
        }
        assert_eq!(last, restart(5, 5));
    }

    #[test]
    fn a_conflict_restarts_at_once_but_counts_against_the_budget() {
        use FailureKind::{ChildEscalated, Conflict};
        assert_eq!(
            run(&[
                (0, Conflict),
                (1, ChildEscalated),
                (2, Conflict),
                (3, Conflict)
            ]),
            [
                restart(1, 0),
                restart(2, 2),
                restart(3, 0),
                RestartDecision::Exhausted { restarts: 3 }
            ]
        );
    }

    #[test]
    fn failures_older_than_the_window_no_longer_count() {
        use FailureKind::SpawnFailed;
        assert_eq!(
            run(&[
                (0, SpawnFailed),
                (1, SpawnFailed),
                (2, SpawnFailed),
                (100, SpawnFailed)
            ]),
            [restart(1, 1), restart(2, 2), restart(3, 4), restart(1, 1)]
        );
    }

    #[test]
    fn a_typed_twin_failure_is_classified_and_anything_else_is_a_bug() {
        let cases: Vec<(ActorProcessingErr, FailureKind)> = vec![
            (
                Fatal::Persist(PersistFailure::Conflict { first: 1, next: 2 }).into(),
                FailureKind::Conflict,
            ),
            (Fatal::PersistTimeout.into(), FailureKind::Journal),
            (
                Fatal::ChildEnded {
                    child: "c".into(),
                    reason: "r".into(),
                }
                .into(),
                FailureKind::ChildEscalated,
            ),
            (Fatal::SeqOverflow.into(), FailureKind::Bug),
            ("a panic message".into(), FailureKind::Bug),
        ];
        for (error, kind) in cases {
            assert_eq!(classify(&error).kind, kind, "{error}");
        }
    }

    struct Supervised {
        _temporary: tempfile::TempDir,
        runtime: RuntimeId,
        supervisor: ActorRef<MachineSupervisorMessage>,
        reports: tokio::sync::mpsc::UnboundedReceiver<MachineReport>,
    }

    /// A supervisor over a memory journal; twins build nothing until asked to start.
    async fn supervised(policy: RestartPolicy) -> Supervised {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let paths = RuntimePaths::under(root.clone(), root).unwrap();
        let (journal, _) = Actor::spawn(
            None,
            Journal,
            JournalArgs {
                store: crate::journal::JournalStore::Memory,
                clock: crate::clock::Clock::System,
            },
        )
        .await
        .unwrap();
        let (reporter, reports) = probe::<MachineReport>().await;
        let (from_vm, _) = probe::<FromVm>().await;
        let runtime = RuntimeId::mint();
        let (supervisor, _) = Actor::spawn(
            None,
            MachineSupervisor,
            MachineSupervisorArgs {
                runtime,
                report: reporter.get_derived(),
                from_vm: from_vm.get_derived(),
                journal,
                paths,
                vm_defaults: VmDefaults {
                    build: BuildCommand::Scripted(crate::plan::argv::Argv::for_tests(&["true"])),
                    placement: Placement::Direct,
                    process: crate::runtime::timing::ProcessTiming::quick(),
                },
                restart: policy,
                persist_timeout: Duration::from_secs(5),
            },
        )
        .await
        .unwrap();
        Supervised {
            _temporary: temporary,
            runtime,
            supervisor,
            reports,
        }
    }

    fn quick(max_restarts: u32) -> RestartPolicy {
        RestartPolicy {
            max_restarts,
            window: Duration::from_secs(60),
            backoff_first: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
        }
    }

    fn vm(name: &str) -> VmName {
        VmName::parse(name.into()).unwrap()
    }

    /// Kills one of the twin's children, found by the name the twin gave it.
    fn kill_child(runtime: RuntimeId, name: &str, twin: &ActorRef<TwinMessage>, role: &str) {
        let child = format!("{runtime}/vm/{name}/{role}@{}", twin.get_id());
        ractor::registry::where_is(child).unwrap().kill();
    }

    /// The next report's twin, if it is `Up`.
    async fn up(
        reports: &mut tokio::sync::mpsc::UnboundedReceiver<MachineReport>,
    ) -> Option<ActorRef<TwinMessage>> {
        match next(reports).await {
            MachineReport::Up { twin, .. } => Some(twin),
            MachineReport::Restarting { .. }
            | MachineReport::Exhausted { .. }
            | MachineReport::Terminated { .. } => None,
        }
    }

    #[tokio::test]
    async fn a_twin_whose_child_ends_is_restarted_as_a_unit() {
        let mut supervised = supervised(quick(3)).await;
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Ensure { name: vm("unit") })
            .unwrap();
        let first = up(&mut supervised.reports).await.unwrap();
        kill_child(supervised.runtime, "unit", &first, "hypervisor");
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Restarting {
                failure: TwinFailure {
                    kind: FailureKind::ChildEscalated,
                    ..
                },
                attempt: 1,
                ..
            }
        ));
        let second = up(&mut supervised.reports).await.unwrap();
        assert_ne!(first.get_id(), second.get_id());
        // The new twin's children are its own, under fresh names.
        kill_child(supervised.runtime, "unit", &second, "builder");
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Restarting { attempt: 2, .. }
        ));
        supervised.supervisor.stop(None);
    }

    #[tokio::test]
    async fn a_twin_that_keeps_failing_is_given_up_after_its_budget() {
        let mut supervised = supervised(quick(1)).await;
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Ensure { name: vm("flaky") })
            .unwrap();
        let first = up(&mut supervised.reports).await.unwrap();
        kill_child(supervised.runtime, "flaky", &first, "guest_vm");
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Restarting { attempt: 1, .. }
        ));
        let second = up(&mut supervised.reports).await.unwrap();
        kill_child(supervised.runtime, "flaky", &second, "guest_vm");
        let MachineReport::Exhausted {
            restarts, failures, ..
        } = next(&mut supervised.reports).await
        else {
            return assert_eq!("exhausted", "something else");
        };
        assert_eq!((restarts, failures.len()), (1, 2));
        // Given up: ensuring it again starts over with a fresh budget.
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Ensure { name: vm("flaky") })
            .unwrap();
        up(&mut supervised.reports).await.unwrap();
        supervised.supervisor.stop(None);
    }

    #[tokio::test]
    async fn a_terminated_twin_is_not_restarted() {
        let mut supervised = supervised(quick(3)).await;
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Ensure { name: vm("gone") })
            .unwrap();
        let twin = up(&mut supervised.reports).await.unwrap();
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Terminate { name: vm("gone") })
            .unwrap();
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Terminated { .. }
        ));
        assert_eq!(twin.get_status(), ractor::ActorStatus::Stopped);
        let quiet =
            tokio::time::timeout(Duration::from_millis(300), supervised.reports.recv()).await;
        assert!(quiet.is_err(), "{quiet:?}");
        supervised.supervisor.stop(None);
    }

    #[tokio::test(start_paused = true)]
    async fn after_a_quiet_window_a_failure_restarts_again_instead_of_giving_up() {
        let policy = RestartPolicy {
            max_restarts: 1,
            window: Duration::from_secs(600),
            backoff_first: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
        };
        let mut supervised = supervised(policy).await;
        supervised
            .supervisor
            .cast(MachineSupervisorMessage::Ensure {
                name: vm("patient"),
            })
            .unwrap();
        let first = up(&mut supervised.reports).await.unwrap();
        kill_child(supervised.runtime, "patient", &first, "builder");
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Restarting { attempt: 1, .. }
        ));
        let second = up(&mut supervised.reports).await.unwrap();
        // Ten quiet minutes in virtual time: the first failure no longer counts.
        tokio::time::advance(Duration::from_secs(601)).await;
        kill_child(supervised.runtime, "patient", &second, "builder");
        assert!(matches!(
            next(&mut supervised.reports).await,
            MachineReport::Restarting { attempt: 1, .. }
        ));
        supervised.supervisor.stop(None);
    }
}
