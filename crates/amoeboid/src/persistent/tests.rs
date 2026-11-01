#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use serde::Deserialize;

use super::machine::{Action, Core, Fatal, Input, Mode, ModeName, step};
use super::*;
use crate::journal::{EventPayload, Journal, JournalArgs, PersistFailure, Replay, StoredEvent};

/// A counter that is reconciled once opened: by an `Open` observation (`Manual`), or by itself
/// on recovery (`Auto`, for end-to-end tests where nothing else would send the observation).
struct Counter<const AUTO: bool>;
type Manual = Counter<false>;
type Auto = Counter<true>;

#[derive(Debug)]
enum CounterCommand {
    Add(u32),
    Read,
}

#[derive(Debug)]
enum CounterObservation {
    Open,
    /// A fact that is persisted but doesn't open the counter (it stays reconciling).
    Note(u32),
}

#[derive(Debug, Serialize, Deserialize)]
enum CounterEvent {
    Added(u32),
    Opened,
}

#[derive(Default)]
struct State {
    total: u32,
    open: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum Out {
    Total(u32),
    Refused,
    Unrecoverable,
}

type Peers = tokio::sync::mpsc::UnboundedSender<Out>;

impl<const AUTO: bool> Behaviour for Counter<AUTO> {
    type Command = CounterCommand;
    type Observation = CounterObservation;
    type Event = CounterEvent;
    type State = State;
    type Out = Out;
    type Setup = Peers;
    type Peers = Peers;
    const EVENT_VERSION: u16 = 1;

    fn initial() -> State {
        State::default()
    }
    fn on_command(state: &State, command: CounterCommand) -> Effect<CounterEvent, Out> {
        match command {
            CounterCommand::Add(amount) => Effect::Persist {
                events: NonEmpty::new(CounterEvent::Added(amount), vec![]),
                then: vec![Out::Total(state.total + amount)],
            },
            CounterCommand::Read => Effect::Send(NonEmpty::new(Out::Total(state.total), vec![])),
        }
    }
    fn on_observation(state: &State, observation: CounterObservation) -> Effect<CounterEvent, Out> {
        match observation {
            CounterObservation::Open if !state.open => Effect::Persist {
                events: NonEmpty::new(CounterEvent::Opened, vec![]),
                then: vec![],
            },
            CounterObservation::Open => Effect::None,
            CounterObservation::Note(amount) => Effect::Persist {
                events: NonEmpty::new(CounterEvent::Added(amount), vec![]),
                then: vec![],
            },
        }
    }
    fn on_event(state: State, event: &CounterEvent) -> State {
        match event {
            CounterEvent::Added(amount) => State {
                total: state.total + amount,
                ..state
            },
            CounterEvent::Opened => State {
                open: true,
                ..state
            },
        }
    }
    fn on_recovered(state: &State) -> Effect<CounterEvent, Out> {
        if AUTO && !state.open {
            Effect::Persist {
                events: NonEmpty::new(CounterEvent::Opened, vec![]),
                then: vec![],
            }
        } else {
            Effect::None
        }
    }
    fn reconciled(state: &State) -> bool {
        state.open
    }
    fn refuse(command: CounterCommand, _: &Unrecoverable) -> Option<Out> {
        match command {
            CounterCommand::Read => Some(Out::Refused),
            CounterCommand::Add(_) => None,
        }
    }
    fn on_unrecoverable(_: &Unrecoverable) -> Out {
        Out::Unrecoverable
    }
    fn send(peers: &Peers, out: Out) {
        let _ = peers.send(out);
    }
    fn start(
        setup: Peers,
        _: ractor::ActorCell,
    ) -> impl Future<Output = Result<Peers, ActorProcessingErr>> + Send {
        std::future::ready(Ok(setup))
    }
    fn child_ended(_: ChildEnded) -> ChildPolicy<CounterObservation> {
        ChildPolicy::Escalate
    }
}

fn sequence(amount: u64) -> Sequence {
    std::iter::successors(Some(Sequence::FIRST), |s| s.next())
        .nth(usize::try_from(amount - 1).unwrap())
        .unwrap()
}

fn stored(amount: u64, json: &str) -> StoredEvent {
    StoredEvent {
        sequence: sequence(amount),
        payload: serde_json::from_str::<serde_json::Value>(json)
            .map(|version| EventPayload::encode(&version).unwrap())
            .unwrap(),
    }
}

fn run(core: Core<Manual>, inputs: Vec<Input<Manual>>) -> (Core<Manual>, Vec<Action<Manual>>) {
    inputs
        .into_iter()
        .fold((core, vec![]), |(core, mut all), input| {
            let (core, actions) = step(core, input);
            all.extend(actions);
            (core, all)
        })
}

fn sent(actions: &[Action<Manual>]) -> Vec<Out> {
    actions
        .iter()
        .filter_map(|a| {
            if let Action::Send(out) = a {
                Some(out.clone())
            } else {
                None
            }
        })
        .collect()
}

fn persisted_batch(actions: &[Action<Manual>]) -> BatchId {
    actions
        .iter()
        .rev()
        .find_map(|a| {
            if let Action::Persist { batch, .. } = a {
                Some(*batch)
            } else {
                None
            }
        })
        .unwrap()
}

/// Recovered from `events`, then opened, so `Ready`.
fn ready_with(events: Vec<StoredEvent>) -> Core<Manual> {
    let last = events.len();
    let (core, actions) = run(
        Core::recovering(),
        vec![
            Input::Replayed(Replay::of(events)),
            Input::Observation(CounterObservation::Open),
        ],
    );
    let batch = persisted_batch(&actions);
    let (core, _) = step(
        core,
        Input::Persisted {
            batch,
            last: sequence(u64::try_from(last).unwrap() + 1),
        },
    );
    assert!(matches!(core.mode, Mode::Ready));
    core
}

#[test]
fn replay_folds_events_into_state_and_tracks_the_last_position() {
    let (core, _) = step(
        Core::<Manual>::recovering(),
        Input::Replayed(Replay::of(vec![
            stored(1, r#"{"version":1,"event":{"Added":2}}"#),
            stored(2, r#"{"version":1,"event":{"Added":3}}"#),
        ])),
    );
    assert_eq!(core.state.total, 5);
    assert_eq!(core.last, Some(sequence(2)));
    assert!(matches!(core.mode, Mode::Reconciling { .. }));
}

#[test]
fn commands_wait_until_reconciled_then_run_in_arrival_order() {
    let (core, actions) = run(
        Core::recovering(),
        vec![
            Input::Command(CounterCommand::Read),
            Input::Replayed(Replay::of(vec![stored(
                1,
                r#"{"version":1,"event":{"Added":4}}"#,
            )])),
            Input::Command(CounterCommand::Read),
        ],
    );
    assert_eq!(sent(&actions), []);
    let (core, actions) = step(core, Input::Observation(CounterObservation::Open));
    let batch = persisted_batch(&actions);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch,
            last: sequence(2),
        },
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::Entered(ModeName::Ready)))
    );
    assert_eq!(sent(&actions), [Out::Total(4), Out::Total(4)]);
    assert!(matches!(core.mode, Mode::Ready));
}

#[test]
fn past_a_tombstone_the_next_persist_goes_after_the_retired_events() {
    let (_, actions) = run(
        Core::<Manual>::recovering(),
        vec![
            Input::Replayed(Replay {
                events: vec![],
                last: Some(sequence(5)),
            }),
            Input::Observation(CounterObservation::Open),
        ],
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Persist { first, .. } if *first == sequence(6)))
    );
}

#[test]
fn then_messages_are_sent_only_once_the_batch_is_durable() {
    let core = ready_with(vec![]);
    let (core, actions) = step(core, Input::Command(CounterCommand::Add(7)));
    assert_eq!(sent(&actions), []);
    assert!(matches!(
        actions.first(),
        Some(Action::Persist { first, .. }) if *first == sequence(2)
    ));
    let batch = persisted_batch(&actions);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch,
            last: sequence(2),
        },
    );
    assert_eq!(sent(&actions), [Out::Total(7)]);
    assert_eq!(core.state.total, 7);
}

#[test]
fn inputs_during_a_persist_are_held_and_run_after_it_in_order() {
    let core = ready_with(vec![]);
    let (core, actions) = run(
        core,
        vec![
            Input::Command(CounterCommand::Add(1)),
            Input::Command(CounterCommand::Read),
            Input::Command(CounterCommand::Add(2)),
        ],
    );
    let first = persisted_batch(&actions);
    assert_eq!(sent(&actions), []);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch: first,
            last: sequence(2),
        },
    );
    // Add(1) completes, Read sees 1, Add(2) starts the next batch.
    assert_eq!(sent(&actions), [Out::Total(1), Out::Total(1)]);
    let second = persisted_batch(&actions);
    assert_ne!(first, second);
    let (_, actions) = step(
        core,
        Input::Persisted {
            batch: second,
            last: sequence(3),
        },
    );
    assert_eq!(sent(&actions), [Out::Total(3)]);
}

#[test]
fn a_failed_or_timed_out_batch_stops_the_actor() {
    for input in [
        |batch| Input::PersistFailed {
            batch,
            reason: PersistFailure::Conflict { first: 2, next: 3 },
        },
        Input::PersistTimeout,
    ] {
        let (core, actions) = step(ready_with(vec![]), Input::Command(CounterCommand::Add(1)));
        let batch = persisted_batch(&actions);
        let (_, actions) = step(core, input(batch));
        assert!(matches!(
            actions.last(),
            Some(Action::Stop(Fatal::Persist(_) | Fatal::PersistTimeout))
        ));
    }
}

#[test]
fn a_timeout_for_an_earlier_batch_is_ignored() {
    let (core, actions) = step(ready_with(vec![]), Input::Command(CounterCommand::Add(1)));
    let first = persisted_batch(&actions);
    let (core, _) = step(
        core,
        Input::Persisted {
            batch: first,
            last: sequence(2),
        },
    );
    let (_, actions) = step(core, Input::PersistTimeout(first));
    assert!(matches!(actions.as_slice(), [Action::Ignored(_)]));
}

#[test]
fn a_store_error_during_replay_stops_the_actor() {
    let (_, actions) = step(
        Core::<Manual>::recovering(),
        Input::ReplayFailed(crate::journal::tests_support::store_error("disk")),
    );
    assert!(matches!(
        actions.last(),
        Some(Action::Stop(Fatal::Replay(_)))
    ));
}

#[test]
fn an_undecodable_or_unknown_version_stream_is_quarantined_not_stopped() {
    for (json, expect_version) in [
        (r#"{"version":1,"event":{"Nope":1}}"#, false),
        (r#"{"version":9,"event":{"Added":1}}"#, true),
    ] {
        let (core, actions) = run(
            Core::recovering(),
            vec![
                Input::Command(CounterCommand::Read),
                Input::Command(CounterCommand::Add(1)),
                Input::Replayed(Replay::of(vec![stored(1, json)])),
                Input::Command(CounterCommand::Read),
            ],
        );
        // Report once, refuse the stashed Read (Add expects no answer), refuse the later Read.
        assert_eq!(
            sent(&actions),
            [Out::Unrecoverable, Out::Refused, Out::Refused]
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::Stop(_))));
        assert!(matches!(
            core.mode,
            Mode::Unrecoverable { reason }
                if matches!(reason, Unrecoverable::UnknownEventVersion { found: 9, .. })
                    == expect_version
        ));
    }
}

#[test]
fn observations_before_recovery_are_ignored() {
    let (core, actions) = step(
        Core::<Manual>::recovering(),
        Input::Observation(CounterObservation::Open),
    );
    assert!(matches!(actions.as_slice(), [Action::Ignored(_)]));
    assert!(matches!(core.mode, Mode::Recovering { stash } if stash.is_empty()));
}

// End to end through ractor and the memory journal.

async fn spawn_counter(
    journal: &ActorRef<JournalMessage>,
    id: &PersistenceId,
) -> (
    ActorRef<PersistentMessage<CounterCommand, CounterObservation>>,
    tokio::sync::mpsc::UnboundedReceiver<Out>,
) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (actor, _) = Actor::spawn(
        None,
        Persistent::<Auto>::new(),
        PersistentArgs {
            id: id.clone(),
            journal: journal.clone(),
            setup: sender,
            persist_timeout: Duration::from_secs(5),
        },
    )
    .await
    .unwrap();
    (actor, receiver)
}

#[tokio::test]
async fn state_survives_killing_and_respawning_the_actor() {
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
    let id = PersistenceId::parse("test/counter".into()).unwrap();

    let (actor, mut receiver) = spawn_counter(&journal, &id).await;
    actor
        .cast(PersistentMessage::Command(CounterCommand::Add(2)))
        .unwrap();
    actor
        .cast(PersistentMessage::Command(CounterCommand::Add(3)))
        .unwrap();
    assert_eq!(receiver.recv().await, Some(Out::Total(2)));
    assert_eq!(receiver.recv().await, Some(Out::Total(5)));
    actor.kill_and_wait(None).await.unwrap();

    let (again, mut receiver) = spawn_counter(&journal, &id).await;
    again
        .cast(PersistentMessage::Command(CounterCommand::Read))
        .unwrap();
    assert_eq!(receiver.recv().await, Some(Out::Total(5)));
}

#[test]
fn replies_for_another_batch_than_the_one_in_flight_are_ignored() {
    let (core, actions) = step(ready_with(vec![]), Input::Command(CounterCommand::Add(1)));
    let current = persisted_batch(&actions);
    let stale = current.next();
    let inputs = [
        Input::Persisted {
            batch: stale,
            last: sequence(9),
        },
        Input::PersistFailed {
            batch: stale,
            reason: PersistFailure::Conflict { first: 1, next: 2 },
        },
        Input::PersistTimeout(stale),
    ];
    let mut core = core;
    for input in inputs {
        let (next, actions) = step(core, input);
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::Stop(_))),
            "a stale reply stopped the actor"
        );
        assert!(matches!(next.mode, Mode::Persisting(_)));
        core = next;
    }
}

#[test]
fn a_persist_while_reconciling_resumes_reconciling_until_reconciled() {
    let (core, _) = run(
        Core::<Manual>::recovering(),
        vec![Input::Replayed(Replay::of(vec![]))],
    );
    // Not yet reconciled (the counter opens on its observation); a persisting observation
    // that leaves it closed returns to reconciling.
    let (core, actions) = step(core, Input::Observation(CounterObservation::Note(1)));
    let batch = persisted_batch(&actions);
    let (core, _) = step(
        core,
        Input::Persisted {
            batch,
            last: sequence(1),
        },
    );
    assert!(matches!(core.mode, Mode::Reconciling { .. }));
}
