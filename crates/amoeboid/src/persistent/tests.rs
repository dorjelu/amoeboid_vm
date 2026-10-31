#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use serde::Deserialize;

use super::machine::{Action, Core, Fatal, Input, Mode, ModeName, step};
use super::*;
use crate::journal::{
    EventPayload, Journal, JournalArgs, PersistFailure, Persistence, StoredEvent,
};
use crate::runtime::paths::{AbsDir, RuntimePaths};

/// A counter that is reconciled once opened: by an `Open` observation (`Manual`), or by itself
/// on recovery (`Auto`, for end-to-end tests where nothing else would send the observation).
struct Counter<const AUTO: bool>;
type Manual = Counter<false>;
type Auto = Counter<true>;

#[derive(Debug)]
enum Cmd {
    Add(u32),
    Read,
}

#[derive(Debug)]
enum Obs {
    Open,
}

#[derive(Debug, Serialize, Deserialize)]
enum Ev {
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
    type Command = Cmd;
    type Observation = Obs;
    type Event = Ev;
    type State = State;
    type Out = Out;
    type Peers = Peers;
    const EVENT_VERSION: u16 = 1;

    fn initial() -> State {
        State::default()
    }
    fn on_command(state: &State, command: Cmd) -> Effect<Ev, Out> {
        match command {
            Cmd::Add(n) => Effect::Persist {
                events: NonEmpty::new(Ev::Added(n), vec![]),
                then: vec![Out::Total(state.total + n)],
            },
            Cmd::Read => Effect::Send(NonEmpty::new(Out::Total(state.total), vec![])),
        }
    }
    fn on_observation(state: &State, observation: Obs) -> Effect<Ev, Out> {
        match observation {
            Obs::Open if !state.open => Effect::Persist {
                events: NonEmpty::new(Ev::Opened, vec![]),
                then: vec![],
            },
            Obs::Open => Effect::None,
        }
    }
    fn on_event(state: State, event: &Ev) -> State {
        match event {
            Ev::Added(n) => State {
                total: state.total + n,
                ..state
            },
            Ev::Opened => State {
                open: true,
                ..state
            },
        }
    }
    fn on_recovered(state: &State) -> Effect<Ev, Out> {
        if AUTO && !state.open {
            Effect::Persist {
                events: NonEmpty::new(Ev::Opened, vec![]),
                then: vec![],
            }
        } else {
            Effect::None
        }
    }
    fn reconciled(state: &State) -> bool {
        state.open
    }
    fn refuse(command: Cmd, _: &Unrecoverable) -> Option<Out> {
        match command {
            Cmd::Read => Some(Out::Refused),
            Cmd::Add(_) => None,
        }
    }
    fn on_unrecoverable(_: &Unrecoverable) -> Out {
        Out::Unrecoverable
    }
    fn send(peers: &Peers, out: Out) {
        let _ = peers.send(out);
    }
}

fn seq(n: u64) -> Seq {
    std::iter::successors(Some(Seq::FIRST), |s| s.next())
        .nth(usize::try_from(n - 1).unwrap())
        .unwrap()
}

fn stored(n: u64, json: &str) -> StoredEvent {
    StoredEvent {
        seq: seq(n),
        payload: serde_json::from_str::<serde_json::Value>(json)
            .map(|v| EventPayload::encode(&v).unwrap())
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
        vec![Input::Replayed(events), Input::Observation(Obs::Open)],
    );
    let batch = persisted_batch(&actions);
    let (core, _) = step(
        core,
        Input::Persisted {
            batch,
            last: seq(u64::try_from(last).unwrap() + 1),
        },
    );
    assert!(matches!(core.mode, Mode::Ready));
    core
}

#[test]
fn replay_folds_events_into_state_and_tracks_the_last_position() {
    let (core, _) = step(
        Core::<Manual>::recovering(),
        Input::Replayed(vec![
            stored(1, r#"{"v":1,"event":{"Added":2}}"#),
            stored(2, r#"{"v":1,"event":{"Added":3}}"#),
        ]),
    );
    assert_eq!(core.state.total, 5);
    assert_eq!(core.last, Some(seq(2)));
    assert!(matches!(core.mode, Mode::Reconciling { .. }));
}

#[test]
fn commands_wait_until_reconciled_then_run_in_arrival_order() {
    let (core, actions) = run(
        Core::recovering(),
        vec![
            Input::Command(Cmd::Read),
            Input::Replayed(vec![stored(1, r#"{"v":1,"event":{"Added":4}}"#)]),
            Input::Command(Cmd::Read),
        ],
    );
    assert_eq!(sent(&actions), []);
    let (core, actions) = step(core, Input::Observation(Obs::Open));
    let batch = persisted_batch(&actions);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch,
            last: seq(2),
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
fn then_messages_are_sent_only_once_the_batch_is_durable() {
    let core = ready_with(vec![]);
    let (core, actions) = step(core, Input::Command(Cmd::Add(7)));
    assert_eq!(sent(&actions), []);
    assert!(matches!(
        actions.first(),
        Some(Action::Persist { first, .. }) if *first == seq(2)
    ));
    let batch = persisted_batch(&actions);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch,
            last: seq(2),
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
            Input::Command(Cmd::Add(1)),
            Input::Command(Cmd::Read),
            Input::Command(Cmd::Add(2)),
        ],
    );
    let first = persisted_batch(&actions);
    assert_eq!(sent(&actions), []);
    let (core, actions) = step(
        core,
        Input::Persisted {
            batch: first,
            last: seq(2),
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
            last: seq(3),
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
        let (core, actions) = step(ready_with(vec![]), Input::Command(Cmd::Add(1)));
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
    let (core, actions) = step(ready_with(vec![]), Input::Command(Cmd::Add(1)));
    let first = persisted_batch(&actions);
    let (core, _) = step(
        core,
        Input::Persisted {
            batch: first,
            last: seq(2),
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
        (r#"{"v":1,"event":{"Nope":1}}"#, false),
        (r#"{"v":9,"event":{"Added":1}}"#, true),
    ] {
        let (core, actions) = run(
            Core::recovering(),
            vec![
                Input::Command(Cmd::Read),
                Input::Command(Cmd::Add(1)),
                Input::Replayed(vec![stored(1, json)]),
                Input::Command(Cmd::Read),
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
    let (core, actions) = step(Core::<Manual>::recovering(), Input::Observation(Obs::Open));
    assert!(matches!(actions.as_slice(), [Action::Ignored(_)]));
    assert!(matches!(core.mode, Mode::Recovering { stash } if stash.is_empty()));
}

// End to end through ractor and the memory journal.

async fn spawn_counter(
    journal: &ActorRef<JournalMsg>,
    id: &PersistenceId,
) -> (
    ActorRef<PersistentMsg<Cmd, Obs>>,
    tokio::sync::mpsc::UnboundedReceiver<Out>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (actor, _) = Actor::spawn(
        None,
        Persistent::<Auto>::new(),
        PersistentArgs {
            id: id.clone(),
            journal: journal.clone(),
            peers: tx,
            persist_timeout: Duration::from_secs(5),
        },
    )
    .await
    .unwrap();
    (actor, rx)
}

#[tokio::test]
async fn state_survives_killing_and_respawning_the_actor() {
    let dir = tempfile::tempdir().unwrap();
    let abs = AbsDir::parse(dir.path().to_path_buf()).unwrap();
    let (journal, _) = Actor::spawn(
        None,
        Journal,
        JournalArgs {
            persistence: Persistence::Memory,
            paths: RuntimePaths::under(abs.clone(), abs).unwrap(),
        },
    )
    .await
    .unwrap();
    let id = PersistenceId::parse("test/counter".into()).unwrap();

    let (actor, mut rx) = spawn_counter(&journal, &id).await;
    actor.cast(PersistentMsg::Command(Cmd::Add(2))).unwrap();
    actor.cast(PersistentMsg::Command(Cmd::Add(3))).unwrap();
    assert_eq!(rx.recv().await, Some(Out::Total(2)));
    assert_eq!(rx.recv().await, Some(Out::Total(5)));
    actor.kill_and_wait(None).await.unwrap();

    let (again, mut rx) = spawn_counter(&journal, &id).await;
    again.cast(PersistentMsg::Command(Cmd::Read)).unwrap();
    assert_eq!(rx.recv().await, Some(Out::Total(5)));
}
