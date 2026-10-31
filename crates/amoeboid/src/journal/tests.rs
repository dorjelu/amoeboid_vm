//! Conformance suite: every property runs against both stores.

#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use std::os::unix::fs::PermissionsExt as _;

use super::store::Store;
use super::*;
use crate::runtime::paths::AbsDir;

fn id(s: &str) -> PersistenceId {
    PersistenceId::parse(s.to_owned()).unwrap()
}

fn payloads(items: &[&str]) -> NonEmpty<EventPayload> {
    NonEmpty::from_vec(
        items
            .iter()
            .map(|s| EventPayload::encode(s).unwrap())
            .collect(),
    )
    .unwrap()
}

fn sequence(n: u64) -> Sequence {
    Sequence(NonZeroU64::new(n).unwrap())
}

struct Fixture {
    _temporary: tempfile::TempDir,
    paths: RuntimePaths,
}

fn fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let directory = AbsDir::parse(temporary.path().to_path_buf()).unwrap();
    let paths = RuntimePaths::under(directory.clone(), directory).unwrap();
    Fixture {
        _temporary: temporary,
        paths,
    }
}

async fn each_store(test: impl AsyncFn(Store, &Fixture)) {
    for persistence in [Persistence::Memory, Persistence::Sqlite] {
        let fixture = fixture();
        test(
            Store::open(persistence, &fixture.paths).await.unwrap(),
            &fixture,
        )
        .await;
    }
}

#[tokio::test]
async fn appended_events_replay_in_order_with_positions_from_one() {
    each_store(async |mut store, _| {
        store
            .append(&id("vm/a"), Sequence::FIRST, payloads(&["x", "y"]))
            .await
            .unwrap();
        let last = store
            .append(&id("vm/a"), sequence(3), payloads(&["z"]))
            .await
            .unwrap();
        assert_eq!(last, sequence(3));
        let replayed = store.replay(&id("vm/a")).await.unwrap();
        let sequences: Vec<u64> = replayed.iter().map(|event| event.sequence.get()).collect();
        let bodies: Vec<&str> = replayed
            .iter()
            .map(|event| event.payload.as_str())
            .collect();
        assert_eq!(sequences, [1, 2, 3]);
        assert_eq!(bodies, [r#""x""#, r#""y""#, r#""z""#]);
    })
    .await;
}

#[tokio::test]
async fn an_unknown_stream_replays_empty() {
    each_store(async |mut store, _| {
        assert_eq!(store.replay(&id("vm/none")).await.unwrap(), vec![]);
    })
    .await;
}

#[tokio::test]
async fn appending_at_a_taken_position_is_a_conflict_and_writes_nothing() {
    each_store(async |mut store, _| {
        store
            .append(&id("vm/a"), Sequence::FIRST, payloads(&["x"]))
            .await
            .unwrap();
        let stale = store
            .append(&id("vm/a"), Sequence::FIRST, payloads(&["stale"]))
            .await;
        assert!(matches!(
            stale,
            Err(PersistFailure::Conflict { first: 1, next: 2 })
        ));
        assert_eq!(store.replay(&id("vm/a")).await.unwrap().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn appending_past_the_next_position_is_a_conflict() {
    each_store(async |mut store, _| {
        let gap = store
            .append(&id("vm/a"), sequence(2), payloads(&["x"]))
            .await;
        assert!(matches!(
            gap,
            Err(PersistFailure::Conflict { first: 2, next: 1 })
        ));
    })
    .await;
}

#[tokio::test]
async fn streams_are_independent() {
    each_store(async |mut store, _| {
        store
            .append(&id("vm/a"), Sequence::FIRST, payloads(&["a"]))
            .await
            .unwrap();
        store
            .append(&id("vm/b"), Sequence::FIRST, payloads(&["b"]))
            .await
            .unwrap();
        assert_eq!(store.replay(&id("vm/a")).await.unwrap().len(), 1);
        assert_eq!(store.replay(&id("vm/b")).await.unwrap().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn sqlite_events_survive_reopening() {
    let fixture = fixture();
    let mut store = Store::open(Persistence::Sqlite, &fixture.paths)
        .await
        .unwrap();
    store
        .append(&id("vm/a"), Sequence::FIRST, payloads(&["x"]))
        .await
        .unwrap();
    drop(store);
    let mut reopened = Store::open(Persistence::Sqlite, &fixture.paths)
        .await
        .unwrap();
    assert_eq!(reopened.replay(&id("vm/a")).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_sqlite_file_is_created_owner_only() {
    let fixture = fixture();
    Store::open(Persistence::Sqlite, &fixture.paths)
        .await
        .unwrap();
    let mode = std::fs::metadata(fixture.paths.journal())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test]
async fn a_symlink_at_the_journal_path_is_refused() {
    let fixture = fixture();
    let target = fixture.paths.data().as_path().join("elsewhere");
    std::os::unix::fs::symlink(&target, fixture.paths.journal()).unwrap();
    assert!(
        Store::open(Persistence::Sqlite, &fixture.paths)
            .await
            .is_err()
    );
    assert!(!target.exists());
}

#[test]
fn persistence_ids_are_slash_joined_lowercase_segments() {
    assert!(PersistenceId::parse("vm/work/7/share/d0".into()).is_ok());
    for bad in ["", "vm//a", "/vm", "vm/", "VM/a", "vm/a b", "vm/ä"] {
        assert!(
            PersistenceId::parse(bad.into()).is_err(),
            "{bad:?} accepted"
        );
    }
}

#[test]
fn seq_after_an_empty_stream_is_the_first_position() {
    assert_eq!(Sequence::after(None), Some(Sequence::FIRST));
    assert_eq!(Sequence::after(Some(sequence(4))), Some(sequence(5)));
    assert_eq!(Sequence::after(Some(sequence(u64::MAX))), None);
}

#[tokio::test]
async fn the_journal_actor_answers_persist_and_replay_through_derived_refs() {
    // A minimal requester: forwards every JournalReply into a channel.
    struct Probe;
    impl Actor for Probe {
        type Msg = JournalReply;
        type State = tokio::sync::mpsc::UnboundedSender<JournalReply>;
        type Arguments = tokio::sync::mpsc::UnboundedSender<JournalReply>;
        fn pre_start(
            &self,
            _: ActorRef<JournalReply>,
            sender: Self::Arguments,
        ) -> impl Future<Output = Result<Self::State, ActorProcessingErr>> + Send {
            std::future::ready(Ok(sender))
        }
        fn handle(
            &self,
            _: ActorRef<JournalReply>,
            message: JournalReply,
            sender: &mut Self::State,
        ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
            std::future::ready(sender.send(message).map_err(Into::into))
        }
    }

    let fixture = fixture();
    let (journal, _) = Actor::spawn(
        None,
        Journal,
        JournalArgs {
            persistence: Persistence::Memory,
            paths: fixture.paths.clone(),
        },
    )
    .await
    .unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let (probe, _) = Actor::spawn(None, Probe, sender).await.unwrap();
    let reply_to: DerivedActorRef<JournalReply> = probe.get_derived();

    journal
        .cast(JournalMessage::Persist {
            batch: BatchId::FIRST,
            id: id("vm/a"),
            first: Sequence::FIRST,
            events: payloads(&["x"]),
            reply_to: reply_to.clone(),
        })
        .unwrap();
    assert!(matches!(
        receiver.recv().await,
        Some(JournalReply::Persisted { batch: BatchId::FIRST, last }) if last == Sequence::FIRST
    ));

    journal
        .cast(JournalMessage::Replay {
            id: id("vm/a"),
            reply_to,
        })
        .unwrap();
    assert!(matches!(
        receiver.recv().await,
        Some(JournalReply::Replayed { events, .. }) if events.len() == 1
    ));
}
