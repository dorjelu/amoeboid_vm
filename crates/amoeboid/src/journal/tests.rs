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

fn seq(n: u64) -> Seq {
    Seq(NonZeroU64::new(n).unwrap())
}

struct Fixture {
    _dir: tempfile::TempDir,
    paths: RuntimePaths,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let abs = AbsDir::parse(dir.path().to_path_buf()).unwrap();
    let paths = RuntimePaths::under(abs.clone(), abs).unwrap();
    Fixture { _dir: dir, paths }
}

async fn each_store(test: impl AsyncFn(Store, &Fixture)) {
    for persistence in [Persistence::Memory, Persistence::Sqlite] {
        let fx = fixture();
        test(Store::open(persistence, &fx.paths).await.unwrap(), &fx).await;
    }
}

#[tokio::test]
async fn appended_events_replay_in_order_with_positions_from_one() {
    each_store(async |mut store, _| {
        store
            .append(&id("vm/a"), Seq::FIRST, payloads(&["x", "y"]))
            .await
            .unwrap();
        let last = store
            .append(&id("vm/a"), seq(3), payloads(&["z"]))
            .await
            .unwrap();
        assert_eq!(last, seq(3));
        let replayed = store.replay(&id("vm/a")).await.unwrap();
        let seqs: Vec<u64> = replayed.iter().map(|e| e.seq.get()).collect();
        let bodies: Vec<&str> = replayed.iter().map(|e| e.payload.as_str()).collect();
        assert_eq!(seqs, [1, 2, 3]);
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
            .append(&id("vm/a"), Seq::FIRST, payloads(&["x"]))
            .await
            .unwrap();
        let stale = store
            .append(&id("vm/a"), Seq::FIRST, payloads(&["stale"]))
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
        let gap = store.append(&id("vm/a"), seq(2), payloads(&["x"])).await;
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
            .append(&id("vm/a"), Seq::FIRST, payloads(&["a"]))
            .await
            .unwrap();
        store
            .append(&id("vm/b"), Seq::FIRST, payloads(&["b"]))
            .await
            .unwrap();
        assert_eq!(store.replay(&id("vm/a")).await.unwrap().len(), 1);
        assert_eq!(store.replay(&id("vm/b")).await.unwrap().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn sqlite_events_survive_reopening() {
    let fx = fixture();
    let mut store = Store::open(Persistence::Sqlite, &fx.paths).await.unwrap();
    store
        .append(&id("vm/a"), Seq::FIRST, payloads(&["x"]))
        .await
        .unwrap();
    drop(store);
    let mut reopened = Store::open(Persistence::Sqlite, &fx.paths).await.unwrap();
    assert_eq!(reopened.replay(&id("vm/a")).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_sqlite_file_is_created_owner_only() {
    let fx = fixture();
    Store::open(Persistence::Sqlite, &fx.paths).await.unwrap();
    let mode = std::fs::metadata(fx.paths.journal())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test]
async fn a_symlink_at_the_journal_path_is_refused() {
    let fx = fixture();
    let target = fx.paths.data().as_path().join("elsewhere");
    std::os::unix::fs::symlink(&target, fx.paths.journal()).unwrap();
    assert!(Store::open(Persistence::Sqlite, &fx.paths).await.is_err());
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
    assert_eq!(Seq::after(None), Some(Seq::FIRST));
    assert_eq!(Seq::after(Some(seq(4))), Some(seq(5)));
    assert_eq!(Seq::after(Some(seq(u64::MAX))), None);
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
            tx: Self::Arguments,
        ) -> impl Future<Output = Result<Self::State, ActorProcessingErr>> + Send {
            std::future::ready(Ok(tx))
        }
        fn handle(
            &self,
            _: ActorRef<JournalReply>,
            msg: JournalReply,
            tx: &mut Self::State,
        ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
            std::future::ready(tx.send(msg).map_err(Into::into))
        }
    }

    let fx = fixture();
    let (journal, _) = Actor::spawn(
        None,
        Journal,
        JournalArgs {
            persistence: Persistence::Memory,
            paths: fx.paths.clone(),
        },
    )
    .await
    .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (probe, _) = Actor::spawn(None, Probe, tx).await.unwrap();
    let reply_to: DerivedActorRef<JournalReply> = probe.get_derived();

    journal
        .cast(JournalMsg::Persist {
            batch: BatchId::FIRST,
            id: id("vm/a"),
            first: Seq::FIRST,
            events: payloads(&["x"]),
            reply_to: reply_to.clone(),
        })
        .unwrap();
    assert!(matches!(
        rx.recv().await,
        Some(JournalReply::Persisted { batch: BatchId::FIRST, last }) if last == Seq::FIRST
    ));

    journal
        .cast(JournalMsg::Replay {
            id: id("vm/a"),
            reply_to,
        })
        .unwrap();
    assert!(matches!(
        rx.recv().await,
        Some(JournalReply::Replayed { events, .. }) if events.len() == 1
    ));
}
