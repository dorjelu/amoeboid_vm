#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use serde_json::json;
use tokio::net::UnixStream;

use super::*;
use crate::client::{Client, ClientError};
use crate::config::daemon::DaemonConfig;
use crate::domain::path::AbsolutePath;
use crate::protocol::codec;
use crate::protocol::{Refusal, ReplyBody, RequestBody, RuntimeReply, RuntimeRequest, ServerFrame};
use paths::ConfigPaths;

struct Fixture {
    _temporary: tempfile::TempDir,
    paths: RuntimePaths,
}

fn fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let data = AbsolutePath::parse(temporary.path().join("data")).unwrap();
    let run = AbsolutePath::parse(temporary.path().join("run")).unwrap();
    let paths = RuntimePaths::under(data, run).unwrap();
    Fixture {
        _temporary: temporary,
        paths,
    }
}

async fn start(paths: &RuntimePaths) -> Result<Runtime, StartError> {
    start_with(paths, Persistence::Memory, root::VmDefaults::production()).await
}

async fn start_with(
    paths: &RuntimePaths,
    persistence: Persistence,
    vm_defaults: root::VmDefaults,
) -> Result<Runtime, StartError> {
    start_configured(paths, persistence, vm_defaults, DaemonConfig::default()).await
}

/// The configuration directory and home of a fixture: beside its data and run directories.
fn config_paths(paths: &RuntimePaths) -> ConfigPaths {
    let root = paths.data().parent().unwrap();
    ConfigPaths::new(root.join("config"), root.join("home"))
}

async fn start_configured(
    paths: &RuntimePaths,
    persistence: Persistence,
    vm_defaults: root::VmDefaults,
    daemon: DaemonConfig,
) -> Result<Runtime, StartError> {
    Runtime::start(RuntimeConfig {
        paths: paths.clone(),
        config: config_paths(paths),
        hosting: Hosting::InProcess,
        persistence,
        daemon: DaemonConfigSource::Given(daemon),
        version: "test".into(),
        account: crate::vm::Account {
            user: crate::domain::account::UserName::parse("alice".into()).unwrap(),
            user_id: crate::domain::account::UserId::new(1000),
            group_id: crate::domain::account::GroupId::new(100),
            home: crate::domain::account::GuestHome::parse("/home/alice".into()).unwrap(),
        },
        vm_defaults,
        clock: crate::clock::Clock::System,
    })
    .await
}

/// Sends `frames` raw and returns the first frame the runtime answers with.
async fn exchange(paths: &RuntimePaths, frames: &[serde_json::Value]) -> ServerFrame {
    let mut stream = UnixStream::connect(paths.socket()).await.unwrap();
    for frame in frames {
        codec::write(&mut stream, frame).await.unwrap();
    }
    let mut last = None;
    while let Ok(Some(frame)) = codec::read::<ServerFrame, _>(&mut stream).await {
        last = Some(frame);
        if !matches!(last, Some(ServerFrame::HandshakeAccepted { .. })) {
            break;
        }
    }
    last.unwrap()
}

#[tokio::test]
async fn a_client_pings_the_runtime_and_learns_its_id() {
    let fixture = fixture();
    let runtime = start(&fixture.paths).await.unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    assert_eq!(client.runtime(), runtime.id());
    let reply = client
        .request(RequestBody::Runtime(RuntimeRequest::Ping))
        .await
        .unwrap();
    assert_eq!(reply, ReplyBody::Runtime(RuntimeReply::Pong));
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn a_second_runtime_is_refused_while_the_first_holds_the_lock() {
    let fixture = fixture();
    let first = start(&fixture.paths).await.unwrap();
    let second = start(&fixture.paths).await;
    assert!(matches!(
        second,
        Err(StartError::AlreadyRunning { holder: Some(holder) }) if holder.process_id == std::process::id()
    ));
    first.stop();
    first.wait().await;
}

#[tokio::test]
#[allow(
    clippy::await_holding_lock,
    reason = "drops and retakes daemon.lock: no fork may pin it meanwhile; this runtime spawns \
              nothing, so the guard can't deadlock"
)]
async fn a_stopped_runtime_releases_the_lock_and_socket_for_the_next() {
    let _forks = crate::testing::forks();
    let fixture = fixture();
    let first = start(&fixture.paths).await.unwrap();
    first.stop();
    first.wait().await;
    let second = start(&fixture.paths).await.unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    assert_eq!(client.runtime(), second.id());
    client
        .request(RequestBody::Runtime(RuntimeRequest::Ping))
        .await
        .unwrap();
    second.stop();
    second.wait().await;
}

#[tokio::test]
async fn another_protocol_version_is_rejected_with_both_versions() {
    let fixture = fixture();
    let runtime = start(&fixture.paths).await.unwrap();
    let answer = exchange(
        &fixture.paths,
        &[json!({"type": "handshake_start", "body": {"protocol": 999}})],
    )
    .await;
    assert!(
        matches!(answer, ServerFrame::HandshakeRejected { runtime: r, client: c }
        if r == ProtocolVersion::CURRENT && c.to_string() == "999")
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn a_request_before_the_handshake_is_refused() {
    let fixture = fixture();
    let runtime = start(&fixture.paths).await.unwrap();
    let operation = crate::protocol::OperationId::mint();
    let answer = exchange(
        &fixture.paths,
        &[
            json!({"type": "request", "body": {"operation": operation.to_string(),
                 "body": {"domain": "runtime", "request": "ping"}}}),
        ],
    )
    .await;
    assert_eq!(
        answer,
        ServerFrame::Refused {
            operation: Some(operation),
            reason: Refusal::HandshakeRequired
        }
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn an_undecodable_frame_is_refused_as_malformed() {
    let fixture = fixture();
    let runtime = start(&fixture.paths).await.unwrap();
    let answer = exchange(
        &fixture.paths,
        &[
            json!({"type": "handshake_start", "body": {"protocol": 1}}),
            json!({"type": "subscribe", "body": {}}),
        ],
    )
    .await;
    assert_eq!(
        answer,
        ServerFrame::Refused {
            operation: None,
            reason: Refusal::MalformedFrame
        }
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn connecting_without_a_runtime_is_unreachable_with_a_hint() {
    let fixture = fixture();
    let result = Client::connect(&fixture.paths).await;
    assert!(
        matches!(result, Err(ref error @ ClientError::Unreachable { .. }) if crate::diagnostic::Diagnostic::hint(error).is_some())
    );
}

use crate::protocol::ProtocolVersion;

/// Twins of every VM in the journal start with the runtime, not on first request: an
/// interrupted start is noticed (and recorded as failed) without anyone asking.
#[tokio::test]
#[allow(
    clippy::await_holding_lock,
    reason = "drops and retakes daemon.lock: no fork may pin it meanwhile; nothing spawns while \
              the guard is held"
)]
async fn a_new_runtime_starts_every_known_twin_which_recovers_on_its_own() {
    use crate::vm::builder::BuildCommand;
    use crate::vm::local_qemu::Placement;
    let fixture = fixture();
    let slow_build = root::VmDefaults {
        build: BuildCommand::Scripted(crate::plan::argv::Argv::for_tests(&["sleep", "3"])),
        placement: Placement::Direct,
        process: super::timing::ProcessTiming::quick(),
    };
    let first = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        slow_build.clone(),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let name = crate::domain::vm_name::VmName::parse("interrupted".into()).unwrap();
    let request = RequestBody::Vm(crate::protocol::VmRequest::Start {
        name,
        shares: None,
        nix_config: None,
    });
    // The start is building when the runtime goes away.
    let pending = tokio::spawn(async move { client.request(request).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // No fork may pin daemon.lock between this stop and the next start.
    let forks = crate::testing::forks();
    first.stop();
    first.wait().await;
    drop(pending);

    let second = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        slow_build,
    )
    .await
    .unwrap();
    drop(forks);
    // Asked at once: requests wait for the registry, so the VM is already listed.
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let reply = client
        .request(RequestBody::Vm(crate::protocol::VmRequest::Status {
            name: None,
        }))
        .await
        .unwrap();
    let listed = matches!(&reply, ReplyBody::Vm(crate::protocol::VmReply::Status { vms })
        if vms.iter().any(|vm| vm.name.as_str() == "interrupted"));
    assert!(listed, "{reply:?}");
    // No request names the VM; give its twin a moment to recover by itself.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    second.stop();
    second.wait().await;

    let (journal, _) = ractor::Actor::spawn(
        None,
        crate::journal::Journal,
        crate::journal::JournalArgs {
            store: crate::journal::JournalStore::Sqlite(fixture.paths.journal()),
            clock: crate::clock::Clock::System,
        },
    )
    .await
    .unwrap();
    let (reader, mut replies) = crate::testing::probe::<crate::journal::JournalReply>().await;
    journal
        .cast(crate::journal::JournalMessage::Replay {
            id: crate::journal::PersistenceId::parse("vm/interrupted".into()).unwrap(),
            reply_to: reader.get_derived(),
        })
        .unwrap();
    let crate::journal::JournalReply::Replayed { replay, .. } =
        crate::testing::next(&mut replies).await
    else {
        return assert_eq!("a replay", "something else");
    };
    let last = replay
        .events
        .last()
        .map(|event| event.payload.as_str().to_owned());
    assert!(
        last.as_deref()
            .is_some_and(|payload| payload.contains("vm.restarted_while_starting")),
        "the twin didn't recover on its own: {last:?}"
    );
}

#[tokio::test]
async fn status_without_vms_is_an_empty_list_and_stopping_an_unknown_vm_is_refused() {
    use crate::protocol::{VmReply, VmRequest};
    let fixture = fixture();
    let runtime = start(&fixture.paths).await.unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let reply = client
        .request(RequestBody::Vm(VmRequest::Status { name: None }))
        .await
        .unwrap();
    assert_eq!(reply, ReplyBody::Vm(VmReply::Status { vms: Vec::new() }));
    let name = crate::domain::vm_name::VmName::parse("ghost".into()).unwrap();
    let reply = client
        .request(RequestBody::Vm(VmRequest::Stop { name }))
        .await
        .unwrap();
    assert!(matches!(reply, ReplyBody::Failed(report) if report.code == "runtime.vm.unknown"));
    runtime.stop();
    runtime.wait().await;
}

/// Runs `messages` against the fixture's SQLite journal outside any runtime and returns the
/// replies, one per message.
async fn journal_exchange(
    paths: &RuntimePaths,
    messages: Vec<
        Box<
            dyn FnOnce(
                ractor::DerivedActorRef<crate::journal::JournalReply>,
            ) -> crate::journal::JournalMessage,
        >,
    >,
) -> Vec<crate::journal::JournalReply> {
    let (journal, handle) = ractor::Actor::spawn(
        None,
        crate::journal::Journal,
        crate::journal::JournalArgs {
            store: crate::journal::JournalStore::Sqlite(paths.journal()),
            clock: crate::clock::Clock::System,
        },
    )
    .await
    .unwrap();
    let (reader, mut replies) = crate::testing::probe::<crate::journal::JournalReply>().await;
    let mut answers = Vec::new();
    for message in messages {
        journal.cast(message(reader.get_derived())).unwrap();
        answers.push(crate::testing::next(&mut replies).await);
    }
    journal.stop(None);
    handle.await.unwrap();
    answers
}

fn persistence_id(id: &str) -> crate::journal::PersistenceId {
    crate::journal::PersistenceId::parse(id.into()).unwrap()
}

/// A `vm/<name>` stream this binary can't replay (an unknown event version).
async fn plant_unreplayable(paths: &RuntimePaths, name: &str) {
    std::fs::create_dir_all(paths.data().as_path()).unwrap();
    let id = persistence_id(&format!("vm/{name}"));
    let event =
        crate::journal::EventPayload::encode(&json!({"version": 999, "event": {}})).unwrap();
    let answers = journal_exchange(
        paths,
        vec![Box::new(move |reply_to| {
            crate::journal::JournalMessage::Persist {
                batch: crate::journal::BatchId::FIRST,
                id,
                first: crate::journal::Sequence::FIRST,
                events: crate::nonempty::NonEmpty::new(event, Vec::new()),
                reply_to,
            }
        })],
    )
    .await;
    assert!(matches!(
        answers.as_slice(),
        [crate::journal::JournalReply::Persisted { .. }]
    ));
}

async fn status_of(client: &mut Client, name: &str) -> Option<crate::vm::VmCondition> {
    use crate::protocol::{VmReply, VmRequest};
    let reply = client
        .request(RequestBody::Vm(VmRequest::Status { name: None }))
        .await
        .unwrap();
    let ReplyBody::Vm(VmReply::Status { vms }) = reply else {
        return None;
    };
    vms.into_iter()
        .find(|vm| vm.name.as_str() == name)
        .map(|vm| vm.condition)
}

/// Asks status once, while the twin may still be replaying: a VM that can't be replayed is
/// answered quarantined, never unknown. Then waits (up to 5 s) for its quarantine record to be
/// durable, read through a second journal connection, so a test may stop the runtime.
async fn quarantined(
    client: &mut Client,
    paths: &RuntimePaths,
    name: &str,
) -> Option<super::quarantine::QuarantineCause> {
    let Some(crate::vm::VmCondition::Quarantined { cause }) = status_of(client, name).await else {
        return None;
    };
    let id = format!("quarantine/{name}");
    for _ in 0..50 {
        let id = persistence_id(&id);
        let answers = journal_exchange(
            paths,
            vec![Box::new(move |reply_to| {
                crate::journal::JournalMessage::Replay { id, reply_to }
            })],
        )
        .await;
        if let Some(crate::journal::JournalReply::Replayed { replay, .. }) = answers.first()
            && !replay.events.is_empty()
        {
            return Some(cause);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test]
async fn an_unreplayable_vm_is_quarantined_recorded_once_and_refused() {
    use crate::protocol::VmRequest;
    let fixture = fixture();
    plant_unreplayable(&fixture.paths, "broken").await;
    let runtime = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        root::VmDefaults::production(),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let cause = quarantined(&mut client, &fixture.paths, "broken")
        .await
        .unwrap();
    assert!(
        matches!(&cause, super::quarantine::QuarantineCause::Unreplayable { reason }
            if reason.code == "persistent.unrecoverable.unknown_event_version"),
        "{cause:?}"
    );
    let name = crate::domain::vm_name::VmName::parse("broken".into()).unwrap();
    for request in [
        VmRequest::Start {
            name: name.clone(),
            shares: None,
            nix_config: None,
        },
        VmRequest::Stop { name: name.clone() },
    ] {
        let reply = client.request(RequestBody::Vm(request)).await.unwrap();
        assert!(
            matches!(&reply, ReplyBody::Failed(report) if report.code == "runtime.vm.quarantined"),
            "{reply:?}"
        );
    }
    runtime.stop();
    runtime.wait().await;

    let answers = journal_exchange(
        &fixture.paths,
        vec![Box::new(|reply_to| {
            crate::journal::JournalMessage::Replay {
                id: persistence_id("quarantine/broken"),
                reply_to,
            }
        })],
    )
    .await;
    let [crate::journal::JournalReply::Replayed { replay, .. }] = answers.as_slice() else {
        return assert_eq!("a replay", "something else");
    };
    assert_eq!(replay.events.len(), 1);
    assert_eq!(super::quarantine::cause(&replay.events), Some(cause));
}

#[tokio::test]
#[allow(
    clippy::await_holding_lock,
    reason = "drops and retakes daemon.lock: no fork may pin it meanwhile; these runtimes spawn \
              nothing, so the guard can't deadlock"
)]
async fn a_recorded_quarantine_is_known_from_the_start_and_recorded_once() {
    let _forks = crate::testing::forks();
    let fixture = fixture();
    plant_unreplayable(&fixture.paths, "broken").await;
    let first = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        root::VmDefaults::production(),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    quarantined(&mut client, &fixture.paths, "broken")
        .await
        .unwrap();
    first.stop();
    first.wait().await;
    let second = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        root::VmDefaults::production(),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    // Asked at once: the registry is loaded with the record, before any twin could report.
    assert!(matches!(
        status_of(&mut client, "broken").await,
        Some(crate::vm::VmCondition::Quarantined { .. })
    ));
    second.stop();
    second.wait().await;
    let answers = journal_exchange(
        &fixture.paths,
        vec![Box::new(|reply_to| {
            crate::journal::JournalMessage::Replay {
                id: persistence_id("quarantine/broken"),
                reply_to,
            }
        })],
    )
    .await;
    let [crate::journal::JournalReply::Replayed { replay, .. }] = answers.as_slice() else {
        return assert_eq!("a replay", "something else");
    };
    assert_eq!(replay.events.len(), 1);
}

fn vm_name(name: &str) -> crate::domain::vm_name::VmName {
    crate::domain::vm_name::VmName::parse(name.into()).unwrap()
}

async fn vacuum(client: &mut Client, name: &str) -> ReplyBody {
    client
        .request(RequestBody::Vm(crate::protocol::VmRequest::Vacuum {
            name: vm_name(name),
        }))
        .await
        .unwrap()
}

async fn replay(paths: &RuntimePaths, id: &'static str) -> crate::journal::Replay {
    let answers = journal_exchange(
        paths,
        vec![Box::new(|reply_to| {
            crate::journal::JournalMessage::Replay {
                id: persistence_id(id),
                reply_to,
            }
        })],
    )
    .await;
    match answers.into_iter().next() {
        Some(crate::journal::JournalReply::Replayed { replay, .. }) => replay,
        _ => crate::journal::Replay::empty(),
    }
}

#[tokio::test]
async fn vacuuming_a_quarantined_vm_ends_its_processes_removes_its_files_and_retires_it() {
    use crate::adapters::process::{self, Containment, Output, SpawnRequest};
    let fixture = fixture();
    plant_unreplayable(&fixture.paths, "broken").await;
    let vm = fixture.paths.vm(&vm_name("broken")).unwrap();
    std::fs::create_dir_all(vm.guest().as_path()).unwrap();
    // A directory of a VM the runtime doesn't know (the Python launcher's, say): untouched.
    let stranger = fixture.paths.data().as_path().join("stranger");
    std::fs::create_dir_all(&stranger).unwrap();
    let runtime = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        root::VmDefaults::production(),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    quarantined(&mut client, &fixture.paths, "broken")
        .await
        .unwrap();
    let mut leftover = process::spawn(&SpawnRequest {
        argv: crate::plan::argv::Argv::for_tests(&["sleep", "60"]),
        containment: Containment::Direct,
        nonce: Some(crate::domain::process::Nonce::of(
            vm.name(),
            crate::domain::lifecycle::Incarnation::mint(),
            crate::domain::lifecycle::Generation::FIRST,
            &crate::domain::process::ProcessRole::Local(crate::domain::process::LocalRole::Qemu),
        )),
        directory: AbsolutePath::parse("/".into()).unwrap(),
        output: Output::Discard,
    })
    .unwrap();

    let reply = vacuum(&mut client, "broken").await;
    assert!(
        matches!(&reply, ReplyBody::Vm(crate::protocol::VmReply::Vacuumed { name }) if name.as_str() == "broken"),
        "{reply:?}"
    );
    assert!(leftover.child.try_wait().unwrap().is_some());
    assert!(!vm.state().as_path().exists());
    assert!(stranger.exists());
    assert_eq!(status_of(&mut client, "broken").await, None);
    let reply = vacuum(&mut client, "stranger").await;
    assert!(matches!(reply, ReplyBody::Failed(report) if report.code == "runtime.vm.unknown"));
    runtime.stop();
    runtime.wait().await;

    // Retired, not deleted: nothing live, positions kept.
    let vm_stream = replay(&fixture.paths, "vm/broken").await;
    assert_eq!(
        (
            vm_stream.events.len(),
            vm_stream.last.map(crate::journal::Sequence::get)
        ),
        (0, Some(1))
    );
    let record = replay(&fixture.paths, "quarantine/broken").await;
    assert_eq!(
        (
            record.events.len(),
            record.last.map(crate::journal::Sequence::get)
        ),
        (0, Some(1))
    );
}

#[tokio::test]
#[allow(
    clippy::await_holding_lock,
    reason = "drops and retakes daemon.lock: no fork may pin it meanwhile; nothing spawns while \
              the guard is held"
)]
async fn a_vacuumed_name_starts_as_a_new_vm_after_the_tombstone() {
    use crate::protocol::{VmReply, VmRequest};
    use crate::vm::builder::BuildCommand;
    use crate::vm::local_qemu::Placement;
    let fixture = fixture();
    let defaults = |seconds: &'static str| root::VmDefaults {
        build: BuildCommand::Scripted(crate::plan::argv::Argv::for_tests(&["sleep", seconds])),
        placement: Placement::Direct,
        process: super::timing::ProcessTiming::quick(),
    };
    let runtime = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        defaults("3"),
    )
    .await
    .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let start = RequestBody::Vm(VmRequest::Start {
        name: vm_name("reused"),
        shares: None,
        nix_config: None,
    });
    // Vacuumed while building: the stop cancels the start.
    let mut starter = Client::connect(&fixture.paths).await.unwrap();
    let first = start.clone();
    let pending = tokio::spawn(async move { starter.request(first).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let reply = vacuum(&mut client, "reused").await;
    assert!(
        matches!(reply, ReplyBody::Vm(VmReply::Vacuumed { .. })),
        "{reply:?}"
    );
    let cancelled = pending.await.unwrap().unwrap();
    assert!(
        matches!(&cancelled, ReplyBody::Failed(report) if report.code == "vm.cancelled"),
        "{cancelled:?}"
    );
    // No fork may pin daemon.lock between this stop and the next start.
    let forks = crate::testing::forks();
    runtime.stop();
    runtime.wait().await;
    let retired = replay(&fixture.paths, "vm/reused").await;
    assert_eq!(retired.events, Vec::new());
    let tombstone = retired.last.unwrap().get();

    let runtime = start_with(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        defaults("3"),
    )
    .await
    .unwrap();
    drop(forks);
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    assert_eq!(status_of(&mut client, "reused").await, None);
    let mut starter = Client::connect(&fixture.paths).await.unwrap();
    let pending = tokio::spawn(async move { starter.request(start).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    runtime.stop();
    runtime.wait().await;
    drop(pending);
    let reused = replay(&fixture.paths, "vm/reused").await;
    let first = reused.events.first().unwrap();
    assert_eq!(first.sequence.get(), tombstone + 1);
    assert!(first.payload.as_str().contains("\"created\""), "{first:?}");
}

#[tokio::test]
async fn a_twin_out_of_restarts_fails_its_waiters_is_quarantined_and_can_be_released() {
    use super::quarantine::QuarantineCause;
    use crate::protocol::{VmReply, VmRequest};
    use crate::vm::VmCondition;
    use crate::vm::builder::BuildCommand;
    use crate::vm::local_qemu::Placement;
    let fixture = fixture();
    // No restarts allowed: the first failure gives the VM up.
    let defaults = root::VmDefaults {
        build: BuildCommand::Scripted(crate::plan::argv::Argv::for_tests(&["sleep", "3"])),
        placement: Placement::Direct,
        process: super::timing::ProcessTiming::quick(),
    };
    let mut daemon = DaemonConfig::default();
    daemon.restart.max_restarts = 0;
    let runtime = start_configured(
        &fixture.paths,
        Persistence::Sqlite(JournalLocation::Configured),
        defaults,
        daemon,
    )
    .await
    .unwrap();
    let mut starter = Client::connect(&fixture.paths).await.unwrap();
    let start = RequestBody::Vm(VmRequest::Start {
        name: vm_name("halting"),
        shares: None,
        nix_config: None,
    });
    let pending = tokio::spawn(async move { starter.request(start).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // The twin's builder ends mid-build: the twin escalates, and has no restart left.
    let twin = ractor::registry::where_is(format!("{}/vm/halting", runtime.id())).unwrap();
    ractor::registry::where_is(format!(
        "{}/vm/halting/builder@{}",
        runtime.id(),
        twin.get_id()
    ))
    .unwrap()
    .kill();
    let failed = pending.await.unwrap().unwrap();
    assert!(
        matches!(&failed, ReplyBody::Failed(report) if report.code == "runtime.vm.twin_failed"),
        "{failed:?}"
    );
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let cause = quarantined(&mut client, &fixture.paths, "halting")
        .await
        .unwrap();
    assert!(
        matches!(&cause, QuarantineCause::SuspiciousHalt { restarts: 0, failures }
            if failures.len() == 1 && failures[0].code == "persistent.fatal.child_ended"),
        "{cause:?}"
    );
    let refused = client
        .request(RequestBody::Vm(VmRequest::Stop {
            name: vm_name("halting"),
        }))
        .await
        .unwrap();
    assert!(
        matches!(&refused, ReplyBody::Failed(report)
            if report.code == "runtime.vm.quarantined"
                && report.hint.as_deref().is_some_and(|hint| hint.contains("release_vm"))),
        "{refused:?}"
    );

    let released = client
        .request(RequestBody::Vm(VmRequest::Release {
            name: vm_name("halting"),
        }))
        .await
        .unwrap();
    assert!(
        matches!(released, ReplyBody::Vm(VmReply::Released { .. })),
        "{released:?}"
    );
    // A fresh twin replays the interrupted start as failed; the VM is addressable again.
    let condition = status_of(&mut client, "halting").await;
    assert!(
        matches!(&condition, Some(VmCondition::Failed { reason, .. })
            if reason.code == "vm.restarted_while_starting"),
        "{condition:?}"
    );
    let again = client
        .request(RequestBody::Vm(VmRequest::Release {
            name: vm_name("halting"),
        }))
        .await
        .unwrap();
    assert!(
        matches!(&again, ReplyBody::Failed(report) if report.code == "runtime.vm.not_quarantined"),
        "{again:?}"
    );
    runtime.stop();
    runtime.wait().await;
    let record = replay(&fixture.paths, "quarantine/halting").await;
    assert_eq!(
        (
            record.events.len(),
            record.last.map(crate::journal::Sequence::get)
        ),
        (0, Some(1))
    );
}

#[test]
fn the_journal_is_the_builders_else_the_configured_else_the_default_and_never_in_a_vm() {
    let fixture = fixture();
    let data = fixture.paths.data().clone();
    let elsewhere = AbsolutePath::parse("/srv/journal.sqlite".into()).unwrap();
    let configured = AbsolutePath::parse("/var/lib/journal.sqlite".into()).unwrap();
    let store = |persistence, configured: Option<&AbsolutePath>| {
        super::journal_store(&fixture.paths, persistence, configured)
    };
    assert!(matches!(
        store(Persistence::Sqlite(JournalLocation::At(elsewhere.clone())), Some(&configured)),
        Ok(crate::journal::JournalStore::Sqlite(path)) if path == elsewhere
    ));
    assert!(matches!(
        store(Persistence::Sqlite(JournalLocation::Configured), Some(&configured)),
        Ok(crate::journal::JournalStore::Sqlite(path)) if path == configured
    ));
    assert!(matches!(
        store(Persistence::Sqlite(JournalLocation::Configured), None),
        Ok(crate::journal::JournalStore::Sqlite(path)) if path == fixture.paths.journal()
    ));
    let inside_vm = data.join("work").join("journal.sqlite");
    assert!(matches!(
        store(Persistence::Sqlite(JournalLocation::At(inside_vm)), None),
        Err(StartError::JournalInsideVm(_))
    ));
}

#[tokio::test]
async fn an_invalid_daemon_config_stops_the_runtime_from_starting_with_every_problem() {
    let fixture = fixture();
    let config = config_paths(&fixture.paths);
    std::fs::create_dir_all(config.directory().as_path()).unwrap();
    std::fs::write(
        config.daemon_config().as_path(),
        "linger = \"yes\"\n[restart]\nmax_restarts = 1000\n",
    )
    .unwrap();
    let started = Runtime::start(RuntimeConfig {
        paths: fixture.paths.clone(),
        config,
        hosting: Hosting::InProcess,
        persistence: Persistence::Memory,
        daemon: DaemonConfigSource::File,
        version: "test".into(),
        account: crate::vm::Account {
            user: crate::domain::account::UserName::parse("alice".into()).unwrap(),
            user_id: crate::domain::account::UserId::new(1000),
            group_id: crate::domain::account::GroupId::new(100),
            home: crate::domain::account::GuestHome::parse("/home/alice".into()).unwrap(),
        },
        vm_defaults: root::VmDefaults::production(),
        clock: crate::clock::Clock::System,
    })
    .await;
    let Err(StartError::Config(crate::config::LoadError::Invalid(errors))) = started else {
        return assert_eq!("an invalid configuration", "something else");
    };
    let keys: Vec<&str> = errors
        .problems
        .iter()
        .map(|problem| problem.key.as_str())
        .collect();
    assert_eq!(keys, ["linger", "restart.max_restarts"]);
}

#[tokio::test]
async fn an_invalid_run_config_fails_the_start_with_every_problem() {
    use crate::protocol::VmRequest;
    let fixture = fixture();
    let config = config_paths(&fixture.paths);
    std::fs::create_dir_all(config.directory().as_path()).unwrap();
    std::fs::write(
        config.run_config().as_path(),
        "[ssh]\nport = -1\n[guest]\nshell = \"zsh\"\n",
    )
    .unwrap();
    let runtime = start(&fixture.paths).await.unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let reply = client
        .request(RequestBody::Vm(VmRequest::Start {
            name: vm_name("configured"),
            shares: None,
            nix_config: None,
        }))
        .await
        .unwrap();
    assert!(
        matches!(&reply, ReplyBody::Failed(report)
            if report.code == "config.run.invalid"
                && report.message.contains("guest.shell")
                && report.message.contains("ssh.port")),
        "{reply:?}"
    );
    runtime.stop();
    runtime.wait().await;
}
