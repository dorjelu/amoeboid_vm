#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use serde_json::json;
use tokio::net::UnixStream;

use super::*;
use crate::client::{Client, ClientError};
use crate::domain::path::AbsolutePath;
use crate::protocol::codec;
use crate::protocol::{Refusal, ReplyBody, RequestBody, RuntimeReply, RuntimeRequest, ServerFrame};

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
    Runtime::start(RuntimeConfig {
        paths: paths.clone(),
        hosting: Hosting::InProcess,
        persistence,
        version: "test".into(),
        account: crate::vm::Account {
            user: crate::domain::account::UserName::parse("alice".into()).unwrap(),
            user_id: crate::domain::account::UserId::new(1000),
            group_id: crate::domain::account::GroupId::new(100),
            home: crate::domain::account::GuestHome::parse("/home/alice".into()).unwrap(),
        },
        vm_defaults,
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
async fn a_new_runtime_starts_every_known_twin_which_recovers_on_its_own() {
    use crate::vm::builder::BuildCommand;
    use crate::vm::local_qemu::Placement;
    let fixture = fixture();
    let slow_build = root::VmDefaults {
        build: BuildCommand::Scripted(crate::plan::argv::Argv::for_tests(&["sleep", "3"])),
        placement: Placement::Direct,
    };
    let first = start_with(&fixture.paths, Persistence::Sqlite, slow_build.clone())
        .await
        .unwrap();
    let mut client = Client::connect(&fixture.paths).await.unwrap();
    let name = crate::domain::vm_name::VmName::parse("interrupted".into()).unwrap();
    let request = RequestBody::Vm(crate::protocol::VmRequest::Start {
        name,
        shares: Vec::new(),
        nix_config: None,
    });
    // The start is building when the runtime goes away.
    let pending = tokio::spawn(async move { client.request(request).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    first.stop();
    first.wait().await;
    drop(pending);

    let second = start_with(&fixture.paths, Persistence::Sqlite, slow_build)
        .await
        .unwrap();
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
            persistence: Persistence::Sqlite,
            paths: fixture.paths.clone(),
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
