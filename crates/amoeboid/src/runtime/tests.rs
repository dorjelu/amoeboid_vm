#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use serde_json::json;
use tokio::net::UnixStream;

use super::*;
use crate::client::{Client, ClientError};
use crate::protocol::codec;
use crate::protocol::{Refusal, ReplyBody, RequestBody, RuntimeReply, RuntimeRequest, ServerFrame};
use paths::AbsoluteDirectory;

struct Fixture {
    _temporary: tempfile::TempDir,
    paths: RuntimePaths,
}

fn fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let data = AbsoluteDirectory::parse(temporary.path().join("data")).unwrap();
    let run = AbsoluteDirectory::parse(temporary.path().join("run")).unwrap();
    let paths = RuntimePaths::under(data, run).unwrap();
    Fixture {
        _temporary: temporary,
        paths,
    }
}

async fn start(paths: &RuntimePaths) -> Result<Runtime, StartError> {
    Runtime::start(RuntimeConfig {
        paths: paths.clone(),
        hosting: Hosting::InProcess,
        persistence: Persistence::Memory,
        version: "test".into(),
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
async fn a_stopped_runtime_releases_the_lock_and_socket_for_the_next() {
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
