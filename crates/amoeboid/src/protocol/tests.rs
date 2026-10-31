use proptest::prelude::*;
use serde_json::json;

use super::codec::{self, FrameError, MAX_FRAME};
use super::*;

fn operations() -> impl Strategy<Value = OperationId> {
    any::<u128>().prop_map(|bits| {
        let id = uuid::Builder::from_random_bytes(bits.to_le_bytes()).into_uuid();
        // Re-stamp as v7: only the version and variant bits differ.
        let mut bytes = *id.as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        OperationId::try_from(Uuid::from_bytes(bytes)).unwrap()
    })
}

fn client_frames() -> impl Strategy<Value = ClientFrame> {
    prop_oneof![
        any::<u32>().prop_map(|version| ClientFrame::HandshakeStart {
            protocol: ProtocolVersion(version)
        }),
        operations().prop_map(|operation| ClientFrame::Request {
            operation,
            body: RequestBody::Runtime(RuntimeRequest::Ping)
        }),
    ]
}

fn server_frames() -> impl Strategy<Value = ServerFrame> {
    let refusals = prop_oneof![
        Just(Refusal::HandshakeRequired),
        Just(Refusal::HandshakeRepeated),
        Just(Refusal::MalformedFrame),
        any::<u32>().prop_map(|max| Refusal::FrameTooLarge { max }),
    ];
    prop_oneof![
        (any::<u32>(), operations()).prop_map(|(version, operation)| {
            ServerFrame::HandshakeAccepted {
                protocol: ProtocolVersion(version),
                runtime: RuntimeId(Uuid::from(operation)),
            }
        }),
        (any::<u32>(), any::<u32>()).prop_map(|(runtime, client)| ServerFrame::HandshakeRejected {
            runtime: ProtocolVersion(runtime),
            client: ProtocolVersion(client),
        }),
        operations().prop_map(|operation| ServerFrame::Reply {
            operation,
            body: ReplyBody::Runtime(RuntimeReply::Pong)
        }),
        (proptest::option::of(operations()), refusals)
            .prop_map(|(operation, reason)| ServerFrame::Refused { operation, reason }),
    ]
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

fn decode<T: serde::de::DeserializeOwned>(value: &serde_json::Value) -> Result<T, FrameError> {
    let bytes = codec::encode(value).unwrap();
    block_on(codec::read::<T, _>(&mut bytes.as_slice())).map(Option::unwrap)
}

proptest! {
    #[test]
    fn client_frames_survive_encode_then_read(frame in client_frames()) {
        let bytes = codec::encode(&frame).unwrap();
        let back: ClientFrame = block_on(codec::read(&mut bytes.as_slice())).unwrap().unwrap();
        prop_assert_eq!(back, frame);
    }

    #[test]
    fn server_frames_survive_encode_then_read(frame in server_frames()) {
        let bytes = codec::encode(&frame).unwrap();
        let back: ServerFrame = block_on(codec::read(&mut bytes.as_slice())).unwrap().unwrap();
        prop_assert_eq!(back, frame);
    }

    #[test]
    fn eof_inside_a_frame_is_an_error_not_end_of_stream(frame in client_frames(), cut in 1_usize..64) {
        let bytes = codec::encode(&frame).unwrap();
        let cut = cut.min(bytes.len() - 1);
        let result = block_on(codec::read::<ClientFrame, _>(&mut &bytes[..cut]));
        prop_assert!(matches!(result, Err(FrameError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof));
    }
}

#[test]
fn a_request_has_the_documented_wire_shape() {
    let operation = OperationId::mint();
    let frame = ClientFrame::Request {
        operation,
        body: RequestBody::Runtime(RuntimeRequest::Ping),
    };
    assert_eq!(
        serde_json::to_value(&frame).unwrap(),
        json!({"type": "request", "body": {"operation": operation.to_string(),
               "body": {"domain": "runtime", "request": "ping"}}})
    );
}

#[test]
fn eof_before_a_frame_is_end_of_stream() {
    let result = block_on(codec::read::<ClientFrame, _>(&mut [].as_slice())).unwrap();
    assert!(result.is_none());
}

#[test]
fn an_oversized_length_is_refused_before_the_payload_is_read() {
    let mut bytes = (MAX_FRAME + 1).to_be_bytes().to_vec();
    bytes.extend_from_slice(b"{}");
    let result = block_on(codec::read::<ClientFrame, _>(&mut bytes.as_slice()));
    assert!(
        matches!(result, Err(FrameError::TooLarge { length }) if length == u64::from(MAX_FRAME) + 1)
    );
}

#[test]
fn an_oversized_frame_is_refused_on_encode() {
    let big = "x".repeat(usize::try_from(MAX_FRAME).unwrap());
    assert!(matches!(
        codec::encode(&big),
        Err(FrameError::TooLarge { .. })
    ));
}

#[test]
fn an_unknown_frame_type_is_refused() {
    let result = decode::<ClientFrame>(&json!({"type": "subscribe", "body": {}}));
    assert!(matches!(result, Err(FrameError::Decode(_))));
}

#[test]
fn an_unknown_field_is_refused() {
    let result = decode::<ClientFrame>(
        &json!({"type": "handshake_start", "body": {"protocol": 1, "extra": true}}),
    );
    assert!(matches!(result, Err(FrameError::Decode(_))));
}

#[test]
fn an_op_id_that_is_not_v7_is_refused() {
    let v4 = "8c5b6f0e-2f5a-4b8e-9c3d-1a2b3c4d5e6f";
    let result = decode::<ClientFrame>(&json!({"type": "request", "body": {"operation": v4,
        "body": {"domain": "runtime", "request": "ping"}}}));
    assert!(matches!(result, Err(FrameError::Decode(_))));
}
