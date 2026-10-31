//! Framing: 4-byte big-endian length prefix, JSON payload.

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

/// Largest accepted payload, in bytes.
pub const MAX_FRAME: u32 = 1 << 20;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame of {length} bytes exceeds {MAX_FRAME}")]
    TooLarge { length: u64 },
    #[error("can't decode frame: {0}")]
    Decode(serde_json::Error),
    #[error("can't encode frame: {0}")]
    Encode(serde_json::Error),
    #[error("can't read or write frame: {0}")]
    Io(std::io::Error),
}

/// Reads one frame. `Ok(None)` on EOF before the first byte of a frame; EOF inside a frame is
/// `Io(UnexpectedEof)`. An oversized length is refused before its payload is read.
///
/// # Errors
///
/// See [`FrameError`].
pub async fn read<T, R>(reader: &mut R) -> Result<Option<T>, FrameError>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; 4];
    let first = reader.read(&mut header).await.map_err(FrameError::Io)?;
    if first == 0 {
        return Ok(None);
    }
    if let Some(rest) = header.get_mut(first..) {
        reader.read_exact(rest).await.map_err(FrameError::Io)?;
    }
    let length = u32::from_be_bytes(header);
    if length > MAX_FRAME {
        return Err(FrameError::TooLarge {
            length: length.into(),
        });
    }
    let mut payload = vec![0_u8; usize::try_from(length).unwrap_or(usize::MAX)];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(FrameError::Io)?;
    serde_json::from_slice(&payload)
        .map(Some)
        .map_err(FrameError::Decode)
}

/// Encodes one frame, header included.
///
/// # Errors
///
/// `Encode`, or `TooLarge` when the payload exceeds [`MAX_FRAME`].
pub fn encode<T: Serialize>(frame: &T) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(frame).map_err(FrameError::Encode)?;
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|length| *length <= MAX_FRAME)
        .ok_or(FrameError::TooLarge {
            length: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })?;
    let mut bytes = Vec::with_capacity(payload.len() + 4);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Writes one frame and flushes.
///
/// # Errors
///
/// See [`encode`]; `Io` on write failure.
pub async fn write<T, W>(writer: &mut W, frame: &T) -> Result<(), FrameError>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let bytes = encode(frame)?;
    writer.write_all(&bytes).await.map_err(FrameError::Io)?;
    writer.flush().await.map_err(FrameError::Io)
}
