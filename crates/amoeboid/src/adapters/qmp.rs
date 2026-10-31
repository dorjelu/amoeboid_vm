//! QMP, qemu's control protocol, on the VM's `control.sock`: just enough to ask the guest to
//! power off (an ACPI power button press). Lines of JSON; asynchronous `event` lines may arrive
//! between a command and its answer and are skipped.

use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

use crate::domain::path::AbsolutePath;

/// Each step (greeting, each answer) must arrive within this.
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// A line longer than this is not QMP.
const MAX_LINE: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum QmpError {
    #[error("can't connect to qemu's control socket {socket}: {source}")]
    Connect {
        socket: String,
        source: std::io::Error,
    },
    #[error("can't talk to qemu: {0}")]
    Io(std::io::Error),
    #[error("qemu didn't answer within {STEP_TIMEOUT:?}")]
    Timeout,
    #[error("qemu answered something unexpected: {0}")]
    Protocol(String),
}

/// Asks the guest to power off. Returns once qemu accepted the request; the machine stops later.
///
/// # Errors
///
/// See [`QmpError`].
pub async fn powerdown(socket: &AbsolutePath) -> Result<(), QmpError> {
    let stream = UnixStream::connect(socket.as_path())
        .await
        .map_err(|source| QmpError::Connect {
            socket: socket.display().to_string(),
            source,
        })?;
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read);
    let greeting = next_message(&mut lines).await?;
    if greeting.get("QMP").is_none() {
        return Err(QmpError::Protocol(greeting.to_string()));
    }
    for command in ["qmp_capabilities", "system_powerdown"] {
        let mut line = json!({ "execute": command }).to_string();
        line.push('\n');
        write
            .write_all(line.as_bytes())
            .await
            .map_err(QmpError::Io)?;
        let answer = next_message(&mut lines).await?;
        if answer.get("return").is_none() {
            return Err(QmpError::Protocol(answer.to_string()));
        }
    }
    Ok(())
}

/// The next message that is not an asynchronous event.
async fn next_message(
    lines: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Value, QmpError> {
    loop {
        let mut line = String::new();
        let read = tokio::time::timeout(STEP_TIMEOUT, lines.read_line(&mut line))
            .await
            .map_err(|_| QmpError::Timeout)?
            .map_err(QmpError::Io)?;
        if read == 0 {
            return Err(QmpError::Protocol("connection closed".into()));
        }
        if line.len() > MAX_LINE {
            return Err(QmpError::Protocol("line too long".into()));
        }
        let message: Value =
            serde_json::from_str(&line).map_err(|error| QmpError::Protocol(error.to_string()))?;
        if message.get("event").is_none() {
            return Ok(message);
        }
    }
}

impl crate::diagnostic::Diagnostic for QmpError {
    fn code(&self) -> &'static str {
        match self {
            Self::Connect { .. } => "qmp.connect",
            Self::Io(_) => "qmp.io",
            Self::Timeout => "qmp.timeout",
            Self::Protocol(_) => "qmp.protocol",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Connect { .. } | Self::Io(_) | Self::Timeout | Self::Protocol(_) => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use tokio::net::UnixListener;

    use super::*;

    /// A fake qemu control socket: greets, answers each command with `return`, sends an event
    /// before each answer, and reports the commands it received.
    pub(crate) fn fake_qmp(
        socket: &std::path::Path,
    ) -> tokio::sync::mpsc::UnboundedReceiver<String> {
        let listener = UnixListener::bind(socket).unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let _ = write.write_all(b"{\"QMP\": {\"version\": {}}}\n").await;
            while let Ok(Some(line)) = lines.next_line().await {
                let command: Value = serde_json::from_str(&line).unwrap_or_default();
                let name = command["execute"].as_str().unwrap_or_default().to_owned();
                let _ = write
                    .write_all(b"{\"event\": \"RESUME\"}\n{\"return\": {}}\n")
                    .await;
                let _ = sender.send(name);
            }
        });
        receiver
    }

    #[tokio::test]
    async fn powerdown_negotiates_then_asks_skipping_events() {
        let temporary = tempfile::tempdir().unwrap();
        let socket = temporary.path().join("control.sock");
        let mut commands = fake_qmp(&socket);
        powerdown(&AbsolutePath::parse(socket).unwrap())
            .await
            .unwrap();
        assert_eq!(commands.recv().await.as_deref(), Some("qmp_capabilities"));
        assert_eq!(commands.recv().await.as_deref(), Some("system_powerdown"));
    }

    #[tokio::test]
    async fn no_socket_is_a_connect_error() {
        let temporary = tempfile::tempdir().unwrap();
        let socket = AbsolutePath::parse(temporary.path().join("control.sock")).unwrap();
        assert!(matches!(
            powerdown(&socket).await,
            Err(QmpError::Connect { .. })
        ));
    }
}
