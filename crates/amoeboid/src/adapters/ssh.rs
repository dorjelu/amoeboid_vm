//! The guest's ssh identity as pinned by the host, a free loopback port, and the sshd probe.

use std::time::Duration;

use tokio::io::AsyncReadExt as _;

use super::files::{self, FileError};
use super::process::{self, Exit, Output, ProcessError};
use crate::domain::host_key::{HostPublicKey, HostPublicKeyError};
use crate::domain::port::LoopbackPort;
use crate::plan;
use crate::runtime::paths::VmPaths;

#[derive(Debug, thiserror::Error)]
pub enum SshError {
    #[error(transparent)]
    File(#[from] FileError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error("ssh-keygen failed ({0:?})")]
    Keygen(Exit),
    #[error(transparent)]
    Key(#[from] HostPublicKeyError),
    #[error("can't find a free loopback port: {0}")]
    Port(std::io::Error),
}

/// Generates the guest's host key once per VM (outside the guest-writable directory, so the
/// guest can't swap it) and pins it in the VM's `known_hosts`, rewritten every call.
///
/// # Errors
///
/// See [`SshError`].
pub async fn ensure_host_key(vm: &VmPaths) -> Result<HostPublicKey, SshError> {
    if !vm.host_key().as_path().exists() {
        files::ensure_directory(&vm.ssh_directory(), 0o700)?;
        let argv = plan::argv::ssh_keygen(vm.name(), &vm.host_key());
        match process::run(&argv, &vm.ssh_directory(), &Output::Discard).await? {
            Exit::Code(0) => {}
            other @ (Exit::Code(_) | Exit::Signal(_) | Exit::Unknown) => {
                return Err(SshError::Keygen(other));
            }
        }
    }
    let key = HostPublicKey::parse(&files::read_to_string(&vm.host_public_key())?)?;
    files::write_atomically(
        &vm.known_hosts(),
        plan::files::known_hosts(vm.name(), &key).as_bytes(),
        0o644,
    )?;
    Ok(key)
}

/// A loopback port free at the moment of asking (the kernel's pick; another process may take
/// it before qemu binds it, which then fails visibly).
///
/// # Errors
///
/// `Port`.
pub fn free_port() -> Result<LoopbackPort, SshError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(SshError::Port)?;
    let port = listener.local_addr().map_err(SshError::Port)?.port();
    std::num::NonZeroU16::new(port)
        .map(LoopbackPort::new)
        .ok_or_else(|| SshError::Port(std::io::Error::other("the kernel picked port 0")))
}

/// Whether the guest's sshd answers with its banner. qemu accepts on the forwarded port as
/// soon as it starts, so a connect alone proves nothing; only `SSH-` does.
pub async fn banner(port: LoopbackPort) -> bool {
    const TIMEOUT: Duration = Duration::from_secs(2);
    let probe = async {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port.get()))
            .await
            .ok()?;
        let mut greeting = [0_u8; 4];
        stream.read_exact(&mut greeting).await.ok()?;
        Some(&greeting == b"SSH-")
    };
    matches!(tokio::time::timeout(TIMEOUT, probe).await, Ok(Some(true)))
}

impl crate::diagnostic::Diagnostic for SshError {
    fn code(&self) -> &'static str {
        match self {
            Self::File(inner) => inner.code(),
            Self::Process(inner) => inner.code(),
            Self::Keygen(_) => "ssh.keygen",
            Self::Key(inner) => inner.code(),
            Self::Port(_) => "ssh.port",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::File(inner) => inner.hint(),
            Self::Process(inner) => inner.hint(),
            Self::Keygen(_) => Some("check that OpenSSH's ssh-keygen is installed".into()),
            Self::Key(inner) => inner.hint(),
            Self::Port(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use tokio::io::AsyncWriteExt as _;

    use super::*;
    use crate::domain::path::AbsolutePath;
    use crate::domain::vm_name::VmName;
    use crate::runtime::paths::RuntimePaths;

    #[tokio::test]
    async fn the_host_key_is_generated_once_and_pinned_under_the_vm_alias() {
        if which("ssh-keygen").is_none() {
            return;
        }
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let paths = RuntimePaths::under(root.clone(), root).unwrap();
        let vm = paths.vm(&VmName::parse("work".into()).unwrap()).unwrap();
        let first = ensure_host_key(&vm).await.unwrap();
        let second = ensure_host_key(&vm).await.unwrap();
        assert_eq!(first, second);
        let pinned = files::read_to_string(&vm.known_hosts()).unwrap();
        assert_eq!(
            pinned,
            format!("amoeboid-work ssh-ed25519 {}\n", first.key())
        );
    }

    #[tokio::test]
    async fn the_banner_probe_needs_an_ssh_greeting_not_just_a_connection() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = LoopbackPort::new(
            std::num::NonZeroU16::new(listener.local_addr().unwrap().port()).unwrap(),
        );
        let server = tokio::spawn(async move {
            for greeting in [&b"HTTP/1.1"[..], b"SSH-2.0-OpenSSH"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_all(greeting).await.unwrap();
            }
        });
        assert!(!banner(port).await);
        assert!(banner(port).await);
        server.await.unwrap();
    }

    /// Only that the kernel hands one out: whether it is still free a moment later is the race
    /// `free_port` documents, so a test can't assert it.
    #[test]
    fn a_free_port_is_handed_out() {
        assert!(free_port().is_ok());
    }

    fn which(program: &str) -> Option<std::path::PathBuf> {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|directory| directory.join(program))
            .find(|path| path.is_file())
    }
}
