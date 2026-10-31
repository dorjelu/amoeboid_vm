//! Runtime locations, refined once at the boundary.

#![allow(
    clippy::disallowed_types,
    reason = "boundary: reads raw paths from the environment"
)]

use std::ffi::OsString;
use std::path::PathBuf;

use crate::domain::path::{AbsolutePath, PathError};
use crate::domain::vm_name::VmName;

/// `sun_path` holds 108 bytes including the terminating NUL.
const MAX_SOCKET_PATH: usize = 107;

/// Longest socket name in a VM's runtime directory (`ro-store.sock`); share sockets `dN.sock`
/// stay shorter up to d99999.
const LONGEST_VM_SOCKET: &str = "ro-store.sock";

#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error("environment variable {0} is not set")]
    MissingEnvironment(&'static str),
    #[error("socket path {} is longer than {MAX_SOCKET_PATH} bytes", .0.display())]
    SocketTooLong(AbsolutePath),
}

// TODO: give the runtime's own files (daemon.lock, journal.sqlite) a location separate from
// the VM data directory (decision 0002, open questions).
/// `data` holds `daemon.lock`, the journal and one directory per VM; `run`
/// (`$XDG_RUNTIME_DIR/amoeboid`, `0700`) holds the socket and one runtime directory per VM.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RuntimePaths {
    data: AbsolutePath,
    run: AbsolutePath,
    socket: AbsolutePath,
}

impl RuntimePaths {
    /// # Errors
    ///
    /// `SocketTooLong`.
    pub fn under(data: AbsolutePath, run: AbsolutePath) -> Result<Self, LocationError> {
        let socket = run.join("runtime.sock");
        check_socket(&socket)?;
        Ok(Self { data, run, socket })
    }

    /// `data` = `$XDG_DATA_HOME/amoeboid`, else `$HOME/.local/share/amoeboid`;
    /// `run` = `$XDG_RUNTIME_DIR/amoeboid`.
    ///
    /// # Errors
    ///
    /// See [`LocationError`].
    pub fn from_env(
        environment: impl Fn(&'static str) -> Option<OsString>,
    ) -> Result<Self, LocationError> {
        let data = match environment("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
            Some(xdg) => PathBuf::from(xdg).join("amoeboid"),
            None => {
                PathBuf::from(environment("HOME").ok_or(LocationError::MissingEnvironment("HOME"))?)
                    .join(".local/share/amoeboid")
            }
        };
        let run = PathBuf::from(
            environment("XDG_RUNTIME_DIR")
                .filter(|value| !value.is_empty())
                .ok_or(LocationError::MissingEnvironment("XDG_RUNTIME_DIR"))?,
        )
        .join("amoeboid");
        Self::under(AbsolutePath::parse(data)?, AbsolutePath::parse(run)?)
    }

    #[must_use]
    pub fn data(&self) -> &AbsolutePath {
        &self.data
    }

    #[must_use]
    pub fn run(&self) -> &AbsolutePath {
        &self.run
    }

    #[must_use]
    pub fn lock(&self) -> AbsolutePath {
        self.data.join("daemon.lock")
    }

    #[must_use]
    pub fn journal(&self) -> AbsolutePath {
        self.data.join("journal.sqlite")
    }

    #[must_use]
    pub fn socket(&self) -> &AbsolutePath {
        &self.socket
    }

    /// The paths of VM `name`.
    ///
    /// # Errors
    ///
    /// `SocketTooLong` when the VM's sockets would not fit `sun_path`.
    pub fn vm(&self, name: &VmName) -> Result<VmPaths, LocationError> {
        let runtime = self.run.join(name.as_str());
        check_socket(&runtime.join(LONGEST_VM_SOCKET))?;
        Ok(VmPaths {
            name: name.clone(),
            state: self.data.join(name.as_str()),
            runtime,
        })
    }
}

fn check_socket(socket: &AbsolutePath) -> Result<(), LocationError> {
    if socket.as_os_str().len() > MAX_SOCKET_PATH {
        return Err(LocationError::SocketTooLong(socket.clone()));
    }
    Ok(())
}

/// Everything one VM keeps on the host. Owned by the VM's twin. Names that the guest or Nix
/// side depends on are part of the contract with `nix/base.nix` (DEVELOPING.adoc).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VmPaths {
    name: VmName,
    /// Persistent: survives VM restarts and host reboots.
    state: AbsolutePath,
    /// Ephemeral (tmpfs): sockets and the qemu argument file.
    runtime: AbsolutePath,
}

impl VmPaths {
    #[must_use]
    pub fn name(&self) -> &VmName {
        &self.name
    }

    #[must_use]
    pub fn state(&self) -> &AbsolutePath {
        &self.state
    }

    #[must_use]
    pub fn runtime(&self) -> &AbsolutePath {
        &self.runtime
    }

    /// The `nix build` out-link: the runner and virtiofsd live under it.
    #[must_use]
    pub fn build_link(&self) -> AbsolutePath {
        self.state.join("vm")
    }

    /// Shared into the VM at `/mnt/amoeboid`; the guest can write anything in here.
    #[must_use]
    pub fn guest(&self) -> AbsolutePath {
        self.state.join("guest")
    }

    /// The guest account, rendered every start; the flake's `host` input.
    #[must_use]
    pub fn host_module(&self) -> AbsolutePath {
        self.state.join("host.nix")
    }

    /// This VM's own copy of its NixOS configuration (host-owned); what gets built.
    #[must_use]
    pub fn nix_config(&self) -> AbsolutePath {
        self.state.join("nix")
    }

    #[must_use]
    pub fn ssh_directory(&self) -> AbsolutePath {
        self.state.join("ssh")
    }

    #[must_use]
    pub fn host_key(&self) -> AbsolutePath {
        self.ssh_directory().join("ssh_host_ed25519_key")
    }

    #[must_use]
    pub fn host_public_key(&self) -> AbsolutePath {
        self.ssh_directory().join("ssh_host_ed25519_key.pub")
    }

    #[must_use]
    pub fn known_hosts(&self) -> AbsolutePath {
        self.ssh_directory().join("known_hosts")
    }

    /// `nix build` output, appended on every build.
    #[must_use]
    pub fn build_log(&self) -> AbsolutePath {
        self.state.join("build.log")
    }

    /// virtiofsd output, appended by every instance.
    #[must_use]
    pub fn helper_log(&self) -> AbsolutePath {
        self.state.join("virtiofsd.log")
    }

    #[must_use]
    pub fn console_log(&self) -> AbsolutePath {
        self.state.join("console.log")
    }

    /// Word-split by the runner script; named in `nix/base.nix`.
    #[must_use]
    pub fn qemu_arguments(&self) -> AbsolutePath {
        self.runtime.join("amoeboid-qemu-args")
    }

    /// QMP socket; named by `microvm.socket` in `nix/base.nix`.
    #[must_use]
    pub fn control_socket(&self) -> AbsolutePath {
        self.runtime.join("control.sock")
    }

    /// The runner, from the build output's `runner` link farm entry.
    #[must_use]
    pub fn runner(&self) -> AbsolutePath {
        self.build_link()
            .join("runner")
            .join("bin")
            .join("microvm-run")
    }

    /// virtiofsd, from the build output's `virtiofsd` link farm entry.
    #[must_use]
    pub fn virtiofsd(&self) -> AbsolutePath {
        self.build_link()
            .join("virtiofsd")
            .join("bin")
            .join("virtiofsd")
    }
}

impl crate::diagnostic::Diagnostic for LocationError {
    fn code(&self) -> &'static str {
        match self {
            Self::Path(inner) => crate::diagnostic::Diagnostic::code(inner),
            Self::MissingEnvironment(_) => "runtime.path.missing_environment",
            Self::SocketTooLong(_) => "runtime.path.socket_too_long",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::MissingEnvironment("XDG_RUNTIME_DIR") => {
                Some("run inside a systemd user session, which sets XDG_RUNTIME_DIR".into())
            }
            Self::MissingEnvironment(variable) => Some(format!("set {variable}")),
            Self::SocketTooLong(_) => Some("use a shorter XDG_RUNTIME_DIR".into()),
            Self::Path(inner) => crate::diagnostic::Diagnostic::hint(inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn environment(
        variables: &[(&'static str, &str)],
    ) -> impl Fn(&'static str) -> Option<OsString> {
        let variables: BTreeMap<_, _> = variables
            .iter()
            .map(|(name, value)| (*name, OsString::from(value)))
            .collect();
        move |name| variables.get(name).cloned()
    }

    fn absolute(text: &str) -> AbsolutePath {
        AbsolutePath::parse(text.into()).unwrap()
    }

    #[test]
    fn xdg_data_home_wins_over_home() {
        let paths = RuntimePaths::from_env(environment(&[
            ("XDG_DATA_HOME", "/x/data"),
            ("HOME", "/home/u"),
            ("XDG_RUNTIME_DIR", "/run/user/1"),
        ]))
        .unwrap();
        assert_eq!(paths.lock(), absolute("/x/data/amoeboid/daemon.lock"));
        assert_eq!(
            paths.socket(),
            &absolute("/run/user/1/amoeboid/runtime.sock")
        );
    }

    #[test]
    fn home_is_the_fallback_for_data() {
        let paths = RuntimePaths::from_env(environment(&[
            ("XDG_DATA_HOME", ""),
            ("HOME", "/home/u"),
            ("XDG_RUNTIME_DIR", "/run/user/1"),
        ]))
        .unwrap();
        assert_eq!(
            paths.journal(),
            absolute("/home/u/.local/share/amoeboid/journal.sqlite")
        );
    }

    #[test]
    fn a_missing_runtime_dir_is_refused() {
        let result = RuntimePaths::from_env(environment(&[("HOME", "/home/u")]));
        assert!(matches!(
            result,
            Err(LocationError::MissingEnvironment("XDG_RUNTIME_DIR"))
        ));
    }

    #[test]
    fn a_socket_path_beyond_sun_path_is_refused() {
        let long = format!("/{}", "r".repeat(MAX_SOCKET_PATH));
        let result = RuntimePaths::under(absolute("/d"), absolute(&long));
        assert!(matches!(result, Err(LocationError::SocketTooLong(_))));
    }

    #[test]
    fn a_vm_whose_sockets_would_not_fit_is_refused() {
        let run = format!("/{}", "r".repeat(60));
        let paths = RuntimePaths::under(absolute("/d"), absolute(&run)).unwrap();
        let name = VmName::parse("n".repeat(40)).unwrap();
        assert!(matches!(
            paths.vm(&name),
            Err(LocationError::SocketTooLong(_))
        ));
    }

    #[test]
    fn vm_paths_follow_the_python_layout() {
        let paths = RuntimePaths::under(absolute("/d/amoeboid"), absolute("/r/amoeboid")).unwrap();
        let vm = paths.vm(&VmName::parse("work".into()).unwrap()).unwrap();
        assert_eq!(vm.guest(), absolute("/d/amoeboid/work/guest"));
        assert_eq!(
            vm.known_hosts(),
            absolute("/d/amoeboid/work/ssh/known_hosts")
        );
        assert_eq!(
            vm.control_socket(),
            absolute("/r/amoeboid/work/control.sock")
        );
        assert_eq!(
            vm.qemu_arguments(),
            absolute("/r/amoeboid/work/amoeboid-qemu-args")
        );
        assert_eq!(
            vm.runner(),
            absolute("/d/amoeboid/work/vm/runner/bin/microvm-run")
        );
    }
}
