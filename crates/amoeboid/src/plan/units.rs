//! The systemd user units that host the runtime: a socket the CLI connects to, and the daemon
//! it activates. VMs run in their own scopes, so `KillMode=process` only spares them twice over.

/// A value for a unit file: double-quoted, with `\` and `"` escaped and `%` (systemd's
/// specifier prefix) doubled.
#[must_use]
pub fn quoted(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    format!("\"{escaped}\"")
}

pub const SOCKET_NAME: &str = "amoeboid.socket";
pub const SERVICE_NAME: &str = "amoeboid.service";

/// `amoeboid.socket`: the runtime's socket under `$XDG_RUNTIME_DIR/amoeboid`, owner only.
#[must_use]
pub fn socket() -> String {
    "\
[Unit]
Description=amoeboid runtime socket

[Socket]
ListenStream=%t/amoeboid/runtime.sock
SocketMode=0600
DirectoryMode=0700

[Install]
WantedBy=sockets.target
"
    .to_owned()
}

/// `amoeboid.service`: the daemon, restarted if it fails; `path` is the `PATH` it needs to
/// find `nix`, `ssh-keygen` and `systemd-run`.
#[must_use]
pub fn service(daemon: &str, path: &str) -> String {
    format!(
        "\
[Unit]
Description=amoeboid runtime
Requires={SOCKET_NAME}
After={SOCKET_NAME}

[Service]
ExecStart={}
Environment={}
Restart=on-failure
KillMode=process
",
        quoted(daemon),
        quoted(&format!("PATH={path}")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_quoted_and_specifiers_escaped() {
        assert_eq!(
            quoted("/opt/my tools/50%/a\"b"),
            r#""/opt/my tools/50%%/a\"b""#
        );
    }

    #[test]
    fn the_service_runs_the_daemon_with_the_given_path_and_restarts_on_failure() {
        let unit = service("/home/u/.cargo/bin/amoeboid-daemon", "/usr/bin:/bin");
        assert!(unit.contains("ExecStart=\"/home/u/.cargo/bin/amoeboid-daemon\"\n"));
        assert!(unit.contains("Environment=\"PATH=/usr/bin:/bin\"\n"));
        assert!(unit.contains("Restart=on-failure\n"));
        assert!(socket().contains("SocketMode=0600\n"));
    }
}
