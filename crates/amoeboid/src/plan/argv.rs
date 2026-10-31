//! Argument lists for the programs the runtime runs.

#![allow(
    clippy::disallowed_types,
    reason = "argument vectors are OS strings by nature; paths in them may not be UTF-8"
)]

use std::ffi::{OsStr, OsString};

use super::share::VirtiofsShare;
use crate::domain::account::{GroupId, UserId, UserName};
use crate::domain::path::AbsolutePath;
use crate::domain::port::LoopbackPort;
use crate::domain::share::Mode;
use crate::domain::vm_name::VmName;

/// A program and its arguments, built only by the functions in this module.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Argv(Vec<OsString>);

impl Argv {
    /// Program first, then arguments.
    pub fn words(&self) -> impl Iterator<Item = &OsStr> {
        self.0.iter().map(OsString::as_os_str)
    }

    /// Any command, for tests that need a real process.
    #[cfg(test)]
    pub(crate) fn for_tests(words: &[&str]) -> Self {
        Self(words.iter().map(OsString::from).collect())
    }

    /// A line to paste into a POSIX shell, quoted like Python's `shlex.join`. Non-UTF-8 bytes
    /// are replaced, so this is for display, never for running.
    #[must_use]
    pub fn shell_line(&self) -> String {
        self.words()
            .map(|word| quote(&word.to_string_lossy()))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn quote(word: &str) -> String {
    let safe = !word.is_empty()
        && word
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "@%+=:,./-_".contains(character));
    if safe {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', "'\"'\"'"))
    }
}

/// `--key=<path>`, keeping a non-UTF-8 path intact.
fn flag(key: &str, path: &AbsolutePath) -> OsString {
    let mut flag = OsString::from(key);
    flag.push(path.as_os_str());
    flag
}

#[must_use]
pub fn nix_build(
    flake: &AbsolutePath,
    host_module: &AbsolutePath,
    out_link: &AbsolutePath,
) -> Argv {
    Argv(vec![
        "nix".into(),
        "--extra-experimental-features".into(),
        "nix-command flakes".into(),
        "build".into(),
        flag("path:", flake),
        "--override-input".into(),
        "host".into(),
        flag("path:", host_module),
        "--no-write-lock-file".into(),
        "--out-link".into(),
        out_link.as_os_str().into(),
    ])
}

/// The ids the guest user has, and the host user virtiofsd runs as. The guest user's ids map
/// onto the host user's, so files the guest creates (even as root) belong to the host user.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct IdMap {
    pub guest_user: UserId,
    pub guest_group: GroupId,
    pub host_user: UserId,
    pub host_group: GroupId,
}

/// Read-only shares are enforced here, by virtiofsd, not by the guest.
#[must_use]
pub fn virtiofsd(
    binary: &AbsolutePath,
    share: &VirtiofsShare,
    runtime: &AbsolutePath,
    ids: IdMap,
) -> Argv {
    let mut argv = vec![
        binary.as_os_str().into(),
        flag("--socket-path=", &runtime.join(&share.socket())),
        flag("--shared-dir=", &share.source),
        "--sandbox=namespace".into(),
        format!(
            "--uid-map=:{}:{}:1:",
            ids.guest_user.get(),
            ids.host_user.get()
        )
        .into(),
        format!(
            "--gid-map=:{}:{}:1:",
            ids.guest_group.get(),
            ids.host_group.get()
        )
        .into(),
        "--log-level=error".into(),
    ];
    match share.mode {
        Mode::ReadOnly => argv.push("--readonly".into()),
        Mode::ReadWrite => {}
    }
    Argv(argv)
}

#[must_use]
pub fn ssh_keygen(vm: &VmName, key_file: &AbsolutePath) -> Argv {
    Argv(vec![
        "ssh-keygen".into(),
        "-q".into(),
        "-t".into(),
        "ed25519".into(),
        "-N".into(),
        String::new().into(),
        "-C".into(),
        super::ssh_alias(vm).into(),
        "-f".into(),
        key_file.as_os_str().into(),
    ])
}

/// The only way an ssh command line is built: the host key is pinned, and whatever the
/// operator's `~/.ssh/config` says, nothing of the host reaches the VM through ssh.
#[must_use]
pub fn ssh(
    vm: &VmName,
    user: &UserName,
    port: LoopbackPort,
    known_hosts: &AbsolutePath,
    command: &[String],
) -> Argv {
    let mut argv: Vec<OsString> = vec![
        "ssh".into(),
        "-p".into(),
        port.to_string().into(),
        "-o".into(),
        format!("HostKeyAlias={}", super::ssh_alias(vm)).into(),
        "-o".into(),
        flag("UserKnownHostsFile=", known_hosts),
        "-o".into(),
        "StrictHostKeyChecking=yes".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        "-o".into(),
        "ForwardAgent=no".into(),
        "-o".into(),
        "ForwardX11=no".into(),
        "-o".into(),
        "GSSAPIDelegateCredentials=no".into(),
        "-o".into(),
        "PermitLocalCommand=no".into(),
        format!("{}@127.0.0.1", user.as_str()).into(),
    ];
    argv.extend(command.iter().map(OsString::from));
    Argv(argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_lines_quote_like_shlex_join() {
        let argv = Argv(vec![
            "ssh".into(),
            "-o".into(),
            "UserKnownHostsFile=/home/my user/known_hosts".into(),
            String::new().into(),
            "it's".into(),
            "a@127.0.0.1".into(),
        ]);
        assert_eq!(
            argv.shell_line(),
            r#"ssh -o 'UserKnownHostsFile=/home/my user/known_hosts' '' 'it'"'"'s' a@127.0.0.1"#
        );
    }
}
