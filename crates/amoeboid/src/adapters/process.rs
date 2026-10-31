//! Processes: spawn (each in its own scope), identify, check liveness, signal and watch by
//! verified identity. A process is only signalled through a pidfd opened and then checked
//! against the identity, so a reused process id is never hit.

#![allow(clippy::disallowed_types, reason = "boundary: reads /proc by raw path")]

use std::fs::File;
use std::num::NonZeroU32;
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt as _;
use std::path::PathBuf;
use std::process::Stdio;

use rustix::fs::{Mode, OFlags};
use rustix::process::{Pid, PidfdFlags, Signal as KernelSignal};

use crate::domain::path::AbsolutePath;
use crate::domain::process::{BootId, Nonce, ProcessId, ProcessIdentity, SLICE, ScopeName};
use crate::domain::vm_name::VmName;
use crate::plan::argv::Argv;

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("can't read the boot id: {0}")]
    BootId(std::io::Error),
    #[error("can't read /proc/{process}/{file}: {source}")]
    Read {
        process: ProcessId,
        file: &'static str,
        source: std::io::Error,
    },
    #[error("can't make sense of /proc/{process}/{file}")]
    Parse {
        process: ProcessId,
        file: &'static str,
    },
    #[error("can't start {program}: {source}")]
    Spawn {
        program: String,
        source: std::io::Error,
    },
    #[error("can't open the log {path}: {source}")]
    Output {
        path: String,
        source: std::io::Error,
    },
    #[error("can't signal process {process}: {source}")]
    Signal {
        process: ProcessId,
        source: std::io::Error,
    },
    #[error("can't list processes: {0}")]
    List(std::io::Error),
}

/// Where a spawned process lives.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Containment {
    /// Its own transient user scope under `amoeboid.slice`: outlives the runtime.
    Scope(ScopeName),
    /// A plain child of the runtime; tests only.
    Direct,
}

/// Where a spawned process's stdout and stderr go.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Output {
    Discard,
    /// Appended to this file, created `0600`.
    Append(AbsolutePath),
}

pub struct SpawnRequest {
    pub argv: Argv,
    pub containment: Containment,
    /// Set for VM processes, so they can be found again; `None` for one-off commands.
    pub nonce: Option<Nonce>,
    pub directory: AbsolutePath,
    pub output: Output,
}

/// A process this runtime started; `child` reaps it and reports its exit status.
pub struct Spawned {
    pub identity: ProcessIdentity,
    pub child: tokio::process::Child,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Liveness {
    Alive,
    /// Exited, or its id now belongs to another process, or the host rebooted.
    Gone,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Signal {
    Terminate,
    Kill,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Signalled {
    Sent,
    /// The identified process no longer exists; nothing was signalled.
    Gone,
}

/// A process being watched for its exit.
pub enum Watched {
    /// Started by this runtime: waiting reaps it and yields its status.
    Child(tokio::process::Child),
    /// Adopted after a runtime restart: not our child, so only "it exited" is knowable.
    Adopted(OwnedFd),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Exit {
    Code(i32),
    Signal(i32),
    /// An adopted process exited; its status went to its real parent.
    Unknown,
}

/// # Errors
///
/// `BootId`.
pub fn boot_id() -> Result<BootId, ProcessError> {
    let text =
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(ProcessError::BootId)?;
    BootId::parse(text.trim_end().to_owned())
        .map_err(|error| ProcessError::BootId(std::io::Error::other(error)))
}

/// The identity of `process` now; `None` when no such process exists.
///
/// # Errors
///
/// `/proc` is unreadable or malformed.
pub fn identify(process: ProcessId) -> Result<Option<ProcessIdentity>, ProcessError> {
    let stat = match std::fs::read_to_string(format!("/proc/{process}/stat")) {
        Ok(stat) => stat,
        // ESRCH: it exited between opening its entry and reading it.
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) =>
        {
            return Ok(None);
        }
        Err(source) => {
            return Err(ProcessError::Read {
                process,
                file: "stat",
                source,
            });
        }
    };
    let start_time = start_time(&stat).ok_or(ProcessError::Parse {
        process,
        file: "stat",
    })?;
    Ok(Some(ProcessIdentity {
        process,
        start_time,
        boot: boot_id()?,
    }))
}

/// Field `number` of `/proc/PID/stat`. The command name (field 2) may hold spaces and `)`, so
/// fields are counted after its last `)`, where field 3 comes first.
fn stat_field(stat: &str, number: usize) -> Option<u64> {
    let (_, after_name) = stat.rsplit_once(')')?;
    after_name
        .split_ascii_whitespace()
        .nth(number.checked_sub(3)?)?
        .parse()
        .ok()
}

/// Field 22: start time in clock ticks since boot.
fn start_time(stat: &str) -> Option<u64> {
    stat_field(stat, 22)
}

/// Field 4: the parent's process id.
fn parent(stat: &str) -> Option<u64> {
    stat_field(stat, 4)
}

/// # Errors
///
/// `/proc` is unreadable or malformed.
pub fn liveness(identity: &ProcessIdentity) -> Result<Liveness, ProcessError> {
    Ok(match identify(identity.process)? {
        Some(now) if now == *identity && !zombie(identity.process) => Liveness::Alive,
        Some(_) | None => Liveness::Gone,
    })
}

/// A zombie: it has exited and only waits for its parent to reap it.
fn zombie(process: ProcessId) -> bool {
    std::fs::read_to_string(format!("/proc/{process}/stat"))
        .ok()
        .as_deref()
        .and_then(|stat| stat.rsplit_once(')'))
        .and_then(|(_, after_name)| after_name.split_ascii_whitespace().next())
        .is_some_and(|state| matches!(state, "Z" | "X"))
}

/// A pidfd for exactly `identity`, or `None` when that process is gone. Opened first and checked
/// after: the pidfd pins whichever process had the id, and the start time says if it is ours.
fn pidfd(identity: &ProcessIdentity) -> Result<Option<OwnedFd>, ProcessError> {
    let Some(pid) = i32::try_from(identity.process.get())
        .ok()
        .and_then(Pid::from_raw)
    else {
        return Ok(None);
    };
    let descriptor = match rustix::process::pidfd_open(pid, PidfdFlags::NONBLOCK) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::SRCH) => return Ok(None),
        Err(error) => {
            return Err(ProcessError::Signal {
                process: identity.process,
                source: error.into(),
            });
        }
    };
    Ok(match liveness(identity)? {
        Liveness::Alive => Some(descriptor),
        Liveness::Gone => None,
    })
}

/// # Errors
///
/// `/proc` is unreadable, or the kernel refused the signal.
pub fn signal(identity: &ProcessIdentity, signal: Signal) -> Result<Signalled, ProcessError> {
    let Some(descriptor) = pidfd(identity)? else {
        return Ok(Signalled::Gone);
    };
    let kernel = match signal {
        Signal::Terminate => KernelSignal::TERM,
        Signal::Kill => KernelSignal::KILL,
    };
    match rustix::process::pidfd_send_signal(&descriptor, kernel) {
        Ok(()) => Ok(Signalled::Sent),
        Err(rustix::io::Errno::SRCH) => Ok(Signalled::Gone),
        Err(error) => Err(ProcessError::Signal {
            process: identity.process,
            source: error.into(),
        }),
    }
}

/// Watches a process this runtime did not start (or no longer holds the child of).
///
/// # Errors
///
/// `/proc` is unreadable.
pub fn adopt(identity: &ProcessIdentity) -> Result<Option<Watched>, ProcessError> {
    Ok(pidfd(identity)?.map(Watched::Adopted))
}

/// Set to stop waiting on adopted processes (runtime shutdown); they keep running.
pub type Cancel = std::sync::Arc<std::sync::atomic::AtomicBool>;

/// How often a wait on an adopted process checks `cancel`.
const CANCEL_CHECK: rustix::event::Timespec = rustix::event::Timespec {
    tv_sec: 1,
    tv_nsec: 0,
};

/// Resolves when the process exits; `None` when `cancel` was set first (only checked for
/// adopted processes, whose wait holds a blocking thread; a child's wait just ends with its
/// task).
pub async fn exited(watched: Watched, cancel: Cancel) -> Option<Exit> {
    match watched {
        Watched::Child(mut child) => Some(match child.wait().await {
            Ok(status) => status
                .code()
                .map(Exit::Code)
                .or_else(|| status.signal().map(Exit::Signal))
                .unwrap_or(Exit::Unknown),
            Err(_) => Exit::Unknown,
        }),
        Watched::Adopted(descriptor) => {
            // A pidfd turns readable when its process exits. Registering it with tokio's
            // reactor needs `unsafe`, so it is polled on a blocking thread, waking every
            // second to see whether the runtime is shutting down.
            tokio::task::spawn_blocking(move || {
                loop {
                    let mut watch = [rustix::event::PollFd::new(
                        &descriptor,
                        rustix::event::PollFlags::IN,
                    )];
                    match rustix::event::poll(&mut watch, Some(&CANCEL_CHECK)) {
                        Ok(0) | Err(rustix::io::Errno::INTR) => {
                            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                                return None;
                            }
                        }
                        Ok(_) | Err(_) => return Some(Exit::Unknown),
                    }
                }
            })
            .await
            .ok()
            .flatten()
        }
    }
}

/// Starts `request.argv` with the nonce in its environment, stdin closed, in its own process
/// group, inside its containment.
///
/// # Errors
///
/// `Output`, `Spawn`, or reading its identity.
pub fn spawn(request: &SpawnRequest) -> Result<Spawned, ProcessError> {
    let mut words = request.argv.words();
    let program = words
        .next()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let mut command = match &request.containment {
        Containment::Scope(scope) => {
            let mut command = tokio::process::Command::new("systemd-run");
            command
                .args(["--user", "--scope", "--quiet", "--collect"])
                .arg(format!("--slice={SLICE}"))
                .arg(format!("--unit={}", scope.as_str()))
                .arg("--")
                .arg(&program)
                .args(words);
            command
        }
        Containment::Direct => {
            let mut command = tokio::process::Command::new(&program);
            command.args(words);
            command
        }
    };
    if let Some(nonce) = &request.nonce {
        command.env(Nonce::VARIABLE, nonce.value());
    }
    command
        .current_dir(request.directory.as_path())
        .stdin(Stdio::null())
        .process_group(0)
        .kill_on_drop(false);
    match &request.output {
        Output::Discard => {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        Output::Append(path) => {
            let output = |error: std::io::Error| ProcessError::Output {
                path: path.display().to_string(),
                source: error,
            };
            let file = File::from(
                rustix::fs::open(
                    path.as_path(),
                    OFlags::WRONLY
                        | OFlags::APPEND
                        | OFlags::CREATE
                        | OFlags::NOFOLLOW
                        | OFlags::CLOEXEC,
                    Mode::from_raw_mode(0o600),
                )
                .map_err(|error| output(error.into()))?,
            );
            let error_file = file.try_clone().map_err(output)?;
            command.stdout(file).stderr(error_file);
        }
    }
    // Tests that hold lock fds must not see a fork mid-flight (testing::forks).
    #[cfg(test)]
    let forks = crate::testing::forks();
    let child = command.spawn().map_err(|source| ProcessError::Spawn {
        program: program.clone(),
        source,
    })?;
    #[cfg(test)]
    drop(forks);
    let process = child
        .id()
        .and_then(NonZeroU32::new)
        .map(ProcessId::new)
        .ok_or_else(|| ProcessError::Spawn {
            program: program.clone(),
            source: std::io::Error::other("exited before it could be identified"),
        })?;
    // The start time survives exec, so this identifies the process whatever it execs into.
    let identity = identify(process)?.ok_or(ProcessError::Spawn {
        program,
        source: std::io::Error::other("exited before it could be identified"),
    })?;
    Ok(Spawned { identity, child })
}

/// Runs `argv` as a direct child to completion: stdin closed, output as given.
///
/// # Errors
///
/// `Output`, `Spawn`.
pub async fn run(
    argv: &Argv,
    directory: &AbsolutePath,
    output: &Output,
) -> Result<Exit, ProcessError> {
    let spawned = spawn(&SpawnRequest {
        argv: argv.clone(),
        containment: Containment::Direct,
        nonce: None,
        directory: directory.clone(),
        output: output.clone(),
    })?;
    Ok(exited(Watched::Child(spawned.child), Cancel::default())
        .await
        .unwrap_or(Exit::Unknown))
}

/// The process carrying `nonce` in its environment, among those this user may inspect. Children
/// inherit the environment, so among several the topmost is taken: the one whose parent does
/// not carry the nonce too.
///
/// # Errors
///
/// `/proc` can't be listed.
pub fn find_by_nonce(nonce: &Nonce) -> Result<Option<ProcessIdentity>, ProcessError> {
    let wanted = nonce.environment_entry();
    let carriers = carriers(|entry| entry == wanted.as_bytes())?;
    let topmost = carriers.iter().find(|(_, parent)| {
        !carriers
            .iter()
            .any(|(process, _)| u64::from(process.get()) == *parent)
    });
    match topmost {
        Some((process, _)) => identify(*process),
        None => Ok(None),
    }
}

/// Every process carrying a nonce of `vm`, children included, among those this user may
/// inspect; the nonce is the runtime's mark, so this is how a VM's processes are found without
/// its history.
///
/// # Errors
///
/// `/proc` can't be listed.
pub fn find_by_vm(vm: &VmName) -> Result<Vec<ProcessIdentity>, ProcessError> {
    let prefix = Nonce::vm_entry_prefix(vm);
    let mut found = Vec::new();
    for (process, _) in carriers(|entry| entry.starts_with(prefix.as_bytes()))? {
        // Gone between the scan and now: nothing to find.
        if let Ok(Some(identity)) = identify(process) {
            found.push(identity);
        }
    }
    Ok(found)
}

/// Processes with an environment entry matching `wanted`, with their parent's process id.
fn carriers(wanted: impl Fn(&[u8]) -> bool) -> Result<Vec<(ProcessId, u64)>, ProcessError> {
    let mut carriers = Vec::new();
    for entry in std::fs::read_dir("/proc").map_err(ProcessError::List)? {
        let Some(process) = entry
            .ok()
            .and_then(|entry| entry.file_name().to_str()?.parse::<NonZeroU32>().ok())
            .map(ProcessId::new)
        else {
            continue;
        };
        // Other users' environments are unreadable; that is fine, ours are not.
        let Ok(environment) = std::fs::read(PathBuf::from(format!("/proc/{process}/environ")))
        else {
            continue;
        };
        let has_nonce = environment.split(|byte| *byte == 0).any(&wanted);
        let stat = std::fs::read_to_string(format!("/proc/{process}/stat")).ok();
        if let (true, Some(parent)) = (has_nonce, stat.as_deref().and_then(parent)) {
            carriers.push((process, parent));
        }
    }
    Ok(carriers)
}

impl crate::diagnostic::Diagnostic for ProcessError {
    fn code(&self) -> &'static str {
        match self {
            Self::BootId(_) => "process.boot_id",
            Self::Read { .. } => "process.read",
            Self::Parse { .. } => "process.parse",
            Self::Spawn { .. } => "process.spawn",
            Self::Output { .. } => "process.output",
            Self::Signal { .. } => "process.signal",
            Self::List(_) => "process.list",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Spawn { program, .. } if program == "systemd-run" => {
                Some("VMs run in user scopes: run inside a systemd user session".into())
            }
            Self::BootId(_)
            | Self::Read { .. }
            | Self::Parse { .. }
            | Self::Spawn { .. }
            | Self::Output { .. }
            | Self::Signal { .. }
            | Self::List(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn fresh_nonce() -> Nonce {
        let vm = crate::domain::vm_name::VmName::parse("test".into()).unwrap();
        Nonce::of(
            &vm,
            crate::domain::lifecycle::Incarnation::mint(),
            crate::domain::lifecycle::Generation::FIRST,
            &crate::domain::process::ProcessRole::Local(crate::domain::process::LocalRole::Qemu),
        )
    }

    fn sleeper(nonce: &Nonce, containment: Containment) -> Spawned {
        spawn(&SpawnRequest {
            argv: Argv::for_tests(&["sleep", "30"]),
            containment,
            nonce: Some(nonce.clone()),
            directory: AbsolutePath::parse("/".into()).unwrap(),
            output: Output::Discard,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn a_killed_process_is_gone_before_its_parent_reaps_it() {
        let mut spawned = sleeper(&fresh_nonce(), Containment::Direct);
        signal(&spawned.identity, Signal::Kill).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while liveness(&spawned.identity).unwrap() == Liveness::Alive
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // Not reaped yet: the zombie still has its /proc entry.
        assert!(identify(spawned.identity.process).unwrap().is_some());
        assert_eq!(liveness(&spawned.identity).unwrap(), Liveness::Gone);
        spawned.child.wait().await.unwrap();
    }

    #[test]
    fn start_time_is_read_past_a_command_name_with_spaces_and_parens() {
        let stat = "42 (a) b (c) S 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 777 23 24";
        assert_eq!(start_time(stat), Some(777));
        assert_eq!(parent(stat), Some(4));
    }

    #[tokio::test]
    async fn a_spawned_process_is_alive_until_killed_then_its_exit_is_seen() {
        let spawned = sleeper(&fresh_nonce(), Containment::Direct);
        let identity = spawned.identity.clone();
        assert_eq!(liveness(&identity).unwrap(), Liveness::Alive);
        assert_eq!(signal(&identity, Signal::Kill).unwrap(), Signalled::Sent);
        let exit = tokio::time::timeout(
            Duration::from_secs(5),
            exited(Watched::Child(spawned.child), Cancel::default()),
        )
        .await
        .unwrap();
        assert_eq!(exit, Some(Exit::Signal(9)));
        assert_eq!(liveness(&identity).unwrap(), Liveness::Gone);
    }

    #[tokio::test]
    async fn a_mismatched_identity_is_never_signalled() {
        let spawned = sleeper(&fresh_nonce(), Containment::Direct);
        let impostor = ProcessIdentity {
            start_time: spawned.identity.start_time + 1,
            ..spawned.identity.clone()
        };
        assert_eq!(signal(&impostor, Signal::Kill).unwrap(), Signalled::Gone);
        assert_eq!(liveness(&spawned.identity).unwrap(), Liveness::Alive);
        signal(&spawned.identity, Signal::Kill).unwrap();
        exited(Watched::Child(spawned.child), Cancel::default()).await;
    }

    #[tokio::test]
    async fn an_adopted_process_is_watched_through_its_pidfd() {
        let spawned = sleeper(&fresh_nonce(), Containment::Direct);
        let watched = adopt(&spawned.identity).unwrap().unwrap();
        signal(&spawned.identity, Signal::Terminate).unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(5), exited(watched, Cancel::default()))
            .await
            .unwrap();
        assert_eq!(exit, Some(Exit::Unknown));
        exited(Watched::Child(spawned.child), Cancel::default()).await;
    }

    #[tokio::test]
    async fn a_nonce_search_takes_the_topmost_carrier_not_its_children() {
        let nonce = fresh_nonce();
        let parent = spawn(&SpawnRequest {
            argv: Argv::for_tests(&["sh", "-c", "sleep 30 & wait"]),
            containment: Containment::Direct,
            nonce: Some(nonce.clone()),
            directory: AbsolutePath::parse("/".into()).unwrap(),
            output: Output::Discard,
        })
        .unwrap();
        // Let the shell fork its child, which inherits the nonce.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            find_by_nonce(&nonce).unwrap(),
            Some(parent.identity.clone())
        );
        // The shell leads its own process group; take its child with it.
        let group = i32::try_from(parent.identity.process.get())
            .ok()
            .and_then(Pid::from_raw)
            .unwrap();
        rustix::process::kill_process_group(group, KernelSignal::KILL).unwrap();
        exited(Watched::Child(parent.child), Cancel::default()).await;
    }

    #[tokio::test]
    async fn waiting_on_an_adopted_process_ends_when_cancelled_and_leaves_it_running() {
        let spawned = sleeper(&fresh_nonce(), Containment::Direct);
        let watched = adopt(&spawned.identity).unwrap().unwrap();
        let cancel = Cancel::default();
        let wait = tokio::spawn(exited(watched, cancel.clone()));
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        let result = tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(liveness(&spawned.identity).unwrap(), Liveness::Alive);
        signal(&spawned.identity, Signal::Kill).unwrap();
        exited(Watched::Child(spawned.child), Cancel::default()).await;
    }

    #[tokio::test]
    async fn a_process_is_found_by_its_nonce() {
        let nonce = fresh_nonce();
        let spawned = sleeper(&nonce, Containment::Direct);
        assert_eq!(
            find_by_nonce(&nonce).unwrap(),
            Some(spawned.identity.clone())
        );
        assert_eq!(find_by_nonce(&fresh_nonce()).unwrap(), None);
        signal(&spawned.identity, Signal::Kill).unwrap();
        exited(Watched::Child(spawned.child), Cancel::default()).await;
    }

    /// Needs a systemd user session; runs only with `AMOEBOID_TEST_SYSTEMD=1`.
    #[tokio::test]
    async fn a_scoped_process_runs_in_its_scope_and_a_duplicate_scope_fails() {
        if std::env::var_os("AMOEBOID_TEST_SYSTEMD").is_none() {
            return;
        }
        let vm =
            crate::domain::vm_name::VmName::parse(format!("test{}", std::process::id())).unwrap();
        let scope = ScopeName::of(
            &vm,
            &crate::domain::process::ProcessRole::Local(crate::domain::process::LocalRole::Qemu),
        );
        let nonce = fresh_nonce();
        let first = sleeper(&nonce, Containment::Scope(scope.clone()));
        // systemd-run execs the command once the scope exists; wait for that.
        let mut found = None;
        for _ in 0..50 {
            found = find_by_nonce(&nonce).unwrap();
            let cgroup =
                std::fs::read_to_string(format!("/proc/{}/cgroup", first.identity.process))
                    .unwrap_or_default();
            if cgroup.contains(&format!("{}.scope", scope.as_str())) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(found, Some(first.identity.clone()));
        let second = sleeper(&fresh_nonce(), Containment::Scope(scope));
        let exit = tokio::time::timeout(
            Duration::from_secs(10),
            exited(Watched::Child(second.child), Cancel::default()),
        )
        .await
        .unwrap();
        assert!(matches!(exit, Some(Exit::Code(code)) if code != 0));
        signal(&first.identity, Signal::Kill).unwrap();
        exited(Watched::Child(first.child), Cancel::default()).await;
    }
}
