//! End-to-end tests of the runtime through its protocol: a `Runtime` (in-process, SQLite
//! journal) driven by the library `Client`, the same frames the CLI sends. Real processes run,
//! as plain children rather than in scopes, with stand-ins for what Nix would build:
//!
//! ```text
//! build (scripted) ── copies stand-ins into <state>/vm/{runner/bin/microvm-run, virtiofsd/bin/virtiofsd}
//! virtiofsd stand-in ── creates its --socket-path, then sleeps
//! runner stand-in ──── python3: reads the port from amoeboid-qemu-args and answers "SSH-2.0-…"
//!                      there; serves QMP on control.sock and exits on system_powerdown, as a
//!                      guest that powers off
//! ```
//!
//! Needs `python3` and `ssh-keygen` (the host key is real).

#![allow(
    clippy::disallowed_types,
    reason = "test fixtures build raw temp paths"
)]

use std::time::Duration;

use crate::client::Client;
use crate::clock::Clock;
use crate::config::daemon::DaemonConfig;
use crate::domain::path::AbsolutePath;
use crate::domain::vm_name::VmName;
use crate::plan::argv::Argv;
use crate::protocol::{ReplyBody, RequestBody, VmReply, VmRequest};
use crate::runtime::paths::{ConfigPaths, RuntimePaths};
use crate::runtime::root::VmDefaults;
use crate::runtime::timing::ProcessTiming;
use crate::runtime::{
    DaemonConfigSource, Hosting, JournalLocation, Persistence, Runtime, RuntimeConfig,
};
use crate::vm::builder::BuildCommand;
use crate::vm::local_qemu::Placement;
use crate::vm::{Account, VmCondition};

const VIRTIOFSD: &str = "#!/bin/sh\nfor a; do case $a in --socket-path=*) : > \"${a#--socket-path=}\";; esac; done\nexec sleep 600\n";

const RUNNER: &str = r#"
import json, os, re, socket, threading
port = int(re.search(r"hostfwd=tcp:127\.0\.0\.1:(\d+)-:22", open("amoeboid-qemu-args").read()).group(1))
ssh = socket.socket()
ssh.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ssh.bind(("127.0.0.1", port))
ssh.listen()
qmp = socket.socket(socket.AF_UNIX)
qmp.bind("control.sock")
qmp.listen()
def banners():
    while True:
        connection, _ = ssh.accept()
        connection.sendall(b"SSH-2.0-amoeboid-stand-in\r\n")
        connection.close()
threading.Thread(target=banners, daemon=True).start()
while True:
    connection, _ = qmp.accept()
    stream = connection.makefile("rw")
    stream.write('{"QMP": {"version": {}}}\n')
    stream.flush()
    for line in stream:
        command = json.loads(line).get("execute")
        stream.write('{"return": {}}\n')
        stream.flush()
        if command == "system_powerdown":
            os._exit(0)
"#;

/// A scratch installation: data, run and configuration directories, a home, a project to
/// share, and the stand-ins the scripted build installs.
pub(crate) struct Installation {
    _temporary: tempfile::TempDir,
    pub(crate) paths: RuntimePaths,
    pub(crate) config: ConfigPaths,
    pub(crate) project: AbsolutePath,
    stand_ins: AbsolutePath,
}

impl Installation {
    pub(crate) fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let paths = RuntimePaths::under(root.join("data"), root.join("run")).unwrap();
        let config = ConfigPaths::new(root.join("config"), root.join("home"));
        let project = root.join("project");
        let stand_ins = root.join("stand-ins");
        for directory in [&project, config.directory(), config.home()] {
            std::fs::create_dir_all(directory.as_path()).unwrap();
        }
        crate::testing::executable(&stand_ins.as_path().join("virtiofsd"), VIRTIOFSD);
        crate::testing::executable(
            &stand_ins.as_path().join("runner"),
            &format!(
                "#!/bin/sh\nexec python3 {}\n",
                stand_ins.as_path().join("runner.py").display()
            ),
        );
        std::fs::write(stand_ins.as_path().join("runner.py"), RUNNER).unwrap();
        // Every start shares the project by default and waits briefly for the stand-in sshd.
        std::fs::write(
            config.run_config().as_path(),
            format!(
                "[new_vm]\ndefault_shares = [\"{}:rw\"]\n\n[timeouts]\nboot_seconds = 20\n\
                 powerdown_seconds = 5\n",
                project.display()
            ),
        )
        .unwrap();
        Self {
            _temporary: temporary,
            paths,
            config,
            project,
            stand_ins,
        }
    }

    /// The build installs the stand-ins where Nix's out-link would hold the runner and
    /// virtiofsd (the build runs in the VM's state directory).
    fn build(&self) -> BuildCommand {
        let stand_ins = self.stand_ins.display();
        BuildCommand::Scripted(Argv::for_tests(&[
            "sh",
            "-c",
            &format!(
                "mkdir -p vm/runner/bin vm/virtiofsd/bin && \
                 cp {stand_ins}/runner vm/runner/bin/microvm-run && \
                 cp {stand_ins}/virtiofsd vm/virtiofsd/bin/virtiofsd"
            ),
        ]))
    }

    pub(crate) async fn start(&self) -> Runtime {
        Runtime::start(RuntimeConfig {
            paths: self.paths.clone(),
            config: self.config.clone(),
            hosting: Hosting::InProcess,
            persistence: Persistence::Sqlite(JournalLocation::Configured),
            daemon: DaemonConfigSource::Given(DaemonConfig::default()),
            version: "test".into(),
            account: Account::of_this_process(std::env::var_os).unwrap(),
            vm_defaults: VmDefaults {
                build: self.build(),
                placement: Placement::Direct,
                process: ProcessTiming::quick(),
            },
            clock: Clock::System,
        })
        .await
        .unwrap()
    }

    pub(crate) async fn client(&self) -> Client {
        Client::connect(&self.paths).await.unwrap()
    }
}

pub(crate) fn vm(name: &str) -> VmName {
    VmName::parse(name.into()).unwrap()
}

pub(crate) async fn request(client: &mut Client, request: VmRequest) -> ReplyBody {
    client.request(RequestBody::Vm(request)).await.unwrap()
}

pub(crate) async fn start(client: &mut Client, name: &str) -> ReplyBody {
    request(
        client,
        VmRequest::Start {
            name: vm(name),
            shares: None,
            nix_config: None,
        },
    )
    .await
}

pub(crate) async fn condition(client: &mut Client, name: &str) -> Option<VmCondition> {
    match request(
        client,
        VmRequest::Status {
            name: Some(vm(name)),
        },
    )
    .await
    {
        ReplyBody::Vm(VmReply::Status { vms }) => vms.into_iter().next().map(|vm| vm.condition),
        ReplyBody::Runtime(_) | ReplyBody::Vm(_) | ReplyBody::Failed(_) => None,
    }
}

/// Polls status until `accept` holds (up to 10 s); the last condition seen.
pub(crate) async fn until(
    client: &mut Client,
    name: &str,
    accept: impl Fn(&VmCondition) -> bool,
) -> Option<VmCondition> {
    let mut last = None;
    for _ in 0..100 {
        last = condition(client, name).await;
        if last.as_ref().is_some_and(&accept) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    last
}

/// The payloads of `id`'s events containing `needle`, read through a second connection.
fn journal_lines(paths: &RuntimePaths, id: &str, needle: &str) -> Vec<String> {
    let connection = tokio_rusqlite::rusqlite::Connection::open(paths.journal().as_path()).unwrap();
    let mut statement = connection
        .prepare("SELECT payload FROM events WHERE persistence_id = ?1 ORDER BY sequence")
        .unwrap();
    statement
        .query_map([id], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|payload| payload.contains(needle))
        .collect()
}

fn running(condition: &VmCondition) -> bool {
    matches!(condition, VmCondition::Running { .. })
}

#[tokio::test]
async fn a_vm_starts_runs_powers_off_and_is_vacuumed() {
    let installation = Installation::new();
    let runtime = installation.start().await;
    let mut client = installation.client().await;

    let started = start(&mut client, "first").await;
    let ReplyBody::Vm(VmReply::Started { ssh, .. }) = started else {
        return assert_eq!(format!("{started:?}"), "Started");
    };
    let Some(VmCondition::Running { port, .. }) = condition(&mut client, "first").await else {
        return assert_eq!("running", "something else");
    };
    assert_eq!(port, ssh.port);
    // The default share reached the guest's mounts table.
    let mounts = std::fs::read_to_string(
        installation
            .paths
            .vm(&vm("first"))
            .unwrap()
            .guest()
            .as_path()
            .join("mounts"),
    )
    .unwrap();
    assert!(
        mounts.contains(&installation.project.display().to_string()),
        "{mounts}"
    );

    // Powered off through QMP: the stand-in exits when asked, as a guest would.
    let stopped = request(&mut client, VmRequest::Stop { name: vm("first") }).await;
    assert!(
        matches!(stopped, ReplyBody::Vm(VmReply::Stopped { .. })),
        "{stopped:?}"
    );
    assert!(matches!(
        condition(&mut client, "first").await,
        Some(VmCondition::Stopped { last_failure: None })
    ));

    // qemu (the stand-in) exited by itself with 0, as after a guest powerdown: not by signal.
    let qemu_exits = journal_lines(&installation.paths, "vm/first", "\"process_exited\"")
        .into_iter()
        .filter(|line| line.contains("\"qemu\""))
        .collect::<Vec<_>>();
    assert!(
        qemu_exits.len() == 1 && qemu_exits[0].contains("\"Code\":0"),
        "{qemu_exits:?}"
    );

    let vacuumed = request(&mut client, VmRequest::Vacuum { name: vm("first") }).await;
    assert!(
        matches!(vacuumed, ReplyBody::Vm(VmReply::Vacuumed { .. })),
        "{vacuumed:?}"
    );
    assert_eq!(condition(&mut client, "first").await, None);
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
#[allow(
    clippy::await_holding_lock,
    reason = "drops and retakes daemon.lock: no fork may pin it meanwhile; nothing spawns while \
              the guard is held"
)]
async fn a_new_runtime_adopts_a_running_vm_and_can_stop_it() {
    let installation = Installation::new();
    let runtime = installation.start().await;
    let mut client = installation.client().await;
    let started = start(&mut client, "kept").await;
    assert!(
        matches!(started, ReplyBody::Vm(VmReply::Started { .. })),
        "{started:?}"
    );
    let before = condition(&mut client, "kept").await;

    let forks = crate::testing::forks();
    runtime.stop();
    runtime.wait().await;
    let runtime = installation.start().await;
    drop(forks);

    let mut client = installation.client().await;
    let after = until(&mut client, "kept", running).await;
    assert_eq!(after, before);
    let stopped = request(&mut client, VmRequest::Stop { name: vm("kept") }).await;
    assert!(
        matches!(stopped, ReplyBody::Vm(VmReply::Stopped { .. })),
        "{stopped:?}"
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn a_twin_whose_backend_fails_is_restarted_and_adopts_the_running_vm() {
    let installation = Installation::new();
    let runtime = installation.start().await;
    let mut client = installation.client().await;
    let started = start(&mut client, "steady").await;
    assert!(
        matches!(started, ReplyBody::Vm(VmReply::Started { .. })),
        "{started:?}"
    );
    let before = condition(&mut client, "steady").await;

    let twin = ractor::registry::where_is(format!("{}/vm/steady", runtime.id())).unwrap();
    ractor::registry::where_is(format!(
        "{}/vm/steady/hypervisor@{}",
        runtime.id(),
        twin.get_id()
    ))
    .unwrap()
    .kill();

    // Restarting, then up again: the restarted twin adopts what still runs.
    let after = until(&mut client, "steady", running).await;
    assert_eq!(after, before);
    let replaced = ractor::registry::where_is(format!("{}/vm/steady", runtime.id())).unwrap();
    assert_ne!(replaced.get_id(), twin.get_id());
    let stopped = request(&mut client, VmRequest::Stop { name: vm("steady") }).await;
    assert!(
        matches!(stopped, ReplyBody::Vm(VmReply::Stopped { .. })),
        "{stopped:?}"
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn a_machine_that_exits_by_itself_is_seen_stopped() {
    let installation = Installation::new();
    let runtime = installation.start().await;
    let mut client = installation.client().await;
    let started = start(&mut client, "fragile").await;
    assert!(
        matches!(started, ReplyBody::Vm(VmReply::Started { .. })),
        "{started:?}"
    );

    // The guest goes away on its own: kill every process marked as this VM's.
    for identity in crate::adapters::process::find_by_vm(&vm("fragile")).unwrap() {
        let _ = crate::adapters::process::signal(&identity, crate::adapters::process::Signal::Kill);
    }
    let after = until(&mut client, "fragile", |condition| {
        matches!(condition, VmCondition::Stopped { .. })
    })
    .await;
    assert!(
        matches!(after, Some(VmCondition::Stopped { .. })),
        "{after:?}"
    );
    runtime.stop();
    runtime.wait().await;
}

#[tokio::test]
async fn a_start_reports_its_steps_in_order_and_a_retry_or_subscription_replays_them() {
    use crate::protocol::{EventSequence, OperationId, ProgressEvent};
    use crate::vm::StartStep;
    let installation = Installation::new();
    let runtime = installation.start().await;
    let mut client = installation.client().await;
    let operation = OperationId::mint();
    let body = RequestBody::Vm(VmRequest::Start {
        name: vm("watched"),
        shares: None,
        nix_config: None,
    });
    let mut seen = Vec::new();
    let reply = client
        .request_as(operation, body.clone(), |sequence, event| {
            seen.push((sequence, event));
        })
        .await
        .unwrap();
    assert!(
        matches!(reply, ReplyBody::Vm(VmReply::Started { .. })),
        "{reply:?}"
    );
    let steps = [
        StartStep::Building,
        StartStep::PreparingGuest,
        StartStep::Booting,
        StartStep::WaitingForSsh,
    ];
    let expected: Vec<(EventSequence, ProgressEvent)> = steps
        .iter()
        .enumerate()
        .map(|(index, step)| {
            (
                EventSequence::of_index(index).unwrap(),
                ProgressEvent::Start(*step),
            )
        })
        .collect();
    assert_eq!(seen, expected);

    // The same id again: the same answer and progress, and nothing runs a second time.
    let mut retried = Vec::new();
    let again = installation
        .client()
        .await
        .request_as(operation, body, |sequence, event| {
            retried.push((sequence, event));
        })
        .await
        .unwrap();
    assert_eq!(again, reply);
    assert_eq!(retried, expected);
    assert_eq!(
        journal_lines(&installation.paths, "vm/watched", "\"start_requested\"").len(),
        1
    );

    // Re-attached from the third step.
    let mut late = Vec::new();
    let subscribed = installation
        .client()
        .await
        .subscribe(operation, EventSequence::of_index(2), |sequence, event| {
            late.push((sequence, event));
        })
        .await
        .unwrap();
    assert_eq!(subscribed, reply);
    assert_eq!(late, expected[2..]);

    let unknown = installation
        .client()
        .await
        .subscribe(OperationId::mint(), None, |_, _| {})
        .await
        .unwrap();
    assert!(
        matches!(&unknown, ReplyBody::Failed(report) if report.code == "runtime.operation.unknown"),
        "{unknown:?}"
    );
    let stopped = request(
        &mut client,
        VmRequest::Stop {
            name: vm("watched"),
        },
    )
    .await;
    assert!(
        matches!(stopped, ReplyBody::Vm(VmReply::Stopped { .. })),
        "{stopped:?}"
    );
    runtime.stop();
    runtime.wait().await;
}
