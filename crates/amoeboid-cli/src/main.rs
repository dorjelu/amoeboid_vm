//! `amoeboid`: the operator's client for the amoeboid runtime.

mod configuration;
mod daemon;
mod render;
mod shares;

use std::process::ExitCode;

use amoeboid::client::Client;
use amoeboid::diagnostic::{Diagnostic, Report};
use amoeboid::domain::vm_name::VmName;
use amoeboid::protocol::{
    ReplyBody, RequestBody, RuntimeReply, RuntimeRequest, VmReply, VmRequest,
};
use amoeboid::runtime::paths::RuntimePaths;
use amoeboid::runtime::quarantine::QuarantineCause;
use amoeboid::vm::{StartStep, VmCondition};
use clap::{Parser, Subcommand};

/// Build and run NixOS microVMs for coding agents against chosen host directories.
#[derive(Parser)]
#[command(name = "amoeboid", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check that the runtime answers.
    Ping,
    /// Start a VM (creating it on first use) sharing the given directories, and print how to
    /// connect to it once its sshd answers.
    Start {
        /// The VM's name: letters, digits and '-', starting with a letter or digit.
        #[arg(long, default_value = "default")]
        name: String,
        /// Create the VM's NixOS configuration from this preset (see `amoeboid presets`) or
        /// directory, or replace it (rebase). Without it, an existing VM keeps its own and a new
        /// one is created from the base config set in the run configuration.
        #[arg(long, value_name = "PRESET|DIRECTORY")]
        base_nix_config: Option<String>,
        /// Directories to share into the VM, each PATH[:rw|:ro] (default rw). Without any, an
        /// existing VM shares what it shared last, and a new one the default shares set in the
        /// run configuration.
        #[arg(value_name = "SHARE")]
        shares: Vec<String>,
    },
    /// Power a VM off: the guest is asked first, then stopped by force after a grace period.
    /// Also takes down what a failed start left running, and cancels a start in progress.
    Stop {
        /// The VM's name.
        #[arg(long, default_value = "default")]
        name: String,
    },
    /// Manage the runtime that runs the VMs.
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// Manage `run_config.toml`, which the runtime reads at every start.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// List the NixOS config presets new VMs can be created from: directories under the
    /// configuration directory's `nix_presets`, each a flake. `default` is created from the
    /// bundled config the first time it is needed.
    Presets,
    /// Show where each VM is (or one, with --name).
    Status {
        /// Only this VM.
        #[arg(long)]
        name: Option<String>,
    },
    /// Remove a VM and everything it owns: stop it, end any of its processes still running,
    /// delete its directories (its Nix config, host key, guest files and logs) and retire its
    /// history. Works on a quarantined VM. The shared directories are not touched.
    #[command(name = "vacuum_vm")]
    VacuumVm {
        /// The VM's name.
        name: String,
    },
    /// Retry a VM quarantined because its runtime kept failing: lift the quarantine and start
    /// its runtime again with a fresh restart budget. The VM itself is not started or stopped.
    #[command(name = "release_vm")]
    ReleaseVm {
        /// The VM's name.
        name: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Write `run_config.toml` with the defaults, and your git config among the files copied into
    /// each VM's home if you have one. Refuses to replace an existing file.
    Init,
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Write the systemd user units (amoeboid.socket, amoeboid.service) and enable the socket,
    /// so the runtime starts on the first connection and is restarted if it fails.
    Install,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            render::error(&report);
            ExitCode::FAILURE
        }
    }
}

/// Every failure, local or from the runtime, as the operator sees it.
fn report(error: &impl Diagnostic) -> Report {
    Report::of(error)
}

async fn run(command: Command) -> Result<(), Report> {
    match command {
        Command::Daemon {
            action: DaemonAction::Install,
        } => {
            for unit in daemon::install()? {
                render::line(&format!("wrote {unit}"));
            }
            render::line("enabled amoeboid.socket; the runtime starts on the first connection");
            Ok(())
        }
        Command::Ping => {
            let mut client = connect(&runtime_paths()?).await?;
            match request(&mut client, RequestBody::Runtime(RuntimeRequest::Ping)).await? {
                ReplyBody::Runtime(RuntimeReply::Pong) => {
                    render::line(&format!("pong from runtime {}", client.runtime()));
                    Ok(())
                }
                ReplyBody::Failed(failure) => Err(failure),
                other @ (ReplyBody::Vm(_) | ReplyBody::Operation(_)) => Err(unexpected(&other)),
            }
        }
        Command::Config {
            action: ConfigAction::Init,
        } => {
            let written = configuration::init()?;
            render::line(&format!("wrote {written}"));
            Ok(())
        }
        Command::Presets => {
            for line in configuration::presets()? {
                render::line(&line);
            }
            Ok(())
        }
        Command::Stop { name } => stop(&runtime_paths()?, name).await,
        Command::Status { name } => status(&runtime_paths()?, name).await,
        Command::VacuumVm { name } => vacuum(&runtime_paths()?, name).await,
        Command::ReleaseVm { name } => release(&runtime_paths()?, name).await,
        Command::Start {
            name,
            base_nix_config,
            shares,
        } => {
            let name = VmName::parse(name).map_err(|error| report(&error))?;
            let working_directory = std::env::current_dir().map_err(|error| Report {
                code: "cli.working_directory".into(),
                message: format!("can't read the working directory: {error}"),
                hint: None,
            })?;
            let home = shares::home();
            let shares = if shares.is_empty() {
                None
            } else {
                Some(
                    shares
                        .iter()
                        .map(|spec| shares::parse(spec, &working_directory, home.as_deref()))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            };
            let nix_config = base_nix_config
                .as_deref()
                .map(|raw| shares::nix_config_source(raw, &working_directory, home.as_deref()))
                .transpose()?;
            let mut client = connect(&runtime_paths()?).await?;
            render::line(&format!(
                "starting '{name}' (this builds it first; it can take a while)"
            ));
            let body = RequestBody::Vm(VmRequest::Start {
                name,
                shares,
                nix_config,
            });
            match request(&mut client, body).await? {
                ReplyBody::Vm(VmReply::Started { name, ssh }) => {
                    let argv = amoeboid::plan::argv::ssh(
                        &name,
                        &ssh.user,
                        ssh.port,
                        &ssh.known_hosts,
                        &[],
                    );
                    render::line(&format!("'{name}' is running; connect with:"));
                    render::line(&format!("  {}", argv.shell_line()));
                    Ok(())
                }
                ReplyBody::Failed(failure) => Err(failure),
                other @ (ReplyBody::Runtime(_)
                | ReplyBody::Operation(_)
                | ReplyBody::Vm(
                    VmReply::Stopped { .. }
                    | VmReply::Status { .. }
                    | VmReply::Vacuumed { .. }
                    | VmReply::Released { .. },
                )) => Err(unexpected(&other)),
            }
        }
    }
}

async fn stop(paths: &RuntimePaths, name: String) -> Result<(), Report> {
    let name = VmName::parse(name).map_err(|error| report(&error))?;
    let mut client = connect(paths).await?;
    render::line(&format!("stopping '{name}'"));
    match request(&mut client, RequestBody::Vm(VmRequest::Stop { name })).await? {
        ReplyBody::Vm(VmReply::Stopped { name }) => {
            render::line(&format!("'{name}' is stopped"));
            Ok(())
        }
        ReplyBody::Failed(failure) => Err(failure),
        other @ (ReplyBody::Runtime(_)
        | ReplyBody::Operation(_)
        | ReplyBody::Vm(
            VmReply::Started { .. }
            | VmReply::Status { .. }
            | VmReply::Vacuumed { .. }
            | VmReply::Released { .. },
        )) => Err(unexpected(&other)),
    }
}

async fn release(paths: &RuntimePaths, name: String) -> Result<(), Report> {
    let name = VmName::parse(name).map_err(|error| report(&error))?;
    let mut client = connect(paths).await?;
    match request(&mut client, RequestBody::Vm(VmRequest::Release { name })).await? {
        ReplyBody::Vm(VmReply::Released { name }) => {
            render::line(&format!(
                "'{name}' is released; see amoeboid status for its condition"
            ));
            Ok(())
        }
        ReplyBody::Failed(failure) => Err(failure),
        other @ (ReplyBody::Runtime(_)
        | ReplyBody::Operation(_)
        | ReplyBody::Vm(
            VmReply::Started { .. }
            | VmReply::Stopped { .. }
            | VmReply::Status { .. }
            | VmReply::Vacuumed { .. },
        )) => Err(unexpected(&other)),
    }
}

async fn vacuum(paths: &RuntimePaths, name: String) -> Result<(), Report> {
    let name = VmName::parse(name).map_err(|error| report(&error))?;
    let mut client = connect(paths).await?;
    render::line(&format!("vacuuming '{name}'"));
    match request(&mut client, RequestBody::Vm(VmRequest::Vacuum { name })).await? {
        ReplyBody::Vm(VmReply::Vacuumed { name }) => {
            render::line(&format!("'{name}' is removed"));
            Ok(())
        }
        ReplyBody::Failed(failure) => Err(failure),
        other @ (ReplyBody::Runtime(_)
        | ReplyBody::Operation(_)
        | ReplyBody::Vm(
            VmReply::Started { .. }
            | VmReply::Stopped { .. }
            | VmReply::Status { .. }
            | VmReply::Released { .. },
        )) => Err(unexpected(&other)),
    }
}

async fn status(paths: &RuntimePaths, name: Option<String>) -> Result<(), Report> {
    let name = name
        .map(VmName::parse)
        .transpose()
        .map_err(|error| report(&error))?;
    let mut client = connect(paths).await?;
    match request(&mut client, RequestBody::Vm(VmRequest::Status { name })).await? {
        ReplyBody::Vm(VmReply::Status { vms }) => {
            if vms.is_empty() {
                render::line("no VMs yet; create one with amoeboid start");
            }
            for vm in vms {
                render::line(&format!(
                    "{:<20} {}",
                    vm.name.as_str(),
                    describe(&vm.condition)
                ));
            }
            Ok(())
        }
        ReplyBody::Failed(failure) => Err(failure),
        other @ (ReplyBody::Runtime(_)
        | ReplyBody::Operation(_)
        | ReplyBody::Vm(
            VmReply::Started { .. }
            | VmReply::Stopped { .. }
            | VmReply::Vacuumed { .. }
            | VmReply::Released { .. },
        )) => Err(unexpected(&other)),
    }
}

fn describe(condition: &VmCondition) -> String {
    match condition {
        VmCondition::Stopped { last_failure: None } => "stopped".into(),
        VmCondition::Stopped {
            last_failure: Some(failure),
        } => format!("stopped ({})", failure.message),
        VmCondition::Starting { step } => format!(
            "starting: {}",
            match step {
                StartStep::Building => "building",
                StartStep::PreparingGuest => "preparing the guest",
                StartStep::Booting => "booting",
                StartStep::WaitingForSsh => "waiting for sshd",
            }
        ),
        VmCondition::Running {
            user,
            port,
            generation,
        } => format!(
            "running: {}@127.0.0.1 port {port} (start {generation})",
            user.as_str()
        ),
        VmCondition::Stopping => "stopping".into(),
        VmCondition::Failed {
            reason,
            leftovers: 0,
        } => format!("failed: {}", reason.message),
        VmCondition::Failed { reason, leftovers } => format!(
            "failed: {}; {leftovers} processes still running (amoeboid stop takes them down)",
            reason.message
        ),
        VmCondition::Unknown => "unknown: its twin didn't answer".into(),
        VmCondition::Restarting { reason, attempt } => {
            format!("restarting (attempt {attempt}): {}", reason.message)
        }
        VmCondition::Quarantined {
            cause: cause @ QuarantineCause::SuspiciousHalt { .. },
        } => format!(
            "quarantined: {}; kept for inspection (amoeboid release_vm retries it, \
             amoeboid vacuum_vm removes it)",
            cause.summary()
        ),
        VmCondition::Quarantined {
            cause: cause @ QuarantineCause::Unreplayable { .. },
        } => format!(
            "quarantined: {}; kept for inspection (amoeboid vacuum_vm removes it)",
            cause.summary()
        ),
    }
}

fn runtime_paths() -> Result<RuntimePaths, Report> {
    RuntimePaths::from_env(std::env::var_os).map_err(|error| report(&error))
}

async fn connect(paths: &RuntimePaths) -> Result<Client, Report> {
    Client::connect(paths).await.map_err(|error| report(&error))
}

async fn request(client: &mut Client, body: RequestBody) -> Result<ReplyBody, Report> {
    client.request(body).await.map_err(|error| report(&error))
}

fn unexpected(reply: &ReplyBody) -> Report {
    Report {
        code: "cli.unexpected_reply".into(),
        message: format!("unexpected reply from the runtime: {reply:?}"),
        hint: None,
    }
}
