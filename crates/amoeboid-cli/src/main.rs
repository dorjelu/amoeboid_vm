//! `amoeboid`: the operator's client for the amoeboid runtime.

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
        /// Create the VM's NixOS configuration from this directory, or replace it (rebase).
        /// Without it, an existing VM keeps its own and a new one gets the bundled one.
        #[arg(long, value_name = "DIRECTORY")]
        base_nix_config: Option<String>,
        /// Directories to share into the VM, each PATH[:rw|:ro] (default rw).
        #[arg(value_name = "SHARE")]
        shares: Vec<String>,
    },
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
    let paths = RuntimePaths::from_env(std::env::var_os).map_err(|error| report(&error))?;
    match command {
        Command::Ping => {
            let mut client = connect(&paths).await?;
            match request(&mut client, RequestBody::Runtime(RuntimeRequest::Ping)).await? {
                ReplyBody::Runtime(RuntimeReply::Pong) => {
                    render::line(&format!("pong from runtime {}", client.runtime()));
                    Ok(())
                }
                ReplyBody::Failed(failure) => Err(failure),
                other @ ReplyBody::Vm(_) => Err(unexpected(&other)),
            }
        }
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
            let shares = shares
                .iter()
                .map(|spec| shares::parse(spec, &working_directory, home.as_deref()))
                .collect::<Result<Vec<_>, _>>()?;
            let nix_config = base_nix_config
                .as_deref()
                .map(|raw| shares::config_directory(raw, &working_directory))
                .transpose()?;
            let mut client = connect(&paths).await?;
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
                other @ ReplyBody::Runtime(_) => Err(unexpected(&other)),
            }
        }
    }
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
