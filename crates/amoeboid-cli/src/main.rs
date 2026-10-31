//! `amoeboid`: the operator's client for the amoeboid runtime.

mod render;

use std::process::ExitCode;

use amoeboid::client::Client;
use amoeboid::diagnostic::{Diagnostic, Report};
use amoeboid::protocol::{ReplyBody, RequestBody, RuntimeReply, RuntimeRequest};
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
            let mut client = Client::connect(&paths)
                .await
                .map_err(|error| report(&error))?;
            match client
                .request(RequestBody::Runtime(RuntimeRequest::Ping))
                .await
                .map_err(|error| report(&error))?
            {
                ReplyBody::Runtime(RuntimeReply::Pong) => {
                    render::line(&format!("pong from runtime {}", client.runtime()));
                    Ok(())
                }
                ReplyBody::Failed(failure) => Err(failure),
                other @ ReplyBody::Vm(_) => Err(Report {
                    code: "cli.unexpected_reply".into(),
                    message: format!("unexpected reply from the runtime: {other:?}"),
                    hint: None,
                }),
            }
        }
    }
}
