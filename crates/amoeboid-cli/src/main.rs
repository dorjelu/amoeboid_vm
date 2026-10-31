//! `amoeboid`: the operator's client for the amoeboid runtime.

mod render;

use std::process::ExitCode;

use amoeboid::client::{Client, ClientError};
use amoeboid::diagnostic::Diagnostic;
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
        Err(error) => {
            render::error(&error);
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error(transparent)]
    Paths(#[from] amoeboid::runtime::paths::PathError),
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl Diagnostic for CliError {
    fn code(&self) -> &'static str {
        match self {
            Self::Paths(inner) => inner.code(),
            Self::Client(inner) => inner.code(),
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Paths(inner) => inner.hint(),
            Self::Client(inner) => inner.hint(),
        }
    }
}

async fn run(command: Command) -> Result<(), CliError> {
    let paths = RuntimePaths::from_env(std::env::var_os)?;
    match command {
        Command::Ping => {
            let mut client = Client::connect(&paths).await?;
            let ReplyBody::Runtime(RuntimeReply::Pong) = client
                .request(RequestBody::Runtime(RuntimeRequest::Ping))
                .await?;
            render::line(&format!("pong from runtime {}", client.runtime()));
            Ok(())
        }
    }
}
