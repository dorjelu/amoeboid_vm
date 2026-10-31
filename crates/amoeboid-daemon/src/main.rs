//! Hosts the amoeboid runtime under systemd user socket activation.

use std::process::ExitCode;

use amoeboid::journal::Persistence;
use amoeboid::runtime::paths::RuntimePaths;
use amoeboid::runtime::root::VmDefaults;
use amoeboid::runtime::{ActivatedSocket, Hosting, Runtime, RuntimeConfig};
use amoeboid::vm::Account;
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() -> ExitCode {
    // Logs go to stderr; under systemd that is the journal.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("AMOEBOID_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "amoeboid-daemon failed");
            ExitCode::FAILURE
        }
    }
}

/// `Ok` after a stop signal; `Err` when the runtime can't start or ends by itself, so systemd's
/// `Restart=on-failure` brings it back.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let paths = RuntimePaths::from_env(std::env::var_os)?;
    let listener = ActivatedSocket::from_env()?.ok_or(
        "not socket-activated: start amoeboid.socket, or run under systemd-socket-activate",
    )?;
    let account = Account::of_this_process(std::env::var_os)?;
    let runtime = Runtime::start(RuntimeConfig {
        paths,
        hosting: Hosting::SystemdUser { listener },
        persistence: Persistence::Sqlite,
        version: env!("CARGO_PKG_VERSION").to_owned(),
        account,
        vm_defaults: VmDefaults::production(),
        clock: amoeboid::clock::Clock::System,
    })
    .await?;
    let stopper = runtime.stopper();
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let wait = runtime.wait();
    tokio::pin!(wait);
    tokio::select! {
        () = &mut wait => return Err("the runtime ended by itself".into()),
        _ = terminate.recv() => tracing::info!("SIGTERM: stopping"),
        _ = interrupt.recv() => tracing::info!("SIGINT: stopping"),
    }
    stopper.stop();
    wait.await;
    Ok(())
}
