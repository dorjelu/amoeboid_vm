//! Hosts the amoeboid runtime under systemd user socket activation.

mod linger;

use std::process::ExitCode;

use amoeboid::domain::path::AbsolutePath;
use amoeboid::runtime::paths::{ConfigPaths, RuntimePaths};
use amoeboid::runtime::root::VmDefaults;
use amoeboid::runtime::{
    ActivatedSocket, DaemonConfigSource, Hosting, JournalLocation, Persistence, Runtime,
    RuntimeConfig,
};
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
    let journal = journal_argument(std::env::args().skip(1))?;
    let paths = RuntimePaths::from_env(std::env::var_os)?;
    let config = ConfigPaths::from_env(std::env::var_os)?;
    let listener = ActivatedSocket::from_env()?.ok_or(
        "not socket-activated: start amoeboid.socket, or run under systemd-socket-activate",
    )?;
    let account = Account::of_this_process(std::env::var_os)?;
    let daemon = amoeboid::config::daemon::load(&config)?;
    if daemon.linger {
        match linger::ensure(account.user.as_str()) {
            Ok(linger::Lingering::Already) => {}
            Ok(linger::Lingering::Enabled) => tracing::info!("enabled linger"),
            Err(error) => tracing::warn!(%error, "can't enable linger; VMs stop at logout"),
        }
    }
    let runtime = Runtime::start(RuntimeConfig {
        paths,
        config,
        hosting: Hosting::SystemdUser { listener },
        persistence: Persistence::Sqlite(
            journal.map_or(JournalLocation::Configured, JournalLocation::At),
        ),
        daemon: DaemonConfigSource::Given(daemon),
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

/// `--journal PATH`: where the journal lives, ahead of `daemon_config.toml`. Long options only.
fn journal_argument(
    mut arguments: impl Iterator<Item = String>,
) -> Result<Option<AbsolutePath>, Box<dyn std::error::Error>> {
    let mut journal = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--journal" => {
                let path = arguments.next().ok_or("--journal needs a path")?;
                journal = Some(
                    AbsolutePath::parse(path.into())
                        .map_err(|error| format!("--journal: {error}"))?,
                );
            }
            other => {
                return Err(format!(
                    "unknown argument {other:?}; amoeboid-daemon takes only --journal PATH"
                )
                .into());
            }
        }
    }
    Ok(journal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(words: &[&str]) -> impl Iterator<Item = String> {
        words
            .iter()
            .map(|word| (*word).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn the_journal_argument_is_optional_absolute_and_the_only_one() {
        assert!(journal_argument(arguments(&[])).unwrap().is_none());
        let path = journal_argument(arguments(&["--journal", "/srv/journal.sqlite"]))
            .unwrap()
            .unwrap();
        assert_eq!(path.to_str(), Some("/srv/journal.sqlite"));
        for bad in [
            &["--journal"][..],
            &["--journal", "relative.sqlite"],
            &["-j", "/srv/x"],
        ] {
            assert!(journal_argument(arguments(bad)).is_err(), "{bad:?}");
        }
    }
}
