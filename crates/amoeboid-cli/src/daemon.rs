//! `amoeboid daemon install`: writes the systemd user units next to the operator's other user
//! units and enables the socket, so the runtime starts on the first CLI connection.

#![allow(
    clippy::disallowed_types,
    reason = "boundary: locates binaries and unit directories"
)]

use std::path::PathBuf;
use std::process::Command;

use amoeboid::adapters::files;
use amoeboid::config::daemon::DaemonConfig;
use amoeboid::diagnostic::Report;
use amoeboid::plan::units;
use amoeboid::runtime::paths::ConfigPaths;

fn failure(code: &str, message: String, hint: Option<&str>) -> Report {
    Report {
        code: code.into(),
        message,
        hint: hint.map(str::to_owned),
    }
}

/// `amoeboid-daemon` next to this binary.
fn daemon_binary() -> Result<PathBuf, Report> {
    let cli = std::env::current_exe().map_err(|error| {
        failure(
            "cli.daemon.locate",
            format!("can't locate this binary: {error}"),
            None,
        )
    })?;
    let daemon = cli.with_file_name("amoeboid-daemon");
    if daemon.is_file() {
        Ok(daemon)
    } else {
        Err(failure(
            "cli.daemon.missing",
            format!("there is no amoeboid-daemon at {}", daemon.display()),
            Some("install amoeboid-daemon next to amoeboid"),
        ))
    }
}

/// `$XDG_CONFIG_HOME/systemd/user`, else `~/.config/systemd/user`.
fn unit_directory() -> Result<PathBuf, Report> {
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or_else(|| failure("cli.daemon.home", "HOME is not set".into(), None))?;
    Ok(config.join("systemd/user"))
}

fn systemctl(arguments: &[&str]) -> Result<(), Report> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(arguments)
        .status()
        .map_err(|error| {
            failure(
                "cli.daemon.systemctl",
                format!("can't run systemctl: {error}"),
                None,
            )
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(failure(
            "cli.daemon.systemctl",
            format!("systemctl --user {} failed ({status})", arguments.join(" ")),
            Some("check that a systemd user session is running"),
        ))
    }
}

/// Writes both units, reloads the user manager and enables the socket.
///
/// # Errors
///
/// The daemon binary is missing, a unit can't be written, or systemctl fails.
pub fn install() -> Result<Vec<String>, Report> {
    let daemon = daemon_binary()?;
    let daemon = daemon.to_str().ok_or_else(|| {
        failure(
            "cli.daemon.path",
            format!("{} is not valid UTF-8", daemon.display()),
            None,
        )
    })?;
    let path = std::env::var("PATH").unwrap_or_default();
    let directory = unit_directory()?;
    std::fs::create_dir_all(&directory).map_err(|error| {
        failure(
            "cli.daemon.write",
            format!("can't create {}: {error}", directory.display()),
            None,
        )
    })?;
    let mut written = Vec::new();
    for (name, content) in [
        (units::SOCKET_NAME, units::socket()),
        (units::SERVICE_NAME, units::service(daemon, &path)),
    ] {
        let target = directory.join(name);
        std::fs::write(&target, content).map_err(|error| {
            failure(
                "cli.daemon.write",
                format!("can't write {}: {error}", target.display()),
                None,
            )
        })?;
        written.push(target.display().to_string());
    }
    if let Some(config) = seed_daemon_config()? {
        written.push(config);
    }
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", "--now", units::SOCKET_NAME])?;
    Ok(written)
}

/// Writes `daemon_config.toml` with the defaults unless it exists; the written path, if any.
fn seed_daemon_config() -> Result<Option<String>, Report> {
    let paths = ConfigPaths::from_env(std::env::var_os).map_err(|error| Report::of(&error))?;
    let target = paths.daemon_config();
    if target.as_path().exists() {
        return Ok(None);
    }
    files::ensure_directory(paths.directory(), 0o700).map_err(|error| Report::of(&error))?;
    let text = amoeboid::config::daemon::encode(&DaemonConfig::default(), paths.home());
    files::write_atomically(&target, text.as_bytes(), 0o600).map_err(|error| Report::of(&error))?;
    Ok(Some(target.display().to_string()))
}
