//! `amoeboid config init` and `amoeboid presets`: the operator's configuration directory, read
//! and written where the operator runs the command (the runtime reads it too: same user).

#![allow(
    clippy::disallowed_types,
    reason = "boundary: lists and seeds the configuration directory"
)]

use amoeboid::adapters::files;
use amoeboid::config::run::{self, PresetName, RunConfig};
use amoeboid::diagnostic::Report;
use amoeboid::domain::relative_path::RelativePath;
use amoeboid::runtime::paths::ConfigPaths;

fn config_paths() -> Result<ConfigPaths, Report> {
    ConfigPaths::from_env(std::env::var_os).map_err(|error| Report::of(&error))
}

/// Writes `run_config.toml` with the defaults, seeding the home files with the git config when
/// one exists. Never replaces an existing file.
///
/// # Errors
///
/// The file exists, or can't be written.
pub fn init() -> Result<String, Report> {
    let paths = config_paths()?;
    let target = paths.run_config();
    if target.as_path().exists() {
        return Err(Report {
            code: "cli.config.exists".into(),
            message: format!("{} already exists", target.display()),
            hint: Some("edit it; the runtime reads it at every start".into()),
        });
    }
    let mut config = RunConfig::default();
    config.guest.home_files = [".gitconfig", ".config/git/config"]
        .into_iter()
        .filter(|file| paths.home().as_path().join(file).is_file())
        .filter_map(|file| RelativePath::parse(file).ok())
        .collect();
    files::ensure_directory(paths.directory(), 0o700).map_err(|error| Report::of(&error))?;
    let text = run::encode(&config, paths.home());
    files::write_atomically(&target, text.as_bytes(), 0o600).map_err(|error| Report::of(&error))?;
    Ok(target.display().to_string())
}

/// One line per preset, sorted; `default` is listed even before it is created.
///
/// # Errors
///
/// The presets directory exists but can't be read.
pub fn presets() -> Result<Vec<String>, Report> {
    let paths = config_paths()?;
    let directory = paths.presets();
    let mut names: Vec<String> = match std::fs::read_dir(directory.as_path()) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| PresetName::parse(name.clone()).is_ok())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(Report {
                code: "cli.presets.read".into(),
                message: format!("can't read {}: {error}", directory.display()),
                hint: None,
            });
        }
    };
    names.sort();
    let default = PresetName::default_preset();
    let mut lines: Vec<String> = names
        .iter()
        .map(|name| {
            let flake = if directory.as_path().join(name).join("flake.nix").is_file() {
                ""
            } else {
                "  (no flake.nix: can't be built)"
            };
            format!("{name}{flake}")
        })
        .collect();
    if !names.iter().any(|name| name == default.as_str()) {
        lines.insert(
            0,
            format!(
                "{}  (created from the bundled config on first use)",
                default.as_str()
            ),
        );
    }
    lines.push(format!("in {}", directory.display()));
    Ok(lines)
}
