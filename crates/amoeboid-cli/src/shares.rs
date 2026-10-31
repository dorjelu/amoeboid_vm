//! Share arguments, `PATH[:rw|:ro]`, resolved where the operator typed them: `~` expanded,
//! relative paths against the working directory, symlinks resolved. The runtime checks the rest.

#![allow(
    clippy::disallowed_types,
    reason = "boundary: refines paths the operator typed"
)]

use std::path::{Path, PathBuf};

use amoeboid::config::run::{NixConfigSource, PresetName};
use amoeboid::diagnostic::Report;
use amoeboid::domain::path::AbsolutePath;
use amoeboid::domain::share::{HostDirectory, ShareRequest, split_mode};

pub const HELP: &str = "PATH[:rw|:ro], e.g. ~/dev/app or ~/dev/libs:ro (default rw)";

fn failure(code: &str, message: String) -> Report {
    Report {
        code: code.into(),
        message,
        hint: Some(format!("give each share as {HELP}")),
    }
}

/// # Errors
///
/// An empty path, or one that doesn't resolve to an existing path.
pub fn parse(
    spec: &str,
    working_directory: &Path,
    home: Option<&Path>,
) -> Result<ShareRequest, Report> {
    let (raw, mode) = split_mode(spec);
    if raw.is_empty() {
        return Err(failure(
            "cli.share.empty",
            format!("empty path in share {spec:?}"),
        ));
    }
    let expanded = match (raw.strip_prefix('~'), home) {
        (Some(rest), Some(home)) if rest.is_empty() || rest.starts_with('/') => {
            home.join(rest.trim_start_matches('/'))
        }
        _ => PathBuf::from(raw),
    };
    let resolved = working_directory
        .join(expanded)
        .canonicalize()
        .map_err(|error| {
            failure(
                "cli.share.unresolvable",
                format!("can't share {raw}: {error}"),
            )
        })?;
    let absolute = AbsolutePath::parse(resolved)
        .map_err(|error| failure("cli.share.path", error.to_string()))?;
    let directory = HostDirectory::parse(absolute)
        .map_err(|error| failure("cli.share.path", error.to_string()))?;
    Ok(ShareRequest { directory, mode })
}

/// `$HOME`, for expanding `~`.
pub fn home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// `--base-nix-config`: a preset name (`default`, `rust`), or a directory, told apart as in the
/// run configuration: anything with a `/`, a leading `~`, or `.`/`..` is a directory, resolved
/// like a share.
///
/// # Errors
///
/// A bad preset name, or a directory that doesn't resolve.
pub fn nix_config_source(
    raw: &str,
    working_directory: &Path,
    home: Option<&Path>,
) -> Result<NixConfigSource, Report> {
    let is_path = raw.contains('/') || raw.starts_with('~') || raw == "." || raw == "..";
    if !is_path {
        return PresetName::parse(raw.to_owned())
            .map(NixConfigSource::Preset)
            .map_err(|error| Report::of(&error));
    }
    let expanded = match (raw.strip_prefix('~'), home) {
        (Some(rest), Some(home)) if rest.is_empty() || rest.starts_with('/') => {
            home.join(rest.trim_start_matches('/'))
        }
        _ => PathBuf::from(raw),
    };
    let resolved = working_directory
        .join(expanded)
        .canonicalize()
        .map_err(|error| Report {
            code: "cli.base_nix_config.unresolvable".into(),
            message: format!("can't use {raw} as a NixOS config: {error}"),
            hint: Some("give a preset name (see amoeboid presets) or a directory".into()),
        })?;
    AbsolutePath::parse(resolved)
        .map(NixConfigSource::Directory)
        .map_err(|error| Report {
            code: "cli.base_nix_config.path".into(),
            message: error.to_string(),
            hint: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use amoeboid::domain::share::Mode;

    #[test]
    fn modes_home_and_relative_paths_resolve() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("dev/app")).unwrap();
        let share = parse("dev/app:ro", &root, None).unwrap();
        assert_eq!(share.mode, Mode::ReadOnly);
        assert_eq!(
            share.directory.as_str(),
            root.join("dev/app").to_str().unwrap()
        );
        let share = parse("~/dev/app", Path::new("/"), Some(&root)).unwrap();
        assert_eq!(share.mode, Mode::ReadWrite);
        assert_eq!(
            share.directory.as_str(),
            root.join("dev/app").to_str().unwrap()
        );
    }

    #[test]
    fn a_colon_that_is_not_a_mode_stays_in_the_path_and_empty_or_missing_paths_fail() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("a:b")).unwrap();
        assert!(parse("a:b", &root, None).is_ok());
        assert_eq!(
            parse(":ro", &root, None).unwrap_err().code,
            "cli.share.empty"
        );
        assert_eq!(
            parse("missing", &root, None).unwrap_err().code,
            "cli.share.unresolvable"
        );
    }

    #[test]
    fn a_bare_name_is_a_preset_and_a_path_is_resolved_to_a_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("nix/mine")).unwrap();
        assert!(matches!(
            nix_config_source("rust", &root, None).unwrap(),
            NixConfigSource::Preset(name) if name.as_str() == "rust"
        ));
        assert!(matches!(
            nix_config_source("nix/mine", &root, None).unwrap(),
            NixConfigSource::Directory(path) if path.as_path() == root.join("nix/mine")
        ));
        assert!(matches!(
            nix_config_source("~/nix/mine", Path::new("/"), Some(&root)).unwrap(),
            NixConfigSource::Directory(path) if path.as_path() == root.join("nix/mine")
        ));
        assert!(nix_config_source("not a name", &root, None).is_err());
        assert!(nix_config_source("./absent", &root, None).is_err());
    }
}
