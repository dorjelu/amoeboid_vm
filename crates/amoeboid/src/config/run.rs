//! `run_config.toml`: what a start uses, read by Root at every start (0003 §Configuration).
//!
//! ```toml
//! [guest]
//! user = "alice"                 # optional: default, the account the runtime runs as
//! home_files = [".gitconfig"]    # under ~, copied into the guest's home on every start
//!
//! [new_vm]
//! base_nix_config = "default"    # a preset name, or a directory: ~/… or absolute
//! default_shares = ["~/dev/app:rw"]
//!
//! [ssh]
//! port = 0                       # 0: a free loopback port per start
//!
//! [timeouts]
//! boot_seconds = 120
//! powerdown_seconds = 30
//! ```

use std::num::NonZeroU16;

use toml::{Table, Value};

use super::{ConfigErrors, ConfigFile, Decoding, Seconds};
use crate::domain::account::UserName;
use crate::domain::path::AbsolutePath;
use crate::domain::port::LoopbackPort;
use crate::domain::relative_path::RelativePath;
use crate::domain::share::{HostDirectory, ShareRequest, split_mode};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RunConfig {
    pub guest: GuestConfig,
    pub new_vm: NewVmConfig,
    pub ssh: SshConfig,
    pub timeouts: TimeoutsConfig,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GuestConfig {
    /// `None`: the name of the account the runtime runs as.
    pub user: Option<UserName>,
    /// Under the host user's home; copied into the guest's home on every start.
    pub home_files: Vec<RelativePath>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NewVmConfig {
    /// What a new VM's NixOS config is created from.
    pub base_nix_config: NixConfigSource,
    /// What a new VM started without shares shares.
    pub default_shares: Vec<ShareRequest>,
}

/// Where a VM's NixOS config comes from: a preset (a directory under the presets directory) or
/// any directory.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NixConfigSource {
    Preset(PresetName),
    Directory(AbsolutePath),
}

/// `[A-Za-z0-9._-]{1,64}`, not `.` or `..`: one directory name, never a path.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct PresetName(String);

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("{0:?} is not a preset name: letters, digits, '.', '_', '-', at most 64")]
pub struct BadPresetName(String);

impl PresetName {
    /// The preset every installation has: seeded from the bundled config.
    #[must_use]
    pub fn default_preset() -> Self {
        Self("default".into())
    }

    /// # Errors
    ///
    /// Empty, longer than 64, `.`/`..`, or another character.
    pub fn parse(name: String) -> Result<Self, BadPresetName> {
        let valid = !name.is_empty()
            && name.len() <= 64
            && name != "."
            && name != ".."
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
        if valid {
            Ok(Self(name))
        } else {
            Err(BadPresetName(name))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PresetName {
    type Error = BadPresetName;
    fn try_from(name: String) -> Result<Self, BadPresetName> {
        Self::parse(name)
    }
}

impl From<PresetName> for String {
    fn from(name: PresetName) -> Self {
        name.0
    }
}

impl crate::diagnostic::Diagnostic for BadPresetName {
    fn code(&self) -> &'static str {
        "config.preset_name.invalid"
    }
    fn hint(&self) -> Option<String> {
        Some("see the presets with amoeboid presets".into())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SshConfig {
    /// `None`: a free loopback port per start (written as 0).
    pub port: Option<LoopbackPort>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TimeoutsConfig {
    /// From qemu starting to the guest's sshd answering; 1..86400.
    pub boot: Seconds,
    /// How long the guest gets to power off before qemu is terminated; 1..3600.
    pub powerdown: Seconds,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            guest: GuestConfig {
                user: None,
                home_files: Vec::new(),
            },
            new_vm: NewVmConfig {
                base_nix_config: NixConfigSource::Preset(PresetName::default_preset()),
                default_shares: Vec::new(),
            },
            ssh: SshConfig { port: None },
            timeouts: TimeoutsConfig {
                boot: Seconds::new(120),
                powerdown: Seconds::new(30),
            },
        }
    }
}

const HEADER: &str = "# amoeboid run configuration. The runtime reads it at every start; a change \
                      applies to the next one.\n# Paths: ~/… is your home, or absolute.";

/// # Errors
///
/// Every problem in the file: syntax, unknown keys, bad values.
pub fn decode(text: &str, home: &AbsolutePath) -> Result<RunConfig, ConfigErrors> {
    let mut root = super::parse(ConfigFile::Run, text)?;
    let mut decoding = Decoding::new();
    let defaults = RunConfig::default();

    let mut guest = decoding.table(&mut root, "", "guest");
    let user = decoding.optional(&mut guest, "guest", "user", |value| {
        super::string(value)
            .and_then(|name| UserName::parse(name).map_err(|error| error.to_string()))
    });
    let home_files = decoding.list(&mut guest, "guest", "home_files", |value| {
        super::string(value).and_then(|path| {
            RelativePath::parse(&path).map_err(|_| {
                format!("{path:?} must be a path under your home, written without ~ or a leading /")
            })
        })
    });
    decoding.unknown("guest", &guest, &["user", "home_files"]);

    let mut new_vm = decoding.table(&mut root, "", "new_vm");
    let base_nix_config = decoding.value(
        &mut new_vm,
        "new_vm",
        "base_nix_config",
        defaults.new_vm.base_nix_config.clone(),
        |value| super::string(value).and_then(|text| nix_config_source(&text, home)),
    );
    let default_shares = decoding.list(&mut new_vm, "new_vm", "default_shares", |value| {
        super::string(value).and_then(|text| share(&text, home))
    });
    decoding.unknown("new_vm", &new_vm, &["base_nix_config", "default_shares"]);

    let mut ssh = decoding.table(&mut root, "", "ssh");
    let port = decoding.value(&mut ssh, "ssh", "port", None, |value| {
        super::integer(value, 0, u32::from(u16::MAX)).map(|port| {
            u16::try_from(port)
                .ok()
                .and_then(NonZeroU16::new)
                .map(LoopbackPort::new)
        })
    });
    decoding.unknown("ssh", &ssh, &["port"]);

    let mut timeouts = decoding.table(&mut root, "", "timeouts");
    let boot = decoding.value(
        &mut timeouts,
        "timeouts",
        "boot_seconds",
        defaults.timeouts.boot,
        |value| super::seconds(value, 1, 86_400),
    );
    let powerdown = decoding.value(
        &mut timeouts,
        "timeouts",
        "powerdown_seconds",
        defaults.timeouts.powerdown,
        |value| super::seconds(value, 1, 3_600),
    );
    decoding.unknown(
        "timeouts",
        &timeouts,
        &["boot_seconds", "powerdown_seconds"],
    );

    decoding.unknown("", &root, &["guest", "new_vm", "ssh", "timeouts"]);
    decoding.finish(
        ConfigFile::Run,
        RunConfig {
            guest: GuestConfig { user, home_files },
            new_vm: NewVmConfig {
                base_nix_config,
                default_shares,
            },
            ssh: SshConfig { port },
            timeouts: TimeoutsConfig { boot, powerdown },
        },
    )
}

/// `run_config.toml`, or the defaults when there is none.
///
/// # Errors
///
/// It can't be read, or it is invalid.
pub fn load(paths: &crate::runtime::paths::ConfigPaths) -> Result<RunConfig, super::LoadError> {
    match super::read(&paths.run_config())? {
        None => Ok(RunConfig::default()),
        Some(text) => Ok(decode(&text, paths.home())?),
    }
}

/// Canonical text: every key written, paths under `home` as `~/…`, modes explicit.
#[must_use]
pub fn encode(config: &RunConfig, home: &AbsolutePath) -> String {
    let path = |path: &AbsolutePath| super::write_home_path(path, home).unwrap_or_default();
    let mut guest = Table::new();
    if let Some(user) = &config.guest.user {
        guest.insert("user".into(), Value::String(user.as_str().into()));
    }
    guest.insert(
        "home_files".into(),
        Value::Array(
            config
                .guest
                .home_files
                .iter()
                .map(|file| Value::String(file.as_str().into()))
                .collect(),
        ),
    );
    let mut new_vm = Table::new();
    new_vm.insert(
        "base_nix_config".into(),
        Value::String(match &config.new_vm.base_nix_config {
            NixConfigSource::Preset(name) => name.as_str().into(),
            NixConfigSource::Directory(directory) => path(directory),
        }),
    );
    new_vm.insert(
        "default_shares".into(),
        Value::Array(
            config
                .new_vm
                .default_shares
                .iter()
                .map(|share| {
                    Value::String(format!(
                        "{}:{}",
                        path(share.directory.path()),
                        share.mode.as_str()
                    ))
                })
                .collect(),
        ),
    );
    let mut ssh = Table::new();
    ssh.insert(
        "port".into(),
        Value::Integer(config.ssh.port.map_or(0, |port| i64::from(port.get()))),
    );
    let mut timeouts = Table::new();
    timeouts.insert(
        "boot_seconds".into(),
        Value::Integer(i64::from(config.timeouts.boot.get())),
    );
    timeouts.insert(
        "powerdown_seconds".into(),
        Value::Integer(i64::from(config.timeouts.powerdown.get())),
    );
    let mut root = Table::new();
    root.insert("guest".into(), Value::Table(guest));
    root.insert("new_vm".into(), Value::Table(new_vm));
    root.insert("ssh".into(), Value::Table(ssh));
    root.insert("timeouts".into(), Value::Table(timeouts));
    super::write(HEADER, &root)
}

/// A preset name, or a directory written `~/…` or absolute (anything with a `/` or a leading
/// `~`).
fn nix_config_source(text: &str, home: &AbsolutePath) -> Result<NixConfigSource, String> {
    if text.starts_with('~') || text.contains('/') {
        super::home_path(text, home).map(NixConfigSource::Directory)
    } else {
        PresetName::parse(text.to_owned())
            .map(NixConfigSource::Preset)
            .map_err(|error| error.to_string())
    }
}

/// `PATH[:rw|:ro]`, the same syntax as on the command line, but `~/…` or absolute only: a
/// configuration file has no working directory.
fn share(text: &str, home: &AbsolutePath) -> Result<ShareRequest, String> {
    let (path, mode) = split_mode(text);
    let directory = super::home_path(path, home)
        .and_then(|path| HostDirectory::parse(path).map_err(|error| error.to_string()))?;
    Ok(ShareRequest { directory, mode })
}
