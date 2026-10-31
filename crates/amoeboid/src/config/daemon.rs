//! `daemon_config.toml`: runtime-wide settings, read once when the runtime starts
//! (0003 §Configuration).
//!
//! ```toml
//! linger = true                  # keep the user manager (runtime and VMs) running after logout
//!
//! [journal]
//! path = "~/.local/state/amoeboid/journal.sqlite"   # optional: the runtime's default otherwise
//!
//! [restart]
//! max_restarts = 5
//! window_seconds = 600
//! backoff_first_seconds = 1
//! backoff_max_seconds = 30
//!
//! [timeouts]
//! status_seconds = 5
//! persist_seconds = 10
//! ```

use toml::{Table, Value};

use super::{ConfigErrors, ConfigFile, Decoding, Seconds};
use crate::domain::path::AbsolutePath;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DaemonConfig {
    /// Ensure linger for the user at daemon start; never turned off by amoeboid.
    pub linger: bool,
    /// The journal's location when neither the embedding code nor the daemon's command line
    /// names one.
    pub journal: Option<AbsolutePath>,
    pub restart: RestartConfig,
    pub timeouts: RuntimeTimeouts,
}

/// A twin's restart budget and backoff (the machine supervisor's policy).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RestartConfig {
    /// Restarts allowed within the window; 0..100.
    pub max_restarts: u32,
    /// 1..86400.
    pub window: Seconds,
    /// 0..3600, at most `backoff_max`.
    pub backoff_first: Seconds,
    /// 0..3600.
    pub backoff_max: Seconds,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RuntimeTimeouts {
    /// How long a status question waits for twins; 1..600.
    pub status: Seconds,
    /// How long a persistent actor waits for the journal; 1..600.
    pub persist: Seconds,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            linger: true,
            journal: None,
            restart: RestartConfig {
                max_restarts: 5,
                window: Seconds::new(600),
                backoff_first: Seconds::new(1),
                backoff_max: Seconds::new(30),
            },
            timeouts: RuntimeTimeouts {
                status: Seconds::new(5),
                persist: Seconds::new(10),
            },
        }
    }
}

const HEADER: &str = "# amoeboid daemon configuration. Read when the runtime starts; restart it \
                      to apply a change:\n#   systemctl --user restart amoeboid.service\n# \
                      Paths: ~/… is your home, or absolute.";

/// # Errors
///
/// Every problem in the file: syntax, unknown keys, bad values.
pub fn decode(text: &str, home: &AbsolutePath) -> Result<DaemonConfig, ConfigErrors> {
    let mut root = super::parse(ConfigFile::Daemon, text)?;
    let mut decoding = Decoding::new();
    let defaults = DaemonConfig::default();

    let linger = decoding.value(&mut root, "", "linger", defaults.linger, super::boolean);

    let mut journal_table = decoding.table(&mut root, "", "journal");
    let journal = decoding.optional(&mut journal_table, "journal", "path", |value| {
        super::string(value).and_then(|text| super::home_path(&text, home))
    });
    decoding.unknown("journal", &journal_table, &["path"]);

    let mut restart = decoding.table(&mut root, "", "restart");
    let max_restarts = decoding.value(
        &mut restart,
        "restart",
        "max_restarts",
        defaults.restart.max_restarts,
        |value| super::integer(value, 0, 100),
    );
    let window = decoding.value(
        &mut restart,
        "restart",
        "window_seconds",
        defaults.restart.window,
        |value| super::seconds(value, 1, 86_400),
    );
    let backoff_first = decoding.value(
        &mut restart,
        "restart",
        "backoff_first_seconds",
        defaults.restart.backoff_first,
        |value| super::seconds(value, 0, 3_600),
    );
    let backoff_max = decoding.value(
        &mut restart,
        "restart",
        "backoff_max_seconds",
        defaults.restart.backoff_max,
        |value| super::seconds(value, 0, 3_600),
    );
    if backoff_first > backoff_max {
        decoding.problem(
            "restart.backoff_first_seconds",
            format!(
                "{} is more than backoff_max_seconds ({})",
                backoff_first.get(),
                backoff_max.get()
            ),
        );
    }
    decoding.unknown(
        "restart",
        &restart,
        &[
            "max_restarts",
            "window_seconds",
            "backoff_first_seconds",
            "backoff_max_seconds",
        ],
    );

    let mut timeouts = decoding.table(&mut root, "", "timeouts");
    let status = decoding.value(
        &mut timeouts,
        "timeouts",
        "status_seconds",
        defaults.timeouts.status,
        |value| super::seconds(value, 1, 600),
    );
    let persist = decoding.value(
        &mut timeouts,
        "timeouts",
        "persist_seconds",
        defaults.timeouts.persist,
        |value| super::seconds(value, 1, 600),
    );
    decoding.unknown(
        "timeouts",
        &timeouts,
        &["status_seconds", "persist_seconds"],
    );

    decoding.unknown("", &root, &["linger", "journal", "restart", "timeouts"]);
    decoding.finish(
        ConfigFile::Daemon,
        DaemonConfig {
            linger,
            journal,
            restart: RestartConfig {
                max_restarts,
                window,
                backoff_first,
                backoff_max,
            },
            timeouts: RuntimeTimeouts { status, persist },
        },
    )
}

/// `daemon_config.toml`, or the defaults when there is none.
///
/// # Errors
///
/// It can't be read, or it is invalid.
pub fn load(paths: &crate::runtime::paths::ConfigPaths) -> Result<DaemonConfig, super::LoadError> {
    match super::read(&paths.daemon_config())? {
        None => Ok(DaemonConfig::default()),
        Some(text) => Ok(decode(&text, paths.home())?),
    }
}

impl RestartConfig {
    /// The machine supervisor's policy.
    #[must_use]
    pub fn policy(&self) -> crate::runtime::machine_supervisor::RestartPolicy {
        crate::runtime::machine_supervisor::RestartPolicy {
            max_restarts: self.max_restarts,
            window: self.window.duration(),
            backoff_first: self.backoff_first.duration(),
            backoff_max: self.backoff_max.duration(),
        }
    }
}

/// Canonical text: every key written (the journal path only when set), paths under `home` as
/// `~/…`.
#[must_use]
pub fn encode(config: &DaemonConfig, home: &AbsolutePath) -> String {
    let seconds = |value: Seconds| Value::Integer(i64::from(value.get()));
    let mut journal = Table::new();
    if let Some(path) = &config.journal {
        journal.insert(
            "path".into(),
            Value::String(super::write_home_path(path, home).unwrap_or_default()),
        );
    }
    let mut restart = Table::new();
    restart.insert(
        "max_restarts".into(),
        Value::Integer(i64::from(config.restart.max_restarts)),
    );
    restart.insert("window_seconds".into(), seconds(config.restart.window));
    restart.insert(
        "backoff_first_seconds".into(),
        seconds(config.restart.backoff_first),
    );
    restart.insert(
        "backoff_max_seconds".into(),
        seconds(config.restart.backoff_max),
    );
    let mut timeouts = Table::new();
    timeouts.insert("status_seconds".into(), seconds(config.timeouts.status));
    timeouts.insert("persist_seconds".into(), seconds(config.timeouts.persist));
    let mut root = Table::new();
    root.insert("linger".into(), Value::Boolean(config.linger));
    root.insert("journal".into(), Value::Table(journal));
    root.insert("restart".into(), Value::Table(restart));
    root.insert("timeouts".into(), Value::Table(timeouts));
    super::write(HEADER, &root)
}
