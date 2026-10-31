//! The operator's configuration files (0003 §Configuration): `run_config.toml`, read by Root at
//! every start, and `daemon_config.toml`, read once when the runtime starts.
//!
//! Each file is a bijection with its value: `decode(encode(config)) == config`, `encode` writes
//! canonical text. Decoding reports every problem at once. Paths under the host user's home are
//! written `~/…`, so no file names a machine-specific home.

#![allow(
    clippy::disallowed_types,
    reason = "boundary: refines paths written in configuration files"
)]

pub mod daemon;
pub mod run;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use toml::{Table, Value};

use crate::domain::path::AbsolutePath;
use crate::nonempty::NonEmpty;

/// Reading a configuration file failed: it can't be read, or it is invalid.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("can't read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error(transparent)]
    Invalid(#[from] ConfigErrors),
}

impl crate::diagnostic::Diagnostic for LoadError {
    fn code(&self) -> &'static str {
        match self {
            Self::Read { .. } => "config.read",
            Self::Invalid(errors) => errors.code(),
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Read { .. } => None,
            Self::Invalid(errors) => errors.hint(),
        }
    }
}

/// The file's text; `None` when there is no file (the defaults apply).
fn read(path: &AbsolutePath) -> Result<Option<String>, LoadError> {
    match std::fs::read_to_string(path.as_path()) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(LoadError::Read {
            path: path.display().to_string(),
            source,
        }),
    }
}

/// Which file a problem is in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConfigFile {
    Run,
    Daemon,
}

impl ConfigFile {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Run => "run_config.toml",
            Self::Daemon => "daemon_config.toml",
        }
    }
}

/// One problem, at a dotted key (`new_vm.default_shares[1]`); the key is empty for a syntax
/// error.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConfigProblem {
    pub key: String,
    pub message: String,
}

/// Every problem in a file.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("invalid {}:{}", file.name(), render(problems))]
pub struct ConfigErrors {
    pub file: ConfigFile,
    pub problems: NonEmpty<ConfigProblem>,
}

fn render(problems: &NonEmpty<ConfigProblem>) -> String {
    problems
        .iter()
        .map(|problem| match problem.key.as_str() {
            "" => format!("\n  {}", problem.message),
            key => format!("\n  {key}: {}", problem.message),
        })
        .collect()
}

impl crate::diagnostic::Diagnostic for ConfigErrors {
    fn code(&self) -> &'static str {
        match self.file {
            ConfigFile::Run => "config.run.invalid",
            ConfigFile::Daemon => "config.daemon.invalid",
        }
    }
    fn hint(&self) -> Option<String> {
        Some(format!(
            "fix the listed keys in {} (amoeboid's configuration directory)",
            self.file.name()
        ))
    }
}

/// Whole seconds within a range set by the key; what durations are written as.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Seconds(u32);

impl Seconds {
    /// For defaults and tests; decoding checks each key's own range.
    #[must_use]
    pub const fn new(seconds: u32) -> Self {
        Self(seconds)
    }

    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }

    #[must_use]
    pub fn duration(self) -> std::time::Duration {
        std::time::Duration::from_secs(u64::from(self.0))
    }
}

/// Collects problems while a table is taken apart key by key; whatever is left at the end is an
/// unknown key.
struct Decoding {
    problems: Vec<ConfigProblem>,
}

impl Decoding {
    fn new() -> Self {
        Self {
            problems: Vec::new(),
        }
    }

    fn problem(&mut self, key: &str, message: impl Into<String>) {
        self.problems.push(ConfigProblem {
            key: key.to_owned(),
            message: message.into(),
        });
    }

    /// Reports every key left in `table` (under `prefix`) as unknown.
    fn unknown(&mut self, prefix: &str, table: &Table, known: &[&str]) {
        for key in table.keys() {
            self.problem(
                &join(prefix, key),
                format!("unknown key; known: {}", known.join(", ")),
            );
        }
    }

    /// `key`'s table, removed from `table`; `None` (empty) when absent, a problem when not a
    /// table.
    fn table(&mut self, table: &mut Table, prefix: &str, key: &str) -> Table {
        match table.remove(key) {
            None => Table::new(),
            Some(Value::Table(inner)) => inner,
            Some(other) => {
                self.problem(
                    &join(prefix, key),
                    format!("expected a table, got {}", other.type_str()),
                );
                Table::new()
            }
        }
    }

    /// A required-shape value with a default when absent; `decode` refines it.
    fn value<T>(
        &mut self,
        table: &mut Table,
        prefix: &str,
        key: &str,
        default: T,
        decode: impl FnOnce(Value) -> Result<T, String>,
    ) -> T {
        match table.remove(key) {
            None => default,
            Some(value) => decode(value).unwrap_or_else(|message| {
                self.problem(&join(prefix, key), message);
                default
            }),
        }
    }

    /// An optional value: `None` when absent.
    fn optional<T>(
        &mut self,
        table: &mut Table,
        prefix: &str,
        key: &str,
        decode: impl FnOnce(Value) -> Result<T, String>,
    ) -> Option<T> {
        let value = table.remove(key)?;
        decode(value)
            .map_err(|message| self.problem(&join(prefix, key), message))
            .ok()
    }

    /// A list whose items are refined one by one, each failure at `key[i]`.
    fn list<T>(
        &mut self,
        table: &mut Table,
        prefix: &str,
        key: &str,
        decode: impl Fn(Value) -> Result<T, String>,
    ) -> Vec<T> {
        let full = join(prefix, key);
        match table.remove(key) {
            None => Vec::new(),
            Some(Value::Array(items)) => items
                .into_iter()
                .enumerate()
                .filter_map(|(index, item)| {
                    decode(item)
                        .map_err(|message| self.problem(&format!("{full}[{index}]"), message))
                        .ok()
                })
                .collect(),
            Some(other) => {
                self.problem(&full, format!("expected a list, got {}", other.type_str()));
                Vec::new()
            }
        }
    }

    fn finish<T>(self, file: ConfigFile, value: T) -> Result<T, ConfigErrors> {
        match NonEmpty::from_vec(self.problems) {
            None => Ok(value),
            Some(problems) => Err(ConfigErrors { file, problems }),
        }
    }
}

fn join(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_owned()
    } else {
        format!("{prefix}.{key}")
    }
}

/// Parses TOML into a table; a syntax error is the file's only problem.
fn parse(file: ConfigFile, text: &str) -> Result<Table, ConfigErrors> {
    text.parse::<Table>().map_err(|error| ConfigErrors {
        file,
        problems: NonEmpty::new(
            ConfigProblem {
                key: String::new(),
                message: error.message().to_owned(),
            },
            Vec::new(),
        ),
    })
}

fn string(value: Value) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text),
        other @ (Value::Integer(_)
        | Value::Float(_)
        | Value::Boolean(_)
        | Value::Datetime(_)
        | Value::Array(_)
        | Value::Table(_)) => Err(format!("expected a string, got {}", other.type_str())),
    }
}

fn boolean(value: Value) -> Result<bool, String> {
    match value {
        Value::Boolean(flag) => Ok(flag),
        other @ (Value::String(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::Datetime(_)
        | Value::Array(_)
        | Value::Table(_)) => Err(format!("expected true or false, got {}", other.type_str())),
    }
}

/// An integer within `low..=high`.
fn integer(value: Value, low: u32, high: u32) -> Result<u32, String> {
    match value {
        Value::Integer(number) => u32::try_from(number)
            .ok()
            .filter(|number| (low..=high).contains(number))
            .ok_or_else(|| format!("{number} is outside {low}..{high}")),
        other @ (Value::String(_)
        | Value::Float(_)
        | Value::Boolean(_)
        | Value::Datetime(_)
        | Value::Array(_)
        | Value::Table(_)) => Err(format!("expected an integer, got {}", other.type_str())),
    }
}

fn seconds(value: Value, low: u32, high: u32) -> Result<Seconds, String> {
    integer(value, low, high).map(Seconds)
}

/// A path as written in a file: `~`, `~/…` (the host user's home) or absolute.
fn home_path(text: &str, home: &AbsolutePath) -> Result<AbsolutePath, String> {
    let path = match text.strip_prefix('~') {
        Some("") => return Ok(home.clone()),
        Some(rest) if rest.starts_with('/') => home.as_path().join(rest.trim_start_matches('/')),
        Some(_) => return Err(format!("{text:?}: only ~ (your own home) is understood")),
        None => PathBuf::from(text),
    };
    AbsolutePath::parse(path)
        .map_err(|error| format!("{error}; write an absolute path or one under ~/"))
}

/// The inverse of [`home_path`]: `~/…` for a path under `home`. `None` when the path is not
/// valid UTF-8 (such a path can't be written into the file).
fn write_home_path(path: &AbsolutePath, home: &AbsolutePath) -> Option<String> {
    let relative = path.as_path().strip_prefix(home.as_path()).ok();
    match relative {
        Some(rest) if rest == Path::new("") => Some("~".into()),
        Some(rest) => rest.to_str().map(|rest| format!("~/{rest}")),
        None => path.to_str().map(str::to_owned),
    }
}

/// Canonical text: a header comment, then the table.
fn write(header: &str, table: &Table) -> String {
    let body = toml::to_string(table).unwrap_or_default();
    format!("{header}\n{body}")
}
