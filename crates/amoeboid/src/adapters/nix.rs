//! Building a VM: runs `nix build` with its output appended to the VM's build log.

use super::files::FileError;
use super::process::{self, Exit, Output, ProcessError};
use crate::domain::path::AbsolutePath;
use crate::plan::argv::Argv;

/// Lines of the build log quoted in a failure.
const TAIL_LINES: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum NixError {
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    File(#[from] FileError),
    #[error("nix build failed ({exit:?}):\n{tail}")]
    Failed { exit: Exit, tail: String },
}

/// Runs `argv` (from `plan::argv::nix_build`) in `directory`, appending its output to `log`.
///
/// # Errors
///
/// `Failed` with the log's last lines; `Process` when it can't be started.
pub async fn build(
    argv: &Argv,
    directory: &AbsolutePath,
    log: &AbsolutePath,
) -> Result<(), NixError> {
    match process::run(argv, directory, &Output::Append(log.clone())).await? {
        Exit::Code(0) => Ok(()),
        exit @ (Exit::Code(_) | Exit::Signal(_) | Exit::Unknown) => {
            let text = super::files::read_to_string(log)?;
            let lines: Vec<&str> = text.lines().collect();
            let tail = lines[lines.len().saturating_sub(TAIL_LINES)..].join("\n");
            Err(NixError::Failed { exit, tail })
        }
    }
}

impl crate::diagnostic::Diagnostic for NixError {
    fn code(&self) -> &'static str {
        match self {
            Self::Process(inner) => inner.code(),
            Self::File(inner) => inner.code(),
            Self::Failed { .. } => "nix.build_failed",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Process(inner) => inner.hint(),
            Self::File(inner) => inner.hint(),
            Self::Failed { .. } => Some("the full output is in the VM's build.log".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use super::*;

    #[tokio::test]
    async fn a_failed_build_reports_the_tail_of_its_log() {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let log = root.join("build.log");
        let argv = Argv::for_tests(&[
            "sh",
            "-c",
            "for i in $(seq 1 30); do echo line$i; done; exit 3",
        ]);
        let result = build(&argv, &root, &log).await;
        match result {
            Err(NixError::Failed { exit, tail }) => {
                assert_eq!(exit, Exit::Code(3));
                assert!(tail.starts_with("line11\n") && tail.ends_with("line30"));
            }
            other => assert!(matches!(other, Err(NixError::Failed { .. })), "{other:?}"),
        }
    }
}
