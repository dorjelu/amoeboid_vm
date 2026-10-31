//! The only module that prints. Write errors are ignored: a reader that went away (`amoeboid
//! status | head`) is not a failure of the command.

use std::io::Write as _;

use amoeboid::diagnostic::Report;

pub fn line(text: &str) {
    let _ = writeln!(std::io::stdout().lock(), "{text}");
}

pub fn error(report: &Report) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "amoeboid: {}", report.message);
    if let Some(hint) = &report.hint {
        let _ = writeln!(stderr, "  hint: {hint}");
    }
}
