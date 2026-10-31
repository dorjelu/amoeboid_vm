//! The only module that prints.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the CLI's output boundary"
)]

use amoeboid::diagnostic::Report;

pub fn line(text: &str) {
    println!("{text}");
}

pub fn error(report: &Report) {
    eprintln!("amoeboid: {}", report.message);
    if let Some(hint) = &report.hint {
        eprintln!("  hint: {hint}");
    }
}
