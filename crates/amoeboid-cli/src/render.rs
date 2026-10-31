//! The only module that prints.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the CLI's output boundary"
)]

use amoeboid::diagnostic::Diagnostic;

pub fn line(text: &str) {
    println!("{text}");
}

pub fn error(error: &impl Diagnostic) {
    eprintln!("amoeboid: {}", error.message());
    if let Some(hint) = error.hint() {
        eprintln!("  hint: {hint}");
    }
}
