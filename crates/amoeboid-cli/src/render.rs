//! The only module that prints.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the CLI's output boundary"
)]

pub fn line(text: &str) {
    println!("{text}");
}

pub fn error(error: &super::CliError) {
    eprintln!("amoeboid: {error}");
    if let Some(hint) = error.hint() {
        eprintln!("  hint: {hint}");
    }
}
