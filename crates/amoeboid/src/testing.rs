//! Test support shared across modules.

#![allow(
    clippy::disallowed_types,
    reason = "test fixtures read files by raw path"
)]

/// A golden file under `tests/golden/`, parsed as JSON.
pub fn golden_json(name: &str) -> serde_json::Value {
    serde_json::from_str(&golden_text(name)).unwrap()
}

/// A golden file under `tests/golden/`, as text.
pub fn golden_text(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    std::fs::read_to_string(path).unwrap()
}

/// Held by every test that forks or holds a lock fd. A fork copies all open fds until its child
/// execs (`O_CLOEXEC` acts at exec, not fork), so a parallel fork can keep a dropped lock alive.
pub fn forks() -> std::sync::MutexGuard<'static, ()> {
    static FORKS: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FORKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
