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
