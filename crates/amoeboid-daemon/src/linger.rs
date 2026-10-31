//! Linger (0003 §Configuration): with it, the user's service manager (and so the runtime and
//! the VMs' scopes) keeps running after the user logs out. The daemon only ever turns it on.

use std::process::Command;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lingering {
    Already,
    Enabled,
}

/// Turns linger on for `user` unless it already is.
///
/// # Errors
///
/// `loginctl` can't be run, or refuses.
pub fn ensure(user: &str) -> Result<Lingering, String> {
    let shown = Command::new("loginctl")
        .args(["show-user", user, "--property=Linger", "--value"])
        .output()
        .map_err(|error| format!("can't run loginctl: {error}"))?;
    if shown.status.success() && String::from_utf8_lossy(&shown.stdout).trim() == "yes" {
        return Ok(Lingering::Already);
    }
    let enabled = Command::new("loginctl")
        .args(["enable-linger", user])
        .output()
        .map_err(|error| format!("can't run loginctl: {error}"))?;
    if enabled.status.success() {
        Ok(Lingering::Enabled)
    } else {
        Err(format!(
            "loginctl enable-linger {user} failed: {}",
            String::from_utf8_lossy(&enabled.stderr).trim()
        ))
    }
}
