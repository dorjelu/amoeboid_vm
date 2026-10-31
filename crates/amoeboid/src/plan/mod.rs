//! Pure construction of everything the runtime writes or runs for a VM: argument lists, the
//! guest's state files, `host.nix`. No effects. Names and formats shared with `nix/base.nix`
//! are part of the contract listed in DEVELOPING.adoc.

pub mod argv;
pub mod files;
pub mod qemu;
pub mod share;
#[cfg(test)]
mod tests;

/// The guest NIC's MAC address; `nix/base.nix` matches on it.
pub const GUEST_MAC: &str = "02:00:00:a0:0b:01";

/// The guest sshd's port, forwarded from a loopback port on the host.
pub const GUEST_SSH_PORT: u16 = 22;

/// The name a VM's host key is pinned under in its `known_hosts`, whatever port it gets.
#[must_use]
pub fn ssh_alias(vm: &crate::domain::vm_name::VmName) -> String {
    format!("amoeboid-{vm}")
}
