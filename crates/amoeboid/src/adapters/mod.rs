//! Effects on the host, as free functions. Callers are actors, which turn results into
//! observations; nothing here knows about actors.

pub mod files;
pub mod guest;
pub mod nix;
pub mod process;
pub mod qmp;
pub mod ssh;
