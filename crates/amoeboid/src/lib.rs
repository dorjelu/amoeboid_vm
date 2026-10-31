//! Control plane for amoeboid microVMs: runtime, journal, persistent actors, protocol.

pub mod client;
pub mod diagnostic;
pub mod domain;
pub mod journal;
pub mod nonempty;
pub mod persistent;
pub mod protocol;
pub mod runtime;
#[cfg(test)]
mod testing;
