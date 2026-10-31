//! Control plane for amoeboid microVMs: runtime, journal, persistent actors, protocol.

pub mod adapters;
pub mod client;
pub mod clock;
pub mod diagnostic;
pub mod domain;
pub mod journal;
pub mod nonempty;
pub mod persistent;
pub mod plan;
pub mod protocol;
pub mod runtime;
#[cfg(test)]
mod testing;
pub mod vm;
