//! Loopback TCP ports the host forwards to the guest's sshd.

use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

/// A port on 127.0.0.1; never 0.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LoopbackPort(NonZeroU16);

impl LoopbackPort {
    #[must_use]
    pub fn new(port: NonZeroU16) -> Self {
        Self(port)
    }

    #[must_use]
    pub fn get(self) -> u16 {
        self.0.get()
    }
}

impl std::fmt::Display for LoopbackPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
