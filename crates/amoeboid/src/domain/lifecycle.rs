//! Which life of a VM something belongs to.

use serde::{Deserialize, Serialize};

/// One creation of a VM name: minted by Root (a ULID) for the first start, persisted as the
/// twin's first event. A VM removed and created again under the same name gets a new one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Incarnation(ulid::Ulid);

impl Incarnation {
    #[must_use]
    pub fn mint() -> Self {
        Self(ulid::Ulid::generate())
    }
}

impl std::fmt::Display for Incarnation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One start of a VM: bumped by every start, carried by every message inside the VM subtree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(u64);

impl Generation {
    pub const FIRST: Self = Self(1);

    /// `None` past `u64::MAX`.
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
