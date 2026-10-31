//! The virtiofsd instances of a VM: two fixed shares (the host's `/nix/store` read-only, the
//! guest state directory) then one per requested directory. Socket names and tags must agree
//! with `nix/base.nix`.

use crate::domain::path::AbsolutePath;
use crate::domain::process::{LocalRole, ProcessRole, SharedDirectory};
use crate::domain::share::{Mode, Shares};

/// One virtiofsd: a host directory served on a socket in the VM's runtime directory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VirtiofsShare {
    pub directory: SharedDirectory,
    pub source: AbsolutePath,
    pub mode: Mode,
}

impl VirtiofsShare {
    #[must_use]
    pub fn role(&self) -> ProcessRole {
        ProcessRole::Local(LocalRole::Virtiofsd(self.directory))
    }

    /// Socket file name in the runtime directory (`d1.sock`).
    #[must_use]
    pub fn socket(&self) -> String {
        format!("{}.sock", self.directory.name())
    }
}

/// The guest tag of the share at `index` (from 0): `d1`, `d2`, ...
#[must_use]
pub fn tag(index: u16) -> String {
    SharedDirectory::Requested(index).name()
}

/// Shares in socket order: `ro-store`, `state`, then `d1`.. in `shares` order.
#[must_use]
pub fn virtiofs_shares(guest_state: &AbsolutePath, shares: &Shares) -> Vec<VirtiofsShare> {
    let store = AbsolutePath::parse("/nix/store".into());
    let fixed = store.into_iter().map(|store| VirtiofsShare {
        directory: SharedDirectory::NixStore,
        source: store,
        mode: Mode::ReadOnly,
    });
    let state = std::iter::once(VirtiofsShare {
        directory: SharedDirectory::GuestState,
        source: guest_state.clone(),
        mode: Mode::ReadWrite,
    });
    let requested = shares.indexed().map(|(index, share)| VirtiofsShare {
        directory: SharedDirectory::Requested(index),
        source: share.directory.path().clone(),
        mode: share.mode,
    });
    fixed.chain(state).chain(requested).collect()
}
