//! The virtiofsd instances of a VM: two fixed shares (the host's `/nix/store` read-only, the
//! guest state directory) then one per requested directory. Socket names and tags must agree
//! with `nix/base.nix`.

use crate::domain::path::AbsolutePath;
use crate::domain::share::{Mode, Shares};

/// One virtiofsd: a host directory served on a socket in the VM's runtime directory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VirtiofsShare {
    /// Names the process (`virtiofsd-d1`).
    pub role: String,
    /// Socket file name in the runtime directory (`d1.sock`).
    pub socket: String,
    pub source: AbsolutePath,
    pub mode: Mode,
}

/// The guest tag of the share at `index` (from 0): `d1`, `d2`, ...
#[must_use]
pub fn tag(index: usize) -> String {
    format!("d{}", index.saturating_add(1))
}

/// Shares in socket order: `ro-store`, `state`, then `d1`.. in `shares` order.
#[must_use]
pub fn virtiofs_shares(guest_state: &AbsolutePath, shares: &Shares) -> Vec<VirtiofsShare> {
    let store = AbsolutePath::parse("/nix/store".into());
    let fixed = store.into_iter().map(|store| VirtiofsShare {
        role: "virtiofsd-ro-store".into(),
        socket: "ro-store.sock".into(),
        source: store,
        mode: Mode::ReadOnly,
    });
    let state = std::iter::once(VirtiofsShare {
        role: "virtiofsd-state".into(),
        socket: "state.sock".into(),
        source: guest_state.clone(),
        mode: Mode::ReadWrite,
    });
    let requested = shares.as_slice().iter().enumerate().map(|(index, share)| {
        let tag = tag(index);
        VirtiofsShare {
            role: format!("virtiofsd-{tag}"),
            socket: format!("{tag}.sock"),
            source: share.directory.path().clone(),
            mode: share.mode,
        }
    });
    fixed.chain(state).chain(requested).collect()
}
