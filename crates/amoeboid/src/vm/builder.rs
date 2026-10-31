//! Builder: renders `host.nix`, prepares the VM's own Nix config (create or rebase), runs
//! `nix build`. Idempotent, keeps no state. The build output's layout is fixed by `VmPaths`
//! (`runner()`, `virtiofsd()`), so `Built` carries no paths.

use super::{Account, Generation, NixConfig};
use crate::diagnostic::Report;
use crate::runtime::paths::VmPaths;

#[derive(Debug)]
pub enum BuilderCommand {
    Build {
        generation: Generation,
        vm: VmPaths,
        account: Account,
        nix_config: NixConfig,
    },
}

#[derive(Debug)]
pub enum BuilderObservation {
    Built {
        generation: Generation,
    },
    BuildFailed {
        generation: Generation,
        reason: Report,
    },
}
