//! The NixOS configuration shipped in the binary: what a new VM is created from when no config
//! directory is given. Changing it reaches new VMs only; existing ones keep their own copy.

use crate::domain::relative_path::RelativePath;
use crate::domain::tree::Tree;

/// The files of the repository's `nix/`, embedded at build time.
macro_rules! bundled_files {
    ($($name:literal),* $(,)?) => {
        [$(($name, include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../nix/", $name)).as_slice())),*]
    };
}

const FILES: [(&str, &[u8]); 5] = bundled_files!(
    "flake.nix",
    "flake.lock",
    "base.nix",
    "recipe.nix",
    "host-default.nix",
);

/// The bundled configuration as a tree.
#[must_use]
pub fn config() -> Tree {
    let mut tree = Tree::default();
    for (name, content) in FILES {
        if let Ok(path) = RelativePath::parse(name) {
            tree.insert(path, content.to_vec());
        }
    }
    tree
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_bundled_file_is_in_the_tree() {
        let tree = config();
        assert_eq!(tree.files().count(), FILES.len());
        assert!(tree.contains(&RelativePath::parse("flake.nix").unwrap()));
    }
}
