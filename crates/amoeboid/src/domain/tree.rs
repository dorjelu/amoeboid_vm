//! A directory tree as data: regular files by relative path. Symlinks, devices and the like are
//! not part of a tree, so copying one can't carry them along.

use std::collections::BTreeMap;

use super::relative_path::RelativePath;

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Tree(BTreeMap<RelativePath, Vec<u8>>);

impl Tree {
    pub fn insert(&mut self, path: RelativePath, content: Vec<u8>) {
        self.0.insert(path, content);
    }

    pub fn files(&self) -> impl Iterator<Item = (&RelativePath, &[u8])> {
        self.0
            .iter()
            .map(|(path, content)| (path, content.as_slice()))
    }

    #[must_use]
    pub fn contains(&self, path: &RelativePath) -> bool {
        self.0.contains_key(path)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
