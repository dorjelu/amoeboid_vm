//! A list with at least one element.

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NonEmpty<T> {
    head: T,
    tail: Vec<T>,
}

impl<T> NonEmpty<T> {
    pub fn new(head: T, tail: Vec<T>) -> Self {
        Self { head, tail }
    }

    /// `None` for an empty vector.
    #[must_use]
    pub fn from_vec(items: Vec<T>) -> Option<Self> {
        let mut items = items.into_iter();
        let head = items.next()?;
        Some(Self::new(head, items.collect()))
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.head).chain(&self.tail)
    }
}

impl<T> IntoIterator for NonEmpty<T> {
    type Item = T;
    type IntoIter = std::iter::Chain<std::iter::Once<T>, std::vec::IntoIter<T>>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.head).chain(self.tail)
    }
}
