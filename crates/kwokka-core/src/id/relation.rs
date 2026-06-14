//! Heuristic ancestry checks for [`Pip`].
//!
//! These predicates compare depth and sequence numbers and do **not** verify
//! actual structural parentage. Two unrelated ids created in the right order
//! can satisfy these predicates. Use only as a fast filter, never as a
//! security or correctness boundary.

use crate::id::Pip;

impl Pip {
    /// Returns `true` if `self` could be a direct parent of `other`.
    ///
    /// Heuristic: `self.depth() + 1 == other.depth()` and `self.seq() < other.seq()`.
    #[inline]
    pub const fn is_parent_of(self, other: Self) -> bool {
        self.depth().wrapping_add(1) == other.depth() && self.seq() < other.seq()
    }

    /// Returns `true` if `self` could be an ancestor (any depth) of `other`.
    ///
    /// Heuristic: `self.depth() < other.depth()` and `self.seq() < other.seq()`.
    #[inline]
    pub const fn is_ancestor_of(self, other: Self) -> bool {
        self.depth() < other.depth() && self.seq() < other.seq()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_chain() -> (Pip, Pip, Pip) {
        let root = Pip::root();
        let Ok(child) = root.child() else {
            unreachable!("root has depth 0, child cannot overflow")
        };
        let Ok(grandchild) = child.child() else {
            unreachable!("child has depth 1, grandchild cannot overflow")
        };
        (root, child, grandchild)
    }

    fn build_siblings() -> (Pip, Pip, Pip) {
        let root = Pip::root();
        let Ok(a) = root.child() else {
            unreachable!("root has depth 0, child cannot overflow")
        };
        let Ok(b) = root.child() else {
            unreachable!("root has depth 0, child cannot overflow")
        };
        (root, a, b)
    }

    #[test]
    fn is_parent_of_direct_child() {
        let (root, child, _) = build_chain();
        assert!(root.is_parent_of(child));
    }

    #[test]
    fn is_parent_of_rejects_self() {
        let root = Pip::root();
        assert!(!root.is_parent_of(root));
    }

    #[test]
    fn is_parent_of_rejects_grandchild() {
        let (root, _, grandchild) = build_chain();
        assert!(!root.is_parent_of(grandchild));
    }

    #[test]
    fn is_parent_of_rejects_sibling() {
        let (_, a, b) = build_siblings();
        assert!(!a.is_parent_of(b));
    }

    #[test]
    fn is_ancestor_of_chain() {
        let (root, child, grandchild) = build_chain();
        assert!(root.is_ancestor_of(child));
        assert!(root.is_ancestor_of(grandchild));
        assert!(child.is_ancestor_of(grandchild));
    }

    #[test]
    fn is_ancestor_of_rejects_self() {
        let root = Pip::root();
        assert!(!root.is_ancestor_of(root));
    }

    #[test]
    fn is_ancestor_of_rejects_sibling() {
        let (_, a, b) = build_siblings();
        assert!(!a.is_ancestor_of(b));
        assert!(!b.is_ancestor_of(a));
    }
}
