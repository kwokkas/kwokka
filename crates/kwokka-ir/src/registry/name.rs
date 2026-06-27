//! Name-primary registry lookup: a sorted name-to-ordinal binary search.

use core::cmp::Ordering;

use crate::registry::RegistryView;

impl RegistryView<'_> {
    /// Returns the ordinal of the stage named `name`, or `None` if no stage
    /// carries that exact name.
    ///
    /// Binary-searches the sorted index, dereferencing each probe through
    /// the ordinal-to-name table. The comparison is over raw name bytes.
    /// The codec does not enforce that the index is sorted, so a malformed
    /// index yields a defined but possibly-missed result, never a panic.
    #[must_use]
    pub fn lookup(&self, name: &[u8]) -> Option<u32> {
        let mut low = 0;
        let mut high = self.stage_count();
        while low < high {
            let mid = low + (high - low) / 2;
            let ordinal = self.sorted_ordinal(mid)?;
            let probe = self.name(u32::from(ordinal))?;
            match name.cmp(probe) {
                Ordering::Equal => return Some(u32::from(ordinal)),
                Ordering::Less => high = mid,
                Ordering::Greater => low = mid + 1,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::registry::RegistryView;

    /// `(body, name_table, sorted_index)` for ordinal 0 = "beta",
    /// ordinal 1 = "alpha"; sorted order is `["alpha"(1), "beta"(0)]`.
    fn registry() -> ([u8; 9], [u8; 16], [u8; 4]) {
        let body = *b"betaalpha";
        let mut name_table = [0u8; 16];
        name_table[0..4].copy_from_slice(&0u32.to_le_bytes());
        name_table[4..8].copy_from_slice(&4u32.to_le_bytes());
        name_table[8..12].copy_from_slice(&4u32.to_le_bytes());
        name_table[12..16].copy_from_slice(&5u32.to_le_bytes());
        let mut sorted_index = [0u8; 4];
        sorted_index[0..2].copy_from_slice(&1u16.to_le_bytes());
        sorted_index[2..4].copy_from_slice(&0u16.to_le_bytes());
        (body, name_table, sorted_index)
    }

    #[test]
    fn lookup_finds_each_name() {
        let (body, name_table, sorted_index) = registry();
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.lookup(b"alpha"), Some(1));
        assert_eq!(view.lookup(b"beta"), Some(0));
    }

    #[test]
    fn lookup_misses_an_absent_name() {
        let (body, name_table, sorted_index) = registry();
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.lookup(b"gamma"), None);
        assert_eq!(view.lookup(b"a"), None);
    }

    #[test]
    fn empty_lookup_is_none() {
        let view = RegistryView::new(&[], &[], &[], 0);
        assert_eq!(view.lookup(b"x"), None);
    }
}
