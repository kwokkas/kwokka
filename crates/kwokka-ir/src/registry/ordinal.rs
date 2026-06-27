//! Ordinal-primary stage registry: stage ordinal to source name.

/// A stage-name registry over a validated IR blob.
///
/// Maps a stage ordinal to its source name via [`RegistryView::name`], and
/// a name back to its ordinal via [`RegistryView::lookup`] (the sorted-index
/// binary search). Names are raw bytes in the blob's string heap; UTF-8
/// validation is the consumer's responsibility, not the codec's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryView<'a> {
    body: &'a [u8],
    name_table: &'a [u8],
    sorted_index: &'a [u8],
    count: u32,
}

impl<'a> RegistryView<'a> {
    /// Byte length of one ordinal-to-name entry: two `u32` string-ref fields.
    pub(crate) const NAME_ENTRY_LEN: usize = 8;

    /// Byte length of one sorted-index entry: a single `u16` ordinal.
    pub(crate) const SORTED_ENTRY_LEN: usize = 2;

    /// Wraps the registry tables of a validated conductor body.
    ///
    /// Crate-internal: [`ConductorView::parse`] bounds-checks both tables
    /// and every name string-ref before constructing this view.
    ///
    /// [`ConductorView::parse`]: crate::ConductorView
    #[must_use]
    pub(crate) const fn new(
        body: &'a [u8],
        name_table: &'a [u8],
        sorted_index: &'a [u8],
        count: u32,
    ) -> Self {
        Self {
            body,
            name_table,
            sorted_index,
            count,
        }
    }

    /// The number of registered stages; equals the conductor's stage count.
    #[must_use]
    pub const fn stage_count(&self) -> u32 {
        self.count
    }

    /// Returns whether the registry has no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the name of the stage at `ordinal`, or `None` if `ordinal`
    /// is at or beyond [`RegistryView::stage_count`] or its string-ref falls
    /// outside the blob.
    #[must_use]
    pub fn name(&self, ordinal: u32) -> Option<&'a [u8]> {
        if ordinal >= self.count {
            return None;
        }
        let offset = (ordinal as usize).checked_mul(Self::NAME_ENTRY_LEN)?;
        let end = offset.checked_add(Self::NAME_ENTRY_LEN)?;
        let &[o0, o1, o2, o3, l0, l1, l2, l3] = self.name_table.get(offset..end)? else {
            return None;
        };
        let str_offset = u32::from_le_bytes([o0, o1, o2, o3]) as usize;
        let str_len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
        self.body.get(str_offset..str_offset.checked_add(str_len)?)
    }

    /// The ordinal stored at sorted-index position `rank`, or `None` if
    /// `rank` is out of range.
    pub(crate) fn sorted_ordinal(&self, rank: u32) -> Option<u16> {
        let offset = (rank as usize).checked_mul(Self::SORTED_ENTRY_LEN)?;
        let end = offset.checked_add(Self::SORTED_ENTRY_LEN)?;
        let &[a, b] = self.sorted_index.get(offset..end)? else {
            return None;
        };
        Some(u16::from_le_bytes([a, b]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn name_reads_each_ordinal() {
        let (body, name_table, sorted_index) = registry();
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.name(0), Some(&b"beta"[..]));
        assert_eq!(view.name(1), Some(&b"alpha"[..]));
    }

    #[test]
    fn name_past_count_is_none() {
        let (body, name_table, sorted_index) = registry();
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.name(2), None);
    }

    #[test]
    fn reports_stage_count() {
        let (body, name_table, sorted_index) = registry();
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.stage_count(), 2);
        assert!(!view.is_empty());
    }

    #[test]
    fn rejects_a_name_ref_overrun() {
        let (body, mut name_table, sorted_index) = registry();
        name_table[4..8].copy_from_slice(&99u32.to_le_bytes());
        let view = RegistryView::new(&body, &name_table, &sorted_index, 2);
        assert_eq!(view.name(0), None);
    }
}
