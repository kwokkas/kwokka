//! [`Display`] and [`Debug`] for [`Pip`].

use core::fmt;

use crate::id::Pip;

impl fmt::Display for Pip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pip:{:032x}", self.0)
    }
}

impl fmt::Debug for Pip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pip")
            .field("seq", &self.seq())
            .field("depth", &self.depth())
            .field("worker", &self.worker_id())
            .field("raw", &format_args!("0x{:032x}", self.0))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_zero() {
        assert_eq!(Pip(0).to_string(), "pip:00000000000000000000000000000000",);
    }

    #[test]
    fn display_known_pattern() {
        let id = Pip(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210);
        assert_eq!(id.to_string(), "pip:0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn debug_includes_field_names() {
        let id = Pip(0xdead_beef_cafe_u128);
        let dbg = format!("{id:?}");
        assert!(dbg.contains("Pip"));
        assert!(dbg.contains("seq"));
        assert!(dbg.contains("depth"));
        assert!(dbg.contains("worker"));
        assert!(dbg.contains("raw"));
    }
}
