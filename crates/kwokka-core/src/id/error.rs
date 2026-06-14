//! Error type for the `id` module.

use core::fmt;

/// Errors emitted by [`crate::id::Pip`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum PipError {
    /// Returned when a child `Pip` would require nesting depth beyond `u16::MAX`.
    DepthOverflow,
}

impl fmt::Display for PipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DepthOverflow => f.write_str("Pip depth would exceed u16::MAX"),
        }
    }
}

impl core::error::Error for PipError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_format() {
        assert_eq!(
            PipError::DepthOverflow.to_string(),
            "Pip depth would exceed u16::MAX",
        );
    }
}
