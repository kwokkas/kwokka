//! Timeout policy view: an overall stage deadline.

use crate::{error::IrError, flat::reader::read_u64};

/// An overall stage deadline applied around the whole guard chain.
///
/// Decoded from a [`NodeTag::PolicyTimeout`] record body. The duration
/// bounds the entire stage execution, not a single retry attempt: in
/// guard order (`Limiter -> Timeout -> Retry -> Breaker`) the timeout
/// wraps the retry loop, so it caps total wall-clock. A per-attempt
/// timeout is a separate concept reserved for a later wire field.
///
/// [`NodeTag::PolicyTimeout`]: crate::NodeTag::PolicyTimeout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutView {
    duration_ns: u64,
}

impl TimeoutView {
    /// Byte length of a timeout policy body on the wire.
    pub(crate) const LEN: usize = 8;

    /// Decodes a timeout policy body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body is shorter than
    /// [`TimeoutView::LEN`].
    pub(crate) fn parse(body: &[u8]) -> Result<Self, IrError> {
        if body.len() < Self::LEN {
            return Err(IrError::Truncated);
        }
        Ok(Self {
            duration_ns: read_u64(body, 0)?,
        })
    }

    /// The overall stage deadline in nanoseconds.
    #[must_use]
    pub const fn duration_ns(&self) -> u64 {
        self.duration_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_deadline() {
        let mut body = [0u8; 8];
        body.copy_from_slice(&9_999u64.to_le_bytes());
        assert_eq!(
            TimeoutView::parse(&body).map(|view| view.duration_ns()),
            Ok(9_999)
        );
    }

    #[test]
    fn rejects_a_short_body() {
        assert_eq!(TimeoutView::parse(&[0u8; 4]), Err(IrError::Truncated));
    }
}
