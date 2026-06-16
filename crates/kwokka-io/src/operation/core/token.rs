//! Submit token and result types.

/// Opaque handle returned on successful submit; used to identify the op for cancellation.
///
/// Internally equal to the `user_data` value submitted to the ring.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubmitToken(u64);

impl SubmitToken {
    #[allow(
        dead_code,
        reason = "consumed by the backend submit path, not yet implemented"
    )]
    pub(crate) const fn new(user_data: u64) -> Self {
        Self(user_data)
    }

    /// Raw `user_data` value submitted to the ring.
    pub const fn user_data(self) -> u64 {
        self.0
    }
}

/// Result of a submit attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SubmitResult {
    /// Operation was queued; the token identifies it for cancellation.
    Submitted(SubmitToken),
    /// Submission queue was full at the time of the call.
    QueueFull,
    /// Backend does not support this operation.
    Unsupported,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_token_user_data_roundtrip() {
        let token = SubmitToken::new(42);
        assert_eq!(token.user_data(), 42);
    }

    #[test]
    fn submit_token_zero_roundtrip() {
        let token = SubmitToken::new(0);
        assert_eq!(token.user_data(), 0);
    }

    #[test]
    fn submit_token_max_u64_roundtrip() {
        let token = SubmitToken::new(u64::MAX);
        assert_eq!(token.user_data(), u64::MAX);
    }

    #[test]
    fn submit_token_is_copy() {
        let token = SubmitToken::new(1);
        let copy = token;
        assert_eq!(token.user_data(), copy.user_data());
    }

    #[test]
    fn submit_result_submitted_contains_token() {
        let token = SubmitToken::new(99);
        let result = SubmitResult::Submitted(token);
        let SubmitResult::Submitted(t) = result else {
            panic!("expected Submitted variant");
        };
        assert_eq!(t.user_data(), 99);
    }

    #[test]
    fn submit_result_is_copy() {
        let result = SubmitResult::QueueFull;
        let copy = result;
        assert_eq!(result, copy);
    }
}
