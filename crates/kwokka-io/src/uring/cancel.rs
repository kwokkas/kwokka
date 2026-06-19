//! `IORING_OP_ASYNC_CANCEL` submission helpers.
//!
//! Cancels an in-flight operation identified by its `user_data` value.
//! The cancel SQE is submitted via `submit_internal` and the result
//! is mapped to [`CancelError`].

#![allow(dead_code, reason = "pending cancel op wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::{CancelError, operation::SubmitToken};

/// Map a cancel CQE result to [`CancelError`].
///
/// `IORING_OP_ASYNC_CANCEL` returns 0 on success, `-ENOENT` if the
/// target was not found, `-EALREADY` if already completed.
pub(crate) const fn map_cancel_result(result: i32) -> Result<(), CancelError> {
    match result {
        0 => Ok(()),
        -2 => Err(CancelError::NotFound),
        -114 => Err(CancelError::AlreadyCompleted),
        _ => Err(CancelError::BestEffortDetach),
    }
}

/// Extract the target `user_data` from a cancel request's
/// [`SubmitToken`].
pub(crate) const fn cancel_target(token: SubmitToken) -> u64 {
    token.user_data()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_success() {
        assert!(map_cancel_result(0).is_ok());
    }

    #[test]
    fn cancel_not_found() {
        let error = map_cancel_result(-2);
        assert_eq!(error, Err(CancelError::NotFound));
    }

    #[test]
    fn cancel_already_completed() {
        let error = map_cancel_result(-114);
        assert_eq!(error, Err(CancelError::AlreadyCompleted));
    }

    #[test]
    fn cancel_unknown_error_maps_to_detach() {
        let error = map_cancel_result(-999);
        assert_eq!(error, Err(CancelError::BestEffortDetach));
    }
}
