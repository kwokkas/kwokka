//! `IoDriver` trait - uniform completion I/O abstraction over all backends.

use crate::{
    buffer::slot::{BufGroupId, FdSlot},
    capability::CapabilityMatrix,
    operation::{Completion, IoBuf, IoBufMut, IoRequest, SubmitResult, SubmitToken},
};

/// Completion-based I/O backend.
///
/// Single submit entry point; `(OpCode, OpFlags)` selects the concrete kernel op.
/// Prefer `DriverType` enum dispatch over dynamic dispatch in the runtime path.
///
/// # Default methods
///
/// `register_buffers`, `register_files`, and their `unregister_*` counterparts
/// return [`RegisterError::Unsupported`] by default. Thin-fallback backends
/// (epoll, kqueue) inherit these defaults; `UringDriver` overrides them.
///
/// `cancel` returns [`CancelError::BestEffortDetach`] by default - the op
/// completes normally but the result is discarded. `UringDriver` overrides this
/// with `IORING_OP_ASYNC_CANCEL`.
pub trait IoDriver: Send {
    /// Submit an operation where the kernel reads from the buffer (write, send).
    fn submit<B: IoBuf>(&self, request: IoRequest<B>) -> SubmitResult;

    /// Submit an operation where the kernel writes to the buffer (read, recv).
    ///
    /// Completion-based backends (`io_uring`) override this to use
    /// read-specific opcodes. Readiness-based backends (epoll, kqueue)
    /// forward to [`submit`](IoDriver::submit) since the read/write
    /// distinction is handled after fd readiness, not at submission.
    fn submit_read<B: IoBufMut>(&self, request: IoRequest<B>) -> SubmitResult {
        self.submit(request)
    }

    /// Submit a driver-internal operation (timeout, cancel, `msg_ring`, poll).
    #[doc(hidden)]
    fn submit_internal(&self, request: IoRequest<()>) -> SubmitResult;

    /// Drain up to `max` completions into `out`.
    ///
    /// `NOTIF` CQEs from `SEND_ZC` two-stage completions are absorbed
    /// internally and never appended to `out`.
    fn poll_completions(&self, max: usize, out: &mut [Completion]) -> usize;

    /// Capability snapshot detected at ring setup.
    fn capabilities(&self) -> &CapabilityMatrix;

    /// Register a buffer pool for fixed I/O operations.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Unsupported`] on thin-fallback backends.
    fn register_buffers(&self, _bufs: &[&[u8]]) -> Result<BufGroupId, RegisterError> {
        Err(RegisterError::Unsupported)
    }

    /// Deregister a previously registered buffer pool.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Unsupported`] on thin-fallback backends.
    fn unregister_buffers(&self, _group: BufGroupId) -> Result<(), RegisterError> {
        Err(RegisterError::Unsupported)
    }

    /// Register a file-descriptor table for fixed-fd operations.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Unsupported`] on thin-fallback backends.
    fn register_files(&self, _fds: &[i32]) -> Result<FdSlot, RegisterError> {
        Err(RegisterError::Unsupported)
    }

    /// Deregister a registered file-descriptor table.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Unsupported`] on thin-fallback backends.
    fn unregister_files(&self, _slot: FdSlot) -> Result<(), RegisterError> {
        Err(RegisterError::Unsupported)
    }

    /// Cancel an in-flight operation.
    ///
    /// # Errors
    ///
    /// Returns [`CancelError::BestEffortDetach`] by default - the op continues
    /// to completion but its result is discarded. `UringDriver` overrides this
    /// with `IORING_OP_ASYNC_CANCEL`.
    fn cancel(&self, _token: SubmitToken) -> Result<(), CancelError> {
        Err(CancelError::BestEffortDetach)
    }
}

/// Error returned by buffer or fd registration operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegisterError {
    /// Backend does not support this registration operation.
    Unsupported,
    /// All registration slots are in use.
    SlotExhausted,
    /// One or more arguments are out of range or invalid.
    InvalidArgument,
}

/// Error returned by [`IoDriver::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancelError {
    /// Cancellation was not sent; the op will complete normally and its result
    /// will be discarded (best-effort detach).
    BestEffortDetach,
    /// No in-flight op with the given token was found (`-ENOENT`): it may
    /// have already completed, or the token was invalid.
    NotFound,
    /// The op was found but is past the cancel point (`-EALREADY`); it will
    /// complete shortly and its result stands. Buffers must stay owned until
    /// that completion arrives.
    TooLateToCancel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_error_is_copy() {
        let error = RegisterError::Unsupported;
        let copy = error;
        assert_eq!(error, copy);
    }

    #[test]
    fn cancel_error_is_copy() {
        let error = CancelError::BestEffortDetach;
        let copy = error;
        assert_eq!(error, copy);
    }
}
