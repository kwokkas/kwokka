//! Worker wake fd -- an eventfd a parked worker arms on its own ring so
//! another thread can complete the park from outside.
//!
//! The fd runs in counter mode (no semaphore flag): every signal written
//! while a read is in flight or unarmed accumulates, and the next read
//! drains the whole counter in one completion. The re-arm window after a
//! completion therefore cannot lose a wake -- at worst, back-to-back
//! signals coalesce into one CQE, and the woken worker drains its whole
//! inbox regardless.

use std::io;

/// CQE `user_data` marking the wake-fd read, checked before any task
/// decode.
///
/// The completion drain inspects this constant before treating `user_data`
/// as a task handle. In the current slab-only runtime no task submission
/// can produce this value -- a slab handle keeps its top bit clear -- but
/// the maximal arena encoding does collide with it, so this sentinel must
/// be revisited before arena-path tasks submit I/O.
pub const WAKE_FD_USER_DATA: u64 = u64::MAX;

/// Creates the wake eventfd, close-on-exec and nonblocking.
///
/// # Errors
///
/// Returns the OS error when the kernel rejects the creation.
pub fn create_wake_fd() -> io::Result<i32> {
    // SAFETY: Invariant -- eventfd(2) takes a plain initial counter and
    // flag bits; no pointers cross the boundary. Precondition: none beyond
    // a live process. Failure mode: a negative return maps to the OS
    // error and is never used as an fd.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// Signals the wake fd, completing the read a parked worker armed on it.
///
/// A full counter (`EAGAIN`) reports success: the fd is already readable,
/// so the pending read completes and no wake is lost.
///
/// # Errors
///
/// Returns the OS error for anything other than a full counter (e.g. a
/// stale fd).
pub fn signal_wake_fd(fd: i32) -> io::Result<()> {
    let value: u64 = 1;
    // SAFETY: Invariant -- the kernel reads the 8-byte local within this
    // call only; write(2) borrows nothing past its return. Precondition:
    // `fd` came from `create_wake_fd` and is not yet closed. Failure mode:
    // a stale fd returns EBADF, surfaced as an error -- no memory access
    // goes wrong.
    let written = unsafe { libc::write(fd, (&raw const value).cast(), 8) };
    if written == 8 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EAGAIN) {
        return Ok(());
    }
    Err(error)
}

/// Closes a wake fd created by [`create_wake_fd`].
pub fn close_wake_fd(fd: i32) {
    // SAFETY: Invariant -- close(2) consumes the descriptor and touches no
    // memory of ours. Precondition: `fd` came from `create_wake_fd` and is
    // closed exactly once. Failure mode: a double close returns EBADF,
    // which carries nothing actionable -- the descriptor is gone either
    // way.
    unsafe {
        libc::close(fd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_signal_close_round_trip() {
        let Ok(fd) = create_wake_fd() else {
            panic!("eventfd creation must succeed");
        };
        let Ok(()) = signal_wake_fd(fd) else {
            panic!("signaling a fresh wake fd must succeed");
        };
        close_wake_fd(fd);
    }

    #[test]
    fn signal_on_a_stale_fd_reports_the_error() {
        let Ok(fd) = create_wake_fd() else {
            panic!("eventfd creation must succeed");
        };
        close_wake_fd(fd);
        let Err(_error) = signal_wake_fd(fd) else {
            panic!("a stale fd must surface an error");
        };
    }
}
