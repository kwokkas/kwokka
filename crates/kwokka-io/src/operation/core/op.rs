//! I/O operation codes and variant flags.

/// Logical I/O operation.
///
/// Fixed/zc/multishot variants are split into [`OpFlags`] to avoid variant explosion.
/// Backend dispatch uses the `(opcode, flags)` pair to select the concrete SQE op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OpCode {
    // Group A - hot path (10)
    /// Read from a file descriptor.
    Read,
    /// Write to a file descriptor.
    Write,
    /// Send data on a socket.
    Send,
    /// Send data on a socket zero-copy, the kernel reads the buffer in place (`SEND_ZC`).
    SendZc,
    /// Receive data on a socket.
    Recv,
    /// Receive into a kernel-selected provided buffer (`buf_ring`).
    RecvProvided,
    /// Send a message with ancillary data.
    Sendmsg,
    /// Receive a message with ancillary data.
    Recvmsg,
    /// Accept an incoming connection.
    Accept,
    /// Connect to a remote address.
    Connect,

    // Group B - control plane (15)
    /// Open a file.
    Open,
    /// Close a file descriptor.
    Close,
    /// Flush file data and metadata to storage.
    Fsync,
    /// Allocate or deallocate file space.
    Fallocate,
    /// Advise the kernel on file access pattern.
    Fadvise,
    /// Query extended file status attributes.
    Statx,
    /// Create a directory.
    Mkdir,
    /// Rename a file or directory.
    Rename,
    /// Remove a file.
    Unlink,
    /// Create a symbolic link.
    Symlink,
    /// Create a hard link.
    Link,
    /// Move data between file descriptors without copying to userspace.
    Splice,
    /// Duplicate data from one pipe to another without consuming it.
    Tee,
    /// Shut down part of a full-duplex socket connection.
    Shutdown,
    /// Create a socket.
    Socket,

    // Group C - driver-internal, submit_internal only (4)
    /// Arm a completion timeout. Driver-internal; use `submit_internal`.
    Timeout,
    /// Cancel an in-flight operation. Driver-internal; use `submit_internal`.
    Cancel,
    /// Send a message to another `io_uring` ring. Driver-internal; use `submit_internal`.
    MsgRing,
    /// Poll a file descriptor for readiness events. Driver-internal; use `submit_internal`.
    Poll,
}

/// Op variant flags.
///
/// **All flags are set automatically - callers never toggle them directly.**
/// The `IoRequest` builder sets each flag according to these rules:
/// - `fixed_buf`: set by `with_registered_buf(slot)`
/// - `fixed_fd`: set by `with_registered_fd(slot)`
/// - `zero_copy`: set for `Send` ops when a registered buffer and capability `send_zc` are present
/// - `multishot`: determined by builder method name (`accept_multishot()` / `recv_multishot()`)
/// - `vectored`: set in `readv` / `writev` dedicated builders only
/// - `buffer_select`: set by `recv_provided()` (`IOSQE_BUFFER_SELECT`)
#[allow(
    clippy::struct_excessive_bools,
    reason = "six independent op-variant flags; each maps to a distinct SQE modifier with no natural grouping"
)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OpFlags {
    /// Use the registered-buffer variant (`_FIXED`).
    pub fixed_buf: bool,
    /// Use the registered-fd variant.
    pub fixed_fd: bool,
    /// Use the zero-copy send variant (`SEND_ZC`).
    pub zero_copy: bool,
    /// Use the multishot variant (`ACCEPT_MULTI` / `RECV_MULTI`).
    pub multishot: bool,
    /// Use the vectored variant (`readv` / `writev`).
    pub vectored: bool,
    /// Use kernel-selected provided buffers (`IOSQE_BUFFER_SELECT`).
    pub buffer_select: bool,
}

impl OpFlags {
    /// All-false flags value.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            fixed_buf: false,
            fixed_fd: false,
            zero_copy: false,
            multishot: false,
            vectored: false,
            buffer_select: false,
        }
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "consumed by the IoRequest builder, not yet implemented"
    )]
    pub(crate) const fn with_fixed_buf(self, v: bool) -> Self {
        Self {
            fixed_buf: v,
            ..self
        }
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "consumed by the IoRequest builder, not yet implemented"
    )]
    pub(crate) const fn with_fixed_fd(self, v: bool) -> Self {
        Self {
            fixed_fd: v,
            ..self
        }
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "consumed by the IoRequest builder, not yet implemented"
    )]
    pub(crate) const fn with_zero_copy(self, v: bool) -> Self {
        Self {
            zero_copy: v,
            ..self
        }
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "consumed by the IoRequest builder, not yet implemented"
    )]
    pub(crate) const fn with_multishot(self, v: bool) -> Self {
        Self {
            multishot: v,
            ..self
        }
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "consumed by the IoRequest builder, not yet implemented"
    )]
    pub(crate) const fn with_vectored(self, v: bool) -> Self {
        Self {
            vectored: v,
            ..self
        }
    }

    #[must_use]
    pub(crate) const fn with_buffer_select(self, v: bool) -> Self {
        Self {
            buffer_select: v,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_flags_default_all_false() {
        let flags = OpFlags::default();
        assert!(!flags.fixed_buf);
        assert!(!flags.fixed_fd);
        assert!(!flags.zero_copy);
        assert!(!flags.multishot);
        assert!(!flags.vectored);
        assert!(!flags.buffer_select);
    }

    #[test]
    fn op_flags_new_matches_default() {
        assert_eq!(OpFlags::new(), OpFlags::default());
    }

    #[test]
    fn op_flags_builder_chain_sets_only_targeted_fields() {
        let flags = OpFlags::new().with_fixed_buf(true).with_multishot(true);
        assert!(flags.fixed_buf);
        assert!(flags.multishot);
        assert!(!flags.fixed_fd);
        assert!(!flags.zero_copy);
        assert!(!flags.vectored);
    }

    #[test]
    fn op_flags_builder_does_not_mutate_original() {
        let base = OpFlags::new();
        let modified = base.with_fixed_buf(true);
        assert!(!base.fixed_buf);
        assert!(modified.fixed_buf);
    }

    #[test]
    fn op_flags_all_six_flags_independent() {
        let flags = OpFlags::new()
            .with_fixed_buf(true)
            .with_fixed_fd(true)
            .with_zero_copy(true)
            .with_multishot(true)
            .with_vectored(true)
            .with_buffer_select(true);
        assert!(flags.fixed_buf);
        assert!(flags.fixed_fd);
        assert!(flags.zero_copy);
        assert!(flags.multishot);
        assert!(flags.vectored);
        assert!(flags.buffer_select);
    }

    #[test]
    fn op_code_is_copy() {
        let op = OpCode::Read;
        let copy = op;
        assert_eq!(op, copy);
    }

    #[test]
    fn op_code_group_a_has_ten_variants() {
        let variants = [
            OpCode::Read,
            OpCode::Write,
            OpCode::Send,
            OpCode::SendZc,
            OpCode::Recv,
            OpCode::RecvProvided,
            OpCode::Sendmsg,
            OpCode::Recvmsg,
            OpCode::Accept,
            OpCode::Connect,
        ];
        assert_eq!(variants.len(), 10);
    }

    #[test]
    fn op_code_group_c_has_four_variants() {
        let variants = [
            OpCode::Timeout,
            OpCode::Cancel,
            OpCode::MsgRing,
            OpCode::Poll,
        ];
        assert_eq!(variants.len(), 4);
    }
}
