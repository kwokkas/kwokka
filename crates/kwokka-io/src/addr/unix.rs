//! Unix domain socket address types.

use core::fmt;

/// Maximum byte length of a `sun_path` field (108-byte buffer minus null terminator).
pub(super) const SUN_PATH_MAX: usize = 107;

/// Unix domain socket address.
///
/// Used with `SockAddr` for `connect`, `sendmsg`, and `recvmsg` operations
/// on Unix domain sockets. All variants store path bytes inline to avoid
/// heap allocation.
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UnixAddr {
    /// Filesystem-bound path. Maximum 107 bytes (108-byte `sun_path` minus null terminator).
    Path {
        /// Path bytes (zero-padded beyond `len`).
        buf: [u8; SUN_PATH_MAX],
        /// Actual length of the path.
        len: u8,
    },

    /// Linux abstract namespace address (`sun_path[0] == 0`). Maximum 107 bytes.
    ///
    /// Abstract sockets are not bound to the filesystem and disappear when the
    /// last file descriptor referring to them is closed. Stored inline as a
    /// fixed-capacity `[u8; 107]` to avoid heap allocation.
    #[cfg(target_os = "linux")]
    Abstract {
        /// Name bytes (zero-padded beyond `len`).
        buf: [u8; SUN_PATH_MAX],
        /// Actual length of the name.
        len: u8,
    },

    /// Unnamed (autobind or anonymous) socket.
    Unnamed,
}

impl UnixAddr {
    /// Creates a filesystem-path Unix address.
    ///
    /// # Errors
    ///
    /// Returns [`AddrError::PathTooLong`] if the path exceeds 107 bytes.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "bytes.len() validated <= 107, fits in u8"
    )]
    pub fn path(path: impl AsRef<std::path::Path>) -> Result<Self, AddrError> {
        let bytes = path.as_ref().as_os_str().as_encoded_bytes();
        if bytes.len() > SUN_PATH_MAX {
            return Err(AddrError::PathTooLong);
        }
        let mut buf = [0u8; SUN_PATH_MAX];
        buf[..bytes.len()].copy_from_slice(bytes);
        Ok(Self::Path {
            buf,
            len: bytes.len() as u8,
        })
    }

    /// Returns the path bytes, if this is a `Path` variant.
    pub fn path_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Path { buf, len } => Some(&buf[..*len as usize]),
            #[cfg(target_os = "linux")]
            Self::Abstract { .. } | Self::Unnamed => None,
            #[cfg(not(target_os = "linux"))]
            Self::Unnamed => None,
        }
    }

    /// Creates a Linux abstract-namespace Unix address.
    ///
    /// # Errors
    ///
    /// Returns [`AddrError::AbstractTooLong`] if `name` exceeds 107 bytes.
    #[cfg(target_os = "linux")]
    #[allow(
        clippy::cast_possible_truncation,
        reason = "name.len() validated <= 107, fits in u8"
    )]
    pub fn abstract_(name: &[u8]) -> Result<Self, AddrError> {
        if name.len() > SUN_PATH_MAX {
            return Err(AddrError::AbstractTooLong);
        }
        let mut buf = [0u8; SUN_PATH_MAX];
        buf[..name.len()].copy_from_slice(name);
        Ok(Self::Abstract {
            buf,
            len: name.len() as u8,
        })
    }

    /// Byte slice of the abstract name, if this is an `Abstract` variant.
    #[cfg(target_os = "linux")]
    pub fn abstract_name(&self) -> Option<&[u8]> {
        match self {
            Self::Abstract { buf, len } => Some(&buf[..*len as usize]),
            _ => None,
        }
    }
}

impl fmt::Debug for UnixAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path { buf, len } => f
                .debug_tuple("Path")
                .field(&String::from_utf8_lossy(&buf[..*len as usize]))
                .finish(),
            #[cfg(target_os = "linux")]
            Self::Abstract { buf, len } => f
                .debug_tuple("Abstract")
                .field(&&buf[..*len as usize])
                .finish(),
            Self::Unnamed => write!(f, "Unnamed"),
        }
    }
}

/// Error constructing a [`UnixAddr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AddrError {
    /// Path exceeds the 107-byte `sun_path` limit.
    PathTooLong,
    /// Abstract name exceeds the 107-byte limit.
    AbstractTooLong,
}

impl fmt::Display for AddrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PathTooLong => "unix path exceeds 107-byte sun_path limit",
            Self::AbstractTooLong => "abstract name exceeds 107-byte limit",
        })
    }
}

impl core::error::Error for AddrError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_within_limit_succeeds() {
        let Ok(addr) = UnixAddr::path("/tmp/kwokka.sock") else {
            panic!("expected Ok");
        };
        assert_eq!(addr.path_bytes(), Some(b"/tmp/kwokka.sock".as_slice()));
    }

    #[test]
    fn path_at_107_bytes_succeeds() {
        let path = "a".repeat(107);
        let Ok(_) = UnixAddr::path(&path) else {
            panic!("expected Ok for 107-byte path");
        };
    }

    #[test]
    fn path_at_108_bytes_fails() {
        let path = "a".repeat(108);
        assert_eq!(UnixAddr::path(&path), Err(AddrError::PathTooLong));
    }

    #[test]
    fn unnamed_variant_constructs() {
        let addr = UnixAddr::Unnamed;
        assert_eq!(addr, UnixAddr::Unnamed);
    }

    #[test]
    fn addr_error_display_path_too_long() {
        assert_eq!(
            AddrError::PathTooLong.to_string(),
            "unix path exceeds 107-byte sun_path limit"
        );
    }

    #[test]
    fn addr_error_display_abstract_too_long() {
        assert_eq!(
            AddrError::AbstractTooLong.to_string(),
            "abstract name exceeds 107-byte limit"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn abstract_within_limit_succeeds() {
        let Ok(addr) = UnixAddr::abstract_(b"kwokka") else {
            panic!("expected Ok");
        };
        assert_eq!(addr.abstract_name(), Some(b"kwokka".as_slice()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn abstract_at_108_bytes_fails() {
        let name = [b'x'; 108];
        assert_eq!(UnixAddr::abstract_(&name), Err(AddrError::AbstractTooLong));
    }
}
