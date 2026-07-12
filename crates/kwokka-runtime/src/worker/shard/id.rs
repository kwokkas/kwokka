//! [`WorkerId`] -- per-worker thread identifier.

use core::fmt;

/// Maximum number of workers the runtime supports.
///
/// Derived from [`TaskRef`](crate::task::TaskRef) bit layout: 7 bits for
/// worker routing (bits 62-56), yielding IDs in `[0, 127]`.
const MAX_WORKERS: u8 = 128;

/// Per-worker thread identifier.
///
/// Wraps a `u8` in the range `[0, 127]`, matching the 7-bit worker
/// routing field embedded in [`TaskRef`](crate::task::TaskRef).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerId(u8);

/// Errors from [`WorkerId`] construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkerError {
    /// The requested id exceeds the 7-bit maximum (127).
    InvalidId {
        /// The id that was rejected.
        id: u8,
        /// The maximum valid id.
        max: u8,
    },
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId { id, max } => {
                write!(f, "worker id {id} exceeds maximum {max}")
            }
        }
    }
}

impl core::error::Error for WorkerError {}

impl WorkerId {
    /// Maximum valid worker id (`2^7 - 1 = 127`).
    pub const MAX: u8 = MAX_WORKERS - 1;

    /// Create a worker id.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::InvalidId`] when `id` exceeds
    /// [`Self::MAX`] (127).
    pub const fn new(id: u8) -> Result<Self, WorkerError> {
        if id > Self::MAX {
            return Err(WorkerError::InvalidId { id, max: Self::MAX });
        }
        Ok(Self(id))
    }

    /// Returns the raw `u8` value.
    #[inline]
    pub const fn raw(self) -> u8 {
        self.0
    }
}

impl fmt::Debug for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WorkerId({})", self.0)
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker-{}", self.0)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_zero() {
        let id = WorkerId::new(0);
        assert!(id.is_ok());
        assert_eq!(id.ok().map(WorkerId::raw), Some(0));
    }

    #[test]
    fn new_accepts_max() {
        let id = WorkerId::new(127);
        assert!(id.is_ok());
        assert_eq!(id.ok().map(WorkerId::raw), Some(127));
    }

    #[test]
    fn new_rejects_above_max() {
        let id = WorkerId::new(128);
        assert_eq!(id, Err(WorkerError::InvalidId { id: 128, max: 127 }));
    }

    #[test]
    fn max_matches_task_ref_worker_id_max() {
        assert_eq!(WorkerId::MAX, crate::task::TaskRef::WORKER_ID_MAX);
    }

    #[test]
    fn display_format() {
        let Some(id) = WorkerId::new(42).ok() else {
            panic!("new(42) must succeed");
        };
        assert_eq!(format!("{id}"), "worker-42");
    }

    #[test]
    fn debug_format() {
        let Some(id) = WorkerId::new(7).ok() else {
            panic!("new(7) must succeed");
        };
        assert_eq!(format!("{id:?}"), "WorkerId(7)");
    }

    #[test]
    fn copy_and_eq() {
        let Some(first) = WorkerId::new(5).ok() else {
            panic!("new(5) must succeed");
        };
        let second = first;
        assert_eq!(first, second);
    }

    #[test]
    fn hash_consistency() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };

        fn hash_of<T: Hash>(val: T) -> u64 {
            let mut hasher = DefaultHasher::new();
            val.hash(&mut hasher);
            hasher.finish()
        }

        let Some(first) = WorkerId::new(10).ok() else {
            panic!("new(10) must succeed");
        };
        let Some(second) = WorkerId::new(10).ok() else {
            panic!("new(10) must succeed");
        };
        assert_eq!(hash_of(first), hash_of(second));
    }

    #[test]
    fn ord_ordering() {
        let Some(smaller) = WorkerId::new(3).ok() else {
            panic!("new(3) must succeed");
        };
        let Some(larger) = WorkerId::new(7).ok() else {
            panic!("new(7) must succeed");
        };
        assert!(smaller < larger);
    }

    #[test]
    fn error_display() {
        let error = WorkerError::InvalidId { id: 200, max: 127 };
        assert_eq!(format!("{error}"), "worker id 200 exceeds maximum 127");
    }

    #[test]
    fn max_workers_constant() {
        assert_eq!(MAX_WORKERS, 128);
    }
}
