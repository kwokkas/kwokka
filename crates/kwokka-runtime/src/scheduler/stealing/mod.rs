//! Work-stealing substrate, carrying [`TaskRef`] handles only.
//!
//! The 512-byte task bodies never enter queues directly. A thief asks
//! ([`thief`]), a victim serves ([`victim`]), and the task's bytes move across
//! slabs through the transport ([`relocate`]). What is left behind is remembered
//! twice over: the victim keeps a route for stale handles ([`forward`]), and the
//! thief keeps a note of where the task came from so it can report it settled
//! ([`origin`]).
//!
//! The run-loop composition -- when to serve, when to receive, which ring to
//! read -- lives in the runtime layer, not here.
//!
//! [`TaskRef`]: crate::task::TaskRef

pub(crate) mod forward;
pub(crate) mod origin;
pub(crate) mod relocate;
pub(crate) mod thief;
pub(crate) mod victim;
