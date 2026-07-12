//! What the worker's completion drain calls: routing a CQE to its owner,
//! submitting cancels, disposing cancelled ops, and reclaiming slots.

use crate::{
    DriverType, IoDriver,
    boundary::{
        cancel::{
            ACCEPT_CANCEL_SLOT, AcceptCancelSet, CONNECT_CANCEL_SLOT, ConnectCancelSet,
            PROVIDED_RECV_CANCEL_SLOT, ProvidedRecvCancelSet, encode_cancel_sentinel,
            encode_recv_multishot_sentinel, is_multishot_sentinel, multishot_sentinel_generation,
            multishot_sentinel_slot,
        },
        seam::socket::adopt_accepted_fd,
    },
    buffer::{
        multishot::{
            MultishotPush, MultishotSlab, MultishotSlotKey, NO_BUFFER, RecvMultishotPush,
            RecvMultishotSlab, RecvMultishotSlotKey,
        },
        oneshot::inflight::{InflightBufSlab, InflightSlotKey},
    },
    operation::{CqeFlags, IoRequest, SubmitToken},
};

/// The wake and retire targets a multishot CQE resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultishotCompletion {
    /// The owner task to wake, set when the result was queued for it.
    pub wake: Option<u64>,
    /// The owner task whose one counted SQE retires, set on the terminal
    /// (no-`MORE`) CQE regardless of wake, so the worker's in-flight accounting
    /// pairs with the submit even when the owning stream already dropped.
    pub retire: Option<u64>,
}

/// Routes a multishot op's completion CQE into the worker's registry.
///
/// The completion drain calls this on a CQE whose `user_data` is a multishot
/// sentinel (see [`is_multishot_sentinel`]). It queues the result for the owning
/// stream and returns the [`MultishotCompletion`] targets: [`wake`](MultishotCompletion::wake)
/// names the owner to wake when a result was queued, and [`retire`](MultishotCompletion::retire)
/// names the owner to retire on the terminal (no-`MORE`) CQE so the in-flight
/// count pairs with the submit even when the stream already dropped. Nothing is
/// queued when the slot is stale, the FIFO overflowed, or the stream dropped --
/// a cancel-pending slot, whose intermediate results are discarded and whose
/// terminal CQE frees the slot here.
///
/// A discarded nonnegative accept result is a kernel-created fd; it is closed
/// here so a dropped or overflowed stream does not leak the descriptor.
pub fn push_multishot_completion(
    slab: &mut MultishotSlab,
    user_data: u64,
    result: i32,
    flags: CqeFlags,
) -> MultishotCompletion {
    let slot = multishot_sentinel_slot(user_data);
    let generation = multishot_sentinel_generation(user_data);
    let is_more = flags.contains(CqeFlags::MORE);
    // The one SQE `poll_one` counted retires when the op posts its terminal CQE,
    // live or cancel-pending. Read the owner before any free so a cancel-pending
    // terminal still retires it; a stale slot reads `None` and retires nothing.
    let owner = slab.owner(slot, generation);
    let retire = if is_more { None } else { owner };
    if slab.is_cancel_pending(slot, generation) {
        // The owning stream dropped. Each intermediate CQE (`MORE` set) carries
        // an accepted fd it will never take, so close it; the terminal CQE (the
        // op's `-ECANCELED`, or the cancel op's own status) carries no
        // descriptor and frees the slot.
        if is_more {
            dispose_accept_result(result);
        } else {
            slab.free_by_slot(slot, generation);
        }
        return MultishotCompletion { wake: None, retire };
    }
    let wake = match slab.push(slot, generation, result, is_more) {
        MultishotPush::Queued => owner,
        MultishotPush::Overflowed | MultishotPush::Stale => {
            // Only an intermediate CQE carries a descriptor; a terminal status
            // is not an fd.
            if is_more {
                dispose_accept_result(result);
            }
            None
        }
    };
    MultishotCompletion { wake, retire }
}

/// Closes a nonnegative accept result the owning stream will never observe.
///
/// A negative result is an `-errno`, not a descriptor;
/// [`adopt_accepted_fd`](crate::boundary::adopt_accepted_fd) returns `None` and the drop is a
/// no-op.
fn dispose_accept_result(result: i32) {
    drop(adopt_accepted_fd(result));
}

/// Recycles a completion's kernel-selected buffer id to the driver's pool.
///
/// A dropped or overflowed multishot recv completion still consumed a provided
/// buffer the caller will never take; this returns it to the ring exactly once
/// so the ring does not silently shrink. A `None` id (end of stream or error)
/// or a backend with no pool is a no-op.
fn recycle_provided(driver: &DriverType, buf_id: Option<u16>) {
    let Some(id) = buf_id else {
        return;
    };
    if let Some(pool) = driver.provided_recv_pool() {
        pool.recycle(id);
    }
}

/// Routes a multishot recv op's completion CQE into the worker's registry.
///
/// The completion drain calls this on a CQE whose `user_data` is a
/// multishot-recv sentinel (see
/// [`is_recv_multishot_sentinel`](crate::boundary::is_recv_multishot_sentinel)).
/// It queues a
/// data completion's `(result, buf_id)` in the owning stream's FIFO, or stashes
/// the terminal completion in the slot; either way it wakes the owning task and
/// leaves the buffer for the consumer to recycle, and returns the
/// [`MultishotCompletion`] targets, the same wake and retire contract the accept
/// path uses. Nothing is kept when the slot is stale, the FIFO overflowed (a
/// data completion only -- the terminal is stashed, never overflowed), or the
/// stream dropped; in each of those cases the completion's provided buffer is
/// recycled here so it returns to the ring exactly once.
///
/// Unlike the accept path, the buffer id is read and recycled on every CQE
/// regardless of the `MORE` flag: the recv-multishot ABI reports a selected
/// buffer on intermediate and terminal CQEs alike, and a buffer returned twice
/// would let the kernel hand one region to two ops at once.
pub fn push_recv_multishot_completion(
    slab: &mut RecvMultishotSlab,
    driver: &DriverType,
    user_data: u64,
    result: i32,
    flags: CqeFlags,
    buf_id: Option<u16>,
) -> MultishotCompletion {
    let slot = multishot_sentinel_slot(user_data);
    let generation = multishot_sentinel_generation(user_data);
    let is_more = flags.contains(CqeFlags::MORE);
    let owner = slab.owner(slot, generation);
    let retire = if is_more { None } else { owner };
    if slab.is_cancel_pending(slot, generation) {
        // The owning stream dropped. Recycle the buffer this CQE consumed -- on
        // every CQE, MORE or not -- and free the slot on the terminal CQE.
        recycle_provided(driver, buf_id);
        if !is_more {
            slab.free_by_slot(slot, generation);
        }
        return MultishotCompletion { wake: None, retire };
    }
    let wake = match slab.push(
        slot,
        generation,
        result,
        buf_id.unwrap_or(NO_BUFFER),
        is_more,
    ) {
        // A queued data completion and a stashed terminal are both owned by the
        // consumer, which recycles their buffer on drain; wake the owner, recycle
        // nothing here.
        RecvMultishotPush::Queued | RecvMultishotPush::Terminal => owner,
        RecvMultishotPush::Overflowed | RecvMultishotPush::Stale => {
            recycle_provided(driver, buf_id);
            None
        }
    };
    MultishotCompletion { wake, retire }
}

/// Disposes the descriptor from a cancelled single-shot accept's completion.
///
/// The completion drain calls this on every task-token CQE. When `token` names a
/// dropped accept recorded by [`submit_accept_cancel`], the
/// op still produced a descriptor the caller will never take, so it is closed here (a negative
/// result is an `-errno`, not an fd) and the CQE is consumed. Returns whether it
/// handled the CQE; the common empty-set case is an `O(1)` miss.
pub fn dispose_cancelled_accept<const N: usize>(
    accepts: &mut AcceptCancelSet<N>,
    token: u64,
    result: i32,
) -> bool {
    if accepts.is_empty() || !accepts.take(token) {
        return false;
    }
    dispose_accept_result(result);
    true
}

/// Consumes a cancelled single-shot connect's completion, disposing nothing.
///
/// The completion drain calls this before the generic task-token path. When
/// `token` names a dropped connect recorded by
/// [`submit_connect_cancel`], the belated CQE is taken here
/// so its result never reaches a live task's wake slot. Unlike accept, a connect produces no
/// descriptor -- success is result `0`, not an fd -- so there is nothing to close; this never
/// inspects the result, which is why a cancelled successful connect can never close fd `0`.
/// Returns whether it consumed the CQE; the common empty-set case is an `O(1)`
/// miss.
pub fn dispose_cancelled_connect<const N: usize>(
    connects: &mut ConnectCancelSet<N>,
    token: u64,
) -> bool {
    !connects.is_empty() && connects.take(token)
}

/// Submits a cancel for a dropped buffered future's in-flight op and marks its
/// slot retire-pending.
///
/// The worker's cancel-drain calls this for each [`InflightSlotKey`] popped
/// from the cancel inbox. It marks the slot retire-pending, then submits an
/// `ASYNC_CANCEL` SQE to hurry the op toward completion. The slot is freed when
/// the original op posts its completion (see
/// [`reclaim_dropped_slot`]), or, if that op already
/// completed before the cancel, on the cancel's own `-ENOENT` completion (see
/// [`reclaim_cancel_completion`]).
///
/// A refused submit (a full ring, a backend without cancel) leaves the slot
/// retire-pending. If the original op completes, its completion reclaims the
/// slot; otherwise this is a bounded leak until worker teardown, never a slot
/// freed under an in-flight kernel write.
pub fn submit_cancel(driver: &DriverType, slab: &mut InflightBufSlab, key: InflightSlotKey) {
    slab.mark_retire_pending(key);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal returns a best-effort SubmitResult; a refused
    // cancel leaves the slot retire-pending as a bounded leak reclaimed at
    // worker teardown, never a use-after-free.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped multishot stream's op and marks its slot
/// cancel-pending.
///
/// Called by the cancel drain for a queued cancel whose `op_token` is a
/// multishot sentinel. It closes any accepts already queued for the gone stream,
/// marks the registry slot cancel-pending, then submits an `ASYNC_CANCEL`
/// targeting the multishot op by its sentinel `user_data`. The op's terminal
/// completion (its `-ECANCELED`, or the cancel op's own status) frees the slot
/// through [`push_multishot_completion`]; intermediate accepts arriving after the
/// mark are closed there, so no descriptor leaks either way.
pub fn submit_multishot_cancel(
    driver: &DriverType,
    slab: &mut MultishotSlab,
    key: InflightSlotKey,
) {
    let slot = MultishotSlotKey {
        slot: key.slot,
        generation: key.generation,
        worker_id: key.worker_id,
    };
    // The dropped stream will never take the accepts already queued in its FIFO;
    // close each one so the descriptor does not leak. A negative result is an
    // -errno, not an fd, and disposes as a no-op.
    while let Some(result) = slab.pop(slot) {
        dispose_accept_result(result);
    }
    slab.mark_cancel_pending(slot);
    let request =
        IoRequest::<()>::cancel(SubmitToken::new(key.op_token)).with_user_data(key.op_token);
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the slot
    // cancel-pending, and the op's own completions still drive the free, so this
    // is a bounded hurry-up, never a leak or a use-after-free.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped multishot recv stream's op and marks its slot
/// cancel-pending.
///
/// Mirrors [`submit_multishot_cancel`] for the recv registry: it recycles any
/// buffers already queued for the gone stream (a dropped recv never takes them,
/// and each is a provided buffer that must return to the ring) -- both the FIFO
/// data completions and a terminal completion already stashed in the slot (#230).
/// If the op has already terminated, no further CQE will arrive to free the slot,
/// so this frees it at once; otherwise it marks the slot cancel-pending and
/// submits an `ASYNC_CANCEL` targeting the op by its sentinel `user_data`, and
/// the op's terminal completion frees the slot through
/// [`push_recv_multishot_completion`], recycling any buffers arriving after the
/// mark.
pub fn submit_recv_multishot_cancel(
    driver: &DriverType,
    slab: &mut RecvMultishotSlab,
    key: RecvMultishotSlotKey,
) {
    // Recycle every buffer queued for the gone stream so no provided buffer
    // leaks out of the ring; a NO_BUFFER entry carries nothing to recycle.
    while let Some((_, buf_id)) = slab.pop(key) {
        if buf_id != NO_BUFFER {
            recycle_provided(driver, Some(buf_id));
        }
    }
    // The terminal completion lives in the slot's stash, not the FIFO (#230), so
    // it needs the same drain: a terminal CQE that already arrived can carry a
    // real provided buffer the dropped stream will never take.
    if let Some((_, buf_id)) = slab.take_terminal(key) {
        if buf_id != NO_BUFFER {
            recycle_provided(driver, Some(buf_id));
        }
    }
    // A terminated op has posted its final CQE, so no completion remains to free
    // the slot. Marking it cancel-pending would strand it on a best-effort cancel
    // that the ring can refuse; free it here instead.
    if slab.is_terminated(key) {
        slab.free(key);
        return;
    }
    slab.mark_cancel_pending(key);
    let sentinel = encode_recv_multishot_sentinel(key);
    let request = IoRequest::<()>::cancel(SubmitToken::new(sentinel)).with_user_data(sentinel);
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the slot
    // cancel-pending, and the op's own completions still drive the free, so this
    // is a bounded hurry-up, never a leak or a use-after-free.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped single-shot accept.
///
/// The accept op holds no slab slot, so this only submits an `ASYNC_CANCEL`
/// targeting the op by its `user_data` token and records the token in `accepts`.
/// A completion arriving after the cancel disposes the accepted fd through
/// [`dispose_cancelled_accept`] rather than orphaning it in the wake slot; the
/// cancel's own CQE decodes to a slot no registry owns, so
/// [`reclaim_cancel_completion`] treats it as a no-op.
pub fn submit_accept_cancel<const N: usize>(
    driver: &DriverType,
    accepts: &mut AcceptCancelSet<N>,
    key: InflightSlotKey,
) {
    accepts.insert(key.op_token);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the accept
    // running, and its completion still routes through dispose_cancelled_accept.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped provided-buffer recv.
///
/// The recv holds no slab slot, so this only records the token in `cancels`
/// and submits an `ASYNC_CANCEL` targeting the op by its `user_data` token. A
/// completion arriving after the cancel routes through
/// [`dispose_cancelled_op`], which recycles the kernel-selected buffer the
/// dropped future will never take; the cancel's own CQE decodes to a slot no
/// registry owns, so [`reclaim_cancel_completion`]
/// treats it as a no-op.
pub fn submit_provided_recv_cancel<const N: usize>(
    driver: &DriverType,
    cancels: &mut ProvidedRecvCancelSet<N>,
    key: InflightSlotKey,
) {
    cancels.insert(key.op_token);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the recv
    // running, and its completion still routes through dispose_cancelled_op.
    let _ = driver.submit_internal(request);
}

/// Submits a cancel for a dropped single-shot connect.
///
/// The connect holds no slab slot, so this only records the token in `connects`
/// and submits an `ASYNC_CANCEL` targeting the op by its `user_data` token. A
/// completion arriving after the cancel routes through [`dispose_cancelled_op`],
/// which disposes nothing -- a connect owns no descriptor -- but diverts the CQE
/// off the task-token path; the cancel's own CQE decodes to a slot no registry
/// owns, so [`reclaim_cancel_completion`] treats it as
/// a no-op.
pub fn submit_connect_cancel<const N: usize>(
    driver: &DriverType,
    connects: &mut ConnectCancelSet<N>,
    key: InflightSlotKey,
) {
    connects.insert(key.op_token);
    let request = IoRequest::<()>::cancel(SubmitToken::new(key.op_token))
        .with_user_data(encode_cancel_sentinel(key));
    // IGNORE: submit_internal is best-effort; a refused cancel leaves the connect
    // running, and its completion still routes through dispose_cancelled_op.
    let _ = driver.submit_internal(request);
}

/// Routes a queued cancel to the mechanism that owns its op.
///
/// A cancel whose `op_token` is a multishot sentinel targets the multishot
/// registry; the `ACCEPT_CANCEL_SLOT` marker targets a slotless single-shot
/// accept; the `PROVIDED_RECV_CANCEL_SLOT` marker targets a slotless
/// provided-buffer recv; every other cancel is a buffered op's in-flight slot.
pub fn submit_cancel_for<const A: usize, const P: usize, const C: usize>(
    driver: &DriverType,
    inflight: &mut InflightBufSlab,
    multishot: &mut MultishotSlab,
    accepts: &mut AcceptCancelSet<A>,
    provided_recvs: &mut ProvidedRecvCancelSet<P>,
    connects: &mut ConnectCancelSet<C>,
    key: InflightSlotKey,
) {
    if is_multishot_sentinel(key.op_token) {
        submit_multishot_cancel(driver, multishot, key);
    } else if key.slot == ACCEPT_CANCEL_SLOT {
        submit_accept_cancel(driver, accepts, key);
    } else if key.slot == PROVIDED_RECV_CANCEL_SLOT {
        submit_provided_recv_cancel(driver, provided_recvs, key);
    } else if key.slot == CONNECT_CANCEL_SLOT {
        submit_connect_cancel(driver, connects, key);
    } else {
        submit_cancel(driver, inflight, key);
    }
}

/// Disposes a task-token completion that a dropped accept or provided recv
/// will never take.
///
/// The completion drain calls this on every task-token CQE, before the result
/// is stored and the task woken. A CQE carrying a kernel-selected buffer id
/// is definitively a provided recv's -- no other op sets the buffer flag --
/// so when its token names a dropped recv, the buffer recycles into the
/// driver's pool. A bufferless CQE checks the dropped accepts first (a
/// nonnegative accept result is a descriptor to close), then the dropped
/// provided recvs (an end-of-stream or error completion, nothing to recycle).
/// Returns whether it consumed the CQE; the common empty-sets case is an
/// `O(1)` miss.
///
/// One task dropping an in-flight accept AND an in-flight provided recv
/// shares one token across both sets, and a bufferless completion then cannot
/// name its op. An end-of-stream `0` is taken by the provided set first --
/// adopted as an accept result it would close descriptor zero -- and every
/// other bufferless result checks the accepts before the provided recvs, so
/// the residue is a misrouted disposal (a stale entry in the wrong set, or a
/// leaked descriptor), never an unrelated close: the same ambiguity class
/// the accept set already carries against dropped buffered ops sharing a
/// token. The same holds within one kind: a task that drops an in-flight
/// provided recv and reissues one before the first completes shares the
/// token across both ops, and whichever completion lands first is disposed
/// as the dropped one. A per-op registry (the multishot model) is the
/// structural fix, deferred with multishot recv.
pub fn dispose_cancelled_op<const A: usize, const P: usize, const C: usize>(
    driver: &DriverType,
    accepts: &mut AcceptCancelSet<A>,
    provided_recvs: &mut ProvidedRecvCancelSet<P>,
    connects: &mut ConnectCancelSet<C>,
    token: u64,
    result: i32,
    buf_id: Option<u16>,
) -> bool {
    if let Some(id) = buf_id {
        if provided_recvs.is_empty() || !provided_recvs.take(token) {
            return false;
        }
        // The dropped recv's op still consumed a buffer; this CQE is the
        // kernel's done-with-the-bytes signal, so the id returns to the ring
        // here or never.
        if let Some(pool) = driver.provided_recv_pool() {
            pool.recycle(id);
        }
        return true;
    }
    // A dropped connect owns no descriptor, so its belated CQE is taken by token
    // alone, ahead of every result-inspecting path: a successful connect's `0`
    // result never reaches the accept path that would adopt it as descriptor
    // zero, and the diversion keeps the stale result off a live task's wake slot.
    if dispose_cancelled_connect(connects, token) {
        return true;
    }
    // An end-of-stream completion whose token names a dropped provided recv
    // is taken before the accept check: a `0` result adopted as an accept
    // descriptor would close descriptor zero, so the ambiguous case prefers
    // a bounded accept-side leak over an unrelated close.
    if result == 0 && !provided_recvs.is_empty() && provided_recvs.take(token) {
        return true;
    }
    if dispose_cancelled_accept(accepts, token, result) {
        return true;
    }
    !provided_recvs.is_empty() && provided_recvs.take(token)
}

/// Reclaims the retire-pending slot whose op matches `op_token`, if any.
///
/// The completion drain calls this on every task-token CQE. When the owning
/// future has dropped, the slot is retire-pending and its op's completion frees
/// it here -- that CQE is the kernel's done-with-the-bytes signal, for every
/// cancel outcome. When the future is still live no slot is retire-pending for
/// that op, so this is a no-op and the future frees through its own harvest
/// path instead.
pub fn reclaim_dropped_slot(slab: &mut InflightBufSlab, op_token: u64) {
    slab.free_by_op_token(op_token);
}

/// Reclaims a slot on a cancel completion whose target op is already gone.
///
/// The cancel op's own CQE is normally not a free trigger: the original op's
/// completion frees the slot by `op_token` through
/// [`reclaim_dropped_slot`]. The one exception is `-ENOENT`,
/// which means the target op completed and posted its single CQE before the cancel was issued, so
/// no op-token completion is coming for this slot. Only then is the slot freed here, decoded from
/// the sentinel `user_data` and matched on its low 16 generation bits. Every other
/// result (`0` and `-EALREADY`, where the target still has a completion coming,
/// plus `-EINVAL` or any other error) is a no-op, so a slot the kernel may
/// still be writing is never freed early.
pub fn reclaim_cancel_completion(slab: &mut InflightBufSlab, sentinel_user_data: u64, result: i32) {
    // ABI: -ENOENT (errno 2) means the target request could not be located
    // because it completed before the cancel was issued (or an invalid id was
    // used), per io_uring_prep_cancel.3. Its own CQE was already drained, so
    // this cancel completion is the last signal that the slot is free.
    const CANCEL_TARGET_GONE: i32 = -2;
    if result != CANCEL_TARGET_GONE {
        return;
    }
    let slot = (sentinel_user_data & 0xFFFF) as u16;
    let generation_low16 = ((sentinel_user_data >> 16) & 0xFFFF) as u16;
    slab.free_if_retire_pending(slot, generation_low16);
}

/// Marks the slot for `op_token` as awaiting its `SEND_ZC` NOTIF.
///
/// The completion drain calls this on a primary CQE that carried
/// `IORING_CQE_F_MORE`, which means a notification CQE releasing the buffer is
/// still coming (`io_uring_prep_send_zc.3`). Until it lands the kernel may still
/// read the buffer, so a racing `-ENOENT` cancel must not free the slot:
/// `InflightBufSlab::free_if_retire_pending` refuses while the slot is
/// notif-expected but not yet notif-ready. A no-op when the op's slot is not
/// tracked here.
pub fn mark_notif_expected(slab: &mut InflightBufSlab, op_token: u64) {
    slab.mark_notif_expected_by_op_token(op_token);
}

/// Reclaims or arms a `SEND_ZC` slot on its NOTIF completion.
///
/// The completion drain calls this on a NOTIF CQE (`IORING_CQE_F_NOTIF`,
/// `io_uring_prep_send_zc.3`): the kernel has released the buffer. Two cases,
/// distinguished by whether the owning future has dropped. A dropped future
/// left its slot retire-pending, so `InflightBufSlab::free_by_op_token` frees
/// it now -- the NOTIF is the last signal for that slot. A still-live future
/// has no retire-pending slot for the op, so the slot is marked notif-ready
/// instead, and the future frees it on its next poll through
/// [`IoSeam::slot_notif_ready`](crate::boundary::IoSeam::slot_notif_ready).
pub fn reclaim_notif(slab: &mut InflightBufSlab, op_token: u64) {
    if !slab.free_by_op_token(op_token) {
        slab.mark_notif_ready_by_op_token(op_token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::cancel::{encode_multishot_sentinel, is_cancel_sentinel};

    #[test]
    fn reclaim_frees_dropped_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        // The original op's completion, keyed by op_token, frees the slot.
        reclaim_dropped_slot(&mut slab, 0xAA);
        let Some(next) = slab.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.slot, key.slot,
            "the slot is reused after its op completion reclaims it",
        );
    }

    #[test]
    fn reclaim_notif_frees_dropped_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        // Primary carried F_MORE, then the future dropped (retire-pending).
        mark_notif_expected(&mut slab, 0xAA);
        slab.mark_retire_pending(key);
        // The NOTIF is the last signal for a dropped future: free the slot.
        reclaim_notif(&mut slab, 0xAA);
        let Some(next) = slab.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.slot, key.slot,
            "the NOTIF frees a dropped future's slot",
        );
    }

    #[test]
    fn reclaim_notif_arms_live_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        // Primary carried F_MORE; the future is still live (not retire-pending).
        mark_notif_expected(&mut slab, 0xAA);
        reclaim_notif(&mut slab, 0xAA);
        assert!(
            slab.is_notif_ready(key),
            "a live future's slot is armed notif-ready, not freed",
        );
        assert!(
            slab.slot_ptr(key).is_some(),
            "the live slot survives its NOTIF",
        );
    }

    #[test]
    fn cancel_enoent_frees_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        let sentinel = encode_cancel_sentinel(key);
        // -ENOENT means the target op already completed, so this cancel reclaims.
        reclaim_cancel_completion(&mut slab, sentinel, -2);
        let Some(next) = slab.allocate(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(
            next.slot, key.slot,
            "a -ENOENT cancel frees the slot for reuse"
        );
    }

    #[test]
    fn cancel_success_leaves_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        let sentinel = encode_cancel_sentinel(key);
        // 0 and -EALREADY mean the target still has a completion coming, so the
        // slot must not be freed here.
        reclaim_cancel_completion(&mut slab, sentinel, 0);
        reclaim_cancel_completion(&mut slab, sentinel, -114);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a still-completing cancel leaves the slot live",
        );
    }

    #[test]
    fn cancel_generation_mismatch_leaves_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        slab.mark_retire_pending(key);
        // A stale sentinel carrying a different generation must not free the slot.
        let stale = encode_cancel_sentinel(InflightSlotKey {
            generation: key.generation + 1,
            ..key
        });
        reclaim_cancel_completion(&mut slab, stale, -2);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a mismatched generation leaves the slot live",
        );
    }

    #[test]
    fn cancel_enoent_ignores_live_slot() {
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xAA) else {
            panic!("allocate must succeed");
        };
        // Not retire-pending: a live future still owns the slot.
        let sentinel = encode_cancel_sentinel(key);
        reclaim_cancel_completion(&mut slab, sentinel, -2);
        assert!(
            slab.slot_ptr(key).is_some(),
            "a live slot is never freed by a cancel completion",
        );
    }

    #[test]
    fn non_sentinel_routes_to_task() {
        assert!(
            !is_cancel_sentinel(0x1234_5678),
            "a slab-path task token routes to the task path, not slot reclaim",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_marks_retire_pending() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = InflightBufSlab::new(4, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(0xBEEF) else {
            panic!("allocate must succeed");
        };
        submit_cancel(&driver, &mut slab, key);
        assert!(
            slab.is_retire_pending(key.slot),
            "the slot is marked retire-pending even when the backend refuses the cancel",
        );
    }

    #[test]
    fn multishot_completion_queues_and_wakes_owner() {
        let mut slab = MultishotSlab::new(15, 4);
        let Some(key) = slab.allocate(0xF00D) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, 9, CqeFlags::MORE),
            MultishotCompletion {
                wake: Some(0xF00D),
                retire: None,
            },
            "an intermediate completion wakes the owner without retiring the op",
        );
        assert_eq!(slab.pop(key), Some(9), "the result reached the FIFO");
    }

    #[test]
    fn multishot_completion_terminal_retires_live_owner() {
        let mut slab = MultishotSlab::new(23, 4);
        let Some(key) = slab.allocate(0xBEEF) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        // A live terminal CQE queues its result (wake) and retires the one SQE.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -104, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: Some(0xBEEF),
                retire: Some(0xBEEF),
            },
            "a live terminal CQE both wakes and retires the owner",
        );
    }

    #[test]
    fn multishot_completion_cancel_pending_frees_on_terminal() {
        let mut slab = MultishotSlab::new(16, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        slab.mark_cancel_pending(key);
        let sentinel = encode_multishot_sentinel(key);
        // An intermediate CQE for a dropped stream wakes nothing; a negative
        // result carries no fd, so the dispose path closes nothing.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a cancel-pending intermediate wakes and retires nothing",
        );
        assert!(
            slab.is_live(key),
            "the slot survives until its terminal CQE"
        );
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -125, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: None,
                retire: Some(0x1),
            },
            "the terminal CQE retires the owner without waking",
        );
        assert!(
            !slab.is_live(key),
            "the terminal CQE frees the cancel-pending slot",
        );
    }

    #[test]
    fn multishot_completion_stale_sentinel_wakes_nothing() {
        let mut slab = MultishotSlab::new(17, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        slab.free(key);
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a sentinel naming a freed slot routes to nothing",
        );
    }

    #[test]
    fn multishot_completion_overflow_wakes_nothing() {
        use crate::buffer::multishot::MULTISHOT_FIFO_DEPTH;

        let mut slab = MultishotSlab::new(18, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_multishot_sentinel(key);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            slab.push(key.slot, gen_low16, value, true);
        }
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -22, CqeFlags::MORE),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "an overflowing completion routes to nothing",
        );
        // The terminal CQE retires the owner even when the full FIFO drops it.
        assert_eq!(
            push_multishot_completion(&mut slab, sentinel, -104, CqeFlags::EMPTY),
            MultishotCompletion {
                wake: None,
                retire: Some(0x1),
            },
            "a terminal CQE still retires the owner when the FIFO is full",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn multishot_cancel_drains_queued_results() {
        let driver = DriverType::Epoll(());
        let mut slab = MultishotSlab::new(19, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // Two accepts sit unconsumed in the FIFO when the stream drops.
        slab.push(key.slot, gen_low16, -7, true);
        slab.push(key.slot, gen_low16, -9, true);
        let cancel = InflightSlotKey {
            slot: key.slot,
            generation: key.generation,
            worker_id: key.worker_id,
            op_token: encode_multishot_sentinel(key),
        };
        submit_multishot_cancel(&driver, &mut slab, cancel);
        assert_eq!(slab.pop(key), None, "the cancel drained the queued results");
        assert!(
            slab.is_cancel_pending(key.slot, gen_low16),
            "the slot is marked cancel-pending",
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn multishot_cancel_closes_queued_accept_fds() {
        use std::os::fd::IntoRawFd;

        let driver = DriverType::Epoll(());
        let mut slab = MultishotSlab::new(20, 4);
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // A real owned descriptor stands in for a queued accepted connection.
        let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let fd = listener.into_raw_fd();
        slab.push(key.slot, gen_low16, fd, true);
        let cancel = InflightSlotKey {
            slot: key.slot,
            generation: key.generation,
            worker_id: key.worker_id,
            op_token: encode_multishot_sentinel(key),
        };
        submit_multishot_cancel(&driver, &mut slab, cancel);
        // SAFETY: Invariant -- `fd` was owned via `into_raw_fd` and just disposed
        // by the cancel above, so no live handle aliases it. Precondition -- this
        // is a non-destructive `F_GETFD` probe, closing nothing. Failure mode --
        // none; a probe of a closed fd reports `EBADF`, which is the assertion.
        let still_open = unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1;
        assert!(!still_open, "the cancel closed the queued accepted fd");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_completion_wakes_owner() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(30, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0xF00D) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_recv_multishot_sentinel(key);
        assert_eq!(
            push_recv_multishot_completion(
                &mut slab,
                &driver,
                sentinel,
                9,
                CqeFlags::MORE,
                Some(3)
            ),
            MultishotCompletion {
                wake: Some(0xF00D),
                retire: None,
            },
            "an intermediate completion wakes the owner without retiring the op",
        );
        assert_eq!(
            slab.pop(key),
            Some((9, 3)),
            "the byte count and buffer id both reached the FIFO",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_terminal_retires_owner() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(31, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0xBEEF) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_recv_multishot_sentinel(key);
        assert_eq!(
            push_recv_multishot_completion(&mut slab, &driver, sentinel, 0, CqeFlags::EMPTY, None),
            MultishotCompletion {
                wake: Some(0xBEEF),
                retire: Some(0xBEEF),
            },
            "a live terminal CQE both wakes and retires the owner",
        );
        assert_eq!(
            slab.pop(key),
            None,
            "the terminal stashes in the slot rather than the FIFO",
        );
        assert_eq!(
            slab.take_terminal(key),
            Some((0, NO_BUFFER)),
            "the end-of-stream terminal is taken from the slot's stash",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_cancel_frees_on_terminal() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(32, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        slab.mark_cancel_pending(key);
        let sentinel = encode_recv_multishot_sentinel(key);
        // An intermediate CQE for a dropped stream wakes nothing; its buffer is
        // recycled (a no-op against the poolless Epoll seam) rather than queued.
        assert_eq!(
            push_recv_multishot_completion(
                &mut slab,
                &driver,
                sentinel,
                4,
                CqeFlags::MORE,
                Some(2)
            ),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a cancel-pending intermediate wakes and retires nothing",
        );
        assert!(
            slab.is_live(key),
            "the slot survives until its terminal CQE"
        );
        assert_eq!(
            push_recv_multishot_completion(
                &mut slab,
                &driver,
                sentinel,
                -125,
                CqeFlags::EMPTY,
                None
            ),
            MultishotCompletion {
                wake: None,
                retire: Some(0x1),
            },
            "the terminal CQE retires the owner without waking",
        );
        assert!(
            !slab.is_live(key),
            "the terminal CQE frees the cancel-pending slot",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_stale_sentinel_wakes_nothing() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(33, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_recv_multishot_sentinel(key);
        slab.free(key);
        assert_eq!(
            push_recv_multishot_completion(
                &mut slab,
                &driver,
                sentinel,
                4,
                CqeFlags::MORE,
                Some(1)
            ),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "a sentinel naming a freed slot routes to nothing",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_overflow_keeps_terminal() {
        use crate::buffer::multishot::MULTISHOT_FIFO_DEPTH;

        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(34, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let sentinel = encode_recv_multishot_sentinel(key);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            slab.push(key.slot, gen_low16, value, 0, true);
        }
        assert_eq!(
            push_recv_multishot_completion(
                &mut slab,
                &driver,
                sentinel,
                4,
                CqeFlags::MORE,
                Some(5)
            ),
            MultishotCompletion {
                wake: None,
                retire: None,
            },
            "an overflowing data completion recycles its buffer and routes to nothing",
        );
        // #230: the terminal stashes past the full FIFO, so it wakes the owner to
        // deliver the end signal rather than being dropped as an overflow.
        assert_eq!(
            push_recv_multishot_completion(&mut slab, &driver, sentinel, 0, CqeFlags::EMPTY, None),
            MultishotCompletion {
                wake: Some(0x1),
                retire: Some(0x1),
            },
            "a terminal CQE wakes and retires the owner even when the FIFO is full",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_cancel_drains_buffers() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(35, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // Two provided-buffer recvs sit unconsumed in the FIFO when the stream
        // drops; the cancel must drain (and recycle) every one before marking.
        slab.push(key.slot, gen_low16, 8, 2, true);
        slab.push(key.slot, gen_low16, 8, 3, true);
        submit_recv_multishot_cancel(&driver, &mut slab, key);
        assert_eq!(slab.pop(key), None, "the cancel drained the queued buffers");
        assert!(
            slab.is_cancel_pending(key.slot, gen_low16),
            "the slot is marked cancel-pending",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recv_cancel_frees_terminated_slot() {
        let driver = DriverType::Epoll(());
        let Ok(mut slab) = RecvMultishotSlab::new(36, 4) else {
            panic!("the registry mmap must succeed");
        };
        let Some(key) = slab.allocate(0x1) else {
            panic!("the empty registry allocates a slot");
        };
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        // A terminal completion that already arrived carries a real provided
        // buffer and sits stashed when the stream drops. The cancel drains that
        // buffer, and, since the op has already terminated, frees the slot at once
        // rather than stranding it on a best-effort cancel the ring can refuse.
        slab.push(key.slot, gen_low16, 4, 6, true);
        slab.push(key.slot, gen_low16, 0, 7, false);
        submit_recv_multishot_cancel(&driver, &mut slab, key);
        assert!(!slab.is_live(key), "the terminated slot is freed at once");
        let Some(reused) = slab.allocate(0x2) else {
            panic!("the freed slot is available for reuse");
        };
        assert_eq!(reused.slot, key.slot);
        assert_eq!(
            reused.generation,
            key.generation + 1,
            "the freed slot is reused with a bumped generation",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_for_routes_the_accept_marker() {
        let driver = DriverType::Epoll(());
        let Ok(mut inflight) = InflightBufSlab::new(7, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let mut multishot = MultishotSlab::new(7, 4);
        let mut accepts = AcceptCancelSet::<4>::new();
        let mut provided_recvs = ProvidedRecvCancelSet::<4>::new();
        let mut connects = ConnectCancelSet::<4>::new();
        let key = InflightSlotKey {
            slot: ACCEPT_CANCEL_SLOT,
            generation: 0,
            worker_id: 7,
            op_token: 0xF00D,
        };
        submit_cancel_for(
            &driver,
            &mut inflight,
            &mut multishot,
            &mut accepts,
            &mut provided_recvs,
            &mut connects,
            key,
        );
        assert!(
            accepts.take(0xF00D),
            "the accept marker routes the token into the accept set",
        );
    }

    #[test]
    fn dispose_cancelled_accept_consumes_recorded_tokens() {
        let mut accepts = AcceptCancelSet::<4>::new();
        assert!(
            !dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "an empty set disposes nothing",
        );
        accepts.insert(0x1);
        assert!(
            dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "a recorded token is consumed (a negative result closes no fd)",
        );
        assert!(
            !dispose_cancelled_accept(&mut accepts, 0x1, -22),
            "the token is gone after disposal",
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn dispose_cancelled_accept_closes_a_real_fd() {
        use std::os::fd::IntoRawFd;

        let mut accepts = AcceptCancelSet::<4>::new();
        let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let fd = listener.into_raw_fd();
        accepts.insert(0x7);
        assert!(dispose_cancelled_accept(&mut accepts, 0x7, fd));
        // SAFETY: Invariant -- `fd` was owned via `into_raw_fd` and just disposed
        // above, so no live handle aliases it. Precondition -- this is a
        // non-destructive `F_GETFD` probe, closing nothing. Failure mode -- none;
        // a probe of a closed fd reports `EBADF`, which is the assertion.
        let still_open = unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1;
        assert!(!still_open, "the disposal closed the accepted fd");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_for_routes_the_provided_recv_marker() {
        let driver = DriverType::Epoll(());
        let Ok(mut inflight) = InflightBufSlab::new(8, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let mut multishot = MultishotSlab::new(8, 4);
        let mut accepts = AcceptCancelSet::<4>::new();
        let mut provided_recvs = ProvidedRecvCancelSet::<4>::new();
        let mut connects = ConnectCancelSet::<4>::new();
        let key = InflightSlotKey {
            slot: PROVIDED_RECV_CANCEL_SLOT,
            generation: 0,
            worker_id: 8,
            op_token: 0xBEEF,
        };
        submit_cancel_for(
            &driver,
            &mut inflight,
            &mut multishot,
            &mut accepts,
            &mut provided_recvs,
            &mut connects,
            key,
        );
        assert!(
            provided_recvs.take(0xBEEF),
            "the provided-recv marker routes the token into its set",
        );
        assert!(accepts.is_empty(), "the accept set is untouched");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dispose_cancelled_op_routes_by_buffer_and_set() {
        let driver = DriverType::Epoll(());
        let mut accepts = AcceptCancelSet::<4>::new();
        let mut provided_recvs = ProvidedRecvCancelSet::<4>::new();
        let mut connects = ConnectCancelSet::<4>::new();
        assert!(
            !dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x1,
                4,
                Some(2)
            ),
            "empty sets dispose nothing",
        );
        // A buffer-carrying CQE is definitively a provided recv's; a live
        // recv (token not recorded) falls through to the task path.
        provided_recvs.insert(0x1);
        assert!(
            !dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x2,
                4,
                Some(2)
            ),
            "an unrecorded token is a live future's completion",
        );
        assert!(
            dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x1,
                4,
                Some(2)
            ),
            "a recorded token consumes its buffer-carrying completion",
        );
        // A bufferless CQE checks the accepts first, then the provided recvs.
        accepts.insert(0x3);
        provided_recvs.insert(0x4);
        assert!(
            dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x3,
                -125,
                None
            ),
            "a bufferless completion matches the accept set first",
        );
        assert!(
            dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x4,
                -125,
                None
            ),
            "a cancelled provided recv disposes with nothing to recycle",
        );
        assert!(provided_recvs.is_empty());
        // An end-of-stream `0` with the token in both sets prefers the
        // provided set: adopted as an accept result it would close
        // descriptor zero, so the accept entry stays as a bounded leak.
        accepts.insert(0x5);
        provided_recvs.insert(0x5);
        assert!(
            dispose_cancelled_op(
                &driver,
                &mut accepts,
                &mut provided_recvs,
                &mut connects,
                0x5,
                0,
                None
            ),
            "the ambiguous end-of-stream completion is consumed",
        );
        assert!(provided_recvs.is_empty(), "the provided set took it");
        assert!(
            accepts.take(0x5),
            "the accept entry survives instead of adopting descriptor zero",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_cancel_for_routes_the_connect_marker() {
        let driver = DriverType::Epoll(());
        let Ok(mut inflight) = InflightBufSlab::new(11, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let mut multishot = MultishotSlab::new(11, 4);
        let mut accepts = AcceptCancelSet::<4>::new();
        let mut provided_recvs = ProvidedRecvCancelSet::<4>::new();
        let mut connects = ConnectCancelSet::<4>::new();
        let key = InflightSlotKey {
            slot: CONNECT_CANCEL_SLOT,
            generation: 0,
            worker_id: 11,
            op_token: 0xCAFE,
        };
        submit_cancel_for(
            &driver,
            &mut inflight,
            &mut multishot,
            &mut accepts,
            &mut provided_recvs,
            &mut connects,
            key,
        );
        assert!(
            connects.take(0xCAFE),
            "the connect marker routes the token into the connect set",
        );
        assert!(accepts.is_empty(), "the accept set is untouched");
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    #[test]
    fn dispose_cancelled_connect_never_touches_a_descriptor() {
        let mut connects = ConnectCancelSet::<4>::new();
        connects.insert(0xC0);
        // A successful connect's CQE result is exactly 0 -- the value that on the
        // accept path would adopt and close fd 0 (stdin). The connect disposal
        // takes the token by membership alone and never inspects the result.
        assert!(dispose_cancelled_connect(&mut connects, 0xC0));
        assert!(
            !dispose_cancelled_connect(&mut connects, 0xC0),
            "the token is gone after disposal",
        );
        // SAFETY: Invariant -- a non-destructive F_GETFD probe on stdin, closing
        // nothing. Precondition -- none. Failure mode -- none; a probe reports
        // the fd state without touching it.
        let stdin_open = unsafe { libc::fcntl(0, libc::F_GETFD) } != -1;
        assert!(stdin_open, "connect disposal must never adopt fd 0");
    }
}
