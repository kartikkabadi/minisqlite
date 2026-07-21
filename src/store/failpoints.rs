//! Test-only fault injection for producing real in-process indeterminate
//! COMMIT outcomes. Enabled by the `failpoints` feature; never compiled into
//! normal builds.

use std::sync::atomic::{AtomicBool, Ordering};

static FAIL_NEXT_COMMIT: AtomicBool = AtomicBool::new(false);

/// Arm the failpoint: the next COMMIT completes durably, but its result is
/// reported as indeterminate — as if the process lost the COMMIT outcome.
pub fn fail_next_commit() {
    FAIL_NEXT_COMMIT.store(true, Ordering::SeqCst);
}

/// Consume the armed failpoint, if any.
pub(crate) fn take_fail_commit() -> bool {
    FAIL_NEXT_COMMIT.swap(false, Ordering::SeqCst)
}
