// src/id.rs
use std::sync::atomic::{AtomicU64, Ordering};

/// Global request id generator.
///
/// Starts at 1000 to avoid colliding with any low hard-coded ids.
static REQ_ID: AtomicU64 = AtomicU64::new(1000);

/// Return the next request id (monotonic).
pub fn next_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}
