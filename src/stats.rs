//! Statistic structures used to track metrics at runtime.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Simple atomic counter structure (based on `usize`).
///
/// The only difference between this and `AtomicUsize` is that the
/// former returns the new value after an increment call. This value
/// is not guaranteed, but should be considered eventually consistent.
pub struct Counter {
    inner: AtomicUsize,
}

impl Counter {
    /// Constructs a new counter from a starting value.
    pub fn new(start: usize) -> Self {
        Self {
            inner: AtomicUsize::new(start),
        }
    }

    /// Constructs a concurrent counter from a starting value.
    pub fn shared(start: usize) -> Arc<Self> {
        Arc::new(Self::new(start))
    }

    /// Increments this counter by a given amount.
    ///
    /// This will return the value of the counter *after* the value
    /// has been incremented. This value is eventually consistent,
    /// and should not be considered guaranteed to be accurate.
    #[inline]
    pub fn increment(&self, amount: usize) -> usize {
        self.inner.fetch_add(amount, Ordering::Relaxed) + amount
    }
}
