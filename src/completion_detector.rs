//! Reliable completion detection for web crawls.
//!
//! Provides multi-signal completion detection to reliably determine when a crawl is done:
//! - Frontier empty (no queued URLs)
//! - No in-flight requests
//! - Discovery plateau (no new URLs discovered for threshold duration)
//! - Sustained idle state (all signals met for grace period)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Completion detector with multi-signal verification
pub struct CompletionDetector {
    /// Threshold for discovery plateau (seconds without new URLs)
    discovery_plateau_threshold_secs: u64,

    /// Grace period to verify completion (all signals must be true for this duration)
    grace_period_secs: u64,

    /// Last time all completion signals were true
    last_all_signals_true: parking_lot::Mutex<Option<Instant>>,

    /// Completion state (atomic for lock-free reads)
    is_completed: AtomicBool,

    /// Statistics
    checks_performed: AtomicU64,

    /// Last "grace period remaining" value logged, so the crawler's 250 ms
    /// idle tick doesn't print the same line four times a second.
    last_logged_remaining: AtomicU64,
}

/// Completion signals from the crawler
pub struct CompletionSignals {
    pub frontier_empty: bool,
    pub inflight_count: usize,
    pub seconds_since_last_discovery: Option<u64>,
    pub total_discovered: usize,
    pub total_crawled: usize,
}

impl CompletionDetector {
    /// Create a new completion detector
    ///
    /// # Arguments
    /// * `discovery_plateau_threshold_secs` - Seconds without new URLs before considering plateau
    /// * `grace_period_secs` - Seconds all signals must be true before declaring completion
    pub fn new(discovery_plateau_threshold_secs: u64, grace_period_secs: u64) -> Self {
        Self {
            discovery_plateau_threshold_secs,
            grace_period_secs,
            last_all_signals_true: parking_lot::Mutex::new(None),
            is_completed: AtomicBool::new(false),
            checks_performed: AtomicU64::new(0),
            last_logged_remaining: AtomicU64::new(u64::MAX),
        }
    }

    /// Create detector with default thresholds for production use
    #[allow(dead_code)]
    pub fn with_defaults() -> Self {
        Self::new(
            30, // 30 second plateau threshold
            60, // 60 second grace period
        )
    }

    /// Check if crawl is complete based on multiple signals
    ///
    /// Returns `Some(true)` if completion is confirmed, `Some(false)` if definitely not complete,
    /// or `None` if insufficient data to determine (early in crawl).
    pub fn check_completion(&self, signals: &CompletionSignals) -> Option<bool> {
        self.checks_performed.fetch_add(1, Ordering::Relaxed);

        // If already marked complete, return immediately
        if self.is_completed.load(Ordering::Relaxed) {
            return Some(true);
        }

        // Early exit: if frontier has work or requests in-flight, not complete
        if !signals.frontier_empty || signals.inflight_count > 0 {
            // Reset grace period timer
            *self.last_all_signals_true.lock() = None;
            return Some(false);
        }

        // Check discovery plateau
        let is_plateau = match signals.seconds_since_last_discovery {
            Some(elapsed) => elapsed >= self.discovery_plateau_threshold_secs,
            None => {
                // No discoveries yet - insufficient data
                return None;
            }
        };

        if !is_plateau {
            // Still discovering URLs, reset grace period
            *self.last_all_signals_true.lock() = None;
            return Some(false);
        }

        // All signals are true - check if grace period elapsed
        let now = Instant::now();
        let mut last_true = self.last_all_signals_true.lock();

        if let Some(first_true_time) = *last_true
            && now.duration_since(first_true_time) >= Duration::from_secs(self.grace_period_secs)
        {
            // Grace period elapsed - crawl is complete!
            let grace_elapsed = now.duration_since(first_true_time);
            self.is_completed.store(true, Ordering::Relaxed);
            drop(last_true); // Release lock before logging

            eprintln!(
                "[COMPLETION] Crawl complete: {} URLs discovered, {} crawled | Grace period: {}s",
                signals.total_discovered,
                signals.total_crawled,
                grace_elapsed.as_secs()
            );

            Some(true)
        } else if let Some(first_true_time) = *last_true {
            // Still within grace period
            let grace_elapsed = now.duration_since(first_true_time);
            let remaining = self
                .grace_period_secs
                .saturating_sub(grace_elapsed.as_secs());
            if self
                .last_logged_remaining
                .swap(remaining, Ordering::Relaxed)
                == remaining
            {
                return Some(false);
            }
            eprintln!(
                "[COMPLETION] All signals true, grace period: {}s remaining (frontier empty, {} in-flight, {}s since last discovery)",
                remaining,
                signals.inflight_count,
                signals.seconds_since_last_discovery.unwrap_or(0)
            );
            Some(false)
        } else {
            // First time all signals are true
            *last_true = Some(now);
            eprintln!(
                "[COMPLETION] All signals true, starting {}s grace period (frontier empty, {} in-flight, {}s since last discovery)",
                self.grace_period_secs,
                signals.inflight_count,
                signals.seconds_since_last_discovery.unwrap_or(0)
            );
            Some(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle(secs_since_discovery: Option<u64>) -> CompletionSignals {
        CompletionSignals {
            frontier_empty: true,
            inflight_count: 0,
            seconds_since_last_discovery: secs_since_discovery,
            total_discovered: 10,
            total_crawled: 10,
        }
    }

    #[test]
    fn busy_crawl_is_never_complete() {
        let d = CompletionDetector::new(0, 0);
        let mut s = idle(Some(100));
        s.frontier_empty = false;
        assert_eq!(d.check_completion(&s), Some(false));
        let mut s = idle(Some(100));
        s.inflight_count = 3;
        assert_eq!(d.check_completion(&s), Some(false));
    }

    #[test]
    fn needs_plateau_then_grace() {
        let d = CompletionDetector::new(5, 0);
        assert_eq!(
            d.check_completion(&idle(Some(1))),
            Some(false),
            "still discovering"
        );
        // First fully-idle check starts the grace window, the next one completes.
        assert_eq!(d.check_completion(&idle(Some(5))), Some(false));
        assert_eq!(d.check_completion(&idle(Some(6))), Some(true));
        // Sticky once complete.
        let mut busy = idle(Some(0));
        busy.frontier_empty = false;
        assert_eq!(d.check_completion(&busy), Some(true));
    }

    #[test]
    fn new_work_resets_grace_window() {
        let d = CompletionDetector::new(0, 1);
        assert_eq!(d.check_completion(&idle(Some(0))), Some(false));
        let mut busy = idle(Some(0));
        busy.inflight_count = 1;
        assert_eq!(d.check_completion(&busy), Some(false));
        assert!(d.last_all_signals_true.lock().is_none());
    }

    #[test]
    fn no_discovery_signal_is_insufficient_data() {
        // The crawler substitutes time-since-start for this case (#65).
        let d = CompletionDetector::new(0, 0);
        assert_eq!(d.check_completion(&idle(None)), None);
    }
}
