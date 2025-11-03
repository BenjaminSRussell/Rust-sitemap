use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

/// Simple histogram for tracking latencies so we can bucket timings quickly.
#[derive(Debug, Clone)]
pub struct Histogram {
    buckets: Vec<(u64, u64)>, // Store (threshold_ms, count) pairs to update buckets via linear scan.
    sum_ms: u64,
    count: u64,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            buckets: vec![
                (1, 0),
                (5, 0),
                (10, 0),
                (50, 0),
                (100, 0),
                (500, 0),
                (1000, 0),
                (5000, 0),
            ],
            sum_ms: 0,
            count: 0,
        }
    }

    pub fn observe(&mut self, value_ms: u64) {
        self.sum_ms += value_ms;
        self.count += 1;

        for (threshold, count) in &mut self.buckets {
            if value_ms <= *threshold {
                *count += 1;
                break;
            }
        }
    }
}

/// Counter metric so we can accumulate discrete event counts.
#[derive(Debug, Clone)]
pub struct Counter {
    value: u64,
}

impl Counter {
    pub fn new() -> Self {
        Self { value: 0 }
    }

    pub fn inc(&mut self) {
        self.value += 1;
    }

    pub fn add(&mut self, delta: u64) {
        self.value += delta;
    }
}

/// Gauge metric that can increase or decrease so we can observe instantaneous values.
#[derive(Debug, Clone)]
pub struct Gauge {
    value: f64,
}

impl Gauge {
    pub fn new() -> Self {
        Self { value: 0.0 }
    }

    pub fn set(&mut self, value: f64) {
        self.value = value;
    }
}

/// EWMA (Exponentially Weighted Moving Average) tracker so we smooth noisy latency samples.
#[derive(Debug, Clone)]
pub struct Ewma {
    value: f64,
    alpha: f64, // Smoothing factor (0-1) that balances responsiveness versus stability.
}

impl Ewma {
    /// Create an EWMA with the given smoothing factor.
    /// alpha closer to 1.0 gives more weight to recent values.
    /// alpha closer to 0.0 applies more smoothing.
    pub fn new(alpha: f64) -> Self {
        Self {
            value: 0.0,
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    pub fn update(&mut self, new_value: f64) {
        if self.value == 0.0 {
            // Boot-strap with the first value so the EWMA starts at a meaningful baseline.
            self.value = new_value;
        } else {
            self.value = self.alpha * new_value + (1.0 - self.alpha) * self.value;
        }
    }

    pub fn get(&self) -> f64 {
        self.value
    }
}

/// Metrics registry so all subsystems share consistent instrumentation primitives.
pub struct Metrics {
    // Writer metrics so we track WAL commit behavior.
    pub writer_commit_latency: Mutex<Histogram>,
    pub writer_batch_bytes: Mutex<Counter>,
    pub writer_batch_count: Mutex<Counter>,

    // WAL metrics so disk durability can be monitored.
    pub wal_append_count: Mutex<Counter>,
    pub wal_fsync_latency: Mutex<Histogram>,
    pub wal_truncate_offset: Mutex<Gauge>,

    // Flume metrics so channel pressure becomes visible.
    pub flume_send_block_count: Mutex<Counter>,
    pub flume_send_success_count: Mutex<Counter>,

    // Parser metrics so we can see why pages abort parsing.
    pub parser_abort_ratio: Mutex<Counter>,
    pub parser_abort_mem: Mutex<Counter>,
    pub parser_abort_timeout: Mutex<Counter>,
    pub parser_abort_handler_budget: Mutex<Counter>,

    // EWMA for throttling so the governor reads a smoothed commit latency.
    pub writer_commit_ewma: Mutex<Ewma>,

    // Throttle metrics so we expose permit adjustments and holdings.
    pub throttle_permits_held: Mutex<Gauge>,
    pub throttle_adjustments: Mutex<Counter>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            writer_commit_latency: Mutex::new(Histogram::new()),
            writer_batch_bytes: Mutex::new(Counter::new()),
            writer_batch_count: Mutex::new(Counter::new()),
            wal_append_count: Mutex::new(Counter::new()),
            wal_fsync_latency: Mutex::new(Histogram::new()),
            wal_truncate_offset: Mutex::new(Gauge::new()),
            flume_send_block_count: Mutex::new(Counter::new()),
            flume_send_success_count: Mutex::new(Counter::new()),
            parser_abort_ratio: Mutex::new(Counter::new()),
            parser_abort_mem: Mutex::new(Counter::new()),
            parser_abort_timeout: Mutex::new(Counter::new()),
            parser_abort_handler_budget: Mutex::new(Counter::new()),
            writer_commit_ewma: Mutex::new(Ewma::new(0.2)), // Twenty percent weight on new values.
            throttle_permits_held: Mutex::new(Gauge::new()),
            throttle_adjustments: Mutex::new(Counter::new()),
        }
    }

    /// Record writer commit latency.
    pub fn record_commit_latency(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.writer_commit_latency.lock().observe(ms);
        self.writer_commit_ewma.lock().update(ms as f64);
    }

    /// Record the batch size.
    pub fn record_batch(&self, bytes: usize) {
        self.writer_batch_bytes.lock().add(bytes as u64);
        self.writer_batch_count.lock().inc();
    }

    /// Record WAL fsync latency.
    pub fn record_wal_fsync(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.wal_fsync_latency.lock().observe(ms);
    }

    /// Get EWMA commit latency for throttling.
    pub fn get_commit_ewma_ms(&self) -> f64 {
        self.writer_commit_ewma.lock().get()
    }

}

/// Shared metrics instance so multiple components can log metrics through one handle.
pub type SharedMetrics = Arc<Metrics>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let mut hist = Histogram::new();
        hist.observe(5);
        hist.observe(10);
        hist.observe(15);

        assert_eq!(hist.count(), 3);
        assert_eq!(hist.mean(), 10.0);
    }

    #[test]
    fn test_counter() {
        let mut counter = Counter::new();
        counter.inc();
        counter.add(5);
        assert_eq!(counter.get(), 6);
    }

    #[test]
    fn test_ewma() {
        let mut ewma = Ewma::new(0.5);
        ewma.update(100.0);
        assert_eq!(ewma.get(), 100.0);

        ewma.update(200.0);
        assert_eq!(ewma.get(), 150.0); // (0.5 * 200) + (0.5 * 100).
    }
}
