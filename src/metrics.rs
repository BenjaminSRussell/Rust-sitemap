use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Histogram {
    buckets: Vec<(u64, u64)>,
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

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

// Atomic counter for lock-free metric updates (optimization #1)
#[derive(Debug)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self { value: AtomicU64::new(0) }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self {
            value: AtomicU64::new(self.value.load(Ordering::Relaxed)),
        }
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

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

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

/// EWMA with configurable alpha (0=smooth, 1=responsive).
#[derive(Debug, Clone)]
pub struct Ewma {
    value: f64,
    alpha: f64,
}

impl Ewma {
    pub fn new(alpha: f64) -> Self {
        Self {
            value: 0.0,
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    pub fn update(&mut self, new_value: f64) {
        if self.value == 0.0 {
            self.value = new_value;
        } else {
            self.value = self.alpha * new_value + (1.0 - self.alpha) * self.value;
        }
    }

    pub fn get(&self) -> f64 {
        self.value
    }
}

// Optimized metrics using atomics for counters to eliminate lock contention
pub struct Metrics {
    // Histograms remain Mutex-wrapped (require exclusive access for bucket updates)
    pub writer_commit_latency: Mutex<Histogram>,
    pub wal_fsync_latency: Mutex<Histogram>,
    pub codec_gzip_duration_ms: Mutex<Histogram>,
    pub codec_brotli_duration_ms: Mutex<Histogram>,
    pub codec_zstd_duration_ms: Mutex<Histogram>,

    // Gauges remain Mutex-wrapped (require exclusive access for value updates)
    pub wal_truncate_offset: Mutex<Gauge>,
    pub throttle_permits_held: Mutex<Gauge>,

    // EWMA remains Mutex-wrapped (requires exclusive access for formula evaluation)
    pub writer_commit_ewma: Mutex<Ewma>,

    // Lock-free atomic counters (OPTIMIZATION #1: eliminates mutex contention)
    pub writer_batch_bytes: Counter,
    pub writer_batch_count: Counter,
    pub writer_disk_pressure: Counter,
    pub wal_append_count: Counter,
    pub parser_abort_mem: Counter,
    pub throttle_adjustments: Counter,
    pub http_version_h1: Counter,
    pub http_version_h2: Counter,
    pub http_version_h3: Counter,
    pub codec_gzip_bytes_out: Counter,
    pub codec_brotli_bytes_out: Counter,
    pub codec_zstd_bytes_out: Counter,
    pub urls_fetched_total: Counter,
    pub urls_timeout_total: Counter,
    pub urls_failed_total: Counter,
    pub urls_processed_total: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            // Histograms (still use Mutex)
            writer_commit_latency: Mutex::new(Histogram::new()),
            wal_fsync_latency: Mutex::new(Histogram::new()),
            codec_gzip_duration_ms: Mutex::new(Histogram::new()),
            codec_brotli_duration_ms: Mutex::new(Histogram::new()),
            codec_zstd_duration_ms: Mutex::new(Histogram::new()),

            // Gauges (still use Mutex)
            wal_truncate_offset: Mutex::new(Gauge::new()),
            throttle_permits_held: Mutex::new(Gauge::new()),

            // EWMA (still use Mutex)
            writer_commit_ewma: Mutex::new(Ewma::new(0.4)),

            // Lock-free atomic counters (OPTIMIZATION #1)
            writer_batch_bytes: Counter::new(),
            writer_batch_count: Counter::new(),
            writer_disk_pressure: Counter::new(),
            wal_append_count: Counter::new(),
            parser_abort_mem: Counter::new(),
            throttle_adjustments: Counter::new(),
            http_version_h1: Counter::new(),
            http_version_h2: Counter::new(),
            http_version_h3: Counter::new(),
            codec_gzip_bytes_out: Counter::new(),
            codec_brotli_bytes_out: Counter::new(),
            codec_zstd_bytes_out: Counter::new(),
            urls_fetched_total: Counter::new(),
            urls_timeout_total: Counter::new(),
            urls_failed_total: Counter::new(),
            urls_processed_total: Counter::new(),
        }
    }

    pub fn record_commit_latency(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.writer_commit_latency.lock().observe(ms);
        self.writer_commit_ewma.lock().update(ms as f64);
    }

    pub fn record_batch(&self, bytes: usize) {
        // Now lock-free! (OPTIMIZATION #1)
        self.writer_batch_bytes.add(bytes as u64);
        self.writer_batch_count.inc();
    }

    pub fn record_wal_fsync(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.wal_fsync_latency.lock().observe(ms);
    }

    pub fn get_commit_ewma_ms(&self) -> f64 {
        self.writer_commit_ewma.lock().get()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

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

        assert_eq!(hist.count, 3);
        assert_eq!(hist.sum_ms / hist.count, 10);
    }

    #[test]
    fn test_counter() {
        let counter = Counter::new();
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
        assert_eq!(ewma.get(), 150.0);
    }
}
