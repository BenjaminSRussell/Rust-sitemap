use parking_lot::Mutex;
use std::sync::Arc;
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

pub struct Metrics {
    pub writer_commit_latency: Mutex<Histogram>,
    pub writer_batch_bytes: Mutex<Counter>,
    pub writer_batch_count: Mutex<Counter>,

    pub wal_append_count: Mutex<Counter>,
    pub wal_fsync_latency: Mutex<Histogram>,
    pub wal_truncate_offset: Mutex<Gauge>,

    pub flume_send_block_count: Mutex<Counter>,
    pub flume_send_success_count: Mutex<Counter>,

    pub parser_abort_ratio: Mutex<Counter>,
    pub parser_abort_mem: Mutex<Counter>,
    pub parser_abort_timeout: Mutex<Counter>,
    pub parser_abort_handler_budget: Mutex<Counter>,

    pub writer_commit_ewma: Mutex<Ewma>,

    pub throttle_permits_held: Mutex<Gauge>,
    pub throttle_adjustments: Mutex<Counter>,

    pub http_version_h1: Mutex<Counter>,
    pub http_version_h2: Mutex<Counter>,
    pub http_version_h3: Mutex<Counter>,
    pub codec_gzip_bytes_in: Mutex<Counter>,
    pub codec_gzip_bytes_out: Mutex<Counter>,
    pub codec_gzip_duration_ms: Mutex<Histogram>,
    pub codec_brotli_bytes_in: Mutex<Counter>,
    pub codec_brotli_bytes_out: Mutex<Counter>,
    pub codec_brotli_duration_ms: Mutex<Histogram>,
    pub codec_zstd_bytes_in: Mutex<Counter>,
    pub codec_zstd_bytes_out: Mutex<Counter>,
    pub codec_zstd_duration_ms: Mutex<Histogram>,

    // Gate 3: Frontier health
    pub frontier_size: Mutex<Gauge>,
    pub frontier_full_total: Mutex<Counter>,
    pub ready_set_oldest_ms: Mutex<Histogram>,

    // Gate 3: Politeness & connection reuse
    pub politeness_violations_total: Mutex<Counter>,
    pub robots_violations_total: Mutex<Counter>,
    pub host_inflight_total: Mutex<Gauge>,
    pub connection_reused_total: Mutex<Counter>,
    pub connection_new_total: Mutex<Counter>,

    // Gate 3: HTTP/2 transport
    pub goaway_events_total: Mutex<Counter>,
    pub flow_control_stalls_total: Mutex<Counter>,
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
            writer_commit_ewma: Mutex::new(Ewma::new(0.2)),
            throttle_permits_held: Mutex::new(Gauge::new()),
            throttle_adjustments: Mutex::new(Counter::new()),
            http_version_h1: Mutex::new(Counter::new()),
            http_version_h2: Mutex::new(Counter::new()),
            http_version_h3: Mutex::new(Counter::new()),
            codec_gzip_bytes_in: Mutex::new(Counter::new()),
            codec_gzip_bytes_out: Mutex::new(Counter::new()),
            codec_gzip_duration_ms: Mutex::new(Histogram::new()),
            codec_brotli_bytes_in: Mutex::new(Counter::new()),
            codec_brotli_bytes_out: Mutex::new(Counter::new()),
            codec_brotli_duration_ms: Mutex::new(Histogram::new()),
            codec_zstd_bytes_in: Mutex::new(Counter::new()),
            codec_zstd_bytes_out: Mutex::new(Counter::new()),
            codec_zstd_duration_ms: Mutex::new(Histogram::new()),

            frontier_size: Mutex::new(Gauge::new()),
            frontier_full_total: Mutex::new(Counter::new()),
            ready_set_oldest_ms: Mutex::new(Histogram::new()),

            politeness_violations_total: Mutex::new(Counter::new()),
            robots_violations_total: Mutex::new(Counter::new()),
            host_inflight_total: Mutex::new(Gauge::new()),
            connection_reused_total: Mutex::new(Counter::new()),
            connection_new_total: Mutex::new(Counter::new()),

            goaway_events_total: Mutex::new(Counter::new()),
            flow_control_stalls_total: Mutex::new(Counter::new()),
        }
    }

    pub fn record_commit_latency(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.writer_commit_latency.lock().observe(ms);
        self.writer_commit_ewma.lock().update(ms as f64);
    }

    pub fn record_batch(&self, bytes: usize) {
        self.writer_batch_bytes.lock().add(bytes as u64);
        self.writer_batch_count.lock().inc();
    }

    pub fn record_wal_fsync(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.wal_fsync_latency.lock().observe(ms);
    }

    pub fn get_commit_ewma_ms(&self) -> f64 {
        self.writer_commit_ewma.lock().get()
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
        let mut counter = Counter::new();
        counter.inc();
        counter.add(5);
        assert_eq!(counter.value, 6);
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
