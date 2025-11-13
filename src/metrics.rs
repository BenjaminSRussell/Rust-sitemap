//! Thread-safe metrics collection for crawl progress and performance monitoring.
//!
//! Provides counters for:
//! - URLs processed, discovered, and failed
//! - HTTP status code distribution
//! - Request latency histograms
//! - Bytes downloaded

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use dashmap::DashMap;

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

#[derive(Debug, Clone)]
pub struct Counter {
    pub value: u64,
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

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

/// Content-Type filtering statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ContentTypeStats {
    pub html_count: Arc<AtomicUsize>,
    pub pdf_count: Arc<AtomicUsize>,
    pub image_count: Arc<AtomicUsize>,
    pub other_count: Arc<AtomicUsize>,
    pub bytes_by_type: Arc<DashMap<String, AtomicUsize>>,
}

impl ContentTypeStats {
    pub fn new() -> Self {
        Self {
            html_count: Arc::new(AtomicUsize::new(0)),
            pdf_count: Arc::new(AtomicUsize::new(0)),
            image_count: Arc::new(AtomicUsize::new(0)),
            other_count: Arc::new(AtomicUsize::new(0)),
            bytes_by_type: Arc::new(DashMap::new()),
        }
    }

    /// Record content type and bytes downloaded
    #[allow(dead_code)]
    pub fn record(&self, content_type: Option<&str>, bytes: usize) {
        match content_type {
            Some(ct) => {
                let ct_lower = ct.to_lowercase();

                // Increment type-specific counter
                if ct_lower.contains("text/html") || ct_lower.contains("application/xhtml") {
                    self.html_count.fetch_add(1, Ordering::Relaxed);
                } else if ct_lower.contains("application/pdf") {
                    self.pdf_count.fetch_add(1, Ordering::Relaxed);
                } else if ct_lower.starts_with("image/") {
                    self.image_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.other_count.fetch_add(1, Ordering::Relaxed);
                }

                // Track bytes by type
                let simplified_type = Self::simplify_content_type(&ct_lower);
                self.bytes_by_type
                    .entry(simplified_type)
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            None => {
                self.other_count.fetch_add(1, Ordering::Relaxed);
                self.bytes_by_type
                    .entry("unknown".to_string())
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Simplify content type to main category
    #[allow(dead_code)]
    fn simplify_content_type(ct: &str) -> String {
        if ct.contains("text/html") || ct.contains("application/xhtml") {
            "html".to_string()
        } else if ct.contains("application/pdf") {
            "pdf".to_string()
        } else if ct.starts_with("image/") {
            if ct.contains("jpeg") || ct.contains("jpg") {
                "image/jpeg".to_string()
            } else if ct.contains("png") {
                "image/png".to_string()
            } else if ct.contains("gif") {
                "image/gif".to_string()
            } else if ct.contains("webp") {
                "image/webp".to_string()
            } else {
                "image/other".to_string()
            }
        } else if ct.contains("javascript") || ct.contains("ecmascript") {
            "javascript".to_string()
        } else if ct.contains("css") {
            "css".to_string()
        } else if ct.contains("json") {
            "json".to_string()
        } else if ct.contains("xml") {
            "xml".to_string()
        } else {
            "other".to_string()
        }
    }

    /// Get total non-HTML bytes wasted
    #[allow(dead_code)]
    pub fn non_html_bytes(&self) -> usize {
        let mut total = 0;
        for entry in self.bytes_by_type.iter() {
            if entry.key() != "html" {
                total += entry.value().load(Ordering::Relaxed);
            }
        }
        total
    }

    /// Get stats summary
    #[allow(dead_code)]
    pub fn summary(&self) -> String {
        let html = self.html_count.load(Ordering::Relaxed);
        let pdf = self.pdf_count.load(Ordering::Relaxed);
        let image = self.image_count.load(Ordering::Relaxed);
        let other = self.other_count.load(Ordering::Relaxed);
        let total = html + pdf + image + other;

        if total == 0 {
            return "No content fetched yet".to_string();
        }

        let html_pct = (html as f64 / total as f64) * 100.0;
        let non_html_bytes = self.non_html_bytes();
        let non_html_mb = non_html_bytes as f64 / (1024.0 * 1024.0);

        format!(
            "HTML: {} ({:.1}%), PDF: {}, Images: {}, Other: {} | Non-HTML wasted: {:.2} MB",
            html, html_pct, pdf, image, other, non_html_mb
        )
    }
}

impl Default for ContentTypeStats {
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

pub struct Metrics {
    pub writer_commit_latency: Mutex<Histogram>,
    pub writer_batch_bytes: Mutex<Counter>,
    pub writer_batch_count: Mutex<Counter>,
    pub writer_disk_pressure: Mutex<Counter>,

    pub wal_append_count: Mutex<Counter>,
    pub wal_fsync_latency: Mutex<Histogram>,
    pub wal_truncate_offset: Mutex<Gauge>,

    pub _parser_abort_mem: Mutex<Counter>,

    pub writer_commit_ewma: Mutex<Ewma>,

    pub throttle_permits_held: Mutex<Gauge>,
    pub throttle_adjustments: Mutex<Counter>,

    pub http_version_h1: Mutex<Counter>,
    pub http_version_h2: Mutex<Counter>,
    pub http_version_h3: Mutex<Counter>,
    pub http3_fallback_count: Mutex<Counter>, // Track HTTP/3 -> HTTP/2 fallbacks
    pub http3_errors: Mutex<Counter>,          // Track HTTP/3 specific errors

    pub urls_fetched_total: Mutex<Counter>,
    pub urls_timeout_total: Mutex<Counter>,
    pub urls_failed_total: Mutex<Counter>,
    pub urls_processed_total: Mutex<Counter>,

    // Discovery tracking metrics for completion detection
    pub urls_discovered_total: Mutex<Counter>,
    pub last_discovery_time: Mutex<Option<std::time::Instant>>,
    #[allow(dead_code)]
    pub discovery_rate_ewma: Mutex<Ewma>, // URLs per second

    // Content-Type filtering stats
    #[allow(dead_code)]
    pub content_type_stats: ContentTypeStats,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            writer_commit_latency: Mutex::new(Histogram::new()),
            writer_batch_bytes: Mutex::new(Counter::new()),
            writer_batch_count: Mutex::new(Counter::new()),
            writer_disk_pressure: Mutex::new(Counter::new()),
            wal_append_count: Mutex::new(Counter::new()),
            wal_fsync_latency: Mutex::new(Histogram::new()),
            wal_truncate_offset: Mutex::new(Gauge::new()),
            _parser_abort_mem: Mutex::new(Counter::new()),
            writer_commit_ewma: Mutex::new(Ewma::new(0.4)),
            throttle_permits_held: Mutex::new(Gauge::new()),
            throttle_adjustments: Mutex::new(Counter::new()),
            http_version_h1: Mutex::new(Counter::new()),
            http_version_h2: Mutex::new(Counter::new()),
            http_version_h3: Mutex::new(Counter::new()),
            http3_fallback_count: Mutex::new(Counter::new()),
            http3_errors: Mutex::new(Counter::new()),
            urls_fetched_total: Mutex::new(Counter::new()),
            urls_timeout_total: Mutex::new(Counter::new()),
            urls_failed_total: Mutex::new(Counter::new()),
            urls_processed_total: Mutex::new(Counter::new()),
            urls_discovered_total: Mutex::new(Counter::new()),
            last_discovery_time: Mutex::new(None),
            discovery_rate_ewma: Mutex::new(Ewma::new(0.3)), // Moderately responsive
            content_type_stats: ContentTypeStats::new(),
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

    /// Record URL discovery and update discovery rate
    pub fn record_url_discovery(&self, count: usize) {
        let now = std::time::Instant::now();

        // Update total counter
        self.urls_discovered_total.lock().add(count as u64);

        // Update last discovery time
        let mut last_time = self.last_discovery_time.lock();
        *last_time = Some(now);
    }

    /// Get seconds since last URL discovery (for plateau detection)
    pub fn seconds_since_last_discovery(&self) -> Option<u64> {
        self.last_discovery_time
            .lock()
            .map(|last| last.elapsed().as_secs())
    }

    /// Check if crawl has reached a "plateau" state (no new URLs discovered for threshold seconds)
    #[allow(dead_code)]
    pub fn is_plateau(&self, threshold_secs: u64) -> bool {
        match self.seconds_since_last_discovery() {
            Some(elapsed) => elapsed >= threshold_secs,
            None => false, // No URLs discovered yet, not a plateau
        }
    }

    /// Get HTTP version statistics summary
    #[allow(dead_code)]
    pub fn http_version_summary(&self) -> String {
        let h1 = self.http_version_h1.lock().value;
        let h2 = self.http_version_h2.lock().value;
        let h3 = self.http_version_h3.lock().value;
        let h3_fallback = self.http3_fallback_count.lock().value;
        let h3_errors = self.http3_errors.lock().value;
        let total = h1 + h2 + h3;

        if total == 0 {
            return "No HTTP requests yet".to_string();
        }

        let h1_pct = (h1 as f64 / total as f64) * 100.0;
        let h2_pct = (h2 as f64 / total as f64) * 100.0;
        let h3_pct = (h3 as f64 / total as f64) * 100.0;

        if h3 > 0 || h3_fallback > 0 || h3_errors > 0 {
            format!(
                "HTTP/1.1: {} ({:.1}%), HTTP/2: {} ({:.1}%), HTTP/3: {} ({:.1}%) | H3 Fallbacks: {}, H3 Errors: {}",
                h1, h1_pct, h2, h2_pct, h3, h3_pct, h3_fallback, h3_errors
            )
        } else {
            format!(
                "HTTP/1.1: {} ({:.1}%), HTTP/2: {} ({:.1}%)",
                h1, h1_pct, h2, h2_pct
            )
        }
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
