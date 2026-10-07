//! Thread-safe metrics collection for crawl progress and performance monitoring.
//!
//! Provides counters for:
//! - URLs processed, discovered, and failed
//! - HTTP status code distribution
//! - Request latency histograms
//! - Bytes downloaded

use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::state::HostState;

/// A shard's live host-state cache (shared with the frontier, read-only here).
pub type HostStateSource = Arc<DashMap<String, HostState>>;

/// Rows shown in the per-host budget table.
pub const HOST_REPORT_ROWS: usize = 25;

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Why a host can or cannot receive a request right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HostStatus {
    /// Failure threshold reached; host is skipped for the rest of the crawl.
    Blocked,
    /// Exponential backoff after errors.
    Backoff,
    /// All concurrent-request slots in use.
    Saturated,
    /// Waiting out robots.txt crawl-delay.
    Delayed,
    Ready,
}

impl HostStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            HostStatus::Blocked => "blocked",
            HostStatus::Backoff => "backoff",
            HostStatus::Saturated => "saturated",
            HostStatus::Delayed => "delayed",
            HostStatus::Ready => "ready",
        }
    }

    /// Hosts that cannot make progress until something changes.
    pub fn is_starved(self) -> bool {
        matches!(self, HostStatus::Blocked | HostStatus::Backoff)
    }
}

/// Remaining politeness budget for one host (#36).
#[derive(Debug, Clone, PartialEq)]
pub struct HostBudget {
    pub host: String,
    pub inflight: usize,
    pub max_inflight: usize,
    /// Concurrent requests still allowed right now.
    pub remaining_slots: usize,
    pub crawl_delay_secs: u64,
    /// Seconds until crawl-delay allows the next request.
    pub ready_in_secs: u64,
    /// Seconds of error backoff left.
    pub backoff_secs: u64,
    pub failures: u32,
    pub status: HostStatus,
}

impl HostBudget {
    pub fn from_state(s: &HostState, now_secs: u64) -> Self {
        let inflight = s.inflight.load(Ordering::Relaxed);
        let remaining_slots = s.max_inflight.saturating_sub(inflight);
        let ready_in_secs = s.ready_at_secs.saturating_sub(now_secs);
        let backoff_secs = s.backoff_until_secs.saturating_sub(now_secs);
        let status = if s.is_permanently_failed() {
            HostStatus::Blocked
        } else if backoff_secs > 0 {
            HostStatus::Backoff
        } else if remaining_slots == 0 {
            HostStatus::Saturated
        } else if ready_in_secs > 0 {
            HostStatus::Delayed
        } else {
            HostStatus::Ready
        };
        Self {
            host: s.host.clone(),
            inflight,
            max_inflight: s.max_inflight,
            remaining_slots,
            crawl_delay_secs: s.crawl_delay_secs,
            ready_in_secs,
            backoff_secs,
            failures: s.failures,
            status,
        }
    }
}

/// Most constrained first, then busiest, then by name for stable output.
pub fn sort_budgets(rows: &mut [HostBudget]) {
    rows.sort_by(|a, b| {
        a.status
            .cmp(&b.status)
            .then(b.inflight.cmp(&a.inflight))
            .then(b.backoff_secs.cmp(&a.backoff_secs))
            .then(a.host.cmp(&b.host))
    });
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostStatusCounts {
    pub ready: usize,
    pub delayed: usize,
    pub saturated: usize,
    pub backoff: usize,
    pub blocked: usize,
}

impl HostStatusCounts {
    pub fn total(&self) -> usize {
        self.ready + self.delayed + self.saturated + self.backoff + self.blocked
    }
    pub fn starved(&self) -> usize {
        self.backoff + self.blocked
    }
}

/// Render the per-host table; shows an alert banner when any host is starved.
pub fn render_host_budget_table(rows: &[HostBudget], counts: &HostStatusCounts) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = write!(
        out,
        "<h2>Per-host rate-limit budget</h2>\n<p>{} hosts tracked: {} ready, {} delayed, {} saturated, {} backoff, {} blocked</p>\n",
        counts.total(),
        counts.ready,
        counts.delayed,
        counts.saturated,
        counts.backoff,
        counts.blocked
    );
    if counts.starved() > 0 {
        let _ = writeln!(
            out,
            "<p class=\"bad alert\">&#9888; {} host(s) starved (backoff or blocked); their queued URLs are not being fetched.</p>",
            counts.starved()
        );
    }
    if rows.is_empty() {
        out.push_str("<p>No hosts tracked yet.</p>\n");
        return out;
    }
    out.push_str("<table class=\"hosts\">\n<tr><th>host</th><th>status</th><th>inflight / max</th><th>remaining</th><th>crawl-delay (s)</th><th>ready in (s)</th><th>backoff (s)</th><th>failures</th></tr>\n");
    for r in rows {
        let class = if r.status.is_starved() {
            " class=\"bad\""
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "<tr{class}><td>{}</td><td>{}</td><td>{} / {}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&r.host),
            r.status.as_str(),
            r.inflight,
            r.max_inflight,
            r.remaining_slots,
            r.crawl_delay_secs,
            r.ready_in_secs,
            r.backoff_secs,
            r.failures
        );
    }
    out.push_str("</table>\n");
    out
}

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

    pub fn value(&self) -> f64 {
        self.value
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

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub urls_fetched: u64,
    pub urls_processed: u64,
    pub urls_failed: u64,
    pub urls_timeout: u64,
    pub urls_discovered: u64,
    pub throttle_adjustments: u64,
    pub commit_ewma_ms: f64,
    pub discovery_rate_ewma: f64,
    pub content_summary: String,
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub struct Metrics {
    pub writer_commit_latency: Mutex<Histogram>,
    pub writer_batch_bytes: Mutex<Counter>,
    pub writer_batch_count: Mutex<Counter>,
    pub writer_disk_pressure: Mutex<Counter>,

    pub wal_append_count: Mutex<Counter>,
    pub wal_fsync_latency: Mutex<Histogram>,
    pub wal_truncate_offset: Mutex<Gauge>,
    /// Current WAL size in bytes, updated after every commit (#43).
    pub wal_size_bytes: Mutex<Gauge>,
    /// Number of WAL checkpoints (truncate-to-zero) so far (#43).
    pub wal_checkpoint_count: Mutex<Counter>,

    pub _parser_abort_mem: Mutex<Counter>,

    pub writer_commit_ewma: Mutex<Ewma>,

    pub throttle_permits_held: Mutex<Gauge>,
    pub throttle_adjustments: Mutex<Counter>,

    pub http_version_h1: Mutex<Counter>,
    pub http_version_h2: Mutex<Counter>,
    pub http_version_h3: Mutex<Counter>,
    pub http3_fallback_count: Mutex<Counter>, // Track HTTP/3 -> HTTP/2 fallbacks
    pub http3_errors: Mutex<Counter>,         // Track HTTP/3 specific errors

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

    /// Live per-host politeness state from each frontier shard (#36).
    host_sources: Mutex<Vec<HostStateSource>>,
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
            wal_size_bytes: Mutex::new(Gauge::new()),
            wal_checkpoint_count: Mutex::new(Counter::new()),
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
            host_sources: Mutex::new(Vec::new()),
        }
    }

    /// Register a shard's host-state cache so reports can show per-host budgets (#36).
    pub fn register_host_states(&self, source: HostStateSource) {
        self.host_sources.lock().push(source);
    }

    /// Per-host rate-limit budgets across all registered shards, most constrained first.
    pub fn host_budgets(&self, top_n: usize) -> Vec<HostBudget> {
        let now = unix_now_secs();
        let sources = self.host_sources.lock().clone();
        let mut out: Vec<HostBudget> = sources
            .iter()
            .flat_map(|s| {
                s.iter()
                    .map(|e| HostBudget::from_state(e.value(), now))
                    .collect::<Vec<_>>()
            })
            .collect();
        sort_budgets(&mut out);
        out.truncate(top_n);
        out
    }

    /// Count of tracked hosts per status, for Prometheus gauges and report headers.
    pub fn host_status_counts(&self) -> HostStatusCounts {
        let now = unix_now_secs();
        let sources = self.host_sources.lock().clone();
        let mut c = HostStatusCounts::default();
        for s in &sources {
            for e in s.iter() {
                match HostBudget::from_state(e.value(), now).status {
                    HostStatus::Ready => c.ready += 1,
                    HostStatus::Delayed => c.delayed += 1,
                    HostStatus::Saturated => c.saturated += 1,
                    HostStatus::Backoff => c.backoff += 1,
                    HostStatus::Blocked => c.blocked += 1,
                }
            }
        }
        c
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

    /// One-line WAL health summary for operator logs / reports.
    pub fn wal_summary(&self) -> String {
        format!(
            "WAL: {:.1} KiB, {} appends, {} checkpoints",
            self.wal_size_bytes.lock().value / 1024.0,
            self.wal_append_count.lock().value,
            self.wal_checkpoint_count.lock().value
        )
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

    /// Snapshot key counters for operator reports (#35).
    pub fn snapshot_totals(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            urls_fetched: self.urls_fetched_total.lock().value,
            urls_processed: self.urls_processed_total.lock().value,
            urls_failed: self.urls_failed_total.lock().value,
            urls_timeout: self.urls_timeout_total.lock().value,
            urls_discovered: self.urls_discovered_total.lock().value,
            throttle_adjustments: self.throttle_adjustments.lock().value,
            commit_ewma_ms: self.writer_commit_ewma.lock().get(),
            discovery_rate_ewma: self.discovery_rate_ewma.lock().get(),
            content_summary: self.content_type_stats.summary(),
        }
    }

    /// Write a static HTML crawl report (no Prometheus required).
    pub fn write_html_report<P: AsRef<std::path::Path>>(
        &self,
        path: P,
        start_url: &str,
        data_dir: &str,
    ) -> std::io::Result<()> {
        std::fs::write(path, self.render_html_report(start_url, data_dir))
    }

    /// Render the crawl report HTML (static file or live `/report`).
    pub fn render_html_report(&self, start_url: &str, data_dir: &str) -> String {
        let s = self.snapshot_totals();
        format!(
            r#"<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Rust-sitemap crawl report</title>
<style>
body{{font-family:system-ui,sans-serif;margin:2rem;background:#0b0f14;color:#e7ecf3}}
h1{{font-size:1.4rem}} table{{border-collapse:collapse}} td,th{{border:1px solid #2a3340;padding:.45rem .7rem;text-align:left}}
th{{background:#151b24}} .ok{{color:#6ee7b7}} .bad{{color:#fca5a5}} h2{{font-size:1.1rem;margin-top:2rem}}
</style></head><body>
<h1>Crawl report</h1>
<p>start: <code>{start}</code><br/>data: <code>{data}</code></p>
<table>
<tr><th>metric</th><th>value</th></tr>
<tr><td>urls_discovered</td><td>{disc}</td></tr>
<tr><td>urls_fetched</td><td>{fetched}</td></tr>
<tr><td>urls_processed</td><td>{proc}</td></tr>
<tr><td>urls_failed</td><td class="bad">{fail}</td></tr>
<tr><td>urls_timeout</td><td class="bad">{timeout}</td></tr>
<tr><td>throttle_adjustments</td><td>{throttle}</td></tr>
<tr><td>writer_commit_ewma_ms</td><td>{ewma:.2}</td></tr>
<tr><td>discovery_rate_ewma</td><td>{rate:.2}</td></tr>
<tr><td>content_types</td><td>{content}</td></tr>
</table>
{hosts}
</body></html>"#,
            start = html_escape(start_url),
            data = html_escape(data_dir),
            disc = s.urls_discovered,
            fetched = s.urls_fetched,
            proc = s.urls_processed,
            fail = s.urls_failed,
            timeout = s.urls_timeout,
            throttle = s.throttle_adjustments,
            ewma = s.commit_ewma_ms,
            rate = s.discovery_rate_ewma,
            content = html_escape(&s.content_summary),
            hosts = self.host_budget_html(HOST_REPORT_ROWS),
        )
    }

    /// HTML fragment: per-host rate-limit budget table with a starvation alert (#36).
    pub fn host_budget_html(&self, top_n: usize) -> String {
        let counts = self.host_status_counts();
        let rows = self.host_budgets(top_n);
        render_host_budget_table(&rows, &counts)
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

#[cfg(test)]
mod report_tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_html_report_contains_metrics() {
        let m = Metrics::new();
        m.urls_processed_total.lock().add(7);
        m.urls_failed_total.lock().add(1);
        let f = NamedTempFile::new().unwrap();
        m.write_html_report(f.path(), "https://example.com/", "./data")
            .unwrap();
        let body = std::fs::read_to_string(f.path()).unwrap();
        assert!(body.contains("urls_processed"));
        assert!(body.contains(">7<"));
        assert!(body.contains("https://example.com/"));
    }
}

#[cfg(test)]
mod host_budget_tests {
    use super::*;

    fn host(name: &str, f: impl FnOnce(&mut HostState)) -> HostState {
        let mut h = HostState::new(name.to_string());
        f(&mut h);
        h
    }

    #[test]
    fn status_classification() {
        let now = unix_now_secs();
        let ready = HostBudget::from_state(&host("a.test", |_| {}), now);
        assert_eq!(ready.status, HostStatus::Ready);
        assert_eq!(ready.remaining_slots, ready.max_inflight);

        let delayed = HostBudget::from_state(
            &host("b.test", |h| {
                h.crawl_delay_secs = 10;
                h.ready_at_secs = now + 7;
            }),
            now,
        );
        assert_eq!(delayed.status, HostStatus::Delayed);
        assert_eq!(delayed.ready_in_secs, 7);

        let sat = HostBudget::from_state(
            &host("c.test", |h| {
                h.max_inflight = 2;
                h.inflight.store(2, Ordering::Relaxed);
            }),
            now,
        );
        assert_eq!(sat.status, HostStatus::Saturated);
        assert_eq!(sat.remaining_slots, 0);

        let backoff = HostBudget::from_state(
            &host("d.test", |h| {
                h.failures = 1;
                h.backoff_until_secs = now + 30;
            }),
            now,
        );
        assert_eq!(backoff.status, HostStatus::Backoff);
        assert!(backoff.status.is_starved());

        let blocked = HostBudget::from_state(
            &host("e.test", |h| h.failures = HostState::MAX_FAILURES_THRESHOLD),
            now,
        );
        assert_eq!(blocked.status, HostStatus::Blocked);
    }

    #[test]
    fn budgets_across_shards_sorted_and_truncated() {
        let m = Metrics::new();
        let now = unix_now_secs();
        let s1: HostStateSource = Arc::new(DashMap::new());
        let s2: HostStateSource = Arc::new(DashMap::new());
        s1.insert("ok.test".into(), host("ok.test", |_| {}));
        s1.insert(
            "slow.test".into(),
            host("slow.test", |h| {
                h.failures = 2;
                h.backoff_until_secs = now + 60;
            }),
        );
        s2.insert("dead.test".into(), host("dead.test", |h| h.failures = 5));
        m.register_host_states(s1);
        m.register_host_states(s2);

        let rows = m.host_budgets(10);
        let names: Vec<_> = rows.iter().map(|r| r.host.as_str()).collect();
        assert_eq!(names, vec!["dead.test", "slow.test", "ok.test"]);
        assert_eq!(m.host_budgets(1).len(), 1);

        let c = m.host_status_counts();
        assert_eq!((c.total(), c.starved(), c.ready), (3, 2, 1));
    }

    #[test]
    fn html_report_has_host_table_and_alert() {
        let m = Metrics::new();
        let s: HostStateSource = Arc::new(DashMap::new());
        s.insert("x<y>.test".into(), host("x<y>.test", |h| h.failures = 9));
        m.register_host_states(s);
        let f = tempfile::NamedTempFile::new().unwrap();
        m.write_html_report(f.path(), "https://example.com/", "./data")
            .unwrap();
        let body = std::fs::read_to_string(f.path()).unwrap();
        assert!(body.contains("Per-host rate-limit budget"));
        assert!(body.contains("1 host(s) starved"));
        assert!(body.contains("x&lt;y&gt;.test"));
        assert!(body.contains("blocked"));
    }

    #[test]
    fn empty_host_table() {
        let html = render_host_budget_table(&[], &HostStatusCounts::default());
        assert!(html.contains("No hosts tracked yet"));
        assert!(!html.contains("starved"));
    }
}
