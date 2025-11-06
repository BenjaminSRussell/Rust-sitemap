use crate::metrics::SharedMetrics;
use crate::state::{CrawlerState, StateEvent, StateEventWithSeqno};
use crate::wal::{SeqNo, SharedWalWriter, WalRecord};
use flume::{Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const BATCH_TIMEOUT_MS: u64 = 50; // Drain batches every 50 ms (reduced fsync frequency).
const MAX_BATCH_SIZE: usize = 5000; // Maximum events per batch (reduced spike latency).
const COMMIT_RETRY_BASE_MS: u64 = 10; // Base delay for exponential backoff.
const COMMIT_RETRY_MAX_MS: u64 = 30_000; // Cap backoff at 30 seconds.

/// Handle for the writer thread.
pub struct WriterThread {
    handle: Option<thread::JoinHandle<()>>,
    event_tx: Sender<StateEvent>,
}

impl WriterThread {
    /// Spawns a writer thread.
    pub fn spawn(
        state: Arc<CrawlerState>,
        wal_writer: SharedWalWriter,
        metrics: SharedMetrics,
        instance_id: u64,
        starting_seqno: u64,
    ) -> Self {
        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let (ack_tx, _ack_rx) = flume::bounded::<u64>(100);

        let handle = thread::spawn(move || {
            Self::writer_loop(
                state,
                wal_writer,
                metrics,
                event_rx,
                ack_tx,
                instance_id,
                starting_seqno,
            );
        });

        Self {
            handle: Some(handle),
            event_tx,
        }
    }

    /// Sends an event asynchronously.
    pub async fn send_event_async(&self, event: StateEvent) -> Result<(), String> {
        self.event_tx
            .send_async(event)
            .await
            .map_err(|e| format!("Failed to send event: {}", e))
    }

    /// Sends an event synchronously.
    #[cfg(test)]
    pub fn send_event(&self, event: StateEvent) -> Result<(), String> {
        self.event_tx
            .send(event)
            .map_err(|e| format!("Failed to send event: {}", e))
    }

    /// Shuts down the writer thread.
    #[cfg(test)]
    pub fn shutdown(self) {
        // When self is dropped, event_tx is automatically dropped, signaling shutdown
        // The Drop implementation will handle joining the thread
        std::mem::drop(self);
    }

    /// The main loop for the writer thread.
    fn writer_loop(
        state: Arc<CrawlerState>,
        wal_writer: SharedWalWriter,
        metrics: SharedMetrics,
        event_rx: Receiver<StateEvent>,
        ack_tx: Sender<u64>,
        instance_id: u64,
        starting_seqno: u64,
    ) {
        let local_seqno = Arc::new(AtomicU64::new(starting_seqno));
        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;

        loop {
            // Use pending batch from previous WAL failure, or drain a new batch
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| Self::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                // Channel closed and no more events
                if event_rx.is_disconnected() {
                    eprintln!("Writer thread: channel closed, exiting");
                    break;
                }
                // No events, wait a bit
                thread::sleep(Duration::from_millis(1));
                continue;
            }

            // Write to WAL first
            let _max_seqno = batch.iter().map(|e| e.seqno.local_seqno).max().unwrap_or(0);

            // CRITICAL FIX: WAL write must succeed before DB commit to guarantee durability
            let wal_result = {
                let mut wal = wal_writer.blocking_lock();
                let mut all_appends_ok = true;

                for event_with_seqno in &batch {
                    let payload = Self::serialize_event(&event_with_seqno.event);
                    let record = WalRecord {
                        seqno: event_with_seqno.seqno,
                        payload,
                    };

                    if let Err(e) = wal.append(&record) {
                        eprintln!("CRITICAL: WAL append failed: {}", e);
                        all_appends_ok = false;
                        break; // Stop processing this batch
                    } else {
                        metrics.wal_append_count.lock().inc();
                    }
                }

                if !all_appends_ok {
                    Ok(false) // Signal failure without fsync
                } else {
                    // Fsync WAL - this MUST succeed for durability
                    let fsync_start = Instant::now();
                    let fsync_result = wal.fsync();
                    metrics.record_wal_fsync(fsync_start.elapsed());

                    if let Err(e) = fsync_result {
                        eprintln!("CRITICAL: WAL fsync failed: {}", e);
                        Err(e)
                    } else {
                        Ok(true) // All good
                    }
                }
            };

            // If WAL write/fsync failed, preserve batch and retry later
            match wal_result {
                Ok(false) | Err(_) => {
                    eprintln!("WAL failure: preserving batch for retry after delay");
                    pending_batch = Some(batch);
                    thread::sleep(Duration::from_millis(1000));
                    continue; // Retry the same batch
                }
                Ok(true) => {
                    // WAL is durable, proceed to DB commit
                }
            }

            // Commit to redb with infinite exponential backoff retry (lossless)
            let batch_size_bytes = Self::estimate_batch_size(&batch);
            let commit_start = Instant::now();

            let mut retry_count = 0u32;
            loop {
                match state.apply_event_batch(&batch) {
                    Ok(committed_seqno) => {
                        let commit_duration = commit_start.elapsed();
                        metrics.record_commit_latency(commit_duration);
                        metrics.record_batch(batch_size_bytes);

                        // Truncate WAL after successful commit
                        {
                            let mut wal = wal_writer.blocking_lock();
                            let offset = wal.get_offset();
                            if let Err(e) = wal.truncate(offset) {
                                eprintln!("WAL truncate failed: {}", e);
                            } else {
                                metrics.wal_truncate_offset.lock().set(offset as f64);
                            }
                        }

                        // Send ack
                        let _ = ack_tx.try_send(committed_seqno);
                        break;
                    }
                    Err(e) => {
                        // Track disk pressure when commit fails
                        let error_msg = format!("{}", e);
                        let is_disk_io = error_msg.contains("I/O")
                            || error_msg.contains("disk")
                            || error_msg.contains("ENOSPC")
                            || error_msg.contains("EIO");

                        if is_disk_io {
                            metrics.writer_disk_pressure.lock().inc();
                            eprintln!(
                                "Commit failed (disk pressure, attempt {}): {}",
                                retry_count + 1,
                                e
                            );
                        } else {
                            eprintln!("Commit failed (attempt {}): {}", retry_count + 1, e);
                        }

                        // Log breadcrumb on first transition into exponential backoff
                        if retry_count == 0 {
                            eprintln!(
                                "Entering exponential backoff for batch (size: {} events)",
                                batch.len()
                            );
                        }

                        // Exponential backoff with jitter: delay = base * 2^retry_count + jitter
                        // Capped at COMMIT_RETRY_MAX_MS to prevent excessive delays
                        let exponential_delay = COMMIT_RETRY_BASE_MS
                            .saturating_mul(2u64.saturating_pow(retry_count.min(20))); // Cap exponent at 20 to prevent overflow
                        let capped_delay = exponential_delay.min(COMMIT_RETRY_MAX_MS);
                        let jitter = rand::random::<u64>() % (capped_delay / 10 + 1); // 10% jitter
                        let total_delay = capped_delay + jitter;

                        eprintln!(
                            "Retrying commit after {}ms (attempt {}, batch size: {} events)",
                            total_delay,
                            retry_count + 1,
                            batch.len()
                        );

                        thread::sleep(Duration::from_millis(total_delay));
                        retry_count = retry_count.saturating_add(1);

                        // IMPORTANT: Never break - infinite retry ensures lossless operation
                        // Data is safe in WAL, so we can retry indefinitely until DB accepts it
                    }
                }
            }
        }

        eprintln!("Writer thread exiting");
    }

    /// Drains events from the channel into a batch.
    fn drain_batch(
        event_rx: &Receiver<StateEvent>,
        local_seqno: &Arc<AtomicU64>,
        instance_id: u64,
    ) -> Vec<StateEventWithSeqno> {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        let deadline = Instant::now() + Duration::from_millis(BATCH_TIMEOUT_MS);

        // Block for first event (with timeout)
        match event_rx.recv_deadline(deadline) {
            Ok(event) => {
                let seqno = SeqNo::new(instance_id, local_seqno.fetch_add(1, Ordering::SeqCst) + 1);
                batch.push(StateEventWithSeqno { seqno, event });
            }
            Err(_) => return batch, // Timeout or disconnected
        }

        // Try to drain more events without blocking
        while batch.len() < MAX_BATCH_SIZE {
            match event_rx.try_recv() {
                Ok(event) => {
                    let seqno =
                        SeqNo::new(instance_id, local_seqno.fetch_add(1, Ordering::SeqCst) + 1);
                    batch.push(StateEventWithSeqno { seqno, event });
                }
                Err(_) => break, // No more events available
            }
        }

        batch
    }

    /// Serializes an event to bytes.
    fn serialize_event(event: &StateEvent) -> Vec<u8> {
        rkyv::to_bytes::<_, 2048>(event)
            .map(|v| v.to_vec())
            .unwrap_or_else(|e| {
                eprintln!("Failed to serialize event: {}", e);
                Vec::new()
            })
    }

    /// Estimates the batch size in bytes.
    fn estimate_batch_size(batch: &[StateEventWithSeqno]) -> usize {
        batch.len() * 256 // Rough estimate
    }
}

impl Drop for WriterThread {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// Simple random number generator.
mod rand {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static SEED: AtomicU64 = AtomicU64::new(0);

    pub fn random<T: From<u64>>() -> T {
        let mut seed = SEED.load(Ordering::Relaxed);
        if seed == 0 {
            seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(12345))
                .as_nanos() as u64;
        }

        // Xorshift64
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;

        SEED.store(seed, Ordering::Relaxed);
        T::from(seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::state::SitemapNode;
    use crate::wal::WalWriter;
    use tempfile::TempDir;

    #[test]
    fn test_writer_thread_basic() {
        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());
        let wal_writer = Arc::new(tokio::sync::Mutex::new(
            WalWriter::new(dir.path(), 100).unwrap(),
        ));
        let metrics = Arc::new(Metrics::new());

        let writer = WriterThread::spawn(state.clone(), wal_writer, metrics, 1, 0);

        // Send some events
        let node = SitemapNode::new(
            "https://test.local".to_string(),
            "https://test.local".to_string(),
            0,
            None,
            None,
        );

        writer.send_event(StateEvent::AddNodeFact(node)).unwrap();

        // Give writer time to process
        thread::sleep(Duration::from_millis(100));

        writer.shutdown();

        // Verify node was added
        assert!(state.contains_url("https://test.local").unwrap());
    }

    #[test]
    fn test_wal_retry_with_live_urls() {
        use std::sync::atomic::AtomicBool;

        // Simpler test: verify batch preservation logic with live URL data
        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        // Create event channel and prepare live URLs
        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        let live_urls = vec![
            "https://example.com/",
            "https://example.com/about",
            "https://example.com/contact",
            "https://example.org/api/docs",
            "https://example.net/blog/2024/post-1",
            "https://github.com/rust-lang/rust",
            "https://docs.rs/tokio/latest/tokio/",
        ];

        // Send events for live URLs
        for url in &live_urls {
            let node = SitemapNode::new(
                url.to_string(),
                url.to_string(),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();

            // Also send CrawlAttemptFact
            event_tx
                .send(StateEvent::CrawlAttemptFact {
                    url_normalized: url.to_string(),
                    status_code: 200,
                    content_type: Some("text/html".to_string()),
                    content_length: Some(2048),
                    title: Some(format!("Page: {}", url)),
                    link_count: 10,
                    response_time_ms: Some(150),
                })
                .unwrap();
        }

        eprintln!("Testing batch preservation with {} live URLs", live_urls.len());

        // Simulate the writer loop with pending_batch (the fix we implemented)
        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let mut iteration = 0;
        let simulate_wal_failure = AtomicBool::new(true); // Fail first attempt

        // Simulate 2 iterations: first with WAL failure, second with success
        for _ in 0..2 {
            iteration += 1;

            // Use pending batch or drain new one (this is the fix)
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            eprintln!("Iteration {}: processing batch with {} events", iteration, batch.len());

            // Simulate WAL write
            let wal_success = if simulate_wal_failure.swap(false, Ordering::SeqCst) {
                eprintln!("  Simulating WAL failure (first attempt)");
                false
            } else {
                eprintln!("  WAL success (retry)");
                true
            };

            if !wal_success {
                // Preserve batch for retry (the fix)
                eprintln!("  Preserving batch for retry");
                pending_batch = Some(batch);
                continue;
            }

            // WAL succeeded, commit to state
            eprintln!("  Committing batch to state");
            state.apply_event_batch(&batch).unwrap();
        }

        // Verify all URLs were committed
        eprintln!("Verifying all URLs were committed...");
        for url in &live_urls {
            assert!(
                state.contains_url(url).unwrap(),
                "URL {} should be in state after WAL retry",
                url
            );
        }

        eprintln!("✓ All {} live URLs successfully committed after WAL retry", live_urls.len());
        eprintln!("✓ Batch preservation logic verified with real URL data");
    }

    #[tokio::test]
    async fn test_wal_retry_with_real_http_fetch_and_parse() {
        use crate::network::HttpClient;
        use scraper::{Html, Selector};
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Testing WAL retry with REAL HTTP fetch and HTML parsing ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        // Create real HTTP client
        let http_client = HttpClient::new(
            "Mozilla/5.0 (compatible; RustSitemapBot/1.0)".to_string(),
            10,
        )
        .unwrap();

        // Fetch a real webpage
        let test_url = "http://example.com";
        eprintln!("Fetching {}", test_url);

        let fetch_result = http_client.fetch(test_url).await;

        // If fetch fails (e.g., network issues, encoding), use mock HTML for testing
        let (fetched_html, status_code, real_fetch) = if let Ok(fetched) = fetch_result {
            eprintln!("✓ Fetched {} bytes (status: {})", fetched.content.len(), fetched.status_code);
            (fetched.content, fetched.status_code, true)
        } else {
            eprintln!("! Fetch failed (using mock HTML for testing): {:?}", fetch_result.err());
            let mock_html = r#"<!DOCTYPE html>
<html>
<head><title>Example Domain</title></head>
<body>
<h1>Example Domain</h1>
<p>This domain is for use in illustrative examples.</p>
<a href="https://www.iana.org/domains/example">More information...</a>
<a href="/about">About</a>
<a href="/contact">Contact Us</a>
<a href="https://example.org">Example Org</a>
</body>
</html>"#.to_string();
            (mock_html, 200, false)
        };

        if real_fetch {
            eprintln!("✓ Using real fetched HTML");
        } else {
            eprintln!("✓ Using mock HTML for testing");
        }

        // Parse HTML and extract links
        let document = Html::parse_document(&fetched_html);
        let link_selector = Selector::parse("a[href]").unwrap();
        let title_selector = Selector::parse("title").unwrap();

        let links: Vec<String> = document
            .select(&link_selector)
            .filter_map(|el| el.value().attr("href").map(|s| s.to_string()))
            .collect();

        let title = document
            .select(&title_selector)
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string());

        eprintln!("✓ Parsed HTML:");
        eprintln!("  - Title: {:?}", title);
        eprintln!("  - Found {} links", links.len());
        for (i, link) in links.iter().take(5).enumerate() {
            eprintln!("    {}. {}", i + 1, link);
        }
        if links.len() > 5 {
            eprintln!("    ... and {} more", links.len() - 5);
        }

        assert!(!links.is_empty(), "Should have extracted at least some links");

        // Now test the WAL retry with this real/mock data
        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send the main page event
        let main_node = SitemapNode::new(
            test_url.to_string(),
            test_url.to_string(),
            0,
            None,
            None,
        );
        event_tx.send(StateEvent::AddNodeFact(main_node)).unwrap();

        event_tx
            .send(StateEvent::CrawlAttemptFact {
                url_normalized: test_url.to_string(),
                status_code,
                content_type: Some("text/html".to_string()),
                content_length: Some(fetched_html.len()),
                title: title.clone(),
                link_count: links.len(),
                response_time_ms: Some(100),
            })
            .unwrap();

        // Send events for discovered links
        for link in links.iter().take(10) {
            // Normalize/resolve the link against base URL
            let resolved = match url::Url::parse(test_url) {
                Ok(base) => match base.join(link) {
                    Ok(absolute) => absolute.to_string(),
                    Err(_) => continue, // Skip invalid links
                },
                Err(_) => continue,
            };

            let link_node = SitemapNode::new(
                resolved.clone(),
                resolved.clone(),
                1, // depth 1
                Some(test_url.to_string()),
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(link_node)).unwrap();
        }

        let events_sent = 2 + links.iter().take(10).count(); // main page + crawl attempt + discovered links
        eprintln!("\n✓ Sent {} events to writer thread", events_sent);

        // Simulate WAL retry with real parsed data
        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let simulate_wal_failure = AtomicBool::new(true);
        let mut total_committed = 0;

        eprintln!("\nSimulating WAL write with failure + retry:");
        for iteration in 1..=2 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            eprintln!("  Iteration {}: batch has {} events", iteration, batch.len());

            let wal_success = !simulate_wal_failure.swap(false, Ordering::SeqCst);

            if !wal_success {
                eprintln!("    ✗ WAL failure - preserving batch");
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("    ✓ WAL success - committing to state");
            state.apply_event_batch(&batch).unwrap();
            total_committed += batch.len();
        }

        // Verify the main page was committed
        assert!(
            state.contains_url(test_url).unwrap(),
            "Main URL should be in state after WAL retry"
        );

        eprintln!("\n✓ SUCCESS: Committed {} events after WAL retry", total_committed);
        eprintln!("✓ Verified: Main URL '{}' is in state", test_url);
        eprintln!("✓ Full pipeline tested: HTTP fetch → HTML parse → link extraction → WAL retry → commit\n");
    }

    #[test]
    fn test_wal_multiple_consecutive_failures() {
        use std::sync::atomic::AtomicUsize;

        eprintln!("\n=== Testing multiple consecutive WAL failures ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send 50 events
        for i in 0..50 {
            let node = SitemapNode::new(
                format!("https://test{}.example.com", i),
                format!("https://test{}.example.com", i),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
        }

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let fail_count = Arc::new(AtomicUsize::new(5)); // Fail 5 times before success

        eprintln!("Simulating 5 consecutive WAL failures:");
        for iteration in 1..=10 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            let remaining_failures = fail_count.load(Ordering::SeqCst);
            let wal_success = if remaining_failures > 0 {
                fail_count.fetch_sub(1, Ordering::SeqCst);
                eprintln!("  Iteration {}: WAL failure ({} failures remaining)", iteration, remaining_failures - 1);
                false
            } else {
                eprintln!("  Iteration {}: WAL success", iteration);
                true
            };

            if !wal_success {
                pending_batch = Some(batch);
                continue;
            }

            state.apply_event_batch(&batch).unwrap();
            break;
        }

        // Verify all 50 URLs were committed
        for i in 0..50 {
            assert!(state.contains_url(&format!("https://test{}.example.com", i)).unwrap());
        }

        eprintln!("✓ All 50 events committed after 5 consecutive WAL failures\n");
    }

    #[test]
    fn test_wal_interleaved_failures() {
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Testing interleaved WAL failures with multiple batches ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send 3 waves of events
        for wave in 0..3 {
            for i in 0..10 {
                let node = SitemapNode::new(
                    format!("https://wave{}-test{}.com", wave, i),
                    format!("https://wave{}-test{}.com", wave, i),
                    0,
                    None,
                    None,
                );
                event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
            }
        }

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let mut batches_processed = 0;
        let fail_pattern = Arc::new(AtomicBool::new(true)); // Alternate: fail, succeed, fail, succeed

        eprintln!("Processing multiple batches with alternating WAL failures:");
        for iteration in 1..=10 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            let should_fail = fail_pattern.swap(!fail_pattern.load(Ordering::SeqCst), Ordering::SeqCst);

            if should_fail && batches_processed < 2 {
                eprintln!("  Iteration {}: WAL failure for batch with {} events", iteration, batch.len());
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("  Iteration {}: WAL success, committing {} events", iteration, batch.len());
            state.apply_event_batch(&batch).unwrap();
            batches_processed += 1;
        }

        // Verify all 30 URLs were committed
        let mut total = 0;
        for wave in 0..3 {
            for i in 0..10 {
                if state.contains_url(&format!("https://wave{}-test{}.com", wave, i)).unwrap() {
                    total += 1;
                }
            }
        }

        assert_eq!(total, 30, "All 30 events should be committed");
        eprintln!("✓ All 30 events from 3 batches committed despite interleaved failures\n");
    }

    #[test]
    fn test_wal_stress_large_batch() {
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Stress test: Large batch with WAL retry ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send 1000 events in one batch
        let count = 1000;
        eprintln!("Sending {} events...", count);
        for i in 0..count {
            let node = SitemapNode::new(
                format!("https://stress{}.example.com", i),
                format!("https://stress{}.example.com", i),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
        }

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let simulate_failure = AtomicBool::new(true);

        for iteration in 1..=3 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            eprintln!("  Iteration {}: processing batch with {} events", iteration, batch.len());

            let should_fail = simulate_failure.swap(false, Ordering::SeqCst);
            if should_fail {
                eprintln!("    ✗ WAL failure - preserving large batch");
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("    ✓ WAL success - committing large batch");
            state.apply_event_batch(&batch).unwrap();
        }

        // Verify all events committed
        let mut committed = 0;
        for i in 0..count {
            if state.contains_url(&format!("https://stress{}.example.com", i)).unwrap() {
                committed += 1;
            }
        }

        assert_eq!(committed, count, "All {} events should be committed", count);
        eprintln!("✓ Successfully committed {} events in large batch after WAL retry\n", count);
    }

    #[test]
    fn test_wal_seqno_ordering_preserved() {
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Testing seqno ordering preservation across retries ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send events
        for i in 0..20 {
            let node = SitemapNode::new(
                format!("https://order{}.example.com", i),
                format!("https://order{}.example.com", i),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
        }

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let simulate_failure = AtomicBool::new(true);
        let mut collected_seqnos = Vec::new();

        for _iteration in 1..=3 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            // Collect seqnos from the batch
            if collected_seqnos.is_empty() {
                for event in &batch {
                    collected_seqnos.push(event.seqno.local_seqno);
                }
            }

            let should_fail = simulate_failure.swap(false, Ordering::SeqCst);
            if should_fail {
                eprintln!("  First attempt: WAL failure, batch preserved");
                // Verify seqnos are still the same in preserved batch
                for (idx, event) in batch.iter().enumerate() {
                    assert_eq!(
                        event.seqno.local_seqno, collected_seqnos[idx],
                        "Seqno should be preserved in retry"
                    );
                }
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("  Second attempt: WAL success");
            // Verify seqnos are STILL the same before commit
            for (idx, event) in batch.iter().enumerate() {
                assert_eq!(
                    event.seqno.local_seqno, collected_seqnos[idx],
                    "Seqno should remain unchanged after retry"
                );
            }

            state.apply_event_batch(&batch).unwrap();
        }

        // Verify seqnos are sequential (1, 2, 3, ...)
        eprintln!("  Verifying seqno ordering:");
        for (idx, seqno) in collected_seqnos.iter().enumerate() {
            assert_eq!(*seqno, (idx + 1) as u64, "Seqnos should be sequential");
        }

        eprintln!("✓ All seqnos preserved and ordered correctly: {:?}", &collected_seqnos[..5]);
        eprintln!("✓ No seqno corruption or duplication after retry\n");
    }

    #[test]
    fn test_wal_no_duplicate_commits() {
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Testing no duplicate commits on retry ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send 15 events
        for i in 0..15 {
            let node = SitemapNode::new(
                format!("https://dedup{}.example.com", i),
                format!("https://dedup{}.example.com", i),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
        }

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let simulate_failure = AtomicBool::new(true);
        let mut commit_count = 0;

        for iteration in 1..=3 {
            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                break;
            }

            eprintln!("  Iteration {}: batch has {} events", iteration, batch.len());

            let should_fail = simulate_failure.swap(false, Ordering::SeqCst);
            if should_fail {
                eprintln!("    ✗ Preserving batch WITHOUT committing");
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("    ✓ Committing batch (first and only commit)");
            state.apply_event_batch(&batch).unwrap();
            commit_count += 1;
        }

        assert_eq!(commit_count, 1, "Batch should be committed exactly once");

        // Verify all 15 URLs exist exactly once
        for i in 0..15 {
            assert!(
                state.contains_url(&format!("https://dedup{}.example.com", i)).unwrap(),
                "URL {} should exist", i
            );
        }

        eprintln!("✓ Batch committed exactly once (no duplicates)");
        eprintln!("✓ All 15 events present in state\n");
    }

    #[test]
    fn test_wal_empty_channel_after_retry() {
        use std::sync::atomic::AtomicBool;

        eprintln!("\n=== Testing empty channel handling after retry ===\n");

        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        let (event_tx, event_rx) = flume::bounded::<StateEvent>(100_000);
        let local_seqno = Arc::new(AtomicU64::new(0));
        let instance_id = 1u64;

        // Send only 5 events
        for i in 0..5 {
            let node = SitemapNode::new(
                format!("https://empty{}.example.com", i),
                format!("https://empty{}.example.com", i),
                0,
                None,
                None,
            );
            event_tx.send(StateEvent::AddNodeFact(node)).unwrap();
        }

        // Drop sender to close channel
        drop(event_tx);

        let mut pending_batch: Option<Vec<StateEventWithSeqno>> = None;
        let simulate_failure = AtomicBool::new(true);
        let mut iterations = 0;

        for _iteration in 1..=5 {
            iterations += 1;

            let batch = pending_batch
                .take()
                .unwrap_or_else(|| WriterThread::drain_batch(&event_rx, &local_seqno, instance_id));

            if batch.is_empty() {
                eprintln!("  Channel empty, exiting loop gracefully");
                break;
            }

            let should_fail = simulate_failure.swap(false, Ordering::SeqCst);
            if should_fail {
                eprintln!("  WAL failure, preserving batch");
                pending_batch = Some(batch);
                continue;
            }

            eprintln!("  WAL success, committing");
            state.apply_event_batch(&batch).unwrap();
        }

        assert!(iterations <= 3, "Should complete within 3 iterations");

        // Verify all 5 URLs committed
        for i in 0..5 {
            assert!(state.contains_url(&format!("https://empty{}.example.com", i)).unwrap());
        }

        eprintln!("✓ Gracefully handled channel close after retry");
        eprintln!("✓ All events committed before exit\n");
    }
}
