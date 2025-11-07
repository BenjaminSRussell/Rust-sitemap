use crate::config::Config;
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
        let (event_tx, event_rx) = flume::bounded::<StateEvent>(Config::EVENT_CHANNEL_BUFFER_SIZE);
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
}
