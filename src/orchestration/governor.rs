//! Dynamic permit adjustment based on commit latency.

use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::Duration;

/// Adjusts crawler permits based on database commit latency.
#[tracing::instrument(skip(permits, metrics, shutdown))]
pub async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250;

    let throttle_threshold_ms = std::env::var("GOVERNOR_THROTTLE_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(2000.0); // Increased 4x to reduce false positives.

    let unthrottle_threshold_ms = std::env::var("GOVERNOR_UNTHROTTLE_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(200.0); // Increased 2x for smoother recovery.

    let min_permits = std::env::var("GOVERNOR_MIN_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256); // Increased 8x for higher baseline.

    let max_permits = std::env::var("GOVERNOR_MAX_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024); // Increased 2x for peak capacity.

    let mut shrink_bin: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
    let mut last_urls_fetched = 0u64;

    loop {
        if *shutdown.borrow() {
            eprintln!("Governor: Shutdown signal received, exiting");
            break;
        }

        tokio::time::sleep(Duration::from_millis(ADJUSTMENT_INTERVAL_MS)).await;

        let commit_ewma_ms = metrics.get_commit_ewma_ms();
        let available = permits.available_permits();
        let urls_processed = metrics.urls_processed_total.lock().value;
        let work_done = urls_processed > last_urls_fetched;
        last_urls_fetched = urls_processed;

        if commit_ewma_ms > throttle_threshold_ms {
            if available > min_permits
                && let Ok(permit) = permits.clone().try_acquire_owned()
            {
                shrink_bin.push(permit);
                metrics.throttle_adjustments.lock().inc();
            }
        } else if commit_ewma_ms < unthrottle_threshold_ms && commit_ewma_ms > 0.0 && work_done {
            if let Some(permit) = shrink_bin.pop() {
                drop(permit);
                metrics.throttle_adjustments.lock().inc();
            } else if available < max_permits {
                permits.add_permits(1);
                metrics.throttle_adjustments.lock().inc();
            }
        }

        metrics
            .throttle_permits_held
            .lock()
            .set(available as f64);
    }
}
