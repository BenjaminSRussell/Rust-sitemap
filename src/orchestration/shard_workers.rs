//! Shard worker spawning logic shared between main binary and Python library.

use crate::frontier::FrontierShard;

// [Zencoder Task Doc]
// WHAT: Spawns background tokio tasks for each frontier shard to asynchronously process URLs with politeness-based scheduling.
// USED_BY: src/main.rs (Crawl and Resume commands), src/lib.rs (Crawler::run_crawl)

/// Spawn background workers for all frontier shards.
/// Each shard processes incoming URLs and schedules them based on politeness delays.
pub fn spawn_shard_workers(frontier_shards: Vec<FrontierShard>, start_url_domain: String) {
    for mut shard in frontier_shards {
        let domain = start_url_domain.clone();
        tokio::spawn(async move {
            loop {
                shard.process_incoming_urls(&domain).await;

                if shard.get_next_url().await.is_none() {
                    if shard.has_queued_urls() {
                        if let Some(next_ready) = shard.next_ready_time() {
                            let now = std::time::Instant::now();
                            if next_ready > now {
                                let sleep_duration = std::cmp::min(
                                    next_ready.duration_since(now),
                                    tokio::time::Duration::from_millis(100),
                                );
                                tokio::time::sleep(sleep_duration).await;
                            } else {
                                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                            }
                        } else {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });
    }
}
