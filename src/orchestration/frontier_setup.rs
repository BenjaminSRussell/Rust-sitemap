//! Frontier dispatcher and shard setup.

use crate::bfs_crawler::BfsCrawlerConfig;
use crate::frontier::{FrontierDispatcher, FrontierPermit, FrontierShard, ShardedFrontier, SharedFrontierStats};
use crate::network::HttpClient;
use crate::state::CrawlerState;
use crate::writer_thread::WriterThread;
use std::sync::Arc;

/// Builds frontier dispatcher and shards. Repopulates uncrawled URLs if resuming.
#[tracing::instrument(skip(config, state, writer_thread, http))]
pub async fn setup_frontier(
    config: &BfsCrawlerConfig,
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,
    http: Arc<HttpClient>,
    replayed_count: usize,
) -> Result<
    (
        Vec<FrontierShard>,
        Arc<ShardedFrontier>,
        tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, FrontierPermit)>,
        tokio::sync::mpsc::UnboundedReceiver<(String, String, u32, Option<String>, FrontierPermit)>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<tokio::sync::Semaphore>,
    ),
    Box<dyn std::error::Error>,
> {
    let num_shards = num_cpus::get();
    let (frontier_dispatcher, shard_receivers, frontier_size, backpressure) =
        FrontierDispatcher::new(num_shards);

    if replayed_count > 0 {
        let mut uncrawled_urls = Vec::new();
        let iter = state.iter_nodes();
        if let Ok(node_iter) = iter {
            let _ = node_iter.for_each(|node| {
                if node.crawled_at.is_none() {
                    uncrawled_urls.push((node.url, node.depth, node.parent_url));
                }
                Ok(())
            });
        }

        if !uncrawled_urls.is_empty() {
            eprintln!(
                "Repopulating frontier with {} uncrawled URLs from database",
                uncrawled_urls.len()
            );
            let added = frontier_dispatcher.add_links(uncrawled_urls).await;
            eprintln!("Added {} URLs to frontier", added);
        }
    }

    let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
    let shared_stats = SharedFrontierStats::new();

    let mut frontier_shards = Vec::new();
    let mut host_state_caches = Vec::new();

    for (shard_id, url_receiver) in shard_receivers.into_iter().enumerate() {
        let shard = FrontierShard::new(
            shard_id,
            Arc::clone(&state),
            Arc::clone(&writer_thread),
            Arc::clone(&http),
            config.user_agent.clone(),
            config.ignore_robots,
            url_receiver,
            work_tx.clone(),
            Arc::clone(&frontier_size),
            Arc::clone(&backpressure),
            shared_stats.clone(),
        );
        host_state_caches.push(shard.get_host_state_cache());
        frontier_shards.push(shard);
    }

    let sharded_frontier = ShardedFrontier::new(frontier_dispatcher, host_state_caches, shared_stats);
    let frontier = Arc::new(sharded_frontier);

    Ok((frontier_shards, frontier, work_tx, work_rx, frontier_size, backpressure))
}
