//! Frontier dispatcher and shard setup.

use crate::bfs_crawler::BfsCrawlerConfig;
use crate::frontier::{
    FrontierDispatcher, FrontierPermit, FrontierShard, ShardedFrontier, SharedFrontierStats,
};
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

    // Restore on explicit `resume`, or whenever WAL replay shows the previous
    // run did not shut down cleanly (#30).
    let plan = if config.restore_frontier || replayed_count > 0 {
        let started = std::time::Instant::now();
        let plan = collect_restore_plan(&state, num_shards);
        eprintln!(
            "Frontier restore: {} pending URL(s), {} already crawled (scanned in {} ms)",
            plan.pending.len(),
            plan.crawled_by_shard.iter().map(Vec::len).sum::<usize>(),
            started.elapsed().as_millis()
        );
        Some(plan)
    } else {
        None
    };

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

    if let Some(plan) = plan {
        // Warm each shard's dedupe filter first so rediscovered links to finished
        // pages are confirmed against redb rather than fetched again.
        for (shard, crawled) in frontier_shards.iter_mut().zip(plan.crawled_by_shard) {
            for url in crawled {
                shard.mark_previously_crawled(&url);
            }
        }
        if !plan.pending.is_empty() {
            let added = frontier_dispatcher.add_links(plan.pending).await;
            eprintln!("Re-queued {} pending URL(s) from the previous run", added);
        }
    }

    let sharded_frontier =
        ShardedFrontier::new(frontier_dispatcher, host_state_caches, shared_stats);
    let frontier = Arc::new(sharded_frontier);

    Ok((
        frontier_shards,
        frontier,
        work_tx,
        work_rx,
        frontier_size,
        backpressure,
    ))
}

/// What `setup_frontier` needs to rebuild the frontier from redb.
#[derive(Debug, Default)]
pub(crate) struct RestorePlan {
    /// Uncrawled nodes (url, depth, parent) - the persisted frontier.
    pub pending: Vec<(String, u32, Option<String>)>,
    /// Normalized URLs already crawled, bucketed by the shard that owns them.
    pub crawled_by_shard: Vec<Vec<String>>,
}

/// Single pass over the nodes table: uncrawled nodes become the pending
/// frontier, crawled ones seed the per-shard dedupe filters.
pub(crate) fn collect_restore_plan(state: &CrawlerState, num_shards: usize) -> RestorePlan {
    let mut plan = RestorePlan {
        pending: Vec::new(),
        crawled_by_shard: vec![Vec::new(); num_shards.max(1)],
    };
    match state.iter_nodes() {
        Ok(iter) => {
            let result = iter.for_each(|node| {
                if node.crawled_at.is_some() {
                    if let Some(shard) =
                        crate::frontier::shard_for_url(&node.url_normalized, num_shards.max(1))
                    {
                        plan.crawled_by_shard[shard].push(node.url_normalized);
                    }
                } else {
                    plan.pending.push((node.url, node.depth, node.parent_url));
                }
                Ok(())
            });
            if let Err(e) = result {
                eprintln!("Warning: frontier restore stopped early: {}", e);
            }
        }
        Err(e) => eprintln!("Warning: could not scan state for frontier restore: {}", e),
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::persistence::initialize_persistence;
    use crate::state::{SitemapNode, StateEvent, StateEventWithSeqno};

    fn add_node(url: &str) -> StateEvent {
        let n = SitemapNode::normalize_url(url);
        StateEvent::AddNodeFact(SitemapNode::new(url.to_string(), n, 1, None, None))
    }

    fn crawled(url: &str) -> StateEvent {
        StateEvent::CrawlAttemptFact {
            url_normalized: SitemapNode::normalize_url(url),
            status_code: 200,
            content_type: Some("text/html".into()),
            content_length: Some(10),
            title: None,
            link_count: 0,
            response_time_ms: Some(1),
            description: None,
            canonical_url: None,
            author: None,
            language: None,
            keywords: None,
            article_text_length: None,
            metadata_json: None,
            set_cookies: None,
            third_party_api_calls: None,
            external_resources: None,
            privacy_metadata_json: None,
            structured_data_json: None,
            tech_profile: None,
        }
    }

    fn apply(state: &CrawlerState, events: Vec<StateEvent>) {
        let batch: Vec<StateEventWithSeqno> = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| StateEventWithSeqno {
                seqno: crate::wal::SeqNo::new(1, i as u64 + 1),
                event,
            })
            .collect();
        state.apply_event_batch(&batch).unwrap();
    }

    #[test]
    fn restore_plan_splits_pending_and_crawled() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _wal, _id) = initialize_persistence(dir.path()).unwrap();
        let mut events = Vec::new();
        for i in 0..10 {
            events.push(add_node(&format!("https://example.com/p{i}")));
        }
        for i in 0..4 {
            events.push(crawled(&format!("https://example.com/p{i}")));
        }
        apply(&state, events);

        let plan = collect_restore_plan(&state, 3);
        let mut pending: Vec<String> = plan.pending.iter().map(|p| p.0.clone()).collect();
        pending.sort();
        let expected: Vec<String> = (4..10)
            .map(|i| format!("https://example.com/p{i}"))
            .collect();
        assert_eq!(pending, expected);
        assert_eq!(plan.crawled_by_shard.len(), 3);
        assert_eq!(plan.crawled_by_shard.iter().map(Vec::len).sum::<usize>(), 4);
        assert!(plan.pending.iter().all(|p| p.1 == 1));
    }

    /// Enqueue N URLs, "crash" after crawling some, rebuild from disk and check
    /// the same pending set comes back while crawled URLs are not re-queued.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_restores_pending_set_and_skips_crawled() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (state, _wal, _id) = initialize_persistence(dir.path()).unwrap();
            let mut events = Vec::new();
            for i in 0..25 {
                events.push(add_node(&format!("https://example.com/page/{i}")));
            }
            for i in 0..10 {
                events.push(crawled(&format!("https://example.com/page/{i}")));
            }
            apply(&state, events);
        } // drop = in-memory frontier gone

        let (state, wal_writer, instance_id) = initialize_persistence(dir.path()).unwrap();
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::new());
        let writer = Arc::new(WriterThread::spawn(
            Arc::clone(&state),
            wal_writer,
            metrics,
            instance_id,
            0,
        ));
        let http = Arc::new(HttpClient::new("test-agent".into(), 5).unwrap());
        let config = BfsCrawlerConfig {
            restore_frontier: true,
            ignore_robots: true,
            ..Default::default()
        };
        let (mut shards, frontier, _tx, _rx, _size, _bp) =
            setup_frontier(&config, Arc::clone(&state), writer, http, 0)
                .await
                .unwrap();

        let mut queued = 0;
        for shard in shards.iter_mut() {
            while shard.process_incoming_urls("example.com").await > 0 {}
            queued += shard.queued_url_count();
        }
        assert_eq!(
            queued, 15,
            "all uncrawled URLs should be back in the frontier"
        );

        // A link to an already-crawled page discovered after resume is dropped,
        // while a genuinely new one is accepted.
        frontier
            .add_links(vec![
                ("https://example.com/page/3".into(), 2, None),
                ("https://example.com/new".into(), 2, None),
            ])
            .await;
        let mut after = 0;
        for shard in shards.iter_mut() {
            while shard.process_incoming_urls("example.com").await > 0 {}
            after += shard.queued_url_count();
        }
        assert_eq!(after, 16);
    }

    #[tokio::test]
    async fn fresh_crawl_does_not_restore() {
        let dir = tempfile::tempdir().unwrap();
        let (state, wal_writer, instance_id) = initialize_persistence(dir.path()).unwrap();
        apply(&state, vec![add_node("https://example.com/x")]);
        let writer = Arc::new(WriterThread::spawn(
            Arc::clone(&state),
            wal_writer,
            std::sync::Arc::new(crate::metrics::Metrics::new()),
            instance_id,
            0,
        ));
        let http = Arc::new(HttpClient::new("test-agent".into(), 5).unwrap());
        let (mut shards, _f, _tx, _rx, _size, _bp) =
            setup_frontier(&BfsCrawlerConfig::default(), state, writer, http, 0)
                .await
                .unwrap();
        let mut queued = 0;
        for shard in shards.iter_mut() {
            shard.process_incoming_urls("example.com").await;
            queued += shard.queued_url_count();
        }
        assert_eq!(queued, 0);
    }
}
