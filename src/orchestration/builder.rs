//! Main crawler builder function.

use crate::bfs_crawler::{BfsCrawler, BfsCrawlerConfig};
use crate::frontier::{FrontierPermit, FrontierShard};
use crate::metrics::Metrics;
use crate::network::HttpClient;
use crate::writer_thread::WriterThread;
use std::sync::Arc;

use super::distributed::setup_distributed_coordination;
use super::frontier_setup::setup_frontier;
use super::governor::governor_task;
use super::persistence::{initialize_persistence, replay_wal_if_needed};

/// Builds complete crawler instance with all components wired up.
#[tracing::instrument(skip(data_dir, config), fields(start_url = %start_url))]
pub async fn build_crawler<P: AsRef<std::path::Path>>(
    start_url: String,
    data_dir: P,
    config: BfsCrawlerConfig,
) -> Result<
    (
        BfsCrawler,
        Vec<FrontierShard>,
        tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, FrontierPermit)>,
        tokio::sync::watch::Sender<bool>, // Governor shutdown signal.
        tokio::sync::watch::Sender<bool>, // Shard shutdown signal.
    ),
    Box<dyn std::error::Error>,
> {
    let http = Arc::new(HttpClient::new(
        config.user_agent.clone(),
        config.timeout as u64,
    )?);

    let (state, wal_writer, instance_id) = initialize_persistence(&data_dir)?;
    let metrics = Arc::new(Metrics::new());
    let (max_seqno, replayed_count) = replay_wal_if_needed(&data_dir, &state)?;

    let writer_thread = Arc::new(WriterThread::spawn(
        Arc::clone(&state),
        wal_writer,
        Arc::clone(&metrics),
        instance_id,
        max_seqno,
    ));

    let crawler_permits = Arc::new(tokio::sync::Semaphore::new(config.max_workers as usize));
    let (governor_shutdown_tx, governor_shutdown_rx) = tokio::sync::watch::channel(false);
    let (shard_shutdown_tx, _shard_shutdown_rx) = tokio::sync::watch::channel(false);

    let permits = Arc::clone(&crawler_permits);
    let metrics_ref = Arc::clone(&metrics);
    tokio::spawn(async move {
        governor_task(permits, metrics_ref, governor_shutdown_rx).await;
    });

    let (frontier_shards, frontier, work_tx, work_rx, frontier_size, backpressure) =
        setup_frontier(
            &config,
            Arc::clone(&state),
            Arc::clone(&writer_thread),
            Arc::clone(&http),
            replayed_count,
        )
        .await?;

    let lock_manager = setup_distributed_coordination(
        &config,
        instance_id,
        work_tx.clone(),
        Arc::clone(&backpressure),
        Arc::clone(&frontier_size),
        shard_shutdown_tx.clone(),
    )
    .await;

    let crawler = BfsCrawler::new(
        config,
        start_url,
        http,
        state,
        frontier,
        work_rx,
        writer_thread,
        lock_manager,
        metrics,
        crawler_permits,
    );

    Ok((
        crawler,
        frontier_shards,
        work_tx,
        governor_shutdown_tx,
        shard_shutdown_tx,
    ))
}
