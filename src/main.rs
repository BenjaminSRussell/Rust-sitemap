mod bfs_crawler;
mod cli;
mod common_crawl_seeder;
mod config;
mod ct_log_seeder;
mod frontier;
mod metrics;
mod network;
mod robots;
mod seeder;
mod sitemap_seeder;
mod sitemap_writer;
mod state;
mod url_lock_manager;
mod url_utils;
mod wal;
mod work_stealing;
mod writer_thread;

use bfs_crawler::{BfsCrawler, BfsCrawlerConfig};
use cli::{Cli, Commands};
use config::Config;
use frontier::{FrontierDispatcher, FrontierShard, ShardedFrontier};
use metrics::Metrics;
use network::HttpClient;
use sitemap_writer::{SitemapUrl, SitemapWriter};
use state::CrawlerState;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url_lock_manager::UrlLockManager;
use url_utils::normalize_url_for_cli;
use wal::WalWriter;
use work_stealing::WorkStealingCoordinator;
use writer_thread::WriterThread;

#[derive(Error, Debug)]
pub enum MainError {
    #[error("Crawler error: {0}")]
    Crawler(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("State error: {0}")]
    State(String),

    #[error("Export error: {0}")]
    Export(String),
}

impl From<Box<dyn std::error::Error>> for MainError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        MainError::Crawler(err.to_string())
    }
}

fn build_crawler_config(
    workers: usize,
    timeout: u64,
    user_agent: String,
    ignore_robots: bool,
    enable_redis: bool,
    redis_url: String,
    lock_ttl: u64,
    save_interval: u64,
) -> BfsCrawlerConfig {
    BfsCrawlerConfig {
        max_workers: workers as u32,
        timeout: timeout as u32,
        user_agent,
        ignore_robots,
        save_interval,
        redis_url: if enable_redis { Some(redis_url) } else { None },
        lock_ttl,
        enable_redis,
    }
}

/// Monitors commit latency and adjusts concurrency.
async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250;
    const THROTTLE_THRESHOLD_MS: f64 = 500.0;
    const UNTHROTTLE_THRESHOLD_MS: f64 = 100.0;
    const MIN_PERMITS: usize = 32;
    const MAX_PERMITS: usize = 512;

    eprintln!("Governor: Started monitoring commit latency (250ms intervals)");

    let mut shrink_bin: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
    let mut last_urls_fetched = 0u64;

    loop {
        // Check for shutdown signal
        if *shutdown.borrow() {
            eprintln!("Governor: Shutdown signal received, exiting");
            break;
        }

        tokio::time::sleep(Duration::from_millis(ADJUSTMENT_INTERVAL_MS)).await;

        let commit_ewma_ms = metrics.get_commit_ewma_ms();
        let current_permits = permits.available_permits();
        let current_urls_processed = metrics.urls_processed_total.lock().value;
        let work_done = current_urls_processed > last_urls_fetched;
        last_urls_fetched = current_urls_processed;

        if commit_ewma_ms > THROTTLE_THRESHOLD_MS {
            if current_permits > MIN_PERMITS {
                if let Ok(permit) = permits.clone().try_acquire_owned() {
                    shrink_bin.push(permit);
                    metrics.throttle_adjustments.lock().inc();
                    eprintln!(
                        "Governor: Throttling (shrink_bin: {} permits, {} available, commit_ewma: {:.2}ms)",
                        shrink_bin.len(),
                        permits.available_permits(),
                        commit_ewma_ms
                    );
                }
            }
        } else if commit_ewma_ms < UNTHROTTLE_THRESHOLD_MS && commit_ewma_ms > 0.0 && work_done {
            if let Some(permit) = shrink_bin.pop() {
                drop(permit);
                metrics.throttle_adjustments.lock().inc();
                eprintln!(
                    "Governor: Un-throttling (shrink_bin: {} permits, {} available, commit_ewma: {:.2}ms)",
                    shrink_bin.len(),
                    permits.available_permits(),
                    commit_ewma_ms
                );
            } else if current_permits < MAX_PERMITS {
                permits.add_permits(1);
                metrics.throttle_adjustments.lock().inc();
                eprintln!(
                    "Governor: Adding capacity ({} available, commit_ewma: {:.2}ms, processed: {})",
                    permits.available_permits(),
                    commit_ewma_ms,
                    current_urls_processed
                );
            }
        }

        metrics
            .throttle_permits_held
            .lock()
            .set(current_permits as f64);
    }
}

/// Build crawler dependencies and wire concrete components together.
async fn build_crawler<P: AsRef<std::path::Path>>(
    start_url: String,
    data_dir: P,
    config: BfsCrawlerConfig,
) -> Result<
    (
        BfsCrawler,
        Vec<FrontierShard>,
        tokio::sync::mpsc::UnboundedSender<(
            String,
            String,
            u32,
            Option<String>,
            tokio::sync::OwnedSemaphorePermit,
        )>,
        tokio::sync::watch::Sender<bool>, // Governor shutdown sender
        tokio::sync::watch::Sender<bool>, // Shard workers shutdown sender
    ),
    Box<dyn std::error::Error>,
> {
    // Build the HTTP client so we share configuration across the crawler.
    let http = Arc::new(HttpClient::new(
        config.user_agent.clone(),
        config.timeout as u64,
    )?);

    // Build the crawler state so persistence has a backing store.
    let state = Arc::new(CrawlerState::new(&data_dir)?);

    // Create the WAL writer with a 100 ms fsync interval so durability keeps up without thrashing disks.
    let wal_writer = Arc::new(tokio::sync::Mutex::new(WalWriter::new(
        data_dir.as_ref(),
        100,
    )?));

    // Create the metrics registry so runtime feedback is available to operators.
    let metrics = Arc::new(Metrics::new());

    // Generate the instance ID from the timestamp so distributed components can differentiate peers.
    let instance_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::from_secs(0))
        .as_nanos() as u64;

    // Replay the WAL to recover in-flight state after a crash, preventing catastrophic data loss.
    let wal_reader = wal::WalReader::new(data_dir.as_ref());
    let mut replayed_count = 0usize;
    let mut skipped_count = 0usize;
    let max_seqno = wal_reader.replay(|record| {
        // Deserialize the event from the WAL record with error recovery for corrupted records
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: The WAL is written by our own code. If corruption occurs, we catch the panic.
            let archived = unsafe { rkyv::archived_root::<state::StateEvent>(&record.payload) };
            let event: state::StateEvent =
                rkyv::Deserialize::deserialize(archived, &mut rkyv::Infallible).unwrap();
            event
        }));

        match result {
            Ok(event) => {
                // Apply the event directly to the state
                let event_with_seqno = state::StateEventWithSeqno {
                    seqno: record.seqno,
                    event,
                };
                // Use apply_event_batch with a single-item batch to reuse existing logic
                let _ = state.apply_event_batch(&[event_with_seqno]);
                replayed_count += 1;
            }
            Err(_) => {
                eprintln!(
                    "WARNING: Skipping corrupted WAL record at seqno {:?} (deserialization failed)",
                    record.seqno
                );
                skipped_count += 1;
            }
        }
        Ok(())
    })?;

    if replayed_count > 0 || skipped_count > 0 {
        if skipped_count > 0 {
            eprintln!(
                "WAL replay: recovered {} events, skipped {} corrupted records (max_seqno: {})",
                replayed_count, skipped_count, max_seqno
            );
        } else {
            eprintln!(
                "WAL replay: recovered {} events (max_seqno: {})",
                replayed_count, max_seqno
            );
        }
    }

    // Spawn the dedicated writer OS thread so blocking WAL work never stalls async tasks.
    // Initialize with the recovered max_seqno to prevent duplicate sequence numbers.
    let writer_thread = Arc::new(WriterThread::spawn(
        Arc::clone(&state),
        Arc::clone(&wal_writer),
        Arc::clone(&metrics),
        instance_id,
        max_seqno,
    ));

    // Create the semaphore so the governor can adjust crawler concurrency on the fly.
    let crawler_permits = Arc::new(tokio::sync::Semaphore::new(config.max_workers as usize));

    // Create shutdown channel for governor task
    let (governor_shutdown_tx, governor_shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn the governor task so permits respond to observed commit latencies.
    let governor_permits = Arc::clone(&crawler_permits);
    let governor_metrics = Arc::clone(&metrics);
    tokio::spawn(async move {
        governor_task(governor_permits, governor_metrics, governor_shutdown_rx).await;
    });

    // Build the sharded frontier dispatcher so URL scheduling can run concurrently without global locks.
    // GUARD: Any changes that re-enable legacy frontier implementations must preserve sharding behavior.
    let num_shards = num_cpus::get();
    eprintln!("Initializing sharded frontier with {} shards", num_shards);
    let (
        frontier_dispatcher,
        shard_receivers,
        shard_control_receivers,
        shard_control_senders,
        global_frontier_size,
        backpressure_semaphore,
    ) = FrontierDispatcher::new(num_shards);

    // Wrap the dispatcher in ShardedFrontier to provide a unified interface
    let (sharded_frontier, work_rx) =
        ShardedFrontier::new(frontier_dispatcher, shard_control_senders);
    let work_tx = sharded_frontier.work_tx();
    let frontier = Arc::new(sharded_frontier);

    // Create the frontier shards that will run on separate LocalSet threads.
    let mut frontier_shards = Vec::with_capacity(num_shards);

    for (shard_id, (url_receiver, control_receiver)) in shard_receivers
        .into_iter()
        .zip(shard_control_receivers.into_iter())
        .enumerate()
    {
        let shard = FrontierShard::new(
            shard_id,
            Arc::clone(&state),
            Arc::clone(&writer_thread),
            Arc::clone(&http),
            config.user_agent.clone(),
            config.ignore_robots,
            url_receiver,
            control_receiver,
            work_tx.clone(), // Pass work_tx so shard can send URLs to crawler
            Arc::clone(&global_frontier_size),
            Arc::clone(&backpressure_semaphore),
        );
        frontier_shards.push(shard);
    }

    // Optionally build the lock manager so multiple crawler instances can coordinate via Redis.
    let lock_manager = if config.enable_redis {
        if let Some(url) = &config.redis_url {
            // Generate the instance ID string so Redis locks are namespaced per crawler.
            let lock_instance_id = format!("crawler-{}", instance_id);
            match UrlLockManager::new(url, Some(config.lock_ttl), lock_instance_id).await {
                Ok(mgr) => {
                    eprintln!(
                        "Redis locks enabled with instance ID: crawler-{}",
                        instance_id
                    );
                    Some(Arc::new(tokio::sync::Mutex::new(mgr)))
                }
                Err(e) => {
                    eprintln!("Redis lock setup failed: {}", e);
                    None
                }
            }
        } else {
            eprintln!("Redis enabled but URL not provided");
            None
        }
    } else {
        None
    };

    // Optionally build and spawn the work stealing coordinator for distributed crawling.
    if config.enable_redis {
        if let Some(url) = &config.redis_url {
            match WorkStealingCoordinator::new(
                Some(url),
                work_tx.clone(),
                Arc::clone(&backpressure_semaphore),
            ) {
                Ok(coordinator) => {
                    let coordinator = Arc::new(coordinator);
                    tokio::spawn(async move {
                        coordinator.start().await;
                    });
                    eprintln!("Work stealing coordinator started");
                }
                Err(e) => {
                    eprintln!("Work stealing setup failed: {}", e);
                }
            }
        }
    }

    // Wire the dependencies together so the crawler gets a fully-initialized runtime bundle.
    let crawler = BfsCrawler::new(
        config,
        start_url,
        http,
        state,
        frontier,
        work_rx, // Pass the receiver directly for use in tokio::select!
        writer_thread,
        lock_manager,
        metrics,
        crawler_permits, // Share the permit pool so runtime throttling can take effect.
    );

    Ok((crawler, frontier_shards, work_tx, governor_shutdown_tx))
}

async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
) -> Result<(), MainError> {
    println!("Exporting sitemap to {}...", output);

    // For export, only load the state so we avoid spinning up the entire crawler pipeline.
    let state = CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

    let mut writer = SitemapWriter::new(&output).map_err(|e| MainError::Export(e.to_string()))?;

    // Use a streaming iterator to avoid loading all nodes into memory and risk OOM.
    let node_iter = state
        .iter_nodes()
        .map_err(|e| MainError::State(e.to_string()))?;

    node_iter
        .for_each(|node| {
            if node.status_code == Some(200) {
                let lastmod = if include_lastmod {
                    node.crawled_at.map(|ts| {
                        let dt = chrono::DateTime::from_timestamp(ts as i64, 0).unwrap_or_default();
                        dt.format("%Y-%m-%d").to_string()
                    })
                } else {
                    None
                };

                let changefreq = if include_changefreq {
                    Some("weekly".to_string())
                } else {
                    None
                };

                let priority = match node.depth {
                    0 => Some(1.0),
                    1 => Some(0.8),
                    2 => Some(0.6),
                    _ => Some(default_priority),
                };

                writer
                    .add_url(SitemapUrl {
                        loc: node.url.clone(),
                        lastmod,
                        changefreq,
                        priority,
                    })
                    .map_err(|e| state::StateError::Serialization(e.to_string()))?;
            }
            Ok(())
        })
        .map_err(|e| MainError::State(e.to_string()))?;

    let count = writer
        .finish()
        .map_err(|e| MainError::Export(e.to_string()))?;
    println!("Exported {} URLs to {}", count, output);

    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), MainError> {
    let cli = Cli::parse_args();

    match cli.command {
        Commands::Crawl {
            start_url,
            data_dir,
            workers,
            export_jsonl: _,
            user_agent,
            timeout,
            ignore_robots,
            seeding_strategy,
            enable_redis,
            redis_url,
            lock_ttl,
            save_interval,
            max_content_size: _,
            pool_idle_per_host: _,
            pool_idle_timeout: _,
        } => {
            let normalized_start_url = normalize_url_for_cli(&start_url);

            if enable_redis {
                println!(
                    "Crawling {} ({} concurrent requests, {}s timeout, Redis distributed mode)",
                    normalized_start_url, workers, timeout
                );
                println!("Redis URL: {}, Lock TTL: {}s", redis_url, lock_ttl);
            } else {
                println!(
                    "Crawling {} ({} concurrent requests, {}s timeout)",
                    normalized_start_url, workers, timeout
                );
            }

            let config = build_crawler_config(
                workers,
                timeout,
                user_agent,
                ignore_robots,
                enable_redis,
                redis_url,
                lock_ttl,
                save_interval,
            );

            let (mut crawler, frontier_shards, _work_tx, governor_shutdown) =
                build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

            crawler.initialize(&seeding_strategy).await?;

            // No longer need LocalSet since scraper is Send + Sync!
            // Everything now runs on the multi-threaded runtime for maximum performance.

            // Create the shutdown channel so Ctrl+C can trigger a coordinated shutdown.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();
            let gov_shutdown = governor_shutdown.clone();

            tokio::spawn(async move {
                // First Ctrl+C: graceful shutdown
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");
                    println!("Press Ctrl+C again to force quit");

                    // Signal shutdown so other tasks get the stop message.
                    let _ = shutdown_tx.send(true);
                    let _ = gov_shutdown.send(true);

                    // Stop the crawler so new work stops entering the pipeline.
                    c.stop().await;

                    // Spawn a second handler for force-quit
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            eprintln!("\nForce quit requested, exiting immediately...");
                            std::process::exit(1);
                        }
                    });

                    // Sleep briefly so the writer drains pending WAL batches.
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                    println!("Saving state...");
                    if let Err(e) = c.save_state().await {
                        eprintln!("Failed to save state: {}", e);
                    }

                    let path = std::path::Path::new(&dir).join("sitemap.jsonl");
                    if let Err(e) = c.export_to_jsonl(&path).await {
                        eprintln!("Failed to export JSONL: {}", e);
                    } else {
                        println!("Saved to: {}", path.display());
                    }

                    println!("Graceful shutdown complete");
                    std::process::exit(0);
                }
            });

            // Clone the crawler handle so we can export once the run finishes.
            let crawler_for_export = crawler.clone();
            let export_data_dir = data_dir.clone();

            // Spawn shard worker tasks on the multi-threaded runtime.
            // All tasks now run on the multi-threaded runtime for maximum parallelism!
            let start_url_domain = crawler.get_domain(&normalized_start_url);
            for mut shard in frontier_shards {
                let domain_clone = start_url_domain.clone();
                tokio::spawn(async move {
                    loop {
                        // Process control messages to update host state
                        shard.process_control_messages().await;

                        // Process incoming URLs from the dispatcher.
                        shard.process_incoming_urls(&domain_clone).await;

                        // Try to pull work from this shard (it sends via work_tx internally)
                        if shard.get_next_url().await.is_none() {
                            // No work available, small yield
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                    }
                });
            }

            // Run the crawler on the multi-threaded runtime (no more LocalSet bottleneck!)
            let result = crawler.start_crawling().await?;

            // Signal governor to shutdown now that crawling is complete
            let _ = governor_shutdown.send(true);

            // Always export JSONL on completion so users get output even on success.
            let path = std::path::Path::new(&export_data_dir).join("sitemap.jsonl");
            crawler_for_export.export_to_jsonl(&path).await?;
            println!("Exported to: {}", path.display());

            let success_rate = if result.processed > 0 {
                result.successful as f64 / result.processed as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "Crawl complete: discovered {}, processed {} ({} success, {} failed, {} timeout, {:.1}% success rate), {}s, data: {}",
                result.discovered, result.processed, result.successful, result.failed, result.timeout, success_rate, result.duration_secs, data_dir
            );
        }

        Commands::Resume {
            data_dir,
            workers,
            user_agent,
            timeout,
            ignore_robots,
            enable_redis,
            redis_url,
            lock_ttl,
        } => {
            println!(
                "Resuming crawl from {} ({} concurrent requests, {}s timeout)",
                data_dir, workers, timeout
            );

            // Load state to recover the original start_url so resumes start from known context.
            let state =
                CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

            // Use any URL from the database as a placeholder start_url because the frontier will supply real work.
            let mut placeholder_start_url = "https://example.com".to_string();
            if let Ok(mut iter) = state.iter_nodes() {
                if let Some(Ok(node)) = iter.next() {
                    placeholder_start_url = node.url.clone();
                }
            }

            let config = build_crawler_config(
                workers,
                timeout,
                user_agent,
                ignore_robots,
                enable_redis,
                redis_url,
                lock_ttl,
                Config::SAVE_INTERVAL_SECS, // Resume uses default save interval
            );

            let (mut crawler, frontier_shards, _work_tx, governor_shutdown) =
                build_crawler(placeholder_start_url.clone(), &data_dir, config).await?;

            // Initialization checks for saved state so we avoid redundant seeding.
            crawler.initialize("none").await?;

            // No longer need LocalSet - everything runs on multi-threaded runtime!

            // Create the shutdown channel so resume mode can also react to Ctrl+C.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();
            let gov_shutdown = governor_shutdown.clone();

            tokio::spawn(async move {
                // First Ctrl+C: graceful shutdown
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");
                    println!("Press Ctrl+C again to force quit");

                    let _ = shutdown_tx.send(true);
                    let _ = gov_shutdown.send(true);
                    c.stop().await;

                    // Spawn a second handler for force-quit
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            eprintln!("\nForce quit requested, exiting immediately...");
                            std::process::exit(1);
                        }
                    });

                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    println!("Saving state...");
                    if let Err(e) = c.save_state().await {
                        eprintln!("Failed to save state: {}", e);
                    }
                    let path = std::path::Path::new(&dir).join("sitemap.jsonl");
                    if let Err(e) = c.export_to_jsonl(&path).await {
                        eprintln!("Failed to export JSONL: {}", e);
                    } else {
                        println!("Saved to: {}", path.display());
                    }
                    println!("Graceful shutdown complete");
                    std::process::exit(0);
                }
            });

            let crawler_for_export = crawler.clone();
            let export_data_dir = data_dir.clone();

            // Spawn shard worker tasks on the multi-threaded runtime.
            let start_url_domain = crawler.get_domain(&placeholder_start_url);
            for mut shard in frontier_shards {
                let domain_clone = start_url_domain.clone();
                tokio::spawn(async move {
                    loop {
                        // Process control messages to update host state
                        shard.process_control_messages().await;

                        // Process incoming URLs from the dispatcher.
                        shard.process_incoming_urls(&domain_clone).await;

                        // Try to pull work from this shard (it sends via work_tx internally)
                        if shard.get_next_url().await.is_none() {
                            // No work available, small yield
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                    }
                });
            }

            // Run the crawler on the multi-threaded runtime (no more LocalSet!)
            let result = crawler.start_crawling().await?;

            // Signal governor to shutdown now that crawling is complete
            let _ = governor_shutdown.send(true);

            // Export JSONL at the end of resume runs so data stays fresh.
            let path = std::path::Path::new(&export_data_dir).join("sitemap.jsonl");
            crawler_for_export.export_to_jsonl(&path).await?;
            println!("Exported to: {}", path.display());

            let success_rate = if result.processed > 0 {
                result.successful as f64 / result.processed as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "Resume complete: discovered {}, processed {} ({} success, {} failed, {} timeout, {:.1}% success rate), {}s, data: {}",
                result.discovered, result.processed, result.successful, result.failed, result.timeout, success_rate, result.duration_secs, data_dir
            );
        }

        Commands::ExportSitemap {
            data_dir,
            output,
            include_lastmod,
            include_changefreq,
            default_priority,
        } => {
            run_export_sitemap_command(
                data_dir,
                output,
                include_lastmod,
                include_changefreq,
                default_priority,
            )
            .await?;
        }
    }

    Ok(())
}
