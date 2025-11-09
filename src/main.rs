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
    max_urls: Option<usize>,
    duration: Option<u64>,
) -> BfsCrawlerConfig {
    assert!(
        timeout < u32::MAX as u64,
        "Timeout value exceeds u32::MAX and will be truncated."
    );
    BfsCrawlerConfig {
        max_workers: u32::try_from(workers).unwrap_or(u32::MAX),
        timeout: timeout as u32,
        user_agent,
        ignore_robots,
        save_interval,
        redis_url: if enable_redis { Some(redis_url) } else { None },
        lock_ttl,
        enable_redis,
        max_urls,
        duration_secs: duration,
    }
}

async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250;

    // Configurable governor thresholds
    // Optimized for high-volume crawling based on stress testing (see stress_tests/README.md)
    let throttle_threshold_ms = std::env::var("GOVERNOR_THROTTLE_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(2000.0);  // Was 500ms - increased 4x to reduce false positives

    let unthrottle_threshold_ms = std::env::var("GOVERNOR_UNTHROTTLE_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(200.0);  // Was 100ms - increased 2x for smoother recovery

    let min_permits = std::env::var("GOVERNOR_MIN_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256);  // Was 32 - increased 8x for higher baseline throughput

    let max_permits = std::env::var("GOVERNOR_MAX_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);  // Was 512 - increased 2x for peak capacity

    let mut shrink_bin: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
    let mut last_urls_fetched = 0u64;

    loop {
        // Stop adjusting permits once a shutdown message arrives.
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

        if commit_ewma_ms > throttle_threshold_ms {
            if current_permits > min_permits {
                if let Ok(permit) = permits.clone().try_acquire_owned() {
                    shrink_bin.push(permit);
                    metrics.throttle_adjustments.lock().inc();
                }
            }
        } else if commit_ewma_ms < unthrottle_threshold_ms && commit_ewma_ms > 0.0 && work_done {
            if let Some(permit) = shrink_bin.pop() {
                drop(permit);
                metrics.throttle_adjustments.lock().inc();
            } else if current_permits < max_permits {
                permits.add_permits(1);
                metrics.throttle_adjustments.lock().inc();
            }
        }

        metrics
            .throttle_permits_held
            .lock()
            .set(current_permits as f64);
    }
}

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
            frontier::FrontierPermit,
        )>,
        tokio::sync::watch::Sender<bool>, // Signals the governor task to exit.
        tokio::sync::watch::Sender<bool>, // Tells shard workers to exit.
    ),
    Box<dyn std::error::Error>,
> {
    let http = Arc::new(HttpClient::new(
        config.user_agent.clone(),
        config.timeout as u64,
    )?);

    let state = Arc::new(CrawlerState::new(&data_dir)?);

    let wal_writer = Arc::new(tokio::sync::Mutex::new(WalWriter::new(
        data_dir.as_ref(),
        100,
    )?));

    let metrics = Arc::new(Metrics::new());

    let instance_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            eprintln!("Time error: {}. Using default duration.", e);
            std::time::Duration::from_secs(0)
        })
        .as_nanos() as u64;

    let wal_reader = wal::WalReader::new(data_dir.as_ref());
    let mut replayed_count = 0usize;
    // Replay WAL entries so state reflects the last run.
    let max_seqno = wal_reader.replay(|record| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // We trust our WAL format, so treat any panic as corruption and surface it.
            unsafe { rkyv::archived_root::<state::StateEvent>(&record.payload) }
        }));

        let archived = match result {
            Ok(a) => a,
            Err(_) => {
                eprintln!(
                    "FATAL: Corrupted WAL record at seqno {:?} - deserialization panicked. Aborting replay.",
                    record.seqno
                );
                return Err(wal::WalError::CorruptRecord(format!(
                    "Deserialization panic at seqno {:?}",
                    record.seqno
                )));
            }
        };

        // Treat deserialize failures as corruption so we abort immediately.
        let event: state::StateEvent = match rkyv::Deserialize::deserialize(archived, &mut rkyv::Infallible) {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!(
                    "FATAL: Corrupted WAL record at seqno {:?} - deserialization failed: {:?}. Aborting replay.",
                    record.seqno, e
                );
                return Err(wal::WalError::CorruptRecord(format!(
                    "Deserialization error at seqno {:?}: {:?}",
                    record.seqno, e
                )));
            }
        };

        // Apply the recovered event to the live state.
        let event_with_seqno = state::StateEventWithSeqno {
            seqno: record.seqno,
            event,
        };
        // Use the batch path even for single events to keep semantics identical.
        let _ = state.apply_event_batch(&[event_with_seqno]);
        replayed_count += 1;

        Ok(())
    })?;

    if replayed_count > 0 {
        eprintln!(
            "WAL replay: recovered {} events (max_seqno: {})",
            replayed_count, max_seqno
        );
    }

    let writer_thread = Arc::new(WriterThread::spawn(
        Arc::clone(&state),
        Arc::clone(&wal_writer),
        Arc::clone(&metrics),
        instance_id,
        max_seqno,
    ));

    let crawler_permits = Arc::new(tokio::sync::Semaphore::new(config.max_workers as usize));

    let (governor_shutdown_tx, governor_shutdown_rx) = tokio::sync::watch::channel(false);

    let (shard_shutdown_tx, _shard_shutdown_rx) = tokio::sync::watch::channel(false);

    let governor_permits_clone = Arc::clone(&crawler_permits);
    let governor_metrics_clone = Arc::clone(&metrics);
    tokio::spawn(async move {
        governor_task(
            governor_permits_clone,
            governor_metrics_clone,
            governor_shutdown_rx,
        )
        .await;
    });

    let num_shards = num_cpus::get();
    eprintln!("Initializing sharded frontier with {} shards", num_shards);
    let (
        frontier_dispatcher,
        shard_receivers,
        global_frontier_size,
        backpressure_semaphore,
    ) = FrontierDispatcher::new(num_shards);

    // Create work channel first
    let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();

    // Create shared stats for real-time frontier monitoring
    let shared_stats = frontier::SharedFrontierStats::new();

    let mut frontier_shards = Vec::with_capacity(num_shards);
    let mut host_state_caches = Vec::with_capacity(num_shards);

    for (shard_id, url_receiver) in shard_receivers
        .into_iter()
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
            work_tx.clone(),
            Arc::clone(&global_frontier_size),
            Arc::clone(&backpressure_semaphore),
            shared_stats.clone(),
        );
        host_state_caches.push(shard.get_host_state_cache());
        frontier_shards.push(shard);
    }

    let (sharded_frontier, _work_rx_unused) =
        ShardedFrontier::new(frontier_dispatcher, host_state_caches, shared_stats);
    let frontier = Arc::new(sharded_frontier);

    let lock_manager = if config.enable_redis {
        if let Some(url) = &config.redis_url {
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

    if config.enable_redis {
        if let Some(url) = &config.redis_url {
            match WorkStealingCoordinator::new(
                Some(url),
                work_tx.clone(),
                Arc::clone(&backpressure_semaphore),
                Arc::clone(&global_frontier_size),
            ) {
                Ok(coordinator) => {
                    let coordinator = Arc::new(coordinator);
                    let work_stealing_shutdown = shard_shutdown_tx.subscribe();
                    tokio::spawn(async move {
                        coordinator.start(work_stealing_shutdown).await;
                    });
                    eprintln!("Work stealing coordinator started");
                }
                Err(e) => {
                    eprintln!("Work stealing setup failed: {}", e);
                }
            }
        }
    }

    let crawler = BfsCrawler::new(
        config,
        start_url,
        http,
        state,
        frontier,
        work_rx, // Feed the receiver straight into tokio::select!
        writer_thread,
        lock_manager,
        metrics,
        crawler_permits, // Share one semaphore across all crawlers.
    );

    Ok((
        crawler,
        frontier_shards,
        work_tx,
        governor_shutdown_tx,
        shard_shutdown_tx,
    ))
}

async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
) -> Result<(), MainError> {
    println!("Exporting sitemap to {}...", output);

    // Export only needs the stored state, so skip the crawler stack.
    let state = CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

    let mut writer = SitemapWriter::new(&output).map_err(|e| MainError::Export(e.to_string()))?;

    // Stream nodes so the export stays memory-safe.
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
            preset,
            mut workers,
            user_agent,
            mut timeout,
            mut ignore_robots,
            seeding_strategy,
            enable_redis,
            redis_url,
            lock_ttl,
            save_interval,
            mut max_urls,
            duration,
        } => {
            // Apply preset configuration if specified
            if let Some(preset_name) = &preset {
                match preset_name.as_str() {
                    "ben" => {
                        eprintln!("🚀 Applying 'ben' preset: Maximum throughput mode");
                        workers = 1024;
                        timeout = 60;
                        ignore_robots = true;
                        max_urls = None; // Unlimited
                        eprintln!("   Workers: {}", workers);
                        eprintln!("   Timeout: {}s", timeout);
                        eprintln!("   Ignoring robots.txt: true");
                        eprintln!("   Max URLs: unlimited");
                    }
                    _ => {
                        eprintln!("⚠️  Unknown preset '{}', using default settings", preset_name);
                    }
                }
            }

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
                max_urls,
                duration,
            );

            let (mut crawler, frontier_shards, _work_tx, governor_shutdown, shard_shutdown) =
                build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();
            let gov_shutdown = governor_shutdown.clone();
            let shard_shutdown_clone = shard_shutdown.clone();

            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");
                    println!("Press Ctrl+C again to force quit");

                    let _ = shutdown_tx.send(true);
                    let _ = gov_shutdown.send(true);
                    let _ = shard_shutdown_clone.send(true);

                    c.stop().await;

                    // Second handler lets a follow-up Ctrl+C skip the grace period.
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            eprintln!("\nForce quit requested, exiting immediately...");
                            std::process::exit(1);
                        }
                    });

                    // Give the writer thread a moment to flush its WAL batches.
                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        Config::SHUTDOWN_GRACE_PERIOD_SECS,
                    ))
                    .await;

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

            // Keep a crawler handle alive for the post-run export.
            let crawler_for_export = crawler.clone();
            let export_data_dir = data_dir.clone();

            // Initialize before seeding so the frontier starts populated.
            crawler.initialize(&seeding_strategy).await?;

            // Launch shard workers only after initialization finishes.
            let start_url_domain = crawler.get_domain(&normalized_start_url);
            for mut shard in frontier_shards {
                let domain_clone = start_url_domain.clone();
                tokio::spawn(async move {
                    // Shard workers run indefinitely to avoid race conditions during shutdown
                    // They're cleaned up automatically when the process exits
                    loop {
                        // Control messages adjust throttling and host bookkeeping.
                        shard.process_control_messages().await;

                        // Handle URLs the dispatcher assigns to this shard.
                        shard.process_incoming_urls(&domain_clone).await;

                        // Pull runnable work for this shard; work_tx handles routing.
                        if shard.get_next_url().await.is_none() {
                            // Sleep until the politeness delay expires (capped at 100ms).
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
                                        // Delay expired; yield so ready work runs promptly.
                                        tokio::time::sleep(tokio::time::Duration::from_millis(1))
                                            .await;
                                    }
                                } else {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(10))
                                        .await;
                                }
                            } else {
                                // Nothing pending, so yield briefly before polling again.
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            }
                        }
                    }
                });
            }

            let result = crawler.start_crawling().await?;

            // Wait briefly for any remaining tasks to add their links before shutting down shards
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Tell the governor and shards to shut down once crawling ends.
            let _ = governor_shutdown.send(true);
            let _ = shard_shutdown.send(true);

            // Wait for shards to drain their channels
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Export JSONL on success so the latest data is always persisted.
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
            max_urls,
            duration,
        } => {
            println!(
                "Resuming crawl from {} ({} concurrent requests, {}s timeout)",
                data_dir, workers, timeout
            );

            // Reload state to recover the recorded start_url for resumes.
            let state =
                CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

            // Placeholder start_url only satisfies construction; saved frontier drives real work.
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
                Config::SAVE_INTERVAL_SECS, // Resume sticks with the default save cadence.
                max_urls,
                duration,
            );

            let (mut crawler, frontier_shards, _work_tx, governor_shutdown, shard_shutdown) =
                build_crawler(placeholder_start_url.clone(), &data_dir, config).await?;

            // Resume mode also needs a shutdown channel for Ctrl+C handling.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();
            let gov_shutdown = governor_shutdown.clone();
            let shard_shutdown_clone = shard_shutdown.clone();

            tokio::spawn(async move {
                // First Ctrl+C requests a graceful shutdown.
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");
                    println!("Press Ctrl+C again to force quit");

                    let _ = shutdown_tx.send(true);
                    let _ = gov_shutdown.send(true);
                    let _ = shard_shutdown_clone.send(true);
                    c.stop().await;

                    // Second handler makes the next Ctrl+C an immediate abort.
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            eprintln!("\nForce quit requested, exiting immediately...");
                            std::process::exit(1);
                        }
                    });

                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        Config::SHUTDOWN_GRACE_PERIOD_SECS,
                    ))
                    .await;
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

            // Launch shard workers on the multithreaded runtime.
            let start_url_domain = crawler.get_domain(&placeholder_start_url);
            for mut shard in frontier_shards {
                let domain_clone = start_url_domain.clone();
                tokio::spawn(async move {
                    // Shard workers run indefinitely to avoid race conditions during shutdown
                    // They're cleaned up automatically when the process exits
                    loop {
                        // Control messages adjust throttling and host bookkeeping.
                        shard.process_control_messages().await;

                        // Handle URLs routed to this shard.
                        shard.process_incoming_urls(&domain_clone).await;

                        // Pull runnable work for this shard; work_tx handles delivery.
                        if shard.get_next_url().await.is_none() {
                            // Sleep until the politeness delay expires (max 100ms).
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
                                        // Delay expired; yield so ready work runs promptly.
                                        tokio::time::sleep(tokio::time::Duration::from_millis(1))
                                            .await;
                                    }
                                } else {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(10))
                                        .await;
                                }
                            } else {
                                // Nothing pending, so yield briefly before polling again.
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            }
                        }
                    }
                });
            }

            // Initialization reloads saved state, so reseeding would duplicate work.
            crawler.initialize("none").await?;

            let result = crawler.start_crawling().await?;

            // Wait briefly for any remaining tasks to add their links before shutting down shards
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Notify the governor and shards that work finished.
            let _ = governor_shutdown.send(true);
            let _ = shard_shutdown.send(true);

            // Wait for shards to drain their channels
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            // Export JSONL post-resume to keep the artifact current.
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