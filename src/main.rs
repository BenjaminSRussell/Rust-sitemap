mod bfs_crawler;
mod cli;
mod common_crawl_seeder;
mod completion_detector;
mod config;
mod ct_log_seeder;
mod frontier;
mod logging;
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

/// Applies preset configurations, modifying the passed parameters.
fn apply_preset_config(
    preset_name: &str,
    workers: &mut usize,
    timeout: &mut u64,
    ignore_robots: &mut bool,
    max_urls: &mut Option<usize>,
) {
    match preset_name {
        "ben" => {
            eprintln!("Applying 'ben' preset: Maximum throughput mode");
            *workers = 1024;
            *timeout = 60;
            *ignore_robots = true;
            *max_urls = None; // Unlimited
            eprintln!("   Workers: {}", workers);
            eprintln!("   Timeout: {}s", timeout);
            eprintln!("   Ignoring robots.txt: true");
            eprintln!("   Max URLs: unlimited");
        }
        _ => {
            eprintln!("Warning: Unknown preset '{}', using default settings", preset_name);
        }
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

/// Sets up a Ctrl+C signal handler for graceful shutdown.
/// The first Ctrl+C initiates graceful shutdown (stop crawler, save state, export JSONL).
/// A second Ctrl+C forces immediate exit.
fn setup_shutdown_handler(
    crawler: BfsCrawler,
    data_dir: String,
    gov_shutdown: tokio::sync::watch::Sender<bool>,
    shard_shutdown: tokio::sync::watch::Sender<bool>,
) -> tokio::sync::watch::Sender<bool> {
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\nReceived Ctrl+C, initiating graceful shutdown...");
            println!("Press Ctrl+C again to force quit");

            let _ = shutdown_tx.send(true);
            let _ = gov_shutdown.send(true);
            let _ = shard_shutdown.send(true);

            crawler.stop().await;

            // Second handler lets a follow-up Ctrl+C skip the grace period.
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    eprintln!("\nForce quit requested, exiting immediately...");
                    std::process::exit(1);
                }
            });

            // Give the writer thread a moment to flush its WAL batches.
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            println!("Saving state...");
            if let Err(e) = crawler.save_state().await {
                eprintln!("Failed to save state: {}", e);
            }

            let path = std::path::Path::new(&data_dir).join("sitemap.jsonl");
            if let Err(e) = crawler.export_to_jsonl(&path).await {
                eprintln!("Failed to export JSONL: {}", e);
            } else {
                println!("Saved to: {}", path.display());
            }

            println!("Graceful shutdown complete");
            std::process::exit(0);
        }
    });

    shutdown_tx_clone
}

/// Performs post-crawl cleanup: shutdown workers, export results, and print statistics.
#[tracing::instrument(skip(crawler, result, governor_shutdown, shard_shutdown), fields(command = %command_type))]
async fn finish_crawl(
    crawler: BfsCrawler,
    result: bfs_crawler::BfsCrawlerResult,
    export_data_dir: &str,
    data_dir: &str,
    governor_shutdown: tokio::sync::watch::Sender<bool>,
    shard_shutdown: tokio::sync::watch::Sender<bool>,
    command_type: &str,
) -> Result<(), MainError> {
    tracing::debug!("Beginning post-crawl cleanup");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    tracing::debug!("Sending shutdown signals to governor and shards");
    let _ = governor_shutdown.send(true);
    let _ = shard_shutdown.send(true);

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let path = std::path::Path::new(export_data_dir).join("sitemap.jsonl");
    tracing::info!("Exporting results to: {}", path.display());
    crawler.export_to_jsonl(&path).await?;
    println!("Exported to: {}", path.display());

    let success_rate = if result.processed > 0 {
        result.successful as f64 / result.processed as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "{} complete: discovered {}, processed {} ({} success, {} failed, {} timeout, {:.1}% success rate), {}s, data: {}",
        command_type, result.discovered, result.processed, result.successful, result.failed, result.timeout, success_rate, result.duration_secs, data_dir
    );

    Ok(())
}

/// Spawns shard worker tasks that process URLs from the frontier.
/// Each shard runs an infinite loop handling control messages, incoming URLs,
/// and dispatching work items with proper politeness delays.
fn spawn_shard_workers(
    frontier_shards: Vec<FrontierShard>,
    start_url_domain: String,
) {
    for mut shard in frontier_shards {
        let domain_clone = start_url_domain.clone();

        // Spawn shard worker loop
        tokio::spawn(async move {
            loop {
                shard.process_incoming_urls(&domain_clone).await;

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

/// Initializes persistence layer: state database and WAL writer.
fn initialize_persistence<P: AsRef<std::path::Path>>(
    data_dir: P,
) -> Result<
    (
        Arc<CrawlerState>,
        Arc<tokio::sync::Mutex<WalWriter>>,
        u64, // instance_id
    ),
    Box<dyn std::error::Error>,
> {
    let state = Arc::new(CrawlerState::new(&data_dir)?);
    let wal_writer = Arc::new(tokio::sync::Mutex::new(WalWriter::new(
        data_dir.as_ref(),
        100,
    )?));

    let instance_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            eprintln!("Time error: {}. Using default duration.", e);
            std::time::Duration::from_secs(0)
        })
        .as_nanos() as u64;

    Ok((state, wal_writer, instance_id))
}

/// Sets up the frontier: dispatcher, shards, and repopulates from database if needed.
/// Returns (frontier_shards, sharded_frontier, work_tx, work_rx, global_frontier_size, backpressure_semaphore).
#[tracing::instrument(skip(config, state, writer_thread, http))]
async fn setup_frontier(
    config: &BfsCrawlerConfig,
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,
    http: Arc<HttpClient>,
    replayed_count: usize,
) -> Result<
    (
        Vec<FrontierShard>,
        Arc<ShardedFrontier>,
        tokio::sync::mpsc::UnboundedSender<(
            String,
            String,
            u32,
            Option<String>,
            frontier::FrontierPermit,
        )>,
        tokio::sync::mpsc::UnboundedReceiver<(
            String,
            String,
            u32,
            Option<String>,
            frontier::FrontierPermit,
        )>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<tokio::sync::Semaphore>,
    ),
    Box<dyn std::error::Error>,
> {
    let num_shards = num_cpus::get();
    let (
        frontier_dispatcher,
        shard_receivers,
        global_frontier_size,
        backpressure_semaphore,
    ) = FrontierDispatcher::new(num_shards);

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
    let shared_stats = frontier::SharedFrontierStats::new();

    let mut frontier_shards = Vec::with_capacity(num_shards);
    let mut host_state_caches = Vec::with_capacity(num_shards);

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
            Arc::clone(&global_frontier_size),
            Arc::clone(&backpressure_semaphore),
            shared_stats.clone(),
        );
        host_state_caches.push(shard.get_host_state_cache());
        frontier_shards.push(shard);
    }

    let sharded_frontier = ShardedFrontier::new(frontier_dispatcher, host_state_caches, shared_stats);
    let frontier = Arc::new(sharded_frontier);

    Ok((frontier_shards, frontier, work_tx, work_rx, global_frontier_size, backpressure_semaphore))
}

/// Sets up distributed coordination: Redis lock manager and work stealing coordinator.
/// Returns lock_manager (None if Redis is disabled or setup fails).
#[tracing::instrument(skip(config, work_tx, backpressure_semaphore, global_frontier_size, shard_shutdown_tx))]
async fn setup_distributed_coordination(
    config: &BfsCrawlerConfig,
    instance_id: u64,
    work_tx: tokio::sync::mpsc::UnboundedSender<(
        String,
        String,
        u32,
        Option<String>,
        frontier::FrontierPermit,
    )>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    global_frontier_size: Arc<std::sync::atomic::AtomicUsize>,
    shard_shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> Option<Arc<tokio::sync::Mutex<UrlLockManager>>> {
    if !config.enable_redis {
        return None;
    }

    let redis_url = match &config.redis_url {
        Some(url) => url,
        None => {
            eprintln!("Redis enabled but URL not provided");
            return None;
        }
    };

    // Setup Redis lock manager
    let lock_manager = {
        let lock_instance_id = format!("crawler-{}", instance_id);
        match UrlLockManager::new(redis_url, Some(config.lock_ttl), lock_instance_id).await {
            Ok(mgr) => {
                eprintln!("Redis locks enabled with instance ID: crawler-{}", instance_id);
                Some(Arc::new(tokio::sync::Mutex::new(mgr)))
            }
            Err(e) => {
                eprintln!("Redis lock setup failed: {}", e);
                None
            }
        }
    };

    match WorkStealingCoordinator::new(
        Some(redis_url),
        work_tx,
        backpressure_semaphore,
        global_frontier_size,
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

    lock_manager
}

/// Replays WAL entries to recover state from previous crawl.
/// Returns (max_seqno, replayed_count).
fn replay_wal_if_needed<P: AsRef<std::path::Path>>(
    data_dir: P,
    state: &Arc<CrawlerState>,
) -> Result<(u64, usize), Box<dyn std::error::Error>> {
    let wal_reader = wal::WalReader::new(data_dir.as_ref());
    let mut replayed_count = 0usize;

    let max_seqno = wal_reader.replay(|record| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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

        let event_with_seqno = state::StateEventWithSeqno {
            seqno: record.seqno,
            event,
        };
        let _ = state.apply_event_batch(&[event_with_seqno]);
        replayed_count += 1;

        Ok(())
    })?;

    if replayed_count > 0 {
        eprintln!("Recovered {} events from previous crawl", replayed_count);
    }

    Ok((max_seqno, replayed_count))
}

#[tracing::instrument(skip(permits, metrics, shutdown))]
async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250;

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
            if current_permits > min_permits
                && let Ok(permit) = permits.clone().try_acquire_owned() {
                    shrink_bin.push(permit);
                    metrics.throttle_adjustments.lock().inc();
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

#[tracing::instrument(skip(data_dir, config), fields(start_url = %start_url))]
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

    let (state, wal_writer, instance_id) = initialize_persistence(&data_dir)?;
    let metrics = Arc::new(Metrics::new());
    let (max_seqno, replayed_count) = replay_wal_if_needed(&data_dir, &state)?;

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

    let (frontier_shards, frontier, work_tx, work_rx, global_frontier_size, backpressure_semaphore) = setup_frontier(
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
        Arc::clone(&backpressure_semaphore),
        Arc::clone(&global_frontier_size),
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

#[tracing::instrument]
async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
) -> Result<(), MainError> {
    println!("Exporting sitemap to {}...", output);

    let state = CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;
    let mut writer = SitemapWriter::new(&output).map_err(|e| MainError::Export(e.to_string()))?;
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

    // Initialize logging early - logs will be written to <data_dir>/logs/
    // Extract data_dir from the command to set up logging
    let data_dir_for_logging = match &cli.command {
        Commands::Crawl { data_dir, .. } => data_dir,
        Commands::Resume { data_dir, .. } => data_dir,
        Commands::ExportSitemap { data_dir, .. } => data_dir,
        Commands::Wipe { data_dir } => data_dir,
    };

    if let Err(e) = logging::init_logging_in_data_dir(data_dir_for_logging) {
        eprintln!("Warning: Failed to initialize logging: {}", e);
        eprintln!("Continuing without file logging...");
    }

    tracing::info!("Starting rust_sitemap");

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
            if let Some(preset_name) = &preset {
                tracing::info!("Applying preset configuration: {}", preset_name);
                apply_preset_config(preset_name, &mut workers, &mut timeout, &mut ignore_robots, &mut max_urls);
            }

            let normalized_start_url = normalize_url_for_cli(&start_url);

            if enable_redis {
                tracing::info!(
                    "Starting crawl: url={}, workers={}, timeout={}s, mode=distributed",
                    normalized_start_url, workers, timeout
                );
                tracing::debug!("Redis configuration: url={}, lock_ttl={}s", redis_url, lock_ttl);
                println!(
                    "Crawling {} ({} concurrent requests, {}s timeout, Redis distributed mode)",
                    normalized_start_url, workers, timeout
                );
                println!("Redis URL: {}, Lock TTL: {}s", redis_url, lock_ttl);
            } else {
                tracing::info!(
                    "Starting crawl: url={}, workers={}, timeout={}s, mode=standalone",
                    normalized_start_url, workers, timeout
                );
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

            tracing::debug!("Building crawler configuration");
            let (mut crawler, frontier_shards, _work_tx, governor_shutdown, shard_shutdown) =
                build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

            tracing::debug!("Setting up shutdown handler");
            let _shutdown_tx = setup_shutdown_handler(
                crawler.clone(),
                data_dir.clone(),
                governor_shutdown.clone(),
                shard_shutdown.clone(),
            );

            let crawler_for_export = crawler.clone();
            let export_data_dir = data_dir.clone();

            tracing::info!("Initializing crawler with seeding strategy: {}", seeding_strategy);
            crawler.initialize(&seeding_strategy).await?;

            let start_url_domain = crawler.get_domain(&normalized_start_url);
            tracing::debug!("Spawning {} shard workers for domain: {}", frontier_shards.len(), start_url_domain);
            spawn_shard_workers(frontier_shards, start_url_domain);

            tracing::info!("Starting crawl");
            let result = crawler.start_crawling().await?;
            tracing::info!("Crawl completed: discovered={}, processed={}, successful={}",
                result.discovered, result.processed, result.successful);

            finish_crawl(
                crawler_for_export,
                result,
                &export_data_dir,
                &data_dir,
                governor_shutdown,
                shard_shutdown,
                "Crawl",
            )
            .await?;
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
            tracing::info!("Resuming crawl from data_dir={}, workers={}, timeout={}s",
                data_dir, workers, timeout);
            println!(
                "Resuming crawl from {} ({} concurrent requests, {}s timeout)",
                data_dir, workers, timeout
            );

            // Reload state to recover the recorded start_url for resumes.
            let state =
                CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

            // Placeholder start_url only satisfies construction; saved frontier drives real work.
            let mut placeholder_start_url = "https://example.com".to_string();
            if let Ok(mut iter) = state.iter_nodes() && let Some(Ok(node)) = iter.next() {
                placeholder_start_url = node.url.clone();
            }

            let config = build_crawler_config(
                workers,
                timeout,
                user_agent,
                ignore_robots,
                enable_redis,
                redis_url,
                lock_ttl,
                300, // Default save interval: 5 minutes
                max_urls,
                duration,
            );

            let (mut crawler, frontier_shards, _work_tx, governor_shutdown, shard_shutdown) =
                build_crawler(placeholder_start_url.clone(), &data_dir, config).await?;

            let _shutdown_tx = setup_shutdown_handler(
                crawler.clone(),
                data_dir.clone(),
                governor_shutdown.clone(),
                shard_shutdown.clone(),
            );

            let crawler_for_export = crawler.clone();
            let export_data_dir = data_dir.clone();

            let start_url_domain = crawler.get_domain(&placeholder_start_url);
            spawn_shard_workers(frontier_shards, start_url_domain);

            crawler.initialize("none").await?;

            let result = crawler.start_crawling().await?;

            finish_crawl(
                crawler_for_export,
                result,
                &export_data_dir,
                &data_dir,
                governor_shutdown,
                shard_shutdown,
                "Resume",
            )
            .await?;
        }

        Commands::ExportSitemap {
            data_dir,
            output,
            include_lastmod,
            include_changefreq,
            default_priority,
        } => {
            tracing::info!("Exporting sitemap from data_dir={} to output={}", data_dir, output);
            run_export_sitemap_command(
                data_dir,
                output,
                include_lastmod,
                include_changefreq,
                default_priority,
            )
            .await?;
        }

        Commands::Wipe { data_dir } => {
            tracing::warn!("Wiping all crawl data from: {}", data_dir);
            println!("Wiping all crawl data from: {}", data_dir);

            if std::path::Path::new(&data_dir).exists() {
                std::fs::remove_dir_all(&data_dir)
                    .map_err(MainError::Io)?;
                tracing::info!("Successfully wiped data directory: {}", data_dir);
                println!("Successfully wiped: {}", data_dir);
            } else {
                tracing::warn!("Directory does not exist: {}", data_dir);
                println!("Directory does not exist: {}", data_dir);
            }
        }
    }

    tracing::info!("Application shutting down");
    Ok(())
}