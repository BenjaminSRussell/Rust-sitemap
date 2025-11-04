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
mod writer_thread;

use bfs_crawler::{BfsCrawler, BfsCrawlerConfig};
use cli::{Cli, Commands};
use config::Config;
use frontier::Frontier;
use metrics::Metrics;
use network::HttpClient;
use sitemap_writer::{SitemapUrl, SitemapWriter};
use state::CrawlerState;
use url_lock_manager::UrlLockManager;
use url_utils::normalize_url_for_cli;
use wal::WalWriter;
use writer_thread::WriterThread;
use thiserror::Error;
use std::sync::Arc;
use std::time::Duration;

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
) -> BfsCrawlerConfig {
    BfsCrawlerConfig {
        max_workers: workers as u32,
        timeout: timeout as u32,
        user_agent,
        ignore_robots,
        save_interval: Config::SAVE_INTERVAL_SECS,
        redis_url: if enable_redis { Some(redis_url) } else { None },
        lock_ttl,
        enable_redis,
    }
}

/// Governor task that monitors commit latency and adjusts concurrency.
async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250; // Reevaluate permits every 250ms for fast reaction.
    const THROTTLE_THRESHOLD_MS: f64 = 500.0; // Only throttle if EWMA exceeds 500ms.
    const UNTHROTTLE_THRESHOLD_MS: f64 = 100.0; // Only un-throttle if EWMA drops below 100ms.
    const MIN_PERMITS: usize = 32; // Never drop below this concurrency to avoid total stall.
    const MAX_PERMITS: usize = 512; // Avoid exceeding this limit to protect downstream services.

    eprintln!("Governor: Started monitoring commit latency (250ms intervals)");

    // Track held permits so we can release them later
    let mut held_permits: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();

    loop {
        tokio::time::sleep(Duration::from_millis(ADJUSTMENT_INTERVAL_MS)).await;

        let commit_ewma_ms = metrics.get_commit_ewma_ms();
        let current_permits = permits.available_permits();

        // Hysteresis bands: only throttle above 500ms, only un-throttle below 100ms
        if commit_ewma_ms > THROTTLE_THRESHOLD_MS {
            // Reduce concurrency when commits slow down to relieve pressure on the WAL and state store.
            if current_permits > MIN_PERMITS {
                // Acquire and hold a permit to reduce available concurrency
                if let Ok(permit) = permits.clone().try_acquire_owned() {
                    held_permits.push(permit);
                    metrics.throttle_adjustments.lock().inc();
                    eprintln!(
                        "Governor: Throttling (holding {} permits, {} available, commit_ewma: {:.2}ms)",
                        held_permits.len(),
                        permits.available_permits(),
                        commit_ewma_ms
                    );
                }
            }
        } else if commit_ewma_ms < UNTHROTTLE_THRESHOLD_MS && commit_ewma_ms > 0.0 {
            // Increase concurrency when commits are fast enough to capitalize on unused capacity.
            if let Some(permit) = held_permits.pop() {
                // Release a held permit to increase available concurrency
                drop(permit);
                metrics.throttle_adjustments.lock().inc();
                eprintln!(
                    "Governor: Un-throttling (holding {} permits, {} available, commit_ewma: {:.2}ms)",
                    held_permits.len(),
                    permits.available_permits(),
                    commit_ewma_ms
                );
            } else if current_permits < MAX_PERMITS {
                // If we have no held permits, add new capacity
                permits.add_permits(1);
                metrics.throttle_adjustments.lock().inc();
                eprintln!(
                    "Governor: Adding capacity ({} available, commit_ewma: {:.2}ms)",
                    permits.available_permits(),
                    commit_ewma_ms
                );
            }
        }

        metrics.throttle_permits_held.lock().set(current_permits as f64);
    }
}

/// Build crawler dependencies and wire concrete components together.
async fn build_crawler<P: AsRef<std::path::Path>>(
    start_url: String,
    data_dir: P,
    config: BfsCrawlerConfig,
) -> Result<BfsCrawler, Box<dyn std::error::Error>> {
    // Build the HTTP client so we share configuration across the crawler.
    let http = Arc::new(HttpClient::new(
        config.user_agent.clone(),
        config.timeout as u64,
    ));

    // Build the crawler state so persistence has a backing store.
    let state = Arc::new(CrawlerState::new(&data_dir)?);

    // Create the WAL writer with a 100 ms fsync interval so durability keeps up without thrashing disks.
    let wal_writer = Arc::new(tokio::sync::Mutex::new(
        WalWriter::new(data_dir.as_ref(), 100)?
    ));

    // Create the metrics registry so runtime feedback is available to operators.
    let metrics = Arc::new(Metrics::new());

    // Generate the instance ID from the timestamp so distributed components can differentiate peers.
    let instance_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Spawn the dedicated writer OS thread so blocking WAL work never stalls async tasks.
    let writer_thread = Arc::new(WriterThread::spawn(
        Arc::clone(&state),
        Arc::clone(&wal_writer),
        Arc::clone(&metrics),
        instance_id,
    ));

    // Create the semaphore so the governor can adjust crawler concurrency on the fly.
    let crawler_permits = Arc::new(tokio::sync::Semaphore::new(config.max_workers as usize));

    // Spawn the governor task so permits respond to observed commit latencies.
    let governor_permits = Arc::clone(&crawler_permits);
    let governor_metrics = Arc::clone(&metrics);
    tokio::spawn(async move {
        governor_task(governor_permits, governor_metrics).await;
    });

    // Build the frontier so URL scheduling enforces politeness.
    let frontier = Arc::new(Frontier::new(
        Arc::clone(&state),
        Arc::clone(&writer_thread),
        config.user_agent.clone(),
        config.ignore_robots,
    ));

    // Optionally build the lock manager so multiple crawler instances can coordinate via Redis.
    let lock_manager = if config.enable_redis {
        if let Some(url) = &config.redis_url {
            // Generate the instance ID string so Redis locks are namespaced per crawler.
            let lock_instance_id = format!("crawler-{}", instance_id);
            match UrlLockManager::new(url, Some(config.lock_ttl), lock_instance_id).await {
                Ok(mgr) => {
                    eprintln!("Redis locks enabled with instance ID: crawler-{}", instance_id);
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

    // Wire the dependencies together so the crawler gets a fully-initialized runtime bundle.
    Ok(BfsCrawler::new(
        config,
        start_url,
        http,
        state,
        frontier,
        writer_thread,
        lock_manager,
        metrics,
        crawler_permits, // Share the permit pool so runtime throttling can take effect.
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

    // For export, only load the state so we avoid spinning up the entire crawler pipeline.
    let state = CrawlerState::new(&data_dir)
        .map_err(|e| MainError::State(e.to_string()))?;

    let mut writer = SitemapWriter::new(&output)
        .map_err(|e| MainError::Export(e.to_string()))?;

    // Use a streaming iterator to avoid loading all nodes into memory and risk OOM.
    let node_iter = state.iter_nodes()
        .map_err(|e| MainError::State(e.to_string()))?;

    node_iter.for_each(|node| {
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
    }).map_err(|e| MainError::State(e.to_string()))?;

    let count = writer.finish().map_err(|e| MainError::Export(e.to_string()))?;
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
        } => {
            let normalized_start_url = normalize_url_for_cli(&start_url);

            if enable_redis {
                println!("Crawling {} ({} concurrent requests, {}s timeout, Redis distributed mode)",
                         normalized_start_url, workers, timeout);
                println!("Redis URL: {}, Lock TTL: {}s", redis_url, lock_ttl);
            } else {
                println!("Crawling {} ({} concurrent requests, {}s timeout)",
                         normalized_start_url, workers, timeout);
            }

            let config = build_crawler_config(workers, timeout, user_agent, ignore_robots,
                                              enable_redis, redis_url, lock_ttl);

            let mut crawler = build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

            crawler.initialize(&seeding_strategy).await?;

            // Create a LocalSet for !Send HTML parser tasks, but run it on the multi-threaded runtime.
            let local_set = tokio::task::LocalSet::new();

            // Create the shutdown channel so Ctrl+C can trigger a coordinated shutdown.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();

            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");

                    // Signal shutdown so other tasks get the stop message.
                    let _ = shutdown_tx.send(true);

                    // Stop the crawler so new work stops entering the pipeline.
                    c.stop().await;

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

            // Run the crawler in the LocalSet, leveraging work-stealing across the thread pool.
            let result = local_set.run_until(async move {
                crawler.start_crawling().await
            }).await?;

            // Always export JSONL on completion so users get output even on success.
            let path = std::path::Path::new(&export_data_dir).join("sitemap.jsonl");
            crawler_for_export.export_to_jsonl(&path).await?;
            println!("Exported to: {}", path.display());

            println!("Discovered {}, processed {}, {}s, data: {}", result.discovered, result.processed, result.duration_secs, data_dir);
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
            println!("Resuming crawl from {} ({} concurrent requests, {}s timeout)",
                     data_dir, workers, timeout);

            // Load state to recover the original start_url so resumes start from known context.
            let state = CrawlerState::new(&data_dir)
                .map_err(|e| MainError::State(e.to_string()))?;

            // Use any URL from the database as a placeholder start_url because the frontier will supply real work.
            let mut placeholder_start_url = "https://example.com".to_string();
            if let Ok(mut iter) = state.iter_nodes() {
                if let Some(Ok(node)) = iter.next() {
                    placeholder_start_url = node.url.clone();
                }
            }

            let config = build_crawler_config(workers, timeout, user_agent, ignore_robots,
                                              enable_redis, redis_url, lock_ttl);

            let mut crawler = build_crawler(placeholder_start_url, &data_dir, config).await?;

            // Initialization checks for saved state so we avoid redundant seeding.
            crawler.initialize("none").await?;

            // Create a LocalSet for !Send HTML parser tasks.
            let local_set = tokio::task::LocalSet::new();

            // Create the shutdown channel so resume mode can also react to Ctrl+C.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();

            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");
                    let _ = shutdown_tx.send(true);
                    c.stop().await;
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

            // Drive the crawler in the LocalSet on the multi-threaded runtime.
            let result = local_set.run_until(async move {
                crawler.start_crawling().await
            }).await?;

            // Export JSONL at the end of resume runs so data stays fresh.
            let path = std::path::Path::new(&export_data_dir).join("sitemap.jsonl");
            crawler_for_export.export_to_jsonl(&path).await?;
            println!("Exported to: {}", path.display());

            println!("Discovered {}, processed {}, {}s, data: {}", result.discovered, result.processed, result.duration_secs, data_dir);
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
            ).await?;
        }
    }

    Ok(())
}
