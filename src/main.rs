mod bfs_crawler;
mod cli;
mod common_crawl_seeder;
mod completion_detector;
mod config;
mod ct_log_seeder;
mod frontier;
mod logging;
mod metadata;
mod metrics;
mod network;
mod orchestration;
mod privacy_metadata;
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

use bfs_crawler::BfsCrawler;
use cli::{Cli, Commands};
use frontier::FrontierShard;
use orchestration::{apply_preset, build_crawler, build_crawler_config, run_export_sitemap_command, setup_shutdown_handler};
use state::CrawlerState;
use thiserror::Error;
use url_utils::normalize_url_for_cli;

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


/// Shuts down workers, exports results, and prints stats.
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

/// Spawns shard workers that process URLs with politeness delays.
fn spawn_shard_workers(
    frontier_shards: Vec<FrontierShard>,
    start_url_domain: String,
) {
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), MainError> {
    let cli = Cli::parse_args();

    // Set up logging early - writes to <data_dir>/logs/
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
                apply_preset(preset_name, &mut workers, &mut timeout, &mut ignore_robots, &mut max_urls);
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

            // Load state to recover start URL from last crawl.
            let state =
                CrawlerState::new(&data_dir).map_err(|e| MainError::State(e.to_string()))?;

            // Placeholder URL for construction - saved frontier drives the actual work.
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
                300, // Save interval: 5 minutes
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