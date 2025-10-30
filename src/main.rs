mod bfs_crawler;
mod cli;
mod common_crawl_seeder;
mod config;
mod ct_log_seeder;
mod frontier;
mod network;
mod robots;
mod seeder;
mod sitemap_seeder;
mod sitemap_writer;
mod state;
mod url_lock_manager;
mod url_utils;

use bfs_crawler::{BfsCrawler, BfsCrawlerConfig, StateWriteMessage};
use cli::{Cli, Commands};
use config::Config;
use frontier::Frontier;
use network::HttpClient;
use sitemap_writer::{SitemapUrl, SitemapWriter};
use state::{CrawlerState, QueuedUrl};
use url_lock_manager::UrlLockManager;
use url_utils::normalize_url_for_cli;
use thiserror::Error;
use std::sync::Arc;
use tokio::sync::mpsc;

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
) -> BfsCrawlerConfig {
    BfsCrawlerConfig {
        max_workers: workers as u32,
        timeout: timeout as u32,
        user_agent,
        ignore_robots,
        save_interval: Config::SAVE_INTERVAL_SECS,
        redis_url: None,
        lock_ttl: Config::LOCK_TTL_SECS,
        enable_redis: false,
    }
}

/// Build crawler dependencies and wire concrete components together
async fn build_crawler<P: AsRef<std::path::Path>>(
    start_url: String,
    data_dir: P,
    config: BfsCrawlerConfig,
) -> Result<BfsCrawler, Box<dyn std::error::Error>> {
    // Build HTTP client
    let http = Arc::new(HttpClient::new(
        config.user_agent.clone(),
        config.timeout as u64,
    ));

    // Build state
    let state = Arc::new(CrawlerState::new(&data_dir)?);

    // Create MPSC channel for state writes
    let (state_writer_tx, mut state_writer_rx) = mpsc::unbounded_channel::<StateWriteMessage>();

    // Spawn dedicated writer task
    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(msg) = state_writer_rx.recv().await {
            match msg {
                StateWriteMessage::EnqueueUrl { url, depth, parent_url } => {
                    let queued = QueuedUrl::new(url.clone(), depth, parent_url);
                    if let Err(e) = state_clone.enqueue_url(&queued) {
                        eprintln!("Failed to enqueue URL {}: {}", url, e);
                    }
                }
                StateWriteMessage::UpdateNode {
                    url_normalized,
                    status_code,
                    content_type,
                    content_length,
                    title,
                    link_count,
                    response_time_ms,
                } => {
                    if let Err(e) = state_clone.update_node_crawl_data(
                        &url_normalized,
                        status_code,
                        content_type,
                        content_length,
                        title,
                        link_count,
                        response_time_ms,
                    ) {
                        eprintln!("Failed to update node {}: {}", url_normalized, e);
                    }
                }
                StateWriteMessage::AddNode(node) => {
                    if let Err(e) = state_clone.add_node(&node) {
                        eprintln!("Failed to add node {}: {}", node.url_normalized, e);
                    }
                }
                StateWriteMessage::UpdateHostState {
                    host,
                    robots_txt,
                    crawl_delay_secs,
                    success,
                } => {
                    // Get or create host state
                    let mut host_state = state_clone
                        .get_host_state(&host)
                        .unwrap_or_else(|_| None)
                        .unwrap_or_else(|| state::HostState::new(host.clone()));

                    // Update fields
                    if let Some(txt) = robots_txt {
                        host_state.robots_txt = Some(txt);
                    }
                    if let Some(delay) = crawl_delay_secs {
                        host_state.crawl_delay_secs = delay;
                    }
                    if let Some(true) = success {
                        host_state.reset_failures();
                    } else if let Some(false) = success {
                        host_state.record_failure();
                    }

                    if let Err(e) = state_clone.update_host_state(&host_state) {
                        eprintln!("Failed to update host state for {}: {}", host, e);
                    }
                }
            }
        }
    });

    // Build frontier
    let frontier = Arc::new(Frontier::new(
        Arc::clone(&state),
        state_writer_tx.clone(),
        config.user_agent.clone(),
        config.ignore_robots,
    ));

    // Build lock manager (optional)
    let lock_manager = if config.enable_redis {
        if let Some(url) = &config.redis_url {
            match UrlLockManager::new(url, Some(config.lock_ttl)).await {
                Ok(mgr) => Some(Arc::new(tokio::sync::Mutex::new(mgr))),
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

    // Wire everything together via constructor injection
    Ok(BfsCrawler::new(
        config,
        start_url,
        http,
        state,
        frontier,
        state_writer_tx,
        lock_manager,
    ))
}

async fn setup_signal_handler(
    crawler: BfsCrawler,
    data_dir: String,
    export_jsonl: bool,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\nReceived Ctrl+C, initiating graceful shutdown...");

            // Signal all tasks to stop
            let _ = shutdown_tx.send(true);

            // Stop the crawler
            crawler.stop().await;

            // Give the writer task time to drain the MPSC channel
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            println!("Saving state...");
            if let Err(e) = crawler.save_state().await {
                eprintln!("Failed to save state: {}", e);
            }

            if export_jsonl {
                let path = std::path::Path::new(&data_dir).join("sitemap.jsonl");
                if let Err(e) = crawler.export_to_jsonl(&path).await {
                    eprintln!("Failed to export JSONL: {}", e);
                } else {
                    println!("Saved to: {}", path.display());
                }
            }

            println!("Graceful shutdown complete");
            std::process::exit(0);
        }
    })
}

async fn run_crawl_command(
    start_url: String,
    data_dir: String,
    workers: usize,
    export_jsonl: bool,
    _max_depth: u32,
    user_agent: String,
    timeout: u64,
    ignore_robots: bool,
    seeding_strategy: String,
) -> Result<(), MainError> {
    let normalized_start_url = normalize_url_for_cli(&start_url);
    println!(
        "Crawling {} ({} concurrent requests, {}s timeout)",
        normalized_start_url, workers, timeout
    );

    let config = build_crawler_config(workers, timeout, user_agent, ignore_robots);

    let mut crawler = build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

    crawler.initialize(&seeding_strategy).await?;

    // Create shutdown channel
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);

    let _signal_handler = setup_signal_handler(crawler.clone(), data_dir.clone(), export_jsonl, shutdown_tx).await;

    let result = crawler.start_crawling().await?;
    crawler.save_state().await?;

    if export_jsonl {
        let path = std::path::Path::new(&data_dir).join("sitemap.jsonl");
        crawler.export_to_jsonl(&path).await?;
        println!("Exported to: {}", path.display());
    }

    println!(
        "Discovered {}, processed {}, {}s, data: {}",
        result.discovered, result.processed, result.duration_secs, data_dir
    );

    Ok(())
}

async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
) -> Result<(), MainError> {
    println!("Exporting sitemap to {}...", output);

    // For export, we only need the state - no need for a full crawler
    let state = CrawlerState::new(&data_dir)
        .map_err(|e| MainError::State(e.to_string()))?;

    let mut writer = SitemapWriter::new(&output)
        .map_err(|e| MainError::Export(e.to_string()))?;

    // Use streaming iterator to avoid loading all nodes into memory
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

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let cli = Cli::parse_args();

    match cli.command {
        Commands::Crawl {
            start_url,
            data_dir,
            workers,
            export_jsonl,
            max_depth: _, // Depth limit not implemented yet
            user_agent,
            timeout,
            ignore_robots,
            seeding_strategy,
        } => {
            let normalized_start_url = normalize_url_for_cli(&start_url);
            println!("Crawling {} ({} concurrent requests, {}s timeout)", normalized_start_url, workers, timeout);

            let config = build_crawler_config(workers, timeout, user_agent, ignore_robots);

            let mut crawler = build_crawler(normalized_start_url.clone(), &data_dir, config).await?;

            crawler.initialize(&seeding_strategy).await?;

            // Create shutdown channel for graceful shutdown
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let c = crawler.clone();
            let dir = data_dir.clone();

            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("\nReceived Ctrl+C, initiating graceful shutdown...");

                    // Signal shutdown
                    let _ = shutdown_tx.send(true);

                    // Stop the crawler
                    c.stop().await;

                    // Give the writer task time to drain
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

            let result = crawler.start_crawling().await?;
            crawler.save_state().await?;

            if export_jsonl {
                let path = std::path::Path::new(&data_dir).join("sitemap.jsonl");
                crawler.export_to_jsonl(&path).await?;
                println!("Exported to: {}", path.display());
            }

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
