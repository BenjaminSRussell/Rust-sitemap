mod bfs_crawler;
mod cli;
mod config;
mod frontier;
mod network;
mod node_map;
mod rkyv_queue;
mod sitemap_seeder;
mod sitemap_writer;
mod url_lock_manager;

use bfs_crawler::{BfsCrawlerConfig, BfsCrawlerState};
use cli::{Cli, Commands};
use config::Config;
use sitemap_writer::{SitemapUrl, SitemapWriter};

fn normalize_url(url: &str) -> String {
    let trimmed = url.trim();

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }

    if trimmed.contains('.') && !trimmed.contains('/') {
        return format!("https://{}", trimmed);
    }

    format!("https://{}", trimmed)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse_args();

    match cli.command {
        Commands::Crawl {
            start_url,
            data_dir,
            workers,
            export_jsonl,
            max_depth: _, // ignore depth
            user_agent,
            timeout,
            ignore_robots,
        } => {
            let normalized_start_url = normalize_url(&start_url);
            println!("Crawling {} ({} concurrent requests, {}s timeout)", normalized_start_url, workers, timeout);

            let config = BfsCrawlerConfig {
                max_workers: workers as u32,
                timeout: timeout as u32,
                user_agent,
                ignore_robots,
                save_interval: Config::SAVE_INTERVAL_SECS,
                redis_url: None,
                lock_ttl: Config::LOCK_TTL_SECS,
                enable_redis: false,
            };

            let mut crawler =
                BfsCrawlerState::new(normalized_start_url.clone(), &data_dir, config).await?;

            crawler.initialize().await?;

            let c = crawler.clone();
            let dir = data_dir.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    println!("Interrupted, saving state");
                    let _ = c.save_state().await;
                    let path = std::path::Path::new(&dir).join("sitemap.jsonl");
                    if c.export_to_jsonl(&path).await.is_ok() {
                        println!("Saved to: {}", path.display());
                    }
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
            println!("Exporting sitemap to {}...", output);

            let config = BfsCrawlerConfig::default();
            let crawler = BfsCrawlerState::new("https://temp.local".to_string(), &data_dir, config).await?;

            // Get all nodes from the crawler
            let nodes = crawler.get_all_nodes().await?;

            // Create sitemap writer
            let mut writer = SitemapWriter::new(&output)?;

            for node in nodes {
                // Only include successfully crawled URLs (status 200)
                if node.status_code == Some(200) {
                    let lastmod = if include_lastmod {
                        node.crawled_at.map(|ts| {
                            // Convert Unix timestamp to ISO 8601 date
                            let dt = chrono::DateTime::from_timestamp(ts as i64, 0)
                                .unwrap_or_default();
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

                    // Calculate priority based on depth (shallower = higher priority)
                    let priority = match node.depth {
                        0 => Some(1.0),
                        1 => Some(0.8),
                        2 => Some(0.6),
                        _ => Some(default_priority),
                    };

                    writer.add_url(SitemapUrl {
                        loc: node.url.clone(),
                        lastmod,
                        changefreq,
                        priority,
                    })?;
                }
            }

            let count = writer.finish()?;
            println!("Exported {} URLs to {}", count, output);
        }
    }

    Ok(())
}
