mod bfs_crawler;
mod cli;
mod models;
mod network;
mod node_map;
mod parser;
mod rkyv_queue;
mod robots;
mod url_lock_manager;

use bfs_crawler::{BfsCrawlerConfig, BfsCrawlerState};
use cli::{Cli, Commands};

/// Normalize a URL by adding https:// protocol if missing
fn normalize_url(url: &str) -> String {
    let trimmed = url.trim();

    // If it already has a scheme, return as-is
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }

    // If it looks like a domain (contains dots but no slashes), add https://
    if trimmed.contains('.') && !trimmed.contains('/') {
        return format!("https://{}", trimmed);
    }

    // For other cases, assume it needs https://
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
            rate_limit,
            export_jsonl,
            max_depth: _, // Ignore max_depth - we crawl everything
            user_agent,
            timeout,
            ignore_robots,
        } => {
            // Normalize the start URL for display and processing
            let normalized_start_url = normalize_url(&start_url);

            println!("\nCRAWLER CONFIG");
            println!("  Target: {}", normalized_start_url);
            println!("  Data: {}", data_dir);
            println!("  Workers: {}", workers);
            println!("  Rate: {} req/s", rate_limit);
            println!("  Timeout: {}s", timeout);
            println!("  Robots: {}", if ignore_robots { "ignored" } else { "respected" });
            println!();

            let config = BfsCrawlerConfig {
                max_workers: workers as u32,
                rate_limit: rate_limit as u32,
                timeout: timeout as u32,
                user_agent,
                ignore_robots,
                max_memory: 10 * 1024 * 1024 * 1024,
                save_interval: 300,
                redis_url: None,
                lock_ttl: 60,
                enable_redis: false,
            };

            let mut crawler = BfsCrawlerState::new(
                normalized_start_url.clone(),
                &data_dir,
                config
            ).await?;

            crawler.initialize().await?;

            let c = crawler.clone();
            let dir = data_dir.clone();
            tokio::spawn(async move {
                if let Ok(()) = tokio::signal::ctrl_c().await {
                    println!("\n\nInterrupted! Saving...");

                    if let Err(e) = c.save_state().await {
                        eprintln!("Save error: {}", e);
                    }

                    let path = std::path::Path::new(&dir).join("sitemap.jsonl");
                    if let Err(e) = c.export_to_jsonl(&path).await {
                        eprintln!("Export error: {}", e);
                    } else {
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

            println!("\nComplete!");
            println!("  Discovered: {}", result.discovered);
            println!("  Processed: {}", result.processed);
            println!("  Duration: {}s", result.duration_secs);
            println!("  Data: {}", data_dir);
        }

        Commands::OrientMap {
            data_dir,
            start_url: _,
            output,
            include_lastmod: _,
            include_changefreq: _,
            default_priority: _,
        } => {
            println!("Exporting...");

            let config = BfsCrawlerConfig::default();
            let crawler = BfsCrawlerState::new(
                "https://site.local".to_string(),
                &data_dir,
                config,
            ).await?;

            crawler.export_to_jsonl(&output).await?;
            println!("Exported to {}", output);
        }
    }

    Ok(())
}
