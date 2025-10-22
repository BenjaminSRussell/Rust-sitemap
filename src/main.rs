mod bfs_crawler;
mod cli;
mod export;
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

            println!("\n🕷️ BFS CRAWLER");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!("  🎯 Target:        {}", normalized_start_url);
            println!("  💾 Data Dir:      {}", data_dir);
            println!(
                "  ⚡ Workers:       {} (non-blocking, spawn background tasks)",
                workers
            );
            println!("  🔥 Rate Limit:    {} req/s", rate_limit);
            println!("  🌐 User Agent:    {}", user_agent);
            println!(
                "  ⏱️  Timeout:       {}s (slow pages load in background)",
                timeout
            );
            println!(
                "  🤖 Robots.txt:    {}",
                if ignore_robots {
                    "Ignored"
                } else {
                    "Respected"
                }
            );
            println!("  🚀 Architecture:  Workers grab URLs → Spawn async tasks → Move to next");
            println!("  ⚡ Speed Secret:  Fast URLs feed workers → Exponential throughput");
            println!("  📊 Channel Buf:   500K+ URLs (keeps workers fed)");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

            // Create BFS crawler configuration
            let config = BfsCrawlerConfig {
                max_workers: workers as u32,
                rate_limit: rate_limit as u32,
                timeout: timeout as u32,
                user_agent,
                ignore_robots,
                max_memory_bytes: 10 * 1024 * 1024 * 1024, // 10GB for high concurrency
                auto_save_interval: 300,                   // 5 minutes
                redis_url: None,                           // No Redis by default
                lock_ttl: 60,                              // 60 seconds default
                enable_redis_locking: false,               // Disabled by default
            };

            // Initialize BFS crawler
            let mut crawler =
                BfsCrawlerState::new(normalized_start_url.clone(), &data_dir, config).await?;

            // Initialize crawler (loads existing state if available)
            crawler.initialize().await?;

            // Setup Ctrl+C handler to save data on interrupt
            let crawler_clone = crawler.clone();
            let data_dir_clone = data_dir.clone();
            tokio::spawn(async move {
                if let Ok(()) = tokio::signal::ctrl_c().await {
                    println!("\n\n⚠️  Ctrl+C detected! Saving data before exit...");

                    // Save state
                    if let Err(e) = crawler_clone.save_state().await {
                        eprintln!("❌ Error saving state: {}", e);
                    } else {
                        println!("✅ State saved successfully");
                    }

                    // Always export on Ctrl+C
                    let output_path = std::path::Path::new(&data_dir_clone).join("sitemap.jsonl");
                    if let Err(e) = crawler_clone.export_to_jsonl(&output_path).await {
                        eprintln!("❌ Error exporting: {}", e);
                    } else {
                        println!("✅ Exported sitemap to: {}", output_path.display());
                    }

                    println!("👋 Graceful shutdown complete. Exiting...");
                    std::process::exit(0);
                }
            });

            // Start crawling
            let result = crawler.start_crawling().await?;

            // Save final state
            crawler.save_state().await?;

            // Export to JSONL if requested
            if export_jsonl {
                let output_path = std::path::Path::new(&data_dir).join("sitemap.jsonl");
                crawler.export_to_jsonl(&output_path).await?;
                println!("📄 Exported sitemap to: {}", output_path.display());
            }

            println!("\n✅ Crawling completed successfully!");
            println!("   Total URLs discovered: {}", result.total_discovered);
            println!("   Total URLs processed:  {}", result.total_processed);
            println!(
                "   Duration:              {} seconds",
                result.crawl_duration_seconds
            );
            println!("   Data stored in:        {}", data_dir);
        }

        Commands::OrientMap {
            data_dir,
            start_url: _,
            output,
            include_lastmod: _,
            include_changefreq: _,
            default_priority: _,
        } => {
            println!("📝 Exporting sitemap to JSONL...");

            // Use the BFS crawler to export the node map
            let config = BfsCrawlerConfig::default();
            let crawler = BfsCrawlerState::new(
                "https://example.com".to_string(), // Dummy URL, not used for export
                &data_dir,
                config,
            )
            .await?;

            // Export to JSONL
            crawler.export_to_jsonl(&output).await?;

            println!("✅ Successfully exported sitemap to {}", output);
        }
    }

    Ok(())
}
