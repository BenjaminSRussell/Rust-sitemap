mod cli;
mod models;
mod network;
mod export;
mod parser;
mod robots;
mod rkyv_queue;
mod node_map;
mod bfs_crawler;

use cli::{Cli, Commands};
use bfs_crawler::{BfsCrawlerState, BfsCrawlerConfig};

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
            max_depth: _,  // Ignore max_depth - we crawl everything
            user_agent,
            timeout,
            ignore_robots,
        } => {
            // Normalize the start URL for display and processing
            let normalized_start_url = normalize_url(&start_url);
            
            println!("🕷️  Starting BFS Crawler");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!("  Start URL:     {}", normalized_start_url);
            println!("  Data Dir:      {}", data_dir);
            println!("  Workers:       {}", workers);
            println!("  Rate Limit:    {} req/s", rate_limit);
            println!("  Max Depth:     Unlimited (full sitemap)");
            println!("  User Agent:    {}", user_agent);
            println!("  Timeout:       {}s", timeout);
            println!("  Robots.txt:    {}", if ignore_robots { "Ignored" } else { "Respected" });
            println!("  Memory Target: < 2GB RAM");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            
            // Create BFS crawler configuration
            let config = BfsCrawlerConfig {
                max_workers: workers as u32,
                rate_limit: rate_limit as u32,
                timeout: timeout as u32,
                user_agent,
                ignore_robots,
                max_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
                auto_save_interval: 300, // 5 minutes
            };
            
            // Initialize BFS crawler
            let mut crawler = BfsCrawlerState::new(
                normalized_start_url.clone(),
                &data_dir,
                config,
            )?;
            
            // Initialize crawler (loads existing state if available)
            crawler.initialize().await?;
            
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
            println!("   Duration:              {} seconds", result.crawl_duration_seconds);
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
            )?;
            
            // Export to JSONL
            crawler.export_to_jsonl(&output).await?;
            
            println!("✅ Successfully exported sitemap to {}", output);
        }
    }
    
    Ok(())
}
