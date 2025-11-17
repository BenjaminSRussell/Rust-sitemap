//! Graceful shutdown handler.

use crate::bfs_crawler::BfsCrawler;

// [Zencoder Task Doc]
// WHAT: Sets up a Ctrl+C signal handler that triggers graceful shutdown on first press and immediate exit on second press.
// USED_BY: src/main.rs (Crawl and Resume command handlers)

/// First Ctrl+C saves everything gracefully. Second Ctrl+C exits immediately.
pub fn setup_shutdown_handler(
    crawler: BfsCrawler,
    data_dir: String,
    gov_shutdown: tokio::sync::watch::Sender<bool>,
    shard_shutdown: tokio::sync::watch::Sender<bool>,
) -> tokio::sync::watch::Sender<bool> {
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_handle = shutdown_tx.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\nReceived Ctrl+C, initiating graceful shutdown...");
            println!("Press Ctrl+C again to force quit");

            let _ = shutdown_tx.send(true);
            let _ = gov_shutdown.send(true);
            let _ = shard_shutdown.send(true);

            crawler.stop().await;

            // Second Ctrl+C skips the grace period.
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    eprintln!("\nForce quit requested, exiting immediately...");
                    std::process::exit(1);
                }
            });

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

    shutdown_handle
}
