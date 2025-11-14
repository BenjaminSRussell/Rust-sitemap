//! Redis lock manager and work stealing setup.

use crate::bfs_crawler::BfsCrawlerConfig;
use crate::frontier::FrontierPermit;
use crate::url_lock_manager::UrlLockManager;
use crate::work_stealing::WorkStealingCoordinator;
use std::sync::Arc;

/// Sets up Redis lock manager and work stealing. Returns None if Redis is disabled.
#[tracing::instrument(skip(config, work_tx, backpressure, frontier_size, shard_shutdown_tx))]
pub async fn setup_distributed_coordination(
    config: &BfsCrawlerConfig,
    instance_id: u64,
    work_tx: tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, FrontierPermit)>,
    backpressure: Arc<tokio::sync::Semaphore>,
    frontier_size: Arc<std::sync::atomic::AtomicUsize>,
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

    match WorkStealingCoordinator::new(Some(redis_url), work_tx, backpressure, frontier_size) {
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
