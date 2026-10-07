//! Redis lock manager and work stealing setup.

use crate::bfs_crawler::BfsCrawlerConfig;
use crate::frontier::FrontierPermit;
use crate::url_lock_manager::UrlLockManager;
use crate::work_stealing::WorkStealingCoordinator;
use std::sync::Arc;

/// Sets up Redis lock manager and work stealing. Returns `Ok(None)` if Redis is
/// disabled. When `--enable-redis` is set but Redis is missing or unreachable
/// this is an error: silently falling back to single-node mode let several
/// instances double-crawl the same URLs (#40).
#[tracing::instrument(skip(config, work_tx, backpressure, frontier_size, shard_shutdown_tx))]
pub async fn setup_distributed_coordination(
    config: &BfsCrawlerConfig,
    instance_id: u64,
    work_tx: tokio::sync::mpsc::UnboundedSender<(
        String,
        String,
        u32,
        Option<String>,
        FrontierPermit,
    )>,
    backpressure: Arc<tokio::sync::Semaphore>,
    frontier_size: Arc<std::sync::atomic::AtomicUsize>,
    shard_shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> Result<Option<Arc<tokio::sync::Mutex<UrlLockManager>>>, DistributedSetupError> {
    if !config.enable_redis {
        return Ok(None);
    }

    let redis_url = match &config.redis_url {
        Some(url) => url,
        None => return Err(DistributedSetupError::MissingUrl),
    };

    let lock_manager = {
        let lock_instance_id = format!("crawler-{}", instance_id);
        match UrlLockManager::new(redis_url, Some(config.lock_ttl), lock_instance_id).await {
            Ok(mgr) => {
                eprintln!(
                    "Redis locks enabled with instance ID: crawler-{}",
                    instance_id
                );
                Some(Arc::new(tokio::sync::Mutex::new(mgr)))
            }
            Err(e) => {
                return Err(DistributedSetupError::Lock {
                    url: redis_url.clone(),
                    source: e,
                });
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
        Err(e) => return Err(DistributedSetupError::WorkStealing(e.to_string())),
    }

    Ok(lock_manager)
}

/// Why distributed mode could not start.
#[derive(Debug, thiserror::Error)]
pub enum DistributedSetupError {
    #[error("--enable-redis was set but no Redis URL was provided")]
    MissingUrl,
    #[error("could not connect to Redis at {url} for URL locks: {source}")]
    Lock {
        url: String,
        #[source]
        source: redis::RedisError,
    },
    #[error("work stealing setup failed: {0}")]
    WorkStealing(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channels() -> (
        tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, FrontierPermit)>,
        Arc<tokio::sync::Semaphore>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (shutdown, _) = tokio::sync::watch::channel(false);
        (
            tx,
            Arc::new(tokio::sync::Semaphore::new(10)),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            shutdown,
        )
    }

    #[tokio::test]
    async fn disabled_redis_is_none() {
        let (tx, bp, size, sd) = channels();
        let cfg = BfsCrawlerConfig::default();
        assert!(
            setup_distributed_coordination(&cfg, 1, tx, bp, size, sd)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn enabled_without_url_is_an_error() {
        let (tx, bp, size, sd) = channels();
        let cfg = BfsCrawlerConfig {
            enable_redis: true,
            redis_url: None,
            ..Default::default()
        };
        let Err(err) = setup_distributed_coordination(&cfg, 1, tx, bp, size, sd).await else {
            panic!("expected an error");
        };
        assert!(matches!(err, DistributedSetupError::MissingUrl));
    }

    /// Previously this only printed "Redis lock setup failed" and carried on
    /// single-node; now the crawl refuses to start.
    #[tokio::test]
    async fn unreachable_redis_is_an_error() {
        let (tx, bp, size, sd) = channels();
        let cfg = BfsCrawlerConfig {
            enable_redis: true,
            redis_url: Some("redis://127.0.0.1:1".into()),
            ..Default::default()
        };
        let Err(err) = setup_distributed_coordination(&cfg, 1, tx, bp, size, sd).await else {
            panic!("expected an error");
        };
        assert!(matches!(err, DistributedSetupError::Lock { .. }), "{err}");
        assert!(err.to_string().contains("127.0.0.1:1"));
    }

    #[tokio::test]
    async fn reachable_redis_returns_lock_manager() {
        let Some(url) = crate::redis_test_support::redis_url_or_skip("distributed_setup") else {
            return;
        };
        let (tx, bp, size, sd) = channels();
        let cfg = BfsCrawlerConfig {
            enable_redis: true,
            redis_url: Some(url),
            ..Default::default()
        };
        let mgr = setup_distributed_coordination(&cfg, 7, tx, bp, size, sd.clone())
            .await
            .unwrap();
        assert!(mgr.is_some());
        let _ = sd.send(true);
    }
}
