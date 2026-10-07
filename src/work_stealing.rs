use crate::config::Config;
use crate::frontier::FrontierPermit;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, interval};

type WorkItem = (String, String, u32, Option<String>, FrontierPermit);

const MIN_CAPACITY_FOR_STEALING: usize = 100;
const BATCH_SIZE: usize = 10;
/// Shared Redis list other instances push surplus work onto.
pub const DEFAULT_WORK_QUEUE_KEY: &str = "crawler:work_queue";

pub struct WorkStealingCoordinator {
    redis_client: Option<redis::Client>,
    queue_key: String,
    work_tx: UnboundedSender<WorkItem>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    global_frontier_size: Arc<AtomicUsize>,
}

impl WorkStealingCoordinator {
    pub fn new(
        redis_url: Option<&str>,
        work_tx: UnboundedSender<WorkItem>,
        backpressure_semaphore: Arc<tokio::sync::Semaphore>,
        global_frontier_size: Arc<AtomicUsize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let redis_client = if let Some(url) = redis_url {
            Some(redis::Client::open(url)?)
        } else {
            None
        };

        Ok(Self {
            redis_client,
            queue_key: DEFAULT_WORK_QUEUE_KEY.to_string(),
            work_tx,
            backpressure_semaphore,
            global_frontier_size,
        })
    }

    /// Use a different Redis list (tests and multi-tenant deployments).
    #[allow(dead_code)]
    pub fn with_queue_key(mut self, key: impl Into<String>) -> Self {
        self.queue_key = key.into();
        self
    }

    pub async fn start(self: Arc<Self>, shutdown: tokio::sync::watch::Receiver<bool>) {
        if self.redis_client.is_none() {
            eprintln!("Work stealing disabled: Redis not configured");
            return;
        }

        let client = match self.redis_client.as_ref() {
            Some(c) => c,
            None => return,
        };

        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Work stealing: Failed to connect to Redis: {}", e);
                return;
            }
        };

        let mut check_interval = interval(Duration::from_millis(
            Config::WORK_STEALING_CHECK_INTERVAL_MS,
        ));

        loop {
            if *shutdown.borrow() {
                eprintln!("Work stealing coordinator: Shutdown signal received");
                break;
            }

            check_interval.tick().await;

            let available_permits = self.backpressure_semaphore.available_permits();
            if available_permits < MIN_CAPACITY_FOR_STEALING {
                continue;
            }

            let batch_size = std::cmp::min(available_permits / 2, BATCH_SIZE);
            if let Err(e) = self.steal_work_batch(&mut conn, batch_size).await {
                eprintln!("Work stealing error: {}", e);
            }
        }
    }

    /// Pull up to `count` items from the shared queue into the local work
    /// channel. Returns how many were handed to local workers.
    ///
    /// A backpressure permit is reserved *before* each RPOP so an item is never
    /// popped and then dropped for lack of capacity (that silently lost work
    /// before #40).
    pub(crate) async fn steal_work_batch(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        count: usize,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        if count == 0 {
            return Ok(0);
        }

        let mut stolen = 0;

        for _ in 0..count {
            let owned = match self.backpressure_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break,
            };

            let result: Option<String> = redis::cmd("RPOP")
                .arg(&self.queue_key)
                .query_async(conn)
                .await
                .map_err(|e| format!("Redis RPOP error: {}", e))?;

            let work_json = match result {
                Some(json) => json,
                None => break, // permit released on drop
            };

            let work_data: WorkItemData = match serde_json::from_str(&work_json) {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Work stealing: Failed to deserialize work item: {}", e);
                    continue;
                }
            };

            let permit = FrontierPermit::new(owned, Arc::clone(&self.global_frontier_size));
            let work_item = (
                work_data.host,
                work_data.url,
                work_data.depth,
                work_data.parent_url,
                permit,
            );

            if self.work_tx.send(work_item).is_err() {
                return Err("work channel closed; local crawler is gone".into());
            }

            stolen += 1;
        }

        if stolen > 0 {
            eprintln!("Work stealing: Stole {} work items from Redis", stolen);
        }

        Ok(stolen)
    }
}

/// Serializable representation of a work item (without the permit).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WorkItemData {
    host: String,
    url: String,
    depth: u32,
    parent_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(url: &str) -> String {
        serde_json::to_string(&WorkItemData {
            host: "example.com".into(),
            url: url.into(),
            depth: 2,
            parent_url: Some("https://example.com/".into()),
        })
        .unwrap()
    }

    async fn seed(conn: &mut redis::aio::MultiplexedConnection, key: &str, items: &[String]) {
        for i in items {
            let _: i64 = redis::cmd("LPUSH")
                .arg(key)
                .arg(i)
                .query_async(conn)
                .await
                .unwrap();
        }
    }

    async fn queue_len(conn: &mut redis::aio::MultiplexedConnection, key: &str) -> i64 {
        redis::cmd("LLEN").arg(key).query_async(conn).await.unwrap()
    }

    #[tokio::test]
    async fn steals_items_and_skips_garbage() {
        let Some(url) = crate::redis_test_support::redis_url_or_skip("steal") else {
            return;
        };
        let key = crate::redis_test_support::unique_prefix("steal-q");
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        seed(
            &mut conn,
            &key,
            &[
                item("https://example.com/a"),
                "{not json".into(),
                item("https://example.com/b"),
            ],
        )
        .await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let coord = WorkStealingCoordinator::new(
            Some(&url),
            tx,
            Arc::new(tokio::sync::Semaphore::new(1000)),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap()
        .with_queue_key(&key);

        let stolen = coord.steal_work_batch(&mut conn, 10).await.unwrap();
        assert_eq!(stolen, 2);
        let mut got = Vec::new();
        while let Ok((host, u, depth, parent, _permit)) = rx.try_recv() {
            assert_eq!(host, "example.com");
            assert_eq!(depth, 2);
            assert_eq!(parent.as_deref(), Some("https://example.com/"));
            got.push(u);
        }
        assert_eq!(got, vec!["https://example.com/a", "https://example.com/b"]);
        assert_eq!(queue_len(&mut conn, &key).await, 0);
    }

    /// With capacity for one item, exactly one is taken and the rest stay in
    /// Redis for another instance instead of being popped and lost.
    #[tokio::test]
    async fn stealing_respects_backpressure_without_losing_work() {
        let Some(url) = crate::redis_test_support::redis_url_or_skip("steal_bp") else {
            return;
        };
        let key = crate::redis_test_support::unique_prefix("steal-bp");
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        seed(
            &mut conn,
            &key,
            &[
                item("https://example.com/1"),
                item("https://example.com/2"),
                item("https://example.com/3"),
            ],
        )
        .await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let coord = WorkStealingCoordinator::new(
            Some(&url),
            tx,
            Arc::new(tokio::sync::Semaphore::new(1)),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap()
        .with_queue_key(&key);

        assert_eq!(coord.steal_work_batch(&mut conn, 3).await.unwrap(), 1);
        assert!(rx.try_recv().is_ok());
        assert_eq!(
            queue_len(&mut conn, &key).await,
            2,
            "unclaimed work stays queued"
        );
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn closed_work_channel_is_an_error() {
        let Some(url) = crate::redis_test_support::redis_url_or_skip("steal_closed") else {
            return;
        };
        let key = crate::redis_test_support::unique_prefix("steal-closed");
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        seed(&mut conn, &key, &[item("https://example.com/x")]).await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let coord = WorkStealingCoordinator::new(
            Some(&url),
            tx,
            Arc::new(tokio::sync::Semaphore::new(10)),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap()
        .with_queue_key(&key);
        assert!(coord.steal_work_batch(&mut conn, 1).await.is_err());
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    #[test]
    fn invalid_redis_url_is_rejected() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        assert!(
            WorkStealingCoordinator::new(
                Some("definitely not a url"),
                tx,
                Arc::new(tokio::sync::Semaphore::new(1)),
                Arc::new(AtomicUsize::new(0)),
            )
            .is_err()
        );
    }
}
