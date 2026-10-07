use crate::config::Config;
use crate::frontier::FrontierPermit;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{interval, Duration};

type WorkItem = (String, String, u32, Option<String>, FrontierPermit);

const MIN_CAPACITY_FOR_STEALING: usize = 100;
const BATCH_SIZE: usize = 10;

pub struct WorkStealingCoordinator {
    redis_client: Option<redis::Client>,
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
            work_tx,
            backpressure_semaphore,
            global_frontier_size,
        })
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

    async fn steal_work_batch(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if count == 0 {
            return Ok(());
        }

        let work_key = "crawler:work_queue";
        let mut stolen = 0;

        for _ in 0..count {
            let result: Option<String> = redis::cmd("RPOP")
                .arg(work_key)
                .query_async(conn)
                .await
                .map_err(|e| format!("Redis RPOP error: {}", e))?;

            let work_json = match result {
                Some(json) => json,
                None => break,
            };

            let work_data: WorkItemData = match serde_json::from_str(&work_json) {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Work stealing: Failed to deserialize work item: {}", e);
                    continue;
                }
            };

            let owned = match self.backpressure_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break,
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
                break;
            }

            stolen += 1;
        }

        if stolen > 0 {
            eprintln!("Work stealing: Stole {} work items from Redis", stolen);
        }

        Ok(())
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
