use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{interval, Duration};

type WorkItem = (
    String,
    String,
    u32,
    Option<String>,
    tokio::sync::OwnedSemaphorePermit,
);

/// Coordinates work stealing between crawler instances via Redis pub/sub.
pub struct WorkStealingCoordinator {
    redis_client: Option<redis::Client>,
    work_tx: UnboundedSender<WorkItem>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
}

impl WorkStealingCoordinator {
    pub fn new(
        redis_url: Option<&str>,
        work_tx: UnboundedSender<WorkItem>,
        backpressure_semaphore: Arc<tokio::sync::Semaphore>,
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
        })
    }

    /// Starts the work stealing loop and periodically injects Redis work locally.
    pub async fn start(self: Arc<Self>) {
        if self.redis_client.is_none() {
            eprintln!("Work stealing disabled: Redis not configured");
            return;
        }

        let mut check_interval = interval(Duration::from_millis(500));

        loop {
            check_interval.tick().await;

            // Check if we have capacity to accept more work
            let available_permits = self.backpressure_semaphore.available_permits();
            if available_permits < 100 {
                // Not enough capacity, skip this cycle
                continue;
            }

            // Try to steal work from Redis
            if let Err(e) = self.try_steal_work().await {
                eprintln!("Work stealing error: {}", e);
            }
        }
    }

    /// Attempt to steal a work item from Redis and inject it into the local crawler.
    async fn try_steal_work(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.redis_client.as_ref().ok_or("Redis not configured")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("Redis connection error: {}", e))?;

        // Try to pop a work item from the shared work queue
        let work_key = "crawler:work_queue";
        let result: Option<String> = redis::cmd("RPOP")
            .arg(work_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("Redis RPOP error: {}", e))?;

        if let Some(work_json) = result {
            // Deserialize the work item
            let work_data: WorkItemData = serde_json::from_str(&work_json)?;

            // Acquire a permit for backpressure
            let permit = self
                .backpressure_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Failed to acquire permit: {}", e))?;

            // Clone the URL for logging before moving work_data
            let url_for_log = work_data.url.clone();

            // Inject the work item into the local crawler
            let work_item = (
                work_data.host,
                work_data.url,
                work_data.depth,
                work_data.parent_url,
                permit,
            );

            self.work_tx
                .send(work_item)
                .map_err(|e| format!("Failed to send work item: {}", e))?;

            eprintln!(
                "Work stealing: Successfully stole work from Redis (url: {})",
                url_for_log
            );
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
