use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;

mod backoff;
mod bfs_crawler;
mod common_crawl_seeder;
mod completion_detector;
mod config;
mod ct_log_seeder;
mod frontier;
mod json_utils;
mod metadata;
mod metrics;
mod network;
pub mod parsing_modules;
pub mod privacy_metadata;
mod robots;
mod seeder;
mod sitemap_seeder;
mod state;
pub mod tech_classifier;
mod url_lock_manager;
mod url_utils;
mod wal;
mod work_stealing;
mod writer_thread;

use bfs_crawler::{BfsCrawler as RustBfsCrawler, BfsCrawlerConfig};
use frontier::{FrontierDispatcher, FrontierShard, ShardedFrontier};
use metrics::Metrics;
use network::HttpClient;
use state::CrawlerState;
use url_lock_manager::UrlLockManager;
use wal::WalWriter;
use work_stealing::WorkStealingCoordinator;
use writer_thread::WriterThread;

/// Python-exposed crawler configuration
#[pyclass]
#[derive(Clone)]
pub struct CrawlerConfig {
    #[pyo3(get, set)]
    pub workers: usize,
    #[pyo3(get, set)]
    pub timeout: u64,
    #[pyo3(get, set)]
    pub user_agent: String,
    #[pyo3(get, set)]
    pub ignore_robots: bool,
    #[pyo3(get, set)]
    pub save_interval: u64,
    #[pyo3(get, set)]
    pub enable_redis: bool,
    #[pyo3(get, set)]
    pub redis_url: String,
    #[pyo3(get, set)]
    pub lock_ttl: u64,
}

#[pymethods]
impl CrawlerConfig {
    #[new]
    #[pyo3(signature = (workers=256, timeout=20, user_agent="Rust-Sitemap-Crawler/1.0".to_string(), ignore_robots=false, save_interval=300, enable_redis=false, redis_url="redis://localhost".to_string(), lock_ttl=60))]
    fn new(
        workers: usize,
        timeout: u64,
        user_agent: String,
        ignore_robots: bool,
        save_interval: u64,
        enable_redis: bool,
        redis_url: String,
        lock_ttl: u64,
    ) -> Self {
        Self {
            workers,
            timeout,
            user_agent,
            ignore_robots,
            save_interval,
            enable_redis,
            redis_url,
            lock_ttl,
        }
    }
}

/// Python-exposed crawl result
#[pyclass]
#[derive(Clone)]
pub struct CrawlResult {
    #[pyo3(get)]
    pub url: String,
    #[pyo3(get)]
    pub status_code: Option<u16>,
    #[pyo3(get)]
    pub title: Option<String>,
    #[pyo3(get)]
    pub content_type: Option<String>,
    #[pyo3(get)]
    pub depth: u32,
}

#[pymethods]
impl CrawlResult {
    fn __repr__(&self) -> String {
        format!(
            "CrawlResult(url='{}', status_code={:?}, depth={})",
            self.url, self.status_code, self.depth
        )
    }
}

/// Spawns shard worker tasks that process URLs from the frontier.
/// Each shard runs an infinite loop handling control messages, incoming URLs,
/// and dispatching work items with proper politeness delays.
fn spawn_shard_workers(
    frontier_shards: Vec<FrontierShard>,
    start_url_domain: String,
) {
    for mut shard in frontier_shards {
        let domain_clone = start_url_domain.clone();
        tokio::spawn(async move {
            // Shard workers run indefinitely to avoid race conditions during shutdown
            // They're cleaned up automatically when the process exits
            loop {
                // Control messages adjust throttling and host bookkeeping.
                // shard.process_control_messages().await;

                // Handle URLs routed to this shard.
                shard.process_incoming_urls(&domain_clone).await;

                // Pull runnable work for this shard; work_tx handles delivery.
                if shard.get_next_url().await.is_none() {
                    // Sleep until the politeness delay expires (max 100ms).
                    if shard.has_queued_urls() {
                        if let Some(next_ready) = shard.next_ready_time() {
                            let now = std::time::Instant::now();
                            if next_ready > now {
                                let sleep_duration = std::cmp::min(
                                    next_ready.duration_since(now),
                                    tokio::time::Duration::from_millis(100),
                                );
                                tokio::time::sleep(sleep_duration).await;
                            } else {
                                // Delay expired; yield so ready work runs promptly.
                                tokio::time::sleep(tokio::time::Duration::from_millis(1))
                                    .await;
                            }
                        } else {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10))
                                .await;
                        }
                    } else {
                        // Nothing pending, so yield briefly before polling again.
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });
    }
}

/// Python-exposed crawler class
///
/// The crawler automatically saves all progress when:
/// - The crawl completes successfully
/// - An error occurs during crawling
/// - The Python object is dropped/garbage collected
/// - Ctrl+C is pressed (handled by the underlying Rust crawler)
#[pyclass]
pub struct Crawler {
    start_url: String,
    config: CrawlerConfig,
    data_dir: String,
}

#[pymethods]
impl Crawler {
    #[new]
    #[pyo3(signature = (start_url, workers=128, timeout=10, user_agent="Rust-Sitemap-Crawler/1.0".to_string(), ignore_robots=false, data_dir="./crawl_data".to_string()))]
    fn new(
        start_url: String,
        workers: usize,
        timeout: u64,
        user_agent: String,
        ignore_robots: bool,
        data_dir: String,
    ) -> PyResult<Self> {
        Ok(Self {
            start_url,
            config: CrawlerConfig {
                workers,
                timeout,
                user_agent,
                ignore_robots,
                save_interval: 300,
                enable_redis: false,
                redis_url: "redis://localhost".to_string(),
                lock_ttl: 60,
            },
            data_dir,
        })
    }

    /// Run the crawler and return results
    fn crawl(&self, py: Python) -> PyResult<Vec<CrawlResult>> {
        // Release GIL while running async Rust code
        py.allow_threads(|| {
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

            runtime.block_on(async {
                let result = self.run_crawl().await
                    .map_err(|e| PyRuntimeError::new_err(format!("Crawl failed: {}", e)))?;

                Ok(result)
            })
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Crawler(start_url='{}', workers={}, timeout={})",
            self.start_url, self.config.workers, self.config.timeout
        )
    }
}

impl Crawler {
    async fn run_crawl(&self) -> Result<Vec<CrawlResult>, Box<dyn std::error::Error>> {
        let config = BfsCrawlerConfig {
            max_workers: u32::try_from(self.config.workers).unwrap_or(u32::MAX),
            timeout: self.config.timeout as u32,
            user_agent: self.config.user_agent.clone(),
            ignore_robots: self.config.ignore_robots,
            save_interval: self.config.save_interval,
            redis_url: if self.config.enable_redis {
                Some(self.config.redis_url.clone())
            } else {
                None
            },
            lock_ttl: self.config.lock_ttl,
            enable_redis: self.config.enable_redis,
            max_urls: None,  // Not exposed in Python API yet
            duration_secs: None,  // Not exposed in Python API yet
        };

        // Build crawler
        let (mut crawler, frontier_shards, _work_tx, governor_shutdown, shard_shutdown) =
            self.build_crawler(&self.start_url, &self.data_dir, config).await?;

        // Initialize with default seeding strategy
        crawler.initialize("none").await?;

        // Launch shard workers
        let start_url_domain = crawler.get_domain(&self.start_url);
        spawn_shard_workers(frontier_shards, start_url_domain);

        // Start crawling with automatic save on ANY exit (success, error, or panic)
        let crawl_result = crawler.start_crawling().await;

        // CRITICAL: Always shutdown gracefully to ensure data is saved
        eprintln!("Shutting down crawler...");
        let _ = governor_shutdown.send(true);
        let _ = shard_shutdown.send(true);

        // Give writer threads time to flush any pending writes
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // CRITICAL: Always call stop() to ensure proper cleanup
        crawler.stop().await;

        // CRITICAL: Always save state regardless of crawl success/failure
        eprintln!("Saving crawler state...");
        if let Err(e) = crawler.save_state().await {
            eprintln!("Warning: Failed to save state: {}", e);
        } else {
            eprintln!("State saved successfully!");
        }

        // CRITICAL: Always export results to JSONL
        let output_path = std::path::Path::new(&self.data_dir).join("sitemap.jsonl");
        eprintln!("Exporting results to {}...", output_path.display());
        if let Err(e) = crawler.export_to_jsonl(&output_path).await {
            eprintln!("Warning: Failed to export JSONL: {}", e);
        } else {
            eprintln!("Results exported successfully!");
        }

        // Now check if the crawl itself succeeded
        let _crawl_result = crawl_result?;

        // Read results from state
        eprintln!("Reading crawl results...");
        let results = self.read_results().await?;
        eprintln!("Found {} results!", results.len());

        Ok(results)
    }

    async fn read_results(&self) -> Result<Vec<CrawlResult>, Box<dyn std::error::Error>> {
        let state = CrawlerState::new(&self.data_dir)?;
        let mut results = Vec::new();

        let node_iter = state.iter_nodes()?;
        node_iter.for_each(|node| {
            results.push(CrawlResult {
                url: node.url.clone(),
                status_code: node.status_code,
                title: node.title.clone(),
                content_type: node.content_type.clone(),
                depth: node.depth,
            });
            Ok(())
        })?;

        Ok(results)
    }

    async fn build_crawler(
        &self,
        start_url: &str,
        data_dir: &str,
        config: BfsCrawlerConfig,
    ) -> Result<
        (
            RustBfsCrawler,
            Vec<FrontierShard>,
            tokio::sync::mpsc::UnboundedSender<(
                String,
                String,
                u32,
                Option<String>,
                frontier::FrontierPermit,
            )>,
            tokio::sync::watch::Sender<bool>,
            tokio::sync::watch::Sender<bool>,
        ),
        Box<dyn std::error::Error>,
    > {
        let http = Arc::new(HttpClient::new(
            config.user_agent.clone(),
            config.timeout as u64,
        )?);

        let state = Arc::new(CrawlerState::new(data_dir)?);
        let wal_writer = WalWriter::new(std::path::Path::new(data_dir), 100)?;

        let metrics = Arc::new(Metrics::new());

        let instance_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_nanos() as u64;

        let wal_reader = wal::WalReader::new(std::path::Path::new(data_dir));
        let max_seqno = wal_reader.replay(|record| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unsafe { rkyv::archived_root::<state::StateEvent>(&record.payload) }
            }));

            let archived = match result {
                Ok(a) => a,
                Err(_) => {
                    return Err(wal::WalError::CorruptRecord(format!(
                        "Deserialization panic at seqno {:?}",
                        record.seqno
                    )));
                }
            };

            let event: state::StateEvent = match rkyv::Deserialize::deserialize(archived, &mut rkyv::Infallible) {
                Ok(ev) => ev,
                Err(e) => {
                    return Err(wal::WalError::CorruptRecord(format!(
                        "Deserialization error at seqno {:?}: {:?}",
                        record.seqno, e
                    )));
                }
            };

            let event_with_seqno = state::StateEventWithSeqno {
                seqno: record.seqno,
                event,
            };
            let _ = state.apply_event_batch(&[event_with_seqno]);

            Ok(())
        })?;

        let writer_thread = Arc::new(WriterThread::spawn(
            Arc::clone(&state),
            wal_writer,
            Arc::clone(&metrics),
            instance_id,
            max_seqno,
        ));

        let crawler_permits = Arc::new(tokio::sync::Semaphore::new(config.max_workers as usize));

        let (governor_shutdown_tx, governor_shutdown_rx) = tokio::sync::watch::channel(false);
        let (shard_shutdown_tx, _shard_shutdown_rx) = tokio::sync::watch::channel(false);

        let governor_permits_clone = Arc::clone(&crawler_permits);
        let governor_metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            governor_task(
                governor_permits_clone,
                governor_metrics_clone,
                governor_shutdown_rx,
            )
            .await;
        });

        let num_shards = num_cpus::get();
        let (
            frontier_dispatcher,
            shard_receivers,
            global_frontier_size,
            backpressure_semaphore,
        ) = FrontierDispatcher::new(num_shards);

        let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();

        // Create shared stats for real-time frontier monitoring
        let shared_stats = frontier::SharedFrontierStats::new();

        let mut frontier_shards = Vec::new();
        let mut host_state_caches = Vec::new();

        for (shard_id, url_receiver) in shard_receivers.into_iter().enumerate() {
            let shard = FrontierShard::new(
                shard_id,
                Arc::clone(&state),
                Arc::clone(&writer_thread),
                Arc::clone(&http),
                config.user_agent.clone(),
                config.ignore_robots,
                url_receiver,
                work_tx.clone(),
                Arc::clone(&global_frontier_size),
                Arc::clone(&backpressure_semaphore),
                shared_stats.clone(),
            );
            host_state_caches.push(shard.get_host_state_cache());
            frontier_shards.push(shard);
        }

        let sharded_frontier = ShardedFrontier::new(frontier_dispatcher, host_state_caches, shared_stats);
        let frontier = Arc::new(sharded_frontier);

        let lock_manager = if config.enable_redis {
            if let Some(url) = &config.redis_url {
                let lock_instance_id = format!("crawler-{}", instance_id);
                match UrlLockManager::new(url, Some(config.lock_ttl), lock_instance_id).await {
                    Ok(mgr) => Some(Arc::new(tokio::sync::Mutex::new(mgr))),
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            None
        };

        if config.enable_redis {
            if let Some(url) = &config.redis_url {
                match WorkStealingCoordinator::new(
                    Some(url),
                    work_tx.clone(),
                    Arc::clone(&backpressure_semaphore),
                    Arc::clone(&global_frontier_size),
                ) {
                    Ok(coordinator) => {
                        let coordinator = Arc::new(coordinator);
                        let work_stealing_shutdown = shard_shutdown_tx.subscribe();
                        tokio::spawn(async move {
                            coordinator.start(work_stealing_shutdown).await;
                        });
                    }
                    Err(_) => {}
                }
            }
        }

        let crawler = RustBfsCrawler::new(
            config,
            start_url.to_string(),
            http,
            state,
            frontier,
            work_rx,
            writer_thread,
            lock_manager,
            metrics,
            crawler_permits,
        );

        Ok((
            crawler,
            frontier_shards,
            work_tx,
            governor_shutdown_tx,
            shard_shutdown_tx,
        ))
    }
}

async fn governor_task(
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const ADJUSTMENT_INTERVAL_MS: u64 = 250;
    const THROTTLE_THRESHOLD_MS: f64 = 500.0;
    const UNTHROTTLE_THRESHOLD_MS: f64 = 100.0;
    const MIN_PERMITS: usize = 32;
    const MAX_PERMITS: usize = 512;

    let mut shrink_bin: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
    let mut last_urls_fetched = 0u64;

    loop {
        if *shutdown.borrow() {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(ADJUSTMENT_INTERVAL_MS)).await;

        let commit_ewma_ms = metrics.get_commit_ewma_ms();
        let current_permits = permits.available_permits();
        let current_urls_processed = metrics.urls_processed_total.lock().value;
        let work_done = current_urls_processed > last_urls_fetched;
        last_urls_fetched = current_urls_processed;

        if commit_ewma_ms > THROTTLE_THRESHOLD_MS {
            if current_permits > MIN_PERMITS {
                if let Ok(permit) = permits.clone().try_acquire_owned() {
                    shrink_bin.push(permit);
                    metrics.throttle_adjustments.lock().inc();
                }
            }
        } else if commit_ewma_ms < UNTHROTTLE_THRESHOLD_MS && commit_ewma_ms > 0.0 && work_done {
            if let Some(permit) = shrink_bin.pop() {
                drop(permit);
                metrics.throttle_adjustments.lock().inc();
            } else if current_permits < MAX_PERMITS {
                permits.add_permits(1);
                metrics.throttle_adjustments.lock().inc();
            }
        }

        metrics
            .throttle_permits_held
            .lock()
            .set(current_permits as f64);
    }
}

/// Python module definition
#[pymodule]
fn rustmapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Crawler>()?;
    m.add_class::<CrawlerConfig>()?;
    m.add_class::<CrawlResult>()?;
    Ok(())
}
