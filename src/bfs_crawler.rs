//! Breadth-first web crawler with polite host throttling and seeding strategies.
//!
//! This module implements the core crawling logic using a BFS approach with:
//! - Concurrent request processing with configurable worker limits
//! - Per-host politeness via crawl delays from robots.txt
//! - Multiple seeding strategies (sitemap.xml, Certificate Transparency logs, Common Crawl)
//! - Automatic state persistence and WAL-based recovery
//! - Distributed coordination via Redis (optional)

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::common_crawl_seeder::CommonCrawlSeeder;
use crate::config::Config;
use crate::ct_log_seeder::CtLogSeeder;
use crate::frontier::ShardedFrontier;
use crate::network::{FetchError, HttpClient};
use crate::sitemap_seeder::SitemapSeeder;
use crate::state::{CrawlerState, SitemapNode, StateEvent};
use crate::url_lock_manager::UrlLockManager;
use crate::url_utils;
use tokio::sync::mpsc;

pub const PROGRESS_INTERVAL: usize = 100;
pub const PROGRESS_TIME_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMapStats {
    pub total_nodes: usize,
    pub crawled_nodes: usize,
}

impl std::fmt::Display for NodeMapStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Nodes: {} total, {} crawled",
            self.total_nodes, self.crawled_nodes
        )
    }
}

#[derive(Debug, Clone)]
pub struct BfsCrawlerConfig {
    pub max_workers: u32,
    pub timeout: u32,
    pub user_agent: String,
    pub ignore_robots: bool,
    pub save_interval: u64,
    pub redis_url: Option<String>,
    pub lock_ttl: u64,
    pub enable_redis: bool,
}

impl Default for BfsCrawlerConfig {
    fn default() -> Self {
        Self {
            max_workers: 256,
            timeout: 20,
            user_agent: "Rust-Sitemap-Crawler/1.0".to_string(),
            ignore_robots: false,
            save_interval: 300,
            redis_url: None,
            lock_ttl: 60,
            enable_redis: false,
        }
    }
}

#[derive(Clone)]
pub struct BfsCrawler {
    config: BfsCrawlerConfig,
    state: Arc<CrawlerState>,
    frontier: Arc<ShardedFrontier>,
    work_rx: Arc<parking_lot::Mutex<Option<crate::frontier::WorkReceiver>>>,
    http: Arc<HttpClient>,
    start_url: String,
    running: Arc<AtomicBool>,
    lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
    writer_thread: Arc<crate::writer_thread::WriterThread>,
    metrics: Arc<crate::metrics::Metrics>,
    crawler_permits: Arc<tokio::sync::Semaphore>,
    _parse_permits: Arc<tokio::sync::Semaphore>,
}

/// Job handed to parser workers. Contains fetched raw bytes and context needed to
/// produce crawl attempt facts and discovered links.
pub struct ParseJob {
    pub _host: String,
    pub url: String,
    pub depth: u32,
    pub _parent_url: Option<String>,
    pub start_url_domain: String,
    pub html_bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub status_code: u16,
    pub total_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfsCrawlerResult {
    pub start_url: String,
    pub discovered: usize,
    pub processed: usize,
    pub successful: usize,
    pub failed: usize,
    pub timeout: usize,
    pub duration_secs: u64,
    pub stats: NodeMapStats,
}

struct CrawlResult {
    host: String,
    result: Result<Vec<(String, u32, Option<String>)>, FetchError>,
    latency_ms: u64,
}

struct CrawlTask {
    host: String,
    url: String,
    depth: u32,
    _parent_url: Option<String>,
    start_url_domain: String,
    network_permits: Arc<tokio::sync::Semaphore>,
    backpressure_permit: crate::frontier::FrontierPermit,
    // Channel that hands fetched pages to parser workers.
    parse_sender: mpsc::Sender<ParseJob>,
}

impl BfsCrawler {
    pub fn new(
        config: BfsCrawlerConfig,
        start_url: String,
        http: Arc<HttpClient>,
        state: Arc<CrawlerState>,
        frontier: Arc<ShardedFrontier>,
        work_rx: crate::frontier::WorkReceiver,
        writer_thread: Arc<crate::writer_thread::WriterThread>,
        lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
        metrics: Arc<crate::metrics::Metrics>,
        crawler_permits: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        // Limit concurrent HTML parsing to prevent thread pool starvation.
        // Increased from 8 to 128 to match high-speed network throughput and prevent parser bottleneck.
        // With modern CPUs, 128 concurrent parsing tasks can be handled efficiently via tokio's
        // blocking thread pool, which auto-scales based on available cores.
        const MAX_PARSE_CONCURRENT: usize = 128;

        Self {
            config,
            start_url,
            http,
            state,
            frontier,
            work_rx: Arc::new(parking_lot::Mutex::new(Some(work_rx))),
            writer_thread,
            running: Arc::new(AtomicBool::new(false)),
            lock_manager,
            metrics,
            crawler_permits,
            _parse_permits: Arc::new(tokio::sync::Semaphore::new(MAX_PARSE_CONCURRENT)),
        }
    }

    pub async fn initialize(
        &mut self,
        seeding_strategy: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_url_domain = self.get_domain(&self.start_url);

        // Non-sitemap seeders work off the registrable domain, so derive it once.
        let root_domain = self.get_root_domain(&start_url_domain);

        // Seed the frontier even though frontier state is not yet persisted.
        // TODO: Persist the frontier so restarts keep their pending work.

        let mut seed_links: Vec<(String, u32, Option<String>)> = Vec::new();

        // Select only the seeders requested by the CLI so startup work stays targeted.
        let mut seeders: Vec<Box<dyn crate::seeder::Seeder>> = Vec::new();

        // Support comma-separated strategies for mix-and-match seeding.
        let strategies: Vec<&str> = seeding_strategy.split(',').map(|s| s.trim()).collect();
        let enable_all = strategies.contains(&"all");
        let enable_sitemap = enable_all || strategies.contains(&"sitemap");
        let enable_ct = enable_all || strategies.contains(&"ct");
        let enable_commoncrawl = enable_all || strategies.contains(&"commoncrawl");

        if enable_sitemap && !self.config.ignore_robots {
            eprintln!("Enabling seeder: sitemap");
            seeders.push(Box::new(SitemapSeeder::new((*self.http).clone())));
        }

        if enable_ct {
            eprintln!(
                "Enabling seeder: ct-logs (for root domain: {})",
                root_domain
            );
            seeders.push(Box::new(CtLogSeeder::new((*self.http).clone())));
        }

        if enable_commoncrawl {
            eprintln!(
                "Enabling seeder: common-crawl (for root domain: {})",
                root_domain
            );
            seeders.push(Box::new(CommonCrawlSeeder::new((*self.http).clone())));
        }

        // Run each seeder now so the frontier starts with known URLs.
        if !seeders.is_empty() {
            eprintln!("Running {} seeder(s)...", seeders.len());
            use futures_util::StreamExt;

            for seeder in seeders {
                // Use the full URL for sitemap seeding, and the root domain for others.
                let domain_to_seed = if seeder.name() == "sitemap" {
                    &self.start_url
                } else {
                    &root_domain
                };

                let seeder_name = seeder.name();
                let mut url_stream = seeder.seed(domain_to_seed);
                let mut url_count = 0;

                // Stream URLs to the frontier in batches to avoid high memory usage.
                while let Some(url_result) = url_stream.next().await {
                    match url_result {
                        Ok(url) => {
                            seed_links.push((url, 0, None));
                            url_count += 1;

                            // Flush to the frontier every 1000 URLs to prevent memory issues.
                            if seed_links.len() >= 1000 {
                                self.frontier
                                    .add_links(std::mem::take(&mut seed_links))
                                    .await;
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: Seeder '{}' error: {}", seeder_name, e);
                        }
                    }
                }

                eprintln!("Seeder '{}' streamed {} URLs", seeder_name, url_count);
            }
        }

        // Flush any remaining seeded URLs into the frontier.
        if !seed_links.is_empty() {
            let added = self.frontier.add_links(seed_links).await;
            eprintln!(
                "Pre-seeded {} total URLs using strategy '{}'",
                added, seeding_strategy
            );
        }

        // Always include the exact start URL even if a seeder already emitted it.
        let start_links = vec![(self.start_url.clone(), 0, None)];
        self.frontier.add_links(start_links).await;

        // robots.txt is handled on-demand by each FrontierShard.

        Ok(())
    }

    // Main crawl loop. Coordinates scheduling, progress, and shutdown.
    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start = SystemTime::now();
        self.running.store(true, Ordering::SeqCst);

        let save_task = self.spawn_auto_save_task();
        let start_url_domain = self.get_domain(&self.start_url);

        let mut in_flight_tasks = JoinSet::new();
        let max_concurrent = self.config.max_workers as usize;

        let mut processed_count = 0;
        let mut successful_count = 0;
        let mut failed_count = 0;
        let mut timeout_count = 0;
        let mut last_progress_report = std::time::Instant::now();

        // `work_rx` is an Option that is taken once, so `start_crawling` is single-use.
        let mut work_rx = self
            .work_rx
            .lock()
            .take()
            .expect("start_crawling() can only be called once");

        // Create parse queue and spawn parser dispatcher. The queue is bounded so when
        // it's full, send().await will backpressure fetchers automatically.
        // Increased from 100 to 512 to match higher parser concurrency (128 parsers × 4 buffered jobs).
        let (parse_tx, mut parse_rx) = mpsc::channel::<ParseJob>(512);
        let frontier_for_parser = Arc::clone(&self.frontier);
        let writer_for_parser = Arc::clone(&self.writer_thread);
        let metrics_for_parser = Arc::clone(&self.metrics);

        // Limit concurrent parsing with a semaphore (parser pool size)
        // Increased from 8 to 128 to prevent parser bottleneck with fast network speeds.
        let parse_sema = Arc::new(tokio::sync::Semaphore::new(128));
        tokio::spawn(async move {
            while let Some(job) = parse_rx.recv().await {
                let frontier = Arc::clone(&frontier_for_parser);
                let writer = Arc::clone(&writer_for_parser);
                let metrics = Arc::clone(&metrics_for_parser);
                let sem = Arc::clone(&parse_sema);
                // Acquire a parse permit to bound concurrency
                let permit = sem.acquire_owned().await.unwrap();

                tokio::spawn(async move {
                    // Perform parsing on blocking thread pool to avoid blocking tokio runtime
                    let job_url_clone = job.url.clone();
                    let parse_outcome = tokio::task::spawn_blocking(move || {
                        use scraper::{Html, Selector};
                        let html_str = match String::from_utf8(job.html_bytes) {
                            Ok(s) => s,
                            Err(_) => return Err(FetchError::InvalidUtf8),
                        };

                        // Reduced from 10MB to 5MB to prevent memory issues with many concurrent requests
                        if html_str.len() > 5 * 1024 * 1024 {
                            return Err(FetchError::HtmlTooLarge);
                        }

                        let document = Html::parse_document(&html_str);

                        // Extract base href
                        let base_selector = Selector::parse("base[href]").unwrap();
                        let base_href = document
                            .select(&base_selector)
                            .next()
                            .and_then(|el| el.value().attr("href"))
                            .map(|s| s.to_string());

                        let effective_base =
                            base_href.as_deref().unwrap_or(&job_url_clone).to_string();

                        let mut extracted_links = Vec::new();
                        let a_selector = Selector::parse("a[href]").unwrap();
                        for el in document.select(&a_selector) {
                            if let Some(href) = el.value().attr("href") {
                                extracted_links.push(href.to_string());
                            }
                        }

                        // Extract title
                        let title_selector = Selector::parse("title").unwrap();
                        let extracted_title = document
                            .select(&title_selector)
                            .next()
                            .map(|el| el.text().collect::<String>().trim().to_string())
                            .filter(|s| !s.is_empty());

                        Ok((extracted_links, extracted_title, effective_base))
                    })
                    .await;

                    match parse_outcome {
                        Ok(Ok((extracted_links, extracted_title, effective_base))) => {
                            // Resolve links and add to frontier
                            let mut discovered_links = Vec::new();
                            for link in &extracted_links {
                                if let Ok(absolute_url) =
                                    BfsCrawler::convert_to_absolute_url(link, &effective_base)
                                {
                                    if BfsCrawler::is_same_domain(
                                        &absolute_url,
                                        &job.start_url_domain,
                                    ) && BfsCrawler::should_crawl_url(&absolute_url)
                                    {
                                        discovered_links.push((
                                            absolute_url,
                                            job.depth + 1,
                                            Some(job.url.clone()),
                                        ));
                                    }
                                }
                            }

                            // Inform the writer thread of the crawl attempt.
                            let normalized_url = SitemapNode::normalize_url(&job.url);
                            let _ = writer
                                .send_event_async(StateEvent::CrawlAttemptFact {
                                    url_normalized: normalized_url,
                                    status_code: job.status_code,
                                    content_type: job.content_type.clone(),
                                    content_length: Some(job.total_bytes),
                                    title: extracted_title,
                                    link_count: extracted_links.len(),
                                    response_time_ms: None,
                                })
                                .await;

                            if !discovered_links.is_empty() {
                                frontier.add_links(discovered_links).await;
                            }
                            metrics.urls_processed_total.lock().inc();
                        }
                        Ok(Err(e)) => {
                            eprintln!("Parser task error for {}: {}", job.url, e);
                        }
                        Err(e) => {
                            eprintln!("Parser task panicked for {}: {}", job.url, e);
                        }
                    }

                    // drop permit when task finishes
                    drop(permit);
                });
            }
        });

        loop {
            // Exit if the crawler has been stopped.
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            // Wait for a new URL or a completed task.
            tokio::select! {
                // Spawn tasks up to the concurrency limit.
                next_url = work_rx.recv(), if in_flight_tasks.len() < max_concurrent => {
                    match next_url {
                        Some((host, url, depth, parent_url, backpressure_permit)) => {
                            eprintln!("Crawler: Received work item: {} (depth {})", url, depth);
                            let task_state = self.clone();
                            let task_host = host.clone();
                            let task_url = url.clone();
                            let task_depth = depth;
                            let task_parent = parent_url.clone();
                            let task_domain = start_url_domain.clone();
                            let task_permits = Arc::clone(&self.crawler_permits);
                            let task_parse_tx = parse_tx.clone();

                            // scraper is Send + Sync, so we can use `spawn`.
                            in_flight_tasks.spawn(async move {
                                // Create a panic guard to ensure inflight counter is decremented even on panic
                                struct InflightGuard {
                                    frontier: Arc<ShardedFrontier>,
                                    host: String,
                                    completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
                                }
                                impl Drop for InflightGuard {
                                    fn drop(&mut self) {
                                        // Only send failure message if task didn't complete normally
                                        if !self.completed.load(std::sync::atomic::Ordering::Relaxed) {
                                            eprintln!("InflightGuard: Task panicked or was cancelled for host {}, sending failure signal", self.host);
                                            self.frontier.record_failed(&self.host, 0);
                                        }
                                    }
                                }

                                let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                                let _guard = InflightGuard {
                                    frontier: Arc::clone(&task_state.frontier),
                                    host: task_host.clone(),
                                    completed: Arc::clone(&completed),
                                };

                                let task = CrawlTask {
                                    host: task_host,
                                    url: task_url,
                                    depth: task_depth,
                                    _parent_url: task_parent,
                                    start_url_domain: task_domain,
                                    network_permits: task_permits,
                                    backpressure_permit,
                                    parse_sender: task_parse_tx,
                                };
                                let result = task_state.process_url_streaming(task).await;

                                // Mark as completed so the guard doesn't send a duplicate failure
                                completed.store(true, std::sync::atomic::Ordering::Relaxed);

                                result
                            });
                        }
                        None => {
                            // No more URLs available, continue to drain tasks
                        }
                    }
                }

                // Process completed tasks.
                Some(result) = in_flight_tasks.join_next(), if !in_flight_tasks.is_empty() => {
                    match result {
                        Ok(crawl_result) => {
                            processed_count += 1;
                            self.metrics.urls_processed_total.lock().inc();

                            match crawl_result.result {
                                Ok(discovered_links) => {
                                    successful_count += 1;
                                    self.frontier.record_success(&crawl_result.host, crawl_result.latency_ms);

                                    if !discovered_links.is_empty() {
                                        self.frontier
                                            .add_links(discovered_links)
                                            .await;
                                    }
                                }
                                Err(ref e) => {
                                    self.handle_crawl_error(e, &crawl_result, &mut timeout_count, &mut failed_count, true);
                                }
                            }

                            // Print progress periodically.
                            if processed_count % PROGRESS_INTERVAL == 0
                                || last_progress_report.elapsed().as_secs() >= PROGRESS_TIME_SECS
                            {
                                let stats = self.get_stats().await;
                                let frontier_stats = self.frontier.stats();
                                let success_rate = if processed_count > 0 {
                                    successful_count as f64 / processed_count as f64 * 100.0
                                } else {
                                    0.0
                                };
                                eprintln!(
                                    "Progress: {} total ({} success, {} failed, {} timeout, {:.1}% success rate) | {} | {}",
                                    processed_count, successful_count, failed_count, timeout_count, success_rate, stats, frontier_stats
                                );
                                last_progress_report = std::time::Instant::now();
                            }
                        }
                        Err(e) => {
                            eprintln!("Task join error: {}", e);
                            // A task panic will leak the host inflight counter.
                            // However, the backpressure_permit should be properly dropped by Rust's
                            // panic unwinding mechanism, returning it to the semaphore.
                            eprintln!("WARNING: Task panic detected - host inflight counter may leak");
                            eprintln!("Note: backpressure_permit should auto-release on panic unwinding");

                            // The permit is owned by the panicked task and will be dropped during
                            // unwinding, which returns it to the semaphore. However, we've lost
                            // track of which host this was for, so we cannot decrement its inflight
                            // counter. This is unavoidable without more complex state tracking.
                        }
                    }
                }

                // Exit when the frontier is empty and no tasks are in flight.
                else => {
                    if self.frontier.is_empty() && in_flight_tasks.is_empty() {
                        eprintln!("Crawl complete: frontier empty and no tasks in flight");
                        break;
                    }
                    // Yield to avoid a tight loop.
                    tokio::time::sleep(tokio::time::Duration::from_millis(Config::LOOP_YIELD_DELAY_MS)).await;
                }
            }
        }

        // Process any remaining tasks.
        while let Some(result) = in_flight_tasks.join_next().await {
            if let Ok(crawl_result) = result {
                processed_count += 1;
                self.metrics.urls_processed_total.lock().inc();

                match crawl_result.result {
                    Ok(discovered_links) => {
                        successful_count += 1;
                        self.frontier
                            .record_success(&crawl_result.host, crawl_result.latency_ms);
                        if !discovered_links.is_empty() {
                            self.frontier.add_links(discovered_links).await;
                        }
                    }
                    Err(ref e) => {
                        self.handle_crawl_error(
                            e,
                            &crawl_result,
                            &mut timeout_count,
                            &mut failed_count,
                            false,
                        );
                    }
                }
            }
        }

        let elapsed = SystemTime::now().duration_since(start).unwrap_or_default();
        let stats = self.get_stats().await;

        self.stop().await;
        if let Some(task) = save_task {
            task.abort();
            if let Err(e) = task.await {
                if !e.is_cancelled() {
                    eprintln!("Save task error: {}", e);
                }
            }
        }

        // Export results to JSONL.
        let output_path = "crawl_results.jsonl";
        eprintln!("Exporting results to {}...", output_path);
        if let Err(e) = self.export_to_jsonl(output_path).await {
            eprintln!("Warning: Failed to export results: {}", e);
        } else {
            eprintln!(
                "Successfully exported {} nodes to {}",
                stats.total_nodes, output_path
            );
        }

        let result = BfsCrawlerResult {
            start_url: self.start_url.clone(),
            discovered: stats.total_nodes,
            processed: processed_count,
            successful: successful_count,
            failed: failed_count,
            timeout: timeout_count,
            duration_secs: elapsed.as_secs(),
            stats: stats.clone(),
        };

        Ok(result)
    }

    fn handle_crawl_error(
        &self,
        e: &FetchError,
        crawl_result: &CrawlResult,
        timeout_count: &mut usize,
        failed_count: &mut usize,
        log_error: bool,
    ) {
        // Classify error types and only count permanent errors towards host blocking
        let is_permanent_error = match e {
            // Transient errors - don't count towards permanent host blocking
            FetchError::Timeout
            | FetchError::DnsError
            | FetchError::ConnectionRefused
            | FetchError::NetworkError(_)
            | FetchError::SslError
            | FetchError::BodyError(_)
            | FetchError::PermitTimeout
            | FetchError::PermitAcquisition => {
                *timeout_count += 1;
                self.metrics.urls_timeout_total.lock().inc();
                false // Don't block host for transient errors
            }
            // Permanent errors - count towards host blocking threshold
            FetchError::ContentTooLarge(_, _)
            | FetchError::HtmlTooLarge
            | FetchError::InvalidUtf8
            | FetchError::ClientBuildError(_)
            | FetchError::AlreadyLocked => {
                *failed_count += 1;
                self.metrics.urls_failed_total.lock().inc();
                true // Block host after repeated permanent errors
            }
        };

        // CRITICAL FIX: Only record failure for permanent errors to avoid blocking hosts with transient issues
        if is_permanent_error {
            self.frontier
                .record_failed(&crawl_result.host, crawl_result.latency_ms);
        } else {
            // Transient errors: decrement inflight without incrementing failure count
            self.frontier
                .record_completed(&crawl_result.host, crawl_result.latency_ms);
        }

        if log_error {
            eprintln!("{} - {}", crawl_result.host, e);
        }
    }

    async fn _send_event_if_alive(
        &self,
        cancel_token: &CancellationToken,
        lost_lock: &Arc<std::sync::atomic::AtomicBool>,
        event: StateEvent,
    ) {
        use std::sync::atomic::Ordering;

        // Don't send events for zombie tasks.
        if cancel_token.is_cancelled() || lost_lock.load(Ordering::Relaxed) {
            eprintln!("Zombie task detected - not sending event for URL");
            return;
        }

        let _ = self.writer_thread.send_event_async(event).await;
    }

    async fn process_url_streaming(&self, task: CrawlTask) -> CrawlResult {
        let overall_start = std::time::Instant::now();
        let CrawlTask {
            host,
            url,
            depth,
            _parent_url,
            start_url_domain,
            network_permits,
            backpressure_permit,
            parse_sender,
        } = task;
        use crate::url_lock_manager::CrawlLock;
        use std::sync::atomic::AtomicBool;

        // A cancellation token is shared between the parser timeout and lock renewal.
        let cancel_token = CancellationToken::new();

        // Tracks whether the lock has been lost.
        let lost_lock = Arc::new(AtomicBool::new(false));

        // TIMING: Lock acquisition
        let lock_start = std::time::Instant::now();
        // The CrawlLock guard ensures that Redis renewals are automatic and no zombie locks are left behind.
        // The cancellation token allows the lock loss to cancel the parser immediately.
        let _lock_guard = if let Some(lock_manager) = &self.lock_manager {
            match CrawlLock::acquire(
                Arc::clone(lock_manager),
                url.clone(),
                cancel_token.clone(),
                Arc::clone(&lost_lock),
            )
            .await
            {
                Ok(Some(guard)) => Some(guard),
                Ok(None) => {
                    // If another worker has the URL, we bail to avoid duplicate work.
                    return CrawlResult {
                        host,
                        result: Err(FetchError::AlreadyLocked),
                        latency_ms: 0,
                    };
                }
                Err(e) => {
                    eprintln!(
                        "Redis lock error for {}: {}. Proceeding without lock",
                        url, e
                    );
                    None
                }
            }
        } else {
            None
        };
        let _lock_elapsed = lock_start.elapsed();

        let start_time = std::time::Instant::now();

        // TIMING: Permit acquisition
        let permit_start = std::time::Instant::now();
        // Acquire a permit to control the number of active socket connections with timeout
        let _network_permit =
            match tokio::time::timeout(Duration::from_secs(30), network_permits.acquire_owned())
                .await
            {
                Ok(Ok(permit)) => {
                    let _permit_elapsed = permit_start.elapsed();
                    permit
                }
                Ok(Err(_)) => {
                    let latency_ms = start_time.elapsed().as_millis() as u64;
                    return CrawlResult {
                        host,
                        result: Err(FetchError::PermitAcquisition),
                        latency_ms,
                    };
                }
                Err(_) => {
                    let latency_ms = start_time.elapsed().as_millis() as u64;
                    return CrawlResult {
                        host,
                        result: Err(FetchError::PermitTimeout),
                        latency_ms,
                    };
                }
            };

        // TIMING: Network fetch
        let fetch_start = std::time::Instant::now();
        // Stream the response to avoid buffering large pages in memory.
        let fetch_result = match self.http.fetch_stream(&url).await {
            Ok(response) if response.status().as_u16() == 200 => {
                // Increment the fetched counter.
                self.metrics.urls_fetched_total.lock().inc();
                let content_type = response
                    .headers()
                    .get("content-type")
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.to_string());

                if let Some(ct) = content_type.as_ref() {
                    if url_utils::is_html_content_type(ct) {
                        Some(response)
                    } else {
                        let latency_ms = start_time.elapsed().as_millis() as u64;
                        return CrawlResult {
                            host,
                            result: Ok(Vec::new()),
                            latency_ms,
                        };
                    }
                } else {
                    Some(response)
                }
            }
            Ok(_) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Ok(Vec::new()),
                    latency_ms,
                };
            }
            Err(e) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(e),
                    latency_ms,
                };
            }
        };
        let _fetch_elapsed = fetch_start.elapsed();

        // Release the network permit as soon as the fetch is complete.
        drop(_network_permit);

        // Keep the backpressure permit alive for the entire function to keep the semaphore occupied.
        let _backpressure_permit = backpressure_permit;
        // Keep the lock guard alive for the entire function for automatic renewals and release.

        if let Some(response) = fetch_result {
            let status_code = response.status().as_u16();

            // Track the HTTP version.
            match response.version() {
                reqwest::Version::HTTP_09
                | reqwest::Version::HTTP_10
                | reqwest::Version::HTTP_11 => {
                    self.metrics.http_version_h1.lock().inc();
                }
                reqwest::Version::HTTP_2 => {
                    self.metrics.http_version_h2.lock().inc();
                }
                reqwest::Version::HTTP_3 => {
                    self.metrics.http_version_h3.lock().inc();
                }
                _ => {}
            }

            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());

            // TIMING: Body download
            let body_start = std::time::Instant::now();
            // reqwest automatically handles decompression - just get the bytes
            let html_bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    let latency_ms = start_time.elapsed().as_millis() as u64;
                    return CrawlResult {
                        host,
                        result: Err(FetchError::BodyError(e.to_string())),
                        latency_ms,
                    };
                }
            };
            let _body_elapsed = body_start.elapsed();
            let total_bytes = html_bytes.len();

            // Offload parsing to parser workers: create a ParseJob and enqueue it. The
            // bounded channel will provide backpressure if parsers are busy.
            let job = ParseJob {
                _host: host.clone(),
                url: url.clone(),
                depth,
                _parent_url: _parent_url.clone(),
                start_url_domain: start_url_domain.clone(),
                html_bytes: html_bytes.to_vec(),
                content_type: content_type.clone(),
                status_code,
                total_bytes,
            };

            // If the parse queue is full, this await will naturally backpressure the fetcher.
            if let Err(_) = parse_sender.send(job).await {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                eprintln!(
                    "Failed to enqueue parse job for {}: parse queue closed",
                    url
                );
                return CrawlResult {
                    host,
                    result: Err(FetchError::BodyError("parse queue closed".to_string())),
                    latency_ms,
                };
            }

            let latency_ms = start_time.elapsed().as_millis() as u64;
            let _overall_elapsed = overall_start.elapsed();
            // Parsers will add discovered links and emit CrawlAttemptFact, so return success with empty links.
            CrawlResult {
                host,
                result: Ok(Vec::new()),
                latency_ms,
            }
        } else {
            // Skip storing results for non-HTML or error responses because they lack usable links.
            let latency_ms = start_time.elapsed().as_millis() as u64;
            CrawlResult {
                host,
                result: Ok(Vec::new()),
                latency_ms,
            }
        }
        // Dropping the backpressure_permit here releases the semaphore slot so new work can start.
        // Dropping the guard here deliberately releases the Redis lock once processing completes.
    }

    fn spawn_auto_save_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        if self.config.save_interval == 0 {
            return None;
        }

        let interval = Duration::from_secs(self.config.save_interval);
        let running = Arc::clone(&self.running);

        Some(tokio::spawn(async move {
            loop {
                let should_continue = running.load(Ordering::SeqCst);
                if !should_continue {
                    break;
                }

                sleep(interval).await;

                let should_continue = running.load(Ordering::SeqCst);
                if !should_continue {
                    break;
                }

                // redb auto-commits, so there is nothing to persist.
            }
        }))
    }

    fn should_crawl_url(url: &str) -> bool {
        url_utils::should_crawl_url(url)
    }

    fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
        url_utils::convert_to_absolute_url(link, base_url)
    }

    pub fn get_domain(&self, url: &str) -> String {
        url_utils::extract_host(url).unwrap_or_default()
    }

    fn get_root_domain(&self, hostname: &str) -> String {
        url_utils::get_root_domain(hostname)
    }

    fn is_same_domain(url: &str, base_domain: &str) -> bool {
        if let Some(host) = url_utils::extract_host(url) {
            url_utils::is_same_domain(&host, base_domain)
        } else {
            false
        }
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    // TODO: Use count queries instead of full scans.
    // NOTE: get_crawled_node_count() performs a full table scan with deserialization.
    // Optimization would require adding an AtomicUsize counter or separate index table.
    pub async fn get_stats(&self) -> NodeMapStats {
        let total_nodes = self.state.get_node_count().unwrap_or(0);
        let crawled_nodes = self.state.get_crawled_node_count().unwrap_or(0);

        NodeMapStats {
            total_nodes,
            crawled_nodes,
        }
    }

    // Persistence hook for future changes (redb auto-commits).
    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        // redb commits automatically; nothing extra to do.
        Ok(())
    }

    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::cell::RefCell;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?;

        // Use streaming iterator to avoid loading all nodes into memory (prevents OOM on large databases)
        let node_iter = self.state.iter_nodes()?;
        let count = RefCell::new(0);
        let file_ref = RefCell::new(file);

        // Use for_each instead of next() since NodeIterator doesn't implement next() properly
        node_iter.for_each(|node| {
            let json = serde_json::to_string(&node)
                .map_err(|e| crate::state::StateError::Serialization(format!("Serialization error: {}", e)))?;

            let mut file_mut = file_ref.borrow_mut();
            writeln!(file_mut, "{}", json)
                .map_err(|e| crate::state::StateError::Serialization(format!("Write error: {}", e)))?;

            let mut count_mut = count.borrow_mut();
            *count_mut += 1;
            Ok(())
        })?;

        let final_count = count.borrow();
        eprintln!("Exported {} nodes to JSONL", *final_count);
        Ok(())
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_should_crawl_url() {
        assert!(BfsCrawler::should_crawl_url("https://test.local/page"));
        assert!(BfsCrawler::should_crawl_url("http://test.local/page"));
        assert!(!BfsCrawler::should_crawl_url("ftp://test.local/page"));
        assert!(!BfsCrawler::should_crawl_url("https://test.local/file.pdf"));
        assert!(!BfsCrawler::should_crawl_url(
            "https://test.local/image.jpg"
        ));
    }

    #[test]
    fn test_is_same_domain() {
        assert!(BfsCrawler::is_same_domain(
            "https://test.local/page1",
            "test.local"
        ));
        assert!(!BfsCrawler::is_same_domain(
            "https://other.local/page1",
            "test.local"
        ));
    }

    #[test]
    fn test_convert_to_absolute_url() {
        assert_eq!(
            BfsCrawler::convert_to_absolute_url("/page1", "https://test.local/foo").unwrap(),
            "https://test.local/page1"
        );
        assert_eq!(
            BfsCrawler::convert_to_absolute_url("page1", "https://test.local/foo/").unwrap(),
            "https://test.local/foo/page1"
        );
        assert_eq!(
            BfsCrawler::convert_to_absolute_url("https://other.local/page", "https://test.local")
                .unwrap(),
            "https://other.local/page"
        );
    }

    #[tokio::test]
    async fn test_crawler_config_default() {
        let config = BfsCrawlerConfig::default();
        assert_eq!(config.max_workers, 256);
        assert_eq!(config.timeout, 20);
        assert!(!config.ignore_robots);
        assert!(!config.enable_redis);
    }
}
