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
use crate::completion_detector::{CompletionDetector, CompletionSignals};
use crate::config::Config;
use crate::ct_log_seeder::CtLogSeeder;
use crate::frontier::ShardedFrontier;
use crate::network::{FetchError, HttpClient};
use crate::sitemap_seeder::SitemapSeeder;
use crate::state::{CrawlerState, SitemapNode, StateEvent};
use crate::url_lock_manager::UrlLockManager;
use crate::url_utils;
use tokio::sync::mpsc;

pub const PROGRESS_INTERVAL: usize = 5;
pub const PROGRESS_TIME_SECS: u64 = 5;

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
    pub max_urls: Option<usize>,
    pub duration_secs: Option<u64>,
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
            max_urls: None,
            duration_secs: None,
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
    completion_detector: Arc<CompletionDetector>,
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

/// Represents the outcome of a crawl attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrawlOutcome {
    /// Successful HTML fetch (2xx status, HTML content)
    Success,
    /// Non-2xx status code (3xx redirect, 4xx client error, 5xx server error)
    NonSuccessStatus,
    /// 2xx status but non-HTML content (PDF, image, JSON, etc.)
    NonHtmlContent,
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
            completion_detector: Arc::new(CompletionDetector::with_defaults()),
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
        let added = self.frontier.add_links(start_links).await;
        eprintln!("Added {} start URL(s) to frontier", added);

        // robots.txt is handled on-demand by each FrontierShard.

        Ok(())
    }

    /// Take the work receiver channel (can only be called once)
    fn take_work_receiver(&self) -> Result<mpsc::UnboundedReceiver<(String, String, u32, Option<String>, crate::frontier::FrontierPermit)>, Box<dyn std::error::Error>> {
        self.work_rx
            .lock()
            .take()
            .ok_or_else(|| "start_crawling() can only be called once".into())
    }

    /// Spawn the HTML parser dispatcher task
    fn spawn_parser_dispatcher(
        &self,
        mut parse_rx: mpsc::Receiver<ParseJob>,
        frontier: Arc<ShardedFrontier>,
        writer: Arc<crate::writer_thread::WriterThread>,
        metrics: Arc<crate::metrics::Metrics>,
    ) {
        let parse_sema = Arc::new(tokio::sync::Semaphore::new(128));

        tokio::spawn(async move {
            while let Some(job) = parse_rx.recv().await {
                let frontier_clone = Arc::clone(&frontier);
                let writer_clone = Arc::clone(&writer);
                let metrics_clone = Arc::clone(&metrics);
                let sem_clone = Arc::clone(&parse_sema);

                let permit = match sem_clone.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        eprintln!("Failed to acquire parse permit, semaphore closed");
                        continue;
                    }
                };

                tokio::spawn(async move {
                    Self::process_parse_job(job, frontier_clone, writer_clone, metrics_clone).await;
                    drop(permit);
                });
            }
        });
    }

    /// Process a single parse job (extract links and update state)
    async fn process_parse_job(
        job: ParseJob,
        frontier: Arc<ShardedFrontier>,
        writer: Arc<crate::writer_thread::WriterThread>,
        metrics: Arc<crate::metrics::Metrics>,
    ) {
        let job_url_for_logging = job.url.clone();

        // Perform parsing on blocking thread pool
        let parse_outcome = tokio::task::spawn_blocking(move || {
            Self::parse_html_blocking(job)
        }).await;

        match parse_outcome {
            Ok(Ok((extracted_links, extracted_title, effective_base, job))) => {
                Self::handle_parse_success(
                    job,
                    extracted_links,
                    extracted_title,
                    effective_base,
                    job_url_for_logging,
                    frontier,
                    writer,
                    metrics,
                ).await;
            }
            Ok(Err(_)) | Err(_) => {
                // Parser errors or panics - silently skip
            }
        }
    }

    /// Parse HTML on blocking thread (CPU-intensive operation)
    fn parse_html_blocking(job: ParseJob) -> Result<(Vec<String>, Option<String>, String, ParseJob), FetchError> {
        use scraper::{Html, Selector};

        let html_str = String::from_utf8(job.html_bytes.clone())
            .map_err(|_| FetchError::InvalidUtf8)?;

        // Limit HTML size
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

        let effective_base = base_href.as_deref().unwrap_or(&job.url).to_string();

        // Extract links
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

        Ok((extracted_links, extracted_title, effective_base, job))
    }

    /// Handle successful HTML parsing and link extraction
    async fn handle_parse_success(
        job: ParseJob,
        extracted_links: Vec<String>,
        extracted_title: Option<String>,
        effective_base: String,
        job_url_for_logging: String,
        frontier: Arc<ShardedFrontier>,
        writer: Arc<crate::writer_thread::WriterThread>,
        metrics: Arc<crate::metrics::Metrics>,
    ) {
        // REMOVED: Adaptive link budget limits (artificially throttled discovery)
        // Now process ALL discovered links - global frontier backpressure provides natural throttling

        // Process all discovered links
        let mut discovered_links = Vec::new();
        let next_depth = job.depth + 1;

        for link in extracted_links.iter() {
            if let Ok(absolute_url) = BfsCrawler::convert_to_absolute_url(link, &effective_base)
                && BfsCrawler::is_same_domain(&absolute_url, &job.start_url_domain)
                    && BfsCrawler::should_crawl_url(&absolute_url)
                {
                    discovered_links.push((absolute_url, next_depth, Some(job.url.clone())));
                }
        }

        let extracted_count = extracted_links.len();
        let discovered_count = discovered_links.len();

        if extracted_count > 0 || discovered_count > 0 {
            eprintln!("[PARSE] URL: {} | Extracted: {} links | Discovered (same-domain): {} (no link budget) | Title: {:?}",
                job_url_for_logging, extracted_count, discovered_count, extracted_title);
        }

        // Record crawl attempt
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

        // Add discovered links to frontier
        if !discovered_links.is_empty() {
            let added = frontier.add_links(discovered_links).await;
            if added > 0 {
                eprintln!("[FRONTIER] Added {} discovered links from {}", added, job_url_for_logging);
                // Record URL discovery for plateau detection
                metrics.record_url_discovery(added);
            }
        } else if extracted_count > 0 {
            eprintln!("[FRONTIER] No same-domain links discovered from {}", job_url_for_logging);
        }

        metrics.urls_processed_total.lock().inc();
    }

    /// Check if crawl should exit based on limits
    fn should_exit_crawl(&self, start: SystemTime, processed_count: usize) -> Option<String> {
        // Check max_urls limit
        if let Some(max_urls) = self.config.max_urls
            && processed_count >= max_urls {
                return Some(format!("Reached max_urls limit of {}", max_urls));
            }

        // Check duration limit
        if let Some(duration_secs) = self.config.duration_secs {
            let elapsed = start.elapsed().unwrap_or_default().as_secs();
            if elapsed >= duration_secs {
                return Some(format!("Reached duration limit of {}s (elapsed: {}s)", duration_secs, elapsed));
            }
        }

        None
    }

    /// Spawn a new crawl task for a URL
    fn spawn_crawl_task(
        &self,
        in_flight_tasks: &mut JoinSet<CrawlResult>,
        host: String,
        url: String,
        depth: u32,
        parent_url: Option<String>,
        backpressure_permit: crate::frontier::FrontierPermit,
        start_url_domain: String,
        parse_tx: mpsc::Sender<ParseJob>,
    ) {
        let task_state = self.clone();
        let task_host = host.clone();
        let task_permits = Arc::clone(&self.crawler_permits);

        in_flight_tasks.spawn(async move {
            // Create panic guard to ensure inflight counter is decremented
            struct InflightGuard {
                frontier: Arc<ShardedFrontier>,
                host: String,
                completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
            }

            impl Drop for InflightGuard {
                fn drop(&mut self) {
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
                url,
                depth,
                _parent_url: parent_url,
                start_url_domain,
                network_permits: task_permits,
                backpressure_permit,
                parse_sender: parse_tx,
            };

            let result = task_state.process_url_streaming(task).await;
            completed.store(true, std::sync::atomic::Ordering::Relaxed);
            result
        });
    }

    /// Handle a completed crawl task result
    fn handle_task_result(
        &self,
        crawl_result: CrawlResult,
        processed_count: &mut usize,
        successful_count: &mut usize,
        failed_count: &mut usize,
        timeout_count: &mut usize,
    ) {
        *processed_count += 1;
        self.metrics.urls_processed_total.lock().inc();

        match crawl_result.result {
            Ok(_discovered_links) => {
                *successful_count += 1;
                self.frontier.record_success(&crawl_result.host, crawl_result.latency_ms);
                // Note: links will be added by parser, not here
            }
            Err(ref e) => {
                self.handle_crawl_error(e, &crawl_result, timeout_count, failed_count, false);
            }
        }
    }

    /// Print periodic progress report
    fn print_progress_report(
        &self,
        start: SystemTime,
        processed_count: usize,
        successful_count: usize,
        failed_count: usize,
        timeout_count: usize,
        stats: &NodeMapStats,
    ) {
        let frontier_stats = self.frontier.stats();
        let success_rate = if processed_count > 0 {
            successful_count as f64 / processed_count as f64 * 100.0
        } else {
            0.0
        };

        let elapsed = start.elapsed().unwrap_or_default().as_secs();
        let rate = if elapsed > 0 {
            processed_count as f64 / elapsed as f64
        } else {
            0.0
        };

        let time_remaining = if let Some(dur) = self.config.duration_secs {
            if dur > elapsed {
                format!("{}s remaining", dur - elapsed)
            } else {
                "EXPIRED".to_string()
            }
        } else {
            "unlimited".to_string()
        };

        println!();
        println!("================================================================================");
        println!("  PROGRESS REPORT ({}s elapsed, {})", elapsed, time_remaining);
        println!("================================================================================");
        println!("  URLs Processed: {} ({:.1}/sec) | Success: {} | Failed: {} | Timeout: {}",
            processed_count, rate, successful_count, failed_count, timeout_count);
        println!("  Success Rate: {:.1}% | Total Discovered: {}", success_rate, stats.total_nodes);
        println!("  Frontier: {} queued | {} hosts | {} with work | {} in backoff",
            frontier_stats.total_queued, frontier_stats.total_hosts,
            frontier_stats.hosts_with_work, frontier_stats.hosts_in_backoff);
        println!("================================================================================");
    }

    /// Finalize crawl and export results
    async fn finalize_crawl(
        &self,
        save_task: Option<tokio::task::JoinHandle<()>>,
        stats: &NodeMapStats,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.stop().await;

        if let Some(task) = save_task {
            task.abort();
            if let Err(e) = task.await
                && !e.is_cancelled() {
                    eprintln!("Save task error: {}", e);
                }
        }

        // Export results to JSONL
        let output_path = "crawl_results.jsonl";
        eprintln!("Exporting results to {}...", output_path);
        if let Err(e) = self.export_to_jsonl(output_path).await {
            eprintln!("Warning: Failed to export results: {}", e);
        } else {
            eprintln!("Successfully exported {} nodes to {}", stats.total_nodes, output_path);
        }

        Ok(())
    }

    // Main crawl loop. Coordinates scheduling, progress, and shutdown.
    #[tracing::instrument(skip(self), fields(start_url = %self.start_url))]
    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start = SystemTime::now();
        self.running.store(true, Ordering::SeqCst);

        tracing::info!("Crawl started with max_workers={}", self.config.max_workers);
        println!("================================================================================");
        println!("CRAWL STARTED");
        println!("================================================================================");

        // Initialize crawl components
        let save_task = self.spawn_auto_save_task();
        let start_url_domain = self.get_domain(&self.start_url);
        let mut work_rx = self.take_work_receiver()?;
        let (parse_tx, parse_rx) = mpsc::channel::<ParseJob>(512);

        // Spawn parser dispatcher
        self.spawn_parser_dispatcher(
            parse_rx,
            Arc::clone(&self.frontier),
            Arc::clone(&self.writer_thread),
            Arc::clone(&self.metrics),
        );

        // Initialize crawl state tracking
        let mut in_flight_tasks = JoinSet::new();
        let max_concurrent = self.config.max_workers as usize;
        let mut processed_count = 0;
        let mut successful_count = 0;
        let mut failed_count = 0;
        let mut timeout_count = 0;
        let mut last_progress_report = std::time::Instant::now();

        loop {
            // Exit if stopped or limits reached
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Some(reason) = self.should_exit_crawl(start, processed_count) {
                eprintln!("{}, stopping crawl", reason);
                break;
            }

            // Wait for a new URL or a completed task.
            tokio::select! {
                // Spawn tasks up to the concurrency limit.
                next_url = work_rx.recv(), if in_flight_tasks.len() < max_concurrent => {
                    if let Some((host, url, depth, parent_url, backpressure_permit)) = next_url {
                        self.spawn_crawl_task(
                            &mut in_flight_tasks,
                            host,
                            url,
                            depth,
                            parent_url,
                            backpressure_permit,
                            start_url_domain.clone(),
                            parse_tx.clone(),
                        );
                    }
                }

                // Process completed tasks.
                Some(result) = in_flight_tasks.join_next(), if !in_flight_tasks.is_empty() => {
                    match result {
                        Ok(crawl_result) => {
                            self.handle_task_result(
                                crawl_result,
                                &mut processed_count,
                                &mut successful_count,
                                &mut failed_count,
                                &mut timeout_count,
                            );

                            // Print progress periodically
                            if processed_count % PROGRESS_INTERVAL == 0
                                || last_progress_report.elapsed().as_secs() >= PROGRESS_TIME_SECS
                            {
                                let stats = self.get_stats().await;
                                self.print_progress_report(
                                    start,
                                    processed_count,
                                    successful_count,
                                    failed_count,
                                    timeout_count,
                                    &stats,
                                );
                                last_progress_report = std::time::Instant::now();
                            }
                        }
                        Err(e) if e.is_panic() => {
                            eprintln!("Task panicked: {:?}", e);
                            eprintln!("WARNING: Task panic detected - host inflight counter may have leaked");
                            eprintln!("Note: backpressure_permit auto-releases on panic unwinding");
                        }
                        Err(e) => {
                            eprintln!("Task join error: {}", e);
                        }
                    }
                }

                // Check for crawl completion using CompletionDetector
                else => {
                    let frontier_stats = self.frontier.stats();
                    let inflight_count = in_flight_tasks.len();

                    // Get total discovered URLs from state
                    let total_discovered = self.metrics.urls_discovered_total.lock().value as usize;

                    // Build completion signals
                    let signals = CompletionSignals {
                        frontier_empty: self.frontier.is_empty(),
                        inflight_count,
                        seconds_since_last_discovery: self.metrics.seconds_since_last_discovery(),
                        total_discovered,
                        total_crawled: processed_count,
                    };

                    // Check completion with multi-signal detector
                    match self.completion_detector.check_completion(&signals) {
                        Some(true) => {
                            // Crawl is complete!
                            eprintln!("================================================");
                            eprintln!("   GRACEFUL SHUTDOWN: Crawl Complete");
                            eprintln!("================================================");
                            eprintln!("  Completion verified with grace period");
                            eprintln!("  Total URLs processed: {}", processed_count);
                            eprintln!("  Total URLs discovered: {}", total_discovered);
                            eprintln!("  Successful: {}", successful_count);
                            eprintln!("  Failed: {}", failed_count);
                            eprintln!("  Timeout: {}", timeout_count);
                            if let Some(secs_since_discovery) = self.metrics.seconds_since_last_discovery() {
                                eprintln!("  Last discovery: {} seconds ago", secs_since_discovery);
                            }
                            eprintln!("================================================");
                            break;
                        }
                        Some(false) => {
                            // Not complete yet, continue crawling
                        }
                        None => {
                            // Insufficient data to determine (early in crawl)
                        }
                    }

                    // Log status when waiting for work (show what we're waiting for)
                    if !signals.frontier_empty || inflight_count > 0 {
                        static LAST_LOG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let last = LAST_LOG.load(std::sync::atomic::Ordering::Relaxed);

                        // Log every 2 seconds when idle (reduced from 10s for better debugging)
                        if now - last >= 2 {
                            LAST_LOG.store(now, std::sync::atomic::Ordering::Relaxed);
                            let plateau_info = if let Some(secs) = self.metrics.seconds_since_last_discovery() {
                                format!(", last_discovery={}s_ago", secs)
                            } else {
                                "".to_string()
                            };
                            eprintln!(
                                "[WAITING] frontier_empty={}, tasks_in_flight={}, total_queued={}, hosts_with_work={}, hosts_in_backoff={}{}",
                                signals.frontier_empty, inflight_count, frontier_stats.total_queued,
                                frontier_stats.hosts_with_work, frontier_stats.hosts_in_backoff, plateau_info
                            );
                        }
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

        // Finalize crawl and export results
        self.finalize_crawl(save_task, &stats).await?;

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
            // HTTP status errors - classify by status code
            FetchError::HttpStatus(status_code, _) => {
                match status_code {
                    // Transient HTTP errors (5xx server errors, rate limiting)
                    500..=599 | 429 => {
                        *timeout_count += 1;
                        self.metrics.urls_timeout_total.lock().inc();
                        false // Retry later, don't block host
                    }
                    // Permanent HTTP errors (3xx redirects, 4xx client errors)
                    _ => {
                        *failed_count += 1;
                        self.metrics.urls_failed_total.lock().inc();
                        true // Count towards host blocking
                    }
                }
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

    /// Acquire distributed lock for URL to prevent duplicate crawling
    async fn acquire_crawl_lock(
        &self,
        url: &str,
        cancel_token: CancellationToken,
        lost_lock: Arc<std::sync::atomic::AtomicBool>,
    ) -> Option<crate::url_lock_manager::CrawlLock> {
        if let Some(lock_manager) = &self.lock_manager {
            match crate::url_lock_manager::CrawlLock::acquire(
                Arc::clone(lock_manager),
                url.to_string(),
                cancel_token,
                lost_lock,
            )
            .await
            {
                Ok(Some(guard)) => Some(guard),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("Redis lock error for {}: {}. Proceeding without lock", url, e);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Acquire network permit with timeout
    async fn acquire_network_permit(
        &self,
        network_permits: &Arc<tokio::sync::Semaphore>,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, FetchError> {
        match tokio::time::timeout(
            Duration::from_secs(30),
            network_permits.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(FetchError::PermitAcquisition),
            Err(_) => Err(FetchError::PermitTimeout),
        }
    }

    /// Fetch URL and return the response (record all attempts, not just HTML)
    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn fetch_url(&self, url: &str) -> Result<reqwest::Response, FetchError> {
        tracing::debug!("Fetching URL");
        let response = self.http.fetch_stream(url).await?;
        self.metrics.urls_fetched_total.lock().inc();
        tracing::trace!("URL fetch successful, status={}", response.status());
        Ok(response)
    }

    /// Check if a response is HTML content that should be parsed
    fn is_parseable_html(response: &reqwest::Response) -> bool {
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok());

        match content_type {
            Some(ct) => url_utils::is_html_content_type(ct),
            None => true, // No content-type, assume HTML
        }
    }

    /// Track HTTP version metrics
    fn track_http_version(&self, version: reqwest::Version) {
        match version {
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
    }

    /// Download response body as bytes
    async fn download_body(&self, response: reqwest::Response) -> Result<Vec<u8>, FetchError> {
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| FetchError::BodyError(e.to_string()))
    }

    /// Create and send parse job to parser workers
    async fn enqueue_parse_job(
        &self,
        parse_sender: &tokio::sync::mpsc::Sender<ParseJob>,
        host: String,
        url: String,
        depth: u32,
        parent_url: Option<String>,
        start_url_domain: String,
        html_bytes: Vec<u8>,
        content_type: Option<String>,
        status_code: u16,
    ) -> Result<(), FetchError> {
        let total_bytes = html_bytes.len();
        let job = ParseJob {
            _host: host,
            url: url.clone(),
            depth,
            _parent_url: parent_url,
            start_url_domain,
            html_bytes,
            content_type,
            status_code,
            total_bytes,
        };

        parse_sender
            .send(job)
            .await
            .map_err(|_| {
                eprintln!("Failed to enqueue parse job for {}: parse queue closed", url);
                FetchError::BodyError("parse queue closed".to_string())
            })
    }

    async fn process_url_streaming(&self, task: CrawlTask) -> CrawlResult {
        let start_time = std::time::Instant::now();
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
        use std::sync::atomic::AtomicBool;

        // Setup cancellation token and lock tracking
        let cancel_token = CancellationToken::new();
        let lost_lock = Arc::new(AtomicBool::new(false));

        // Acquire distributed lock
        let _lock_guard = self.acquire_crawl_lock(&url, cancel_token, Arc::clone(&lost_lock)).await;
        if _lock_guard.is_none() && self.lock_manager.is_some() {
            // Lock was already held by another worker
            return CrawlResult {
                host,
                result: Err(FetchError::AlreadyLocked),
                latency_ms: 0,
            };
        }

        // Acquire network permit
        let _network_permit = match self.acquire_network_permit(&network_permits).await {
            Ok(permit) => permit,
            Err(e) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(e),
                    latency_ms,
                };
            }
        };

        // Fetch URL - always record the attempt regardless of status or content type
        let response = match self.fetch_url(&url).await {
            Ok(resp) => resp,
            Err(e) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(e),
                    latency_ms,
                };
            }
        };

        // Track HTTP version metrics
        self.track_http_version(response.version());

        // Release network permit early
        drop(_network_permit);

        // Keep permits/guards alive for the entire function
        let _backpressure_permit = backpressure_permit;

        // Get response metadata
        let status_code = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let is_success = response.status().is_success() || status_code == 400;
        let is_html = Self::is_parseable_html(&response);

        // Download body (for all responses, not just HTML)
        let body_bytes = match self.download_body(response).await {
            Ok(bytes) => bytes,
            Err(e) => {
                // Record the attempt even if body download fails
                let normalized_url = SitemapNode::normalize_url(&url);
                let _ = self.writer_thread.send_event_async(StateEvent::CrawlAttemptFact {
                    url_normalized: normalized_url,
                    status_code,
                    content_type,
                    content_length: None,
                    title: None,
                    link_count: 0,
                    response_time_ms: Some(start_time.elapsed().as_millis() as u64),
                }).await;

                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(e),
                    latency_ms,
                };
            }
        };

        let content_length = body_bytes.len();

        // Determine crawl outcome based on status code and content type
        let crawl_outcome = if !is_success {
            // Non-2xx response (3xx redirect, 4xx client error, 5xx server error)
            CrawlOutcome::NonSuccessStatus
        } else if !is_html {
            // 2xx response but non-HTML content (PDF, image, etc.)
            CrawlOutcome::NonHtmlContent
        } else {
            // 2xx HTML response - enqueue for parsing
            CrawlOutcome::Success
        };

        match crawl_outcome {
            CrawlOutcome::Success => {
                // Enqueue parse job for HTML content
                if let Err(e) = self
                    .enqueue_parse_job(
                        &parse_sender,
                        host.clone(),
                        url.clone(),
                        depth,
                        _parent_url,
                        start_url_domain,
                        body_bytes,
                        content_type,
                        status_code,
                    )
                    .await
                {
                    let latency_ms = start_time.elapsed().as_millis() as u64;
                    return CrawlResult {
                        host,
                        result: Err(e),
                        latency_ms,
                    };
                }
                // Parser will record the crawl attempt with title and link count
            }
            CrawlOutcome::NonSuccessStatus | CrawlOutcome::NonHtmlContent => {
                // Record the crawl attempt immediately (no parsing needed)
                let normalized_url = SitemapNode::normalize_url(&url);
                let _ = self.writer_thread.send_event_async(StateEvent::CrawlAttemptFact {
                    url_normalized: normalized_url,
                    status_code,
                    content_type,
                    content_length: Some(content_length),
                    title: None,
                    link_count: 0,
                    response_time_ms: Some(start_time.elapsed().as_millis() as u64),
                }).await;
            }
        }

        // Return result based on outcome
        let latency_ms = start_time.elapsed().as_millis() as u64;
        CrawlResult {
            host,
            result: match crawl_outcome {
                CrawlOutcome::Success => Ok(Vec::new()),
                CrawlOutcome::NonSuccessStatus => Err(FetchError::from_status_code(status_code)),
                CrawlOutcome::NonHtmlContent => Ok(Vec::new()), // Not an error, just non-HTML
            },
            latency_ms,
        }
        // Permits and lock guard dropped here
    }

    fn spawn_auto_save_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        if self.config.save_interval == 0 {
            return None;
        }

        let interval = Duration::from_secs(self.config.save_interval);
        let running = Arc::clone(&self.running);
        let crawler_clone = self.clone();

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

                // Perform incremental export to prevent data loss on crash
                let export_path = "crawl_results_incremental.jsonl";
                match crawler_clone.export_incremental(export_path).await {
                    Ok(count) => {
                        if count > 0 {
                            eprintln!("[AUTO-SAVE] Incremental export completed: {} new nodes saved", count);
                        }
                    }
                    Err(e) => {
                        eprintln!("[AUTO-SAVE] WARNING: Incremental export failed: {}", e);
                        // Continue running despite export failure
                    }
                }
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

    /// Incremental export: append only newly crawled nodes since last export
    /// Uses atomic file writes (temp + rename) to prevent corruption on crash
    pub async fn export_incremental<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        use std::fs::OpenOptions;
        use std::io::{BufWriter, Write};
        use std::cell::RefCell;

        let output_path = output_path.as_ref();
        let temp_path = output_path.with_extension("jsonl.tmp");

        // Get last export timestamp (or 0 if never exported)
        let last_export_ts = self.state.get_last_export_timestamp()?.unwrap_or(0);
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Open output file in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&temp_path)?;

        let mut writer = BufWriter::new(file);
        let count = RefCell::new(0usize);

        // Export only nodes crawled since last export
        let node_iter = self.state.iter_nodes()?;
        node_iter.for_each(|node| {
            // Only export nodes that were crawled after last export
            if let Some(crawled_at) = node.crawled_at
                && crawled_at > last_export_ts {
                    let json = serde_json::to_string(&node)
                        .map_err(|e| crate::state::StateError::Serialization(
                            format!("Serialization error: {}", e)
                        ))?;

                    writeln!(writer, "{}", json)
                        .map_err(|e| crate::state::StateError::Serialization(
                            format!("Write error: {}", e)
                        ))?;

                    let mut count_mut = count.borrow_mut();
                    *count_mut += 1;
                }
            Ok(())
        })?;

        writer.flush()?;
        drop(writer);

        // Atomic rename: only replace output file if write succeeded
        if temp_path.exists() {
            if output_path.exists() {
                // Append temp contents to existing file
                std::fs::copy(&temp_path, output_path)?;
            } else {
                // First export - just rename temp file
                std::fs::rename(&temp_path, output_path)?;
            }
            // Clean up temp file
            let _ = std::fs::remove_file(&temp_path);
        }

        // Update last export timestamp
        self.state.update_last_export_timestamp(now_ts)?;

        let exported_count = *count.borrow();
        if exported_count > 0 {
            eprintln!("[INCREMENTAL EXPORT] Exported {} new nodes to {}",
                exported_count, output_path.display());
        }

        Ok(exported_count)
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
