use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::common_crawl_seeder::CommonCrawlSeeder;
use crate::ct_log_seeder::CtLogSeeder;
use crate::frontier::ShardedFrontier;
use crate::network::{FetchError, HttpClient};
use crate::sitemap_seeder::SitemapSeeder;
use crate::state::{CrawlerState, SitemapNode, StateEvent};
use crate::url_lock_manager::UrlLockManager;
use crate::url_utils;

pub const PROGRESS_INTERVAL: usize = 100;
pub const PROGRESS_TIME_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMapStats {
    pub total_nodes: usize,
    pub crawled_nodes: usize,
}

impl std::fmt::Display for NodeMapStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Nodes: {} total, {} crawled", self.total_nodes, self.crawled_nodes)
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
            timeout: 45,
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
    http: Arc<HttpClient>,
    start_url: String,
    running: Arc<Mutex<bool>>,
    lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
    writer_thread: Arc<crate::writer_thread::WriterThread>,
    metrics: Arc<crate::metrics::Metrics>,
    crawler_permits: Arc<tokio::sync::Semaphore>, // Governor adjusts this to throttle work without rebuilding the crawler.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfsCrawlerResult {
    pub start_url: String,
    pub discovered: usize,
    pub processed: usize,
    pub duration_secs: u64,
    pub stats: NodeMapStats,
}

struct CrawlResult {
    host: String,
    result: Result<Vec<(String, u32, Option<String>)>, String>,
    latency_ms: u64,
}

struct CrawlTask {
    host: String,
    url: String,
    depth: u32,
    _parent_url: Option<String>,
    start_url_domain: String,
    network_permits: Arc<tokio::sync::Semaphore>,
    _backpressure_permit: tokio::sync::OwnedSemaphorePermit,
}

impl BfsCrawler {
    pub fn new(
        config: BfsCrawlerConfig,
        start_url: String,
        http: Arc<HttpClient>,
        state: Arc<CrawlerState>,
        frontier: Arc<ShardedFrontier>,
        writer_thread: Arc<crate::writer_thread::WriterThread>,
        lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
        metrics: Arc<crate::metrics::Metrics>,
        crawler_permits: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            config,
            start_url,
            http,
            state,
            frontier,
            writer_thread,
            running: Arc::new(Mutex::new(false)),
            lock_manager,
            metrics,
            crawler_permits, // Share the permit pool so other components can tighten concurrency.
        }
    }

    pub async fn initialize(
        &mut self,
        seeding_strategy: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_url_domain = self.get_domain(&self.start_url);

        // Derive the root domain so non-sitemap seeders can aim at the base domain.
        let root_domain = self.get_root_domain(&start_url_domain);

        // Note: Sharded frontier does not persist state yet, so we always seed.
        // TODO: Implement frontier state persistence for resume support.

        let mut seed_links: Vec<(String, u32, Option<String>)> = Vec::new();

        // Select the seeders requested by the strategy to avoid unnecessary network calls.
        let mut seeders: Vec<Box<dyn crate::seeder::Seeder>> = Vec::new();

        if (seeding_strategy == "sitemap" || seeding_strategy == "all")
            && !self.config.ignore_robots
        {
            eprintln!("Enabling seeder: sitemap");
            seeders.push(Box::new(SitemapSeeder::new((*self.http).clone())));
        }

        if seeding_strategy == "ct" || seeding_strategy == "all" {
            eprintln!("Enabling seeder: ct-logs (for root domain: {})", root_domain);
            seeders.push(Box::new(CtLogSeeder::new((*self.http).clone())));
        }

        if seeding_strategy == "commoncrawl" || seeding_strategy == "all" {
            eprintln!("Enabling seeder: common-crawl (for root domain: {})", root_domain);
            seeders.push(Box::new(CommonCrawlSeeder::new((*self.http).clone())));
        }

        // Execute each chosen seeder to feed the initial frontier.
        if !seeders.is_empty() {
            eprintln!("Running {} seeder(s)...", seeders.len());
            use futures_util::StreamExt;

            for seeder in seeders {
                // Hand sitemap seeding the full URL while other seeders only need the root.
                let domain_to_seed = if seeder.name() == "sitemap" {
                    &self.start_url
                } else {
                    &root_domain
                };

                let seeder_name = seeder.name();
                let mut url_stream = seeder.seed(domain_to_seed);
                let mut url_count = 0;

                // Stream URLs and add them to the frontier in batches to avoid memory buildup.
                while let Some(url_result) = url_stream.next().await {
                    match url_result {
                        Ok(url) => {
                            seed_links.push((url, 0, None));
                            url_count += 1;

                            // Flush to frontier every 1000 URLs to prevent unbounded memory growth.
                            if seed_links.len() >= 1000 {
                                self.frontier.add_links(std::mem::take(&mut seed_links)).await;
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

        // Push pre-seeded links so the crawl queue is warm before starting.
        if !seed_links.is_empty() {
            let added = self.frontier.add_links(seed_links).await;
            eprintln!(
                "Pre-seeded {} total URLs using strategy '{}'",
                added, seeding_strategy
            );
        }

        // Always queue the explicit start URL so it is processed even without seeders.
        let start_links = vec![(self.start_url.clone(), 0, None)];
        self.frontier.add_links(start_links).await;

        // Note: robots.txt handling is now done per-shard in the FrontierShard.
        // TODO: Pre-fetch robots.txt here for the start domain if needed.

        Ok(())
    }

    // Orchestrate the crawl loop so scheduling, progress, and shutdown remain coordinated.
    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start = SystemTime::now();
        {
            let mut r = self.running.lock();
            *r = true;
        }

        let save_task = self.spawn_auto_save_task();
        let start_url_domain = self.get_domain(&self.start_url);

        let mut in_flight_tasks = JoinSet::new();
        let max_concurrent = self.config.max_workers as usize;

        let mut processed_count = 0;
        let mut last_progress_report = std::time::Instant::now();

        loop {
            // Exit the loop once another component marks the crawler as stopped.
            {
                let running = self.running.lock();
                if !*running {
                    break;
                }
            }

            // Spawn workers until we hit the concurrency ceiling to keep resources busy.
            while in_flight_tasks.len() < max_concurrent {
                let next_url = self.frontier.get_next_url().await;

                match next_url {
                    Some((host, url, depth, parent_url, backpressure_permit)) => {
                        let task_state = self.clone();
                        let task_host = host.clone();
                        let task_url = url.clone();
                        let task_depth = depth;
                        let task_parent = parent_url.clone();
                        let task_domain = start_url_domain.clone();
                        let task_permits = Arc::clone(&self.crawler_permits);

                        in_flight_tasks.spawn_local(async move {
                            let task = CrawlTask {
                                host: task_host,
                                url: task_url,
                                depth: task_depth,
                                _parent_url: task_parent,
                                start_url_domain: task_domain,
                                network_permits: task_permits,
                                _backpressure_permit: backpressure_permit,
                            };
                            task_state.process_url_streaming(task).await
                        });
                    }
                    None => {
                        break;
                    }
                }
            }

            // Drain finished tasks so their results update shared state promptly.
            if let Some(result) = in_flight_tasks.join_next().await {
                match result {
                    Ok(crawl_result) => {
                        processed_count += 1;

                        match crawl_result.result {
                            Ok(discovered_links) => {
                                self.frontier.record_success(&crawl_result.host, crawl_result.latency_ms);

                                if !discovered_links.is_empty() {
                                    self.frontier
                                        .add_links(discovered_links)
                                        .await;
                                }
                            }
                            Err(e) => {
                                self.frontier.record_failure(&crawl_result.host, crawl_result.latency_ms);
                                eprintln!("{} - {}", crawl_result.host, e);
                            }
                        }

                        // Periodically print progress so operators see crawl velocity and backlog.
                        if processed_count % PROGRESS_INTERVAL == 0
                            || last_progress_report.elapsed().as_secs() >= PROGRESS_TIME_SECS
                        {
                            let stats = self.get_stats().await;
                            let frontier_stats = self.frontier.stats();
                            eprintln!(
                                "Progress: {} processed | {} | {}",
                                processed_count, stats, frontier_stats
                            );
                            last_progress_report = std::time::Instant::now();
                        }
                    }
                    Err(e) => {
                        eprintln!("Task join error: {}", e);
                    }
                }
            }

            // Break once no tasks remain in flight and the frontier is empty.
            if self.frontier.is_empty() && in_flight_tasks.is_empty() {
                eprintln!("Crawl complete: frontier empty and no tasks in flight");
                break;
            }

            // Pause briefly to avoid spinning while waiting for new work.
            if in_flight_tasks.is_empty() && self.frontier.is_empty() {
                sleep(Duration::from_millis(100)).await;
            }
        }

        // Collect any straggling tasks to ensure their discoveries reach the frontier.
        while let Some(result) = in_flight_tasks.join_next().await {
            if let Ok(crawl_result) = result {
                match crawl_result.result {
                    Ok(discovered_links) => {
                        self.frontier.record_success(&crawl_result.host, crawl_result.latency_ms);
                        if !discovered_links.is_empty() {
                            self.frontier
                                .add_links(discovered_links)
                                .await;
                        }
                    }
                    Err(_) => {
                        self.frontier.record_failure(&crawl_result.host, crawl_result.latency_ms);
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

        let result = BfsCrawlerResult {
            start_url: self.start_url.clone(),
            discovered: stats.total_nodes,
            processed: processed_count,
            duration_secs: elapsed.as_secs(),
            stats: stats.clone(),
        };

        Ok(result)
    }

    async fn send_event_if_alive(
        &self,
        cancel_token: &CancellationToken,
        lost_lock: &Arc<std::sync::atomic::AtomicBool>,
        event: StateEvent,
    ) {
        use std::sync::atomic::Ordering;

        // Check if we're a zombie task (lock lost or cancelled)
        if cancel_token.is_cancelled() || lost_lock.load(Ordering::Relaxed) {
            // Optionally send a terminal "Aborted" fact here
            eprintln!("Zombie task detected - not sending event for URL");
            return; // Do not send, we are a zombie
        }

        // Send the event normally
        let _ = self.writer_thread.send_event_async(event).await;
    }

    async fn process_url_streaming(&self, task: CrawlTask) -> CrawlResult {
        let CrawlTask {
            host,
            url,
            depth,
            _parent_url,
            start_url_domain,
            network_permits,
            _backpressure_permit,
        } = task;
        use lol_html::{element, HtmlRewriter, Settings};
        use std::sync::Arc as StdArc;
        use parking_lot::Mutex as ParkingMutex;
        use crate::url_lock_manager::CrawlLock;
        use std::sync::atomic::AtomicBool;

        // Share a cancellation token between the parser timeout and lock renewal so either condition aborts work.
        let cancel_token = CancellationToken::new();

        // Track whether we've lost the lock (set by renewal task)
        let lost_lock = Arc::new(AtomicBool::new(false));

        // Acquire the RAII CrawlLock guard so Redis renewals stay automatic and no zombie locks stick around.
        // Feeding the same cancellation token into the guard lets lock loss cancel the parser immediately.
        let _lock_guard = if let Some(lock_manager) = &self.lock_manager {
            match CrawlLock::acquire(Arc::clone(lock_manager), url.clone(), cancel_token.clone(), Arc::clone(&lost_lock)).await {
                Ok(Some(guard)) => Some(guard),
                Ok(None) => {
                    // If another worker holds the URL we bail early to avoid double crawling.
                    return CrawlResult {
                        host,
                        result: Err("Already locked".to_string()),
                        latency_ms: 0,
                    };
                }
                Err(e) => {
                    eprintln!("Redis lock error for {}: {}. Proceeding without lock", url, e);
                    None
                }
            }
        } else {
            None
        };

        let start_time = std::time::Instant::now();

        // Acquire permit only for the network call to control active socket pressure.
        let _network_permit = match network_permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err("Failed to acquire network permit".to_string()),
                    latency_ms,
                };
            }
        };

        // Stream the response so we never buffer huge pages into memory at once.
        let fetch_result = match self.http.fetch_stream(&url).await {
            Ok(response) if response.status().as_u16() == 200 => {
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
            Err(FetchError::Timeout) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err("Timeout".to_string()),
                    latency_ms,
                };
            }
            Err(FetchError::NetworkError(e)) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(format!("Network error: {}", e)),
                    latency_ms,
                };
            }
            Err(FetchError::ContentTooLarge(size, max)) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(format!("Content too large: {} bytes (max: {})", size, max)),
                    latency_ms,
                };
            }
            Err(e) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                return CrawlResult {
                    host,
                    result: Err(format!("Fetch error: {}", e)),
                    latency_ms,
                };
            }
        };

        // Release the network permit immediately after the fetch completes.
        drop(_network_permit);

        // Keep the _lock_guard alive for this entire function so renewals continue and the lock releases automatically.

        if let Some(response) = fetch_result {
            let status_code = response.status().as_u16();

            // Track HTTP version for transport observability.
            match response.version() {
                reqwest::Version::HTTP_09 | reqwest::Version::HTTP_10 | reqwest::Version::HTTP_11 => {
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

            // Detect content encoding BEFORE consuming the response
            let content_encoding = response
                .headers()
                .get("content-encoding")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_lowercase());

            // Parse the body in a streaming fashion so other tasks stay responsive.
            let bytes_stream = response.bytes_stream();

            // Spawn a 30-second watchdog that cancels via the shared token if parsing stalls or the lock is lost.
            let cancel_clone = cancel_token.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                cancel_clone.cancel();
            });

            // Convert the response into AsyncRead so HtmlRewriter can pull bytes incrementally.
            use futures_util::TryStreamExt;
            use tokio_util::io::StreamReader;
            let stream_reader = StreamReader::new(bytes_stream.map_err(std::io::Error::other));

            // Create a decoder based on content-encoding header
            use async_compression::tokio::bufread::{
                GzipDecoder, BrotliDecoder, DeflateDecoder, ZstdDecoder,
            };
            use tokio::io::{AsyncRead, BufReader as TokioBufReader};

            // Track which codec we're using for metrics (using as_deref to avoid clone)
            let codec_start = std::time::Instant::now();

            let decoded_reader: Box<dyn AsyncRead + Unpin + Send> = match content_encoding.as_deref() {
                Some("gzip") | Some("x-gzip") => {
                    Box::new(GzipDecoder::new(TokioBufReader::new(stream_reader)))
                }
                Some("br") => {
                    Box::new(BrotliDecoder::new(TokioBufReader::new(stream_reader)))
                }
                Some("deflate") => {
                    Box::new(DeflateDecoder::new(TokioBufReader::new(stream_reader)))
                }
                Some("zstd") => {
                    Box::new(ZstdDecoder::new(TokioBufReader::new(stream_reader)))
                }
                _ => {
                    // No compression or identity encoding
                    Box::new(stream_reader)
                }
            };

            // Wrap the decoded stream in BufReader to reduce syscalls while still streaming.
            let buf_reader = tokio::io::BufReader::new(decoded_reader);

            let mut total_bytes = 0usize;
            let mut handler_count = 0usize;
            const MAX_HANDLERS: usize = 10_000; // Cap handler invocations to detect parser bombs before they explode CPU usage.
            const CHUNK_SIZE: usize = 16 * 1024; // Use 16KB chunks to feed parser budget and maintain cooperative scheduling.

            // Parse chunk-by-chunk to keep memory usage bounded.
            let (extracted_links, extracted_title) = {
                // Track hrefs so we can enqueue new crawl targets.
                let links = StdArc::new(ParkingMutex::new(Vec::new()));
                let links_clone = StdArc::clone(&links);

                // Track title text so we can enrich sitemap output with metadata.
                let title_chunks = StdArc::new(ParkingMutex::new(Vec::new()));
                let title_chunks_clone = StdArc::clone(&title_chunks);
                let in_title = StdArc::new(ParkingMutex::new(false));

                // Install HtmlRewriter handlers dedicated to link discovery and title capture.
                let mut rewriter = HtmlRewriter::new(
                    Settings {
                        element_content_handlers: vec![
                            element!("a[href]", move |el| {
                                if let Some(href) = el.get_attribute("href") {
                                    links_clone.lock().push(href);
                                }
                                Ok(())
                            }),
                            element!("title", {
                                let in_title_start = StdArc::clone(&in_title);
                                move |_el| {
                                    *in_title_start.lock() = true;
                                    Ok(())
                                }
                            }),
                        ],
                        ..Settings::default()
                    },
                    {
                        let title_chunks = StdArc::clone(&title_chunks);
                        let in_title = StdArc::clone(&in_title);
                        move |text: &[u8]| {
                            if *in_title.lock() {
                                if let Ok(text_str) = std::str::from_utf8(text) {
                                    title_chunks.lock().push(text_str.to_string());
                                }
                                *in_title.lock() = false;
                            }
                        }
                    },
                );

                // Pull chunks from the reader while yielding in between to stay cooperative.
                use tokio::io::AsyncReadExt;
                let mut chunk_buf = vec![0u8; CHUNK_SIZE];
                let mut buf_reader = buf_reader;

                loop {
                    // Abort immediately when the shared cancellation token fires.
                    if cancel_token.is_cancelled() {
                        self.metrics.parser_abort_timeout.lock().inc();
                        let latency_ms = start_time.elapsed().as_millis() as u64;
                        return CrawlResult {
                            host,
                            result: Err("Parser cancelled (timeout or lock lost)".to_string()),
                            latency_ms,
                        };
                    }

                    // Read another slice so parsing can proceed without buffering the whole response.
                    let n = match buf_reader.read(&mut chunk_buf).await {
                        Ok(0) => break, // No more bytes means the response is complete.
                        Ok(n) => n,
                        Err(e) => {
                            let latency_ms = start_time.elapsed().as_millis() as u64;
                            return CrawlResult {
                                host,
                                result: Err(format!("Stream read error: {}", e)),
                                latency_ms,
                            };
                        }
                    };

                    total_bytes += n;

                    // Enforce the maximum allowed size to avoid exhausting memory on huge pages.
                    if total_bytes > self.http.max_content_size {
                        self.metrics.parser_abort_mem.lock().inc();
                        let latency_ms = start_time.elapsed().as_millis() as u64;
                        return CrawlResult {
                            host,
                            result: Err(format!("Content too large: {} bytes", total_bytes)),
                            latency_ms,
                        };
                    }

                    // Feed the chunk to HtmlRewriter so handlers see the DOM as it streams in.
                    if let Err(e) = rewriter.write(&chunk_buf[..n]) {
                        eprintln!("HTML parsing error for {}: {}", url, e);
                        break;
                    }

                    // Guard against parser bombs by enforcing a hard cap on handler invocations.
                    handler_count += 1;
                    if handler_count > MAX_HANDLERS {
                        self.metrics.parser_abort_handler_budget.lock().inc();
                        let latency_ms = start_time.elapsed().as_millis() as u64;
                        return CrawlResult {
                            host,
                            result: Err("Parser handler budget exceeded".to_string()),
                            latency_ms,
                        };
                    }

                    // Yield after each chunk so no single page monopolizes the LocalSet.
                    tokio::task::yield_now().await;
                }

                let _ = rewriter.end();

                // Harvest the parsed links and title before dropping the locks.
                let links = links.lock().clone();
                let title = {
                    let chunks = title_chunks_clone.lock();
                    if chunks.is_empty() {
                        None
                    } else {
                        Some(chunks.join("").trim().to_string())
                    }
                };

                (links, title)
            };

            // Record codec metrics if decompression was used
            if let Some(codec) = content_encoding.as_deref() {
                let codec_duration_ms = codec_start.elapsed().as_millis() as u64;
                match codec {
                    "gzip" | "x-gzip" => {
                        self.metrics.codec_gzip_bytes_out.lock().add(total_bytes as u64);
                        self.metrics.codec_gzip_duration_ms.lock().observe(codec_duration_ms);
                    }
                    "br" => {
                        self.metrics.codec_brotli_bytes_out.lock().add(total_bytes as u64);
                        self.metrics.codec_brotli_duration_ms.lock().observe(codec_duration_ms);
                    }
                    "zstd" => {
                        self.metrics.codec_zstd_bytes_out.lock().add(total_bytes as u64);
                        self.metrics.codec_zstd_duration_ms.lock().observe(codec_duration_ms);
                    }
                    _ => {}
                }
            }

            let response_time_ms = start_time.elapsed().as_millis() as u64;

            // Enqueue the discovered links so the crawl explores new in-scope pages.
            let mut discovered_links = Vec::new();
            for link in &extracted_links {
                if let Ok(absolute_url) = Self::convert_to_absolute_url(link, &url) {
                    if Self::is_same_domain(&absolute_url, &start_url_domain)
                        && Self::should_crawl_url(&absolute_url)
                    {
                        discovered_links.push((absolute_url, depth + 1, Some(url.clone())));
                    }
                }
            }

            // Inform the writer thread so persistence captures the crawl attempt immediately.
            let normalized_url = SitemapNode::normalize_url(&url);
            self.send_event_if_alive(
                &cancel_token,
                &lost_lock,
                StateEvent::CrawlAttemptFact {
                    url_normalized: normalized_url,
                    status_code,
                    content_type: content_type.clone(),
                    content_length: Some(total_bytes),
                    title: extracted_title,
                    link_count: extracted_links.len(),
                    response_time_ms: Some(response_time_ms),
                }
            ).await;

            let latency_ms = start_time.elapsed().as_millis() as u64;
            CrawlResult {
                host,
                result: Ok(discovered_links),
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
                let should_continue = {
                    let guard = running.lock();
                    *guard
                };
                if !should_continue {
                    break;
                }

                sleep(interval).await;

                let should_continue = {
                    let guard = running.lock();
                    *guard
                };
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
        let mut running = self.running.lock();
        *running = false;
    }

            // TODO: Replace full scans with count queries inside the storage layer.
    pub async fn get_stats(&self) -> NodeMapStats {
        let total_nodes = self.state.get_node_count().unwrap_or(0);
        let crawled_nodes = self.state.get_crawled_node_count().unwrap_or(0);

        NodeMapStats {
            total_nodes,
            crawled_nodes,
        }
    }

    // Keep this hook so future persistence changes have a dedicated entry point even though redb auto-commits now.
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

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?;

        let nodes = self.state.get_all_nodes()?;
        for node in nodes {
            let json = serde_json::to_string(&node)?;
            writeln!(file, "{}", json)?;
        }

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
        assert!(!BfsCrawler::should_crawl_url(
            "https://test.local/file.pdf"
        ));
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
            BfsCrawler::convert_to_absolute_url(
                "https://other.local/page",
                "https://test.local"
            )
            .unwrap(),
            "https://other.local/page"
        );
    }

    #[tokio::test]
    async fn test_crawler_config_default() {
        let config = BfsCrawlerConfig::default();
        assert_eq!(config.max_workers, 256);
        assert_eq!(config.timeout, 45);
        assert!(!config.ignore_robots);
        assert!(!config.enable_redis);
    }
}
