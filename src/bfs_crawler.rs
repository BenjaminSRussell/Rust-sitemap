use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

use crate::common_crawl_seeder::CommonCrawlSeeder;
use crate::ct_log_seeder::CtLogSeeder;
use crate::frontier::Frontier;
use crate::network::{FetchError, HttpClient};
use crate::robots;
use crate::sitemap_seeder::SitemapSeeder;
use crate::state::{CrawlerState, SitemapNode};
use crate::url_lock_manager::UrlLockManager;
use crate::url_utils;

pub const PROGRESS_INTERVAL: usize = 100;
pub const PROGRESS_TIME_SECS: u64 = 60;

/// Messages sent to the writer task
#[derive(Debug)]
pub enum StateWriteMessage {
    /// Queue a discovered URL
    EnqueueUrl {
        url: String,
        depth: u32,
        parent_url: Option<String>,
    },
    /// Update a node with crawl data
    UpdateNode {
        url_normalized: String,
        status_code: u16,
        content_type: Option<String>,
        content_length: Option<usize>,
        title: Option<String>,
        link_count: usize,
        response_time_ms: Option<u64>,
    },
    /// Record a new node for deduplication
    AddNode(SitemapNode),
    /// Update host state details
    UpdateHostState {
        host: String,
        robots_txt: Option<String>,
        crawl_delay_secs: Option<u64>,
        success: Option<bool>,
    },
}

/// Crawler progress snapshot
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

/// BFS crawler with constructor-injected dependencies for testability
#[derive(Clone)]
pub struct BfsCrawler {
    config: BfsCrawlerConfig,
    state: Arc<CrawlerState>,
    frontier: Arc<Frontier>,
    http: Arc<HttpClient>,
    start_url: String,
    running: Arc<Mutex<bool>>,
    lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
    state_writer_tx: mpsc::UnboundedSender<StateWriteMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfsCrawlerResult {
    pub start_url: String,
    pub discovered: usize,
    pub processed: usize,
    pub duration_secs: u64,
    pub stats: NodeMapStats,
}

/// Result from handling one URL
struct CrawlResult {
    host: String,
    result: Result<Vec<(String, u32, Option<String>)>, String>,
}

impl BfsCrawler {
    /// Create the crawler with the provided dependencies
    pub fn new(
        config: BfsCrawlerConfig,
        start_url: String,
        http: Arc<HttpClient>,
        state: Arc<CrawlerState>,
        frontier: Arc<Frontier>,
        state_writer_tx: mpsc::UnboundedSender<StateWriteMessage>,
        lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
    ) -> Self {
        Self {
            config,
            start_url,
            http,
            state,
            frontier,
            state_writer_tx,
            running: Arc::new(Mutex::new(false)),
            lock_manager,
        }
    }

    pub async fn initialize(
        &mut self,
        seeding_strategy: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_url_domain = self.get_domain(&self.start_url);

        // Derive root domain (e.g., "www.example.com" -> "example.com")
        let root_domain = self.get_root_domain(&start_url_domain);

        // Resume from stored state instead of seeding again
        if self.frontier.has_saved_state() {
            eprintln!("Resuming previous crawl with saved URLs in queue");
            return Ok(());
        }

        let mut seed_links: Vec<(String, u32, Option<String>)> = Vec::new();

        // Collect enabled seeders
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

        // Run each seeder
        if !seeders.is_empty() {
            eprintln!("Running {} seeder(s)...", seeders.len());
            for seeder in seeders {
                // Sitemap seeds from start URL; others use root domain
                let domain_to_seed = if seeder.name() == "sitemap" {
                    &self.start_url
                } else {
                    &root_domain
                };

                match seeder.seed(domain_to_seed).await {
                    Ok(urls) => {
                        let count_before = seed_links.len();
                        for url in urls {
                            seed_links.push((url, 0, None));
                        }
                        let count_added = seed_links.len() - count_before;
                        eprintln!("Seeder '{}' found {} URLs", seeder.name(), count_added);
                    }
                    Err(e) => {
                        eprintln!("Warning: Seeder '{}' failed: {}", seeder.name(), e);
                    }
                }
            }
        }

        // Push seed links into the frontier
        if !seed_links.is_empty() {
            let added = self.frontier.add_links(seed_links, &start_url_domain).await;
            eprintln!(
                "Pre-seeded {} total URLs using strategy '{}'",
                added, seeding_strategy
            );
        }

        // Always queue the start URL
        let start_links = vec![(self.start_url.clone(), 0, None)];
        self.frontier.add_links(start_links, &start_url_domain).await;

        // Fetch robots.txt for the start domain when enabled
        if !self.config.ignore_robots {
            self.fetch_and_cache_robots(&start_url_domain).await;
        }

        Ok(())
    }

    // Core crawl loop that schedules work, tracks progress, and handles shutdown
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
            // Stop when signaled
            {
                let running = self.running.lock();
                if !*running {
                    break;
                }
            }

            // Phase 1: fill worker pool
            while in_flight_tasks.len() < max_concurrent {
                let next_url = self.frontier.get_next_url().await;

                match next_url {
                    Some((host, url, depth, parent_url)) => {
                        let task_state = self.clone();
                        let task_host = host.clone();
                        let task_url = url.clone();
                        let task_depth = depth;
                        let task_parent = parent_url.clone();
                        let task_domain = start_url_domain.clone();

                        in_flight_tasks.spawn(async move {
                            task_state
                                .process_url_streaming(
                                    task_host,
                                    task_url,
                                    task_depth,
                                    task_parent,
                                    task_domain,
                                )
                                .await
                        });
                    }
                    None => {
                        break;
                    }
                }
            }

            // Phase 2: collect completed tasks
            if let Some(result) = in_flight_tasks.join_next().await {
                match result {
                    Ok(crawl_result) => {
                        processed_count += 1;

                        match crawl_result.result {
                            Ok(discovered_links) => {
                                self.frontier.record_success(&crawl_result.host);

                                if !discovered_links.is_empty() {
                                    self.frontier
                                        .add_links(discovered_links, &start_url_domain)
                                        .await;
                                }
                            }
                            Err(e) => {
                                self.frontier.record_failure(&crawl_result.host);
                                eprintln!("{} - {}", crawl_result.host, e);
                            }
                        }

                        // Report progress
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

            // Phase 3: check for completion
            if self.frontier.is_empty() && in_flight_tasks.is_empty() {
                eprintln!("Crawl complete: frontier empty and no tasks in flight");
                break;
            }

            // Sleep briefly when idle
            if in_flight_tasks.is_empty() && self.frontier.is_empty() {
                sleep(Duration::from_millis(100)).await;
            }
        }

        // Drain any tasks that are still running
        while let Some(result) = in_flight_tasks.join_next().await {
            if let Ok(crawl_result) = result {
                match crawl_result.result {
                    Ok(discovered_links) => {
                        self.frontier.record_success(&crawl_result.host);
                        if !discovered_links.is_empty() {
                            self.frontier
                                .add_links(discovered_links, &start_url_domain)
                                .await;
                        }
                    }
                    Err(_) => {
                        self.frontier.record_failure(&crawl_result.host);
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

    /// Process a single URL
    async fn process_url_streaming(
        &self,
        host: String,
        url: String,
        depth: u32,
        _parent_url: Option<String>,
        start_url_domain: String,
    ) -> CrawlResult {
        use lol_html::{element, HtmlRewriter, Settings};
        use std::sync::Arc as StdArc;
        use parking_lot::Mutex as ParkingMutex;

        // Check Redis lock if enabled
        let redis_lock_acquired = if let Some(lock_manager) = &self.lock_manager {
            let mut manager = lock_manager.lock().await;
            match manager.try_acquire_url(&url).await {
                Ok(true) => true,
                Ok(false) => {
                    // Another worker already owns this URL
                    return CrawlResult {
                        host,
                        result: Err("Already locked".to_string()),
                    };
                }
                Err(e) => {
                    eprintln!("Redis lock error for {}: {}. Proceeding without lock", url, e);
                    false
                }
            }
        } else {
            false
        };

        let start_time = std::time::Instant::now();

        // Fetch URL using streaming client
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
                        return self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(Vec::new())).await;
                    }
                } else {
                    Some(response)
                }
            }
            Ok(_) => {
                return self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(Vec::new())).await;
            }
            Err(FetchError::Timeout) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err("Timeout".to_string()),
                ).await;
            }
            Err(FetchError::NetworkError(e)) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Network error: {}", e)),
                ).await;
            }
            Err(FetchError::ContentTooLarge(size, max)) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Content too large: {} bytes (max: {})", size, max)),
                ).await;
            }
            Err(e) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Fetch error: {}", e)),
                ).await;
            }
        };

        if let Some(response) = fetch_result {
            let status_code = response.status().as_u16();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());

            // Collect body first because HtmlRewriter is not Send
            use futures_util::StreamExt;
            let mut body_bytes = Vec::new();
            let mut total_bytes = 0;
            let mut stream = response.bytes_stream();

            loop {
                match stream.next().await {
                    Some(Ok(chunk)) => {
                        total_bytes += chunk.len();

                        // Enforce size limit while streaming
                        if total_bytes > self.http.max_content_size {
                            return self.finalize_crawl_result(
                                host,
                                url,
                                redis_lock_acquired,
                                Err(format!("Content too large: {} bytes (exceeded while streaming)", total_bytes)),
                            ).await;
                        }

                        body_bytes.extend_from_slice(&chunk);
                    }
                    Some(Err(e)) => {
                        return self.finalize_crawl_result(
                            host,
                            url,
                            redis_lock_acquired,
                            Err(format!("Failed to read body chunk: {}", e)),
                        ).await;
                    }
                    None => break,
                }
            }

            // Parse after buffering because HtmlRewriter is not Send
            let (extracted_links, extracted_title) = {
                // Track links
                let links = StdArc::new(ParkingMutex::new(Vec::new()));
                let links_clone = StdArc::clone(&links);

                // Track title text
                let title_chunks = StdArc::new(ParkingMutex::new(Vec::new()));
                let title_chunks_clone = StdArc::clone(&title_chunks);
                let in_title = StdArc::new(ParkingMutex::new(false));

                // Build HtmlRewriter with link and title handlers
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

                // Parse buffered HTML
                if let Err(e) = rewriter.write(&body_bytes) {
                    eprintln!("HTML parsing error for {}: {}", url, e);
                }
                let _ = rewriter.end();

                // Collect parsed data
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

            let response_time_ms = start_time.elapsed().as_millis() as u64;

            // Queue discovered links
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

            // Notify writer task
            let normalized_url = SitemapNode::normalize_url(&url);
            let _ = self.state_writer_tx.send(StateWriteMessage::UpdateNode {
                url_normalized: normalized_url,
                status_code,
                content_type: content_type.clone(),
                content_length: Some(total_bytes),
                title: extracted_title,
                link_count: extracted_links.len(),
                response_time_ms: Some(response_time_ms),
            });

            self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(discovered_links)).await
        } else {
            // Skip non-HTML or non-200 responses
            self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(Vec::new())).await
        }
    }

    /// Finalize the crawl result and release locks
    async fn finalize_crawl_result(
        &self,
        host: String,
        url: String,
        redis_lock_acquired: bool,
        result: Result<Vec<(String, u32, Option<String>)>, String>,
    ) -> CrawlResult {
        // Release Redis lock if held
        if redis_lock_acquired {
            if let Some(lock_manager) = &self.lock_manager {
                let mut manager = lock_manager.lock().await;
                if let Err(e) = manager.release_url(&url).await {
                    eprintln!("Failed to release Redis lock for {}: {}", url, e);
                }
            }
        }

        CrawlResult { host, result }
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

                // redb auto-commits, so there is nothing to persist here
            }
        }))
    }

    async fn fetch_and_cache_robots(&self, domain: &str) {
        let robots_txt = robots::fetch_robots_txt(&self.http, domain).await;

        match robots_txt {
            Some(content) => {
                self.frontier.set_robots_txt(domain, content);
                eprintln!("Cached robots.txt for {}", domain);
            }
            None => {
                eprintln!("No robots.txt found for {}", domain);
            }
        }
    }

    fn should_crawl_url(url: &str) -> bool {
        url_utils::should_crawl_url(url)
    }

    fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
        url_utils::convert_to_absolute_url(link, base_url)
    }

    fn get_domain(&self, url: &str) -> String {
        url_utils::extract_host(url).unwrap_or_default()
    }

    /// Extract the root domain (e.g., "www.hartford.edu" -> "hartford.edu")
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

    // TODO: replace full scans with count queries inside the storage layer
    pub async fn get_stats(&self) -> NodeMapStats {
        let total_nodes = self.state.get_node_count().unwrap_or(0);
        let crawled_nodes = self.state.get_crawled_node_count().unwrap_or(0);

        NodeMapStats {
            total_nodes,
            crawled_nodes,
        }
    }

    // Placeholder until state persistence needs explicit work
    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        // redb commits automatically; nothing extra to do
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

    pub async fn get_all_nodes(
        &self,
    ) -> Result<Vec<SitemapNode>, Box<dyn std::error::Error>> {
        Ok(self.state.get_all_nodes()?)
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
