use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

use crate::config::Config;
use crate::frontier::Frontier;
use crate::network::{FetchError, HttpClient};
use crate::node_map::{CrawlData, NodeMap, NodeMapStats};
use crate::parser::extract_links;
use crate::rkyv_queue::{QueuedUrl, RkyvQueue};
use crate::sitemap_seeder::SitemapSeeder;
use crate::url_lock_manager::UrlLockManager;

pub const PROGRESS_INTERVAL: usize = 100;
pub const PROGRESS_TIME_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct BfsCrawlerConfig {
    pub max_workers: u32,
    pub timeout: u32,
    pub user_agent: String,
    pub ignore_robots: bool,
    pub max_memory: usize,
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
            max_memory: 10 * 1024 * 1024 * 1024,
            save_interval: 300,
            redis_url: None,
            lock_ttl: 60,
            enable_redis: false,
        }
    }
}

#[derive(Clone)]
pub struct BfsCrawlerState {
    pub config: BfsCrawlerConfig,
    pub node_map: Arc<NodeMap>,
    pub frontier: Arc<Frontier>,
    pub http: Arc<HttpClient>,
    pub start_url: String,
    pub running: Arc<Mutex<bool>>,
    pub lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfsCrawlerResult {
    pub start_url: String,
    pub discovered: usize,
    pub processed: usize,
    pub duration_secs: u64,
    pub stats: NodeMapStats,
}

/// Result of processing a single URL
struct CrawlResult {
    host: String,
    url: String,
    result: Result<Vec<(String, u32, Option<String>)>, String>,
}

impl BfsCrawlerState {
    pub async fn new<P: AsRef<std::path::Path>>(
        start_url: String,
        data_dir: P,
        config: BfsCrawlerConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let http = Arc::new(HttpClient::new(
            config.user_agent.clone(),
            config.timeout as u64,
        ));

        let max_queue = config.max_workers as usize * Config::QUEUE_MULTIPLIER;

        let node_map = Arc::new(NodeMap::new(&data_dir, Config::NODES_IN_MEMORY)?);
        let wal = Arc::new(Mutex::new(RkyvQueue::new(&data_dir, max_queue)?));

        let frontier = Arc::new(Frontier::new(
            wal,
            Arc::clone(&node_map),
            config.user_agent.clone(),
            config.ignore_robots,
        ));

        let lock_manager = if config.enable_redis {
            if let Some(url) = &config.redis_url {
                match UrlLockManager::new(url, Some(config.lock_ttl)).await {
                    Ok(mgr) => Some(Arc::new(tokio::sync::Mutex::new(mgr))),
                    Err(e) => {
                        eprintln!("Redis lock setup failed: {}", e);
                        None
                    }
                }
            } else {
                eprintln!("Redis enabled but URL not provided");
                None
            }
        } else {
            None
        };

        Ok(Self {
            config,
            node_map,
            frontier,
            http,
            start_url,
            running: Arc::new(Mutex::new(false)),
            lock_manager,
        })
    }

    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start_url_domain = self.get_domain(&self.start_url);

        // Pre-seed from sitemaps if robots.txt allows
        if !self.config.ignore_robots {
            let seeder = SitemapSeeder::new(self.config.user_agent.clone());
            let seed_urls = seeder.seed(&self.start_url).await;

            let mut links = Vec::new();
            for url in seed_urls {
                links.push((url, 0, None));
            }

            let added = self.frontier.add_links(links, &start_url_domain).await;
            eprintln!("Pre-seeded {} URLs from sitemaps", added);
        }

        // Always add start URL
        let start_links = vec![(self.start_url.clone(), 0, None)];
        self.frontier.add_links(start_links, &start_url_domain).await;

        // Fetch robots.txt for the start domain
        if !self.config.ignore_robots {
            self.fetch_and_cache_robots(&start_url_domain).await;
        }

        Ok(())
    }

    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start = SystemTime::now();
        {
            let mut r = self.running.lock();
            *r = true;
        }

        let save_task = self.spawn_auto_save_task();
        let start_url_domain = self.get_domain(&self.start_url);

        // The orchestrator event loop
        let mut in_flight_tasks = JoinSet::new();
        let max_concurrent = self.config.max_workers as usize;

        let mut processed_count = 0;
        let mut last_progress_report = std::time::Instant::now();

        loop {
            // Check if we should stop
            {
                let running = self.running.lock();
                if !*running {
                    break;
                }
            }

            // Phase 1: Top-up in-flight tasks
            while in_flight_tasks.len() < max_concurrent {
                // Try to get next URL from frontier
                let next_url = self.frontier.get_next_url().await;

                match next_url {
                    Some((host, url, depth, parent_url)) => {
                        // Spawn a task for this URL
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
                        // No URLs available right now
                        break;
                    }
                }
            }

            // Phase 2: Reap completed tasks
            if let Some(result) = in_flight_tasks.join_next().await {
                match result {
                    Ok(crawl_result) => {
                        processed_count += 1;

                        match crawl_result.result {
                            Ok(discovered_links) => {
                                // Success - add discovered links to frontier
                                self.frontier.record_success(&crawl_result.host);

                                if !discovered_links.is_empty() {
                                    self.frontier
                                        .add_links(discovered_links, &start_url_domain)
                                        .await;
                                }
                            }
                            Err(e) => {
                                // Failure - record it
                                self.frontier.record_failure(&crawl_result.host);
                                eprintln!("{} - {}", crawl_result.host, e);
                            }
                        }

                        // Progress reporting
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

            // Phase 3: Check termination condition
            if self.frontier.is_empty() && in_flight_tasks.is_empty() {
                eprintln!("Crawl complete: frontier empty and no tasks in flight");
                break;
            }

            // Small sleep if no work available to avoid busy-waiting
            if in_flight_tasks.is_empty() && self.frontier.is_empty() {
                sleep(Duration::from_millis(100)).await;
            }
        }

        // Wait for any remaining tasks
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

    /// Process a single URL with streaming (the new task model)
    async fn process_url_streaming(
        &self,
        host: String,
        url: String,
        depth: u32,
        parent_url: Option<String>,
        start_url_domain: String,
    ) -> CrawlResult {
        // Check Redis lock if enabled
        let redis_lock_acquired = if let Some(lock_manager) = &self.lock_manager {
            let mut manager = lock_manager.lock().await;
            match manager.try_acquire_url(&url).await {
                Ok(true) => true,
                Ok(false) => {
                    // Someone else is processing this URL
                    return CrawlResult {
                        host,
                        url,
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

        // Fetch the URL
        let fetch_result = match self.http.fetch(&url).await {
            Ok(result) if result.status_code == 200 => {
                if let Some(content_type) = result.content_type.as_ref() {
                    if Self::is_html_content_type(content_type) {
                        Some(result)
                    } else {
                        None
                    }
                } else {
                    Some(result)
                }
            }
            Ok(_) => None,
            Err(FetchError::Timeout) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err("Timeout".to_string()),
                );
            }
            Err(FetchError::NetworkError(e)) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Network error: {}", e)),
                );
            }
            Err(FetchError::ContentTooLarge(size, max)) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Content too large: {} bytes (max: {})", size, max)),
                );
            }
            Err(e) => {
                return self.finalize_crawl_result(
                    host,
                    url,
                    redis_lock_acquired,
                    Err(format!("Fetch error: {}", e)),
                );
            }
        };

        if let Some(fetch_result) = fetch_result {
            // Extract links
            let links = extract_links(&fetch_result.content);
            let title = Self::extract_title(&fetch_result.content);

            // Update node map
            if let Err(e) = self.node_map.update_node(
                &url,
                CrawlData {
                    status_code: fetch_result.status_code,
                    content_type: fetch_result.content_type.clone(),
                    content_length: Some(fetch_result.content_length),
                    title,
                    links: links.clone(),
                    response_time_ms: Some(fetch_result.response_time_ms),
                },
            ) {
                eprintln!("Failed to update node map for {}: {}", url, e);
            }

            // Process discovered links
            let mut discovered_links = Vec::new();
            for link in links {
                if let Ok(absolute_url) = Self::convert_to_absolute_url(&link, &url) {
                    if Self::is_same_domain(&absolute_url, &start_url_domain)
                        && Self::should_crawl_url(&absolute_url)
                    {
                        discovered_links.push((absolute_url, depth + 1, Some(url.clone())));
                    }
                }
            }

            self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(discovered_links))
        } else {
            // Not HTML or not 200 OK
            self.finalize_crawl_result(host, url, redis_lock_acquired, Ok(Vec::new()))
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
        // Release Redis lock if acquired
        if redis_lock_acquired {
            if let Some(lock_manager) = &self.lock_manager {
                let mut manager = lock_manager.lock().await;
                if let Err(e) = manager.release_url(&url).await {
                    eprintln!("Failed to release Redis lock for {}: {}", url, e);
                }
            }
        }

        CrawlResult { host, url, result }
    }

    fn spawn_auto_save_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        if self.config.save_interval == 0 {
            return None;
        }

        let interval = Duration::from_secs(self.config.save_interval);
        let node_map = Arc::clone(&self.node_map);
        let running = Arc::clone(&self.running);

        Some(tokio::spawn(async move {
            let mut consecutive_errors = 0;
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

                if let Err(e) = node_map.force_flush().await {
                    consecutive_errors += 1;
                    eprintln!("Checkpoint failed (attempt {}): {}", consecutive_errors, e);
                    if consecutive_errors >= 3 {
                        eprintln!("Auto-save disabled after repeated failures");
                        break;
                    }
                } else {
                    consecutive_errors = 0;
                }
            }
        }))
    }

    async fn fetch_and_cache_robots(&self, domain: &str) {
        let robots_url = format!("https://{}/robots.txt", domain);

        match self.http.fetch(&robots_url).await {
            Ok(result) if result.status_code == 200 => {
                // Parse robots.txt for crawl-delay
                let crawl_delay = Self::extract_crawl_delay(&result.content, &self.config.user_agent);

                self.frontier.set_robots_txt(
                    domain,
                    result.content,
                    crawl_delay.map(Duration::from_secs),
                );

                eprintln!("Cached robots.txt for {}", domain);
            }
            _ => {
                eprintln!("No robots.txt found for {}", domain);
            }
        }
    }

    fn extract_crawl_delay(robots_txt: &str, user_agent: &str) -> Option<u64> {
        let mut current_user_agent = false;

        for line in robots_txt.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim().to_lowercase();
                let value = value.trim();

                match key.as_str() {
                    "user-agent" => {
                        current_user_agent = value == "*" || value == user_agent;
                    }
                    "crawl-delay" if current_user_agent => {
                        return value.parse::<u64>().ok();
                    }
                    _ => {}
                }
            }
        }

        None
    }

    fn should_crawl_url(url: &str) -> bool {
        let parsed_url = match url::Url::parse(url) {
            Ok(u) => u,
            Err(_) => return false,
        };

        if !matches!(parsed_url.scheme(), "http" | "https") {
            return false;
        }

        if parsed_url.fragment().is_some()
            && parsed_url.path() == "/"
            && parsed_url.query().is_none()
        {
            return false;
        }

        let path = parsed_url.path().to_lowercase();
        const DISALLOWED_EXTENSIONS: &[&str] = &[
            ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4",
            ".avi", ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
            ".tar", ".gz", ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso", ".apk",
        ];
        if DISALLOWED_EXTENSIONS.iter().any(|ext| path.ends_with(ext)) {
            return false;
        }

        if let Some(query) = parsed_url.query() {
            let query_lower = query.to_ascii_lowercase();
            if query_lower.contains("download") || query_lower.contains("attachment") {
                return false;
            }
        }

        true
    }

    fn is_html_content_type(content_type: &str) -> bool {
        let lower = content_type.to_ascii_lowercase();
        lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml")
    }

    fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
        let base = url::Url::parse(base_url).map_err(|e| e.to_string())?;
        let absolute_url = base.join(link).map_err(|e| e.to_string())?;
        Ok(absolute_url.to_string())
    }

    fn get_domain(&self, url: &str) -> String {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
            .unwrap_or_default()
    }

    fn is_same_domain(url: &str, base_domain: &str) -> bool {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
            .map(|host| {
                host == base_domain
                    || host.ends_with(&format!(".{}", base_domain))
                    || base_domain.ends_with(&format!(".{}", host))
            })
            .unwrap_or(false)
    }

    fn extract_title(content: &str) -> Option<String> {
        if let Some(start) = content.find("<title>") {
            if let Some(end) = content[start..].find("</title>") {
                let title = &content[start + 7..start + end];
                return Some(title.trim().to_string());
            }
        }
        None
    }

    pub async fn stop(&self) {
        let mut running = self.running.lock();
        *running = false;
    }

    pub async fn get_stats(&self) -> NodeMapStats {
        self.node_map.stats()
    }

    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.node_map.force_flush().await?;
        Ok(())
    }

    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.node_map.export_to_jsonl(output_path)?;
        Ok(())
    }

    pub async fn get_all_nodes(
        &self,
    ) -> Result<Vec<crate::node_map::SitemapNode>, Box<dyn std::error::Error>> {
        Ok(self.node_map.get_all_nodes()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_bfs_crawler_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let config = BfsCrawlerConfig::default();
        let crawler =
            BfsCrawlerState::new("https://test.local".to_string(), temp_dir.path(), config)
                .await
                .unwrap();

        assert_eq!(crawler.start_url, "https://test.local");
        assert_eq!(crawler.config.max_workers, 256);
    }

    #[test]
    fn test_should_crawl_url() {
        assert!(BfsCrawlerState::should_crawl_url("https://test.local/page"));
        assert!(BfsCrawlerState::should_crawl_url("http://test.local/page"));
        assert!(!BfsCrawlerState::should_crawl_url("ftp://test.local/page"));
        assert!(!BfsCrawlerState::should_crawl_url(
            "https://test.local/file.pdf"
        ));
        assert!(!BfsCrawlerState::should_crawl_url(
            "https://test.local/image.jpg"
        ));
    }

    #[test]
    fn test_is_same_domain() {
        assert!(BfsCrawlerState::is_same_domain(
            "https://test.local/page1",
            "test.local"
        ));
        assert!(!BfsCrawlerState::is_same_domain(
            "https://other.local/page1",
            "test.local"
        ));
    }

    #[test]
    fn test_get_domain() {
        let dir = TempDir::new().unwrap();
        let config = BfsCrawlerConfig::default();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let crawler = rt.block_on(async {
            BfsCrawlerState::new("https://test.local".to_string(), dir.path(), config)
                .await
                .unwrap()
        });

        let domain = crawler.get_domain("https://test.local/page1");
        assert_eq!(domain, "test.local");
    }

    #[test]
    fn test_convert_to_absolute_url() {
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url("/page1", "https://test.local/foo").unwrap(),
            "https://test.local/page1"
        );
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url("page1", "https://test.local/foo/").unwrap(),
            "https://test.local/foo/page1"
        );
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url(
                "https://other.local/page",
                "https://test.local"
            )
            .unwrap(),
            "https://other.local/page"
        );
    }

    #[test]
    fn test_extract_title() {
        let html = "<html><head><title>Test Page</title></head><body></body></html>";
        let title = BfsCrawlerState::extract_title(html);
        assert_eq!(title, Some("Test Page".to_string()));

        let no_title = "<html><body></body></html>";
        let title = BfsCrawlerState::extract_title(no_title);
        assert_eq!(title, None);
    }

    #[test]
    fn test_is_html_content_type() {
        assert!(BfsCrawlerState::is_html_content_type("text/html"));
        assert!(BfsCrawlerState::is_html_content_type(
            "text/html; charset=utf-8"
        ));
        assert!(!BfsCrawlerState::is_html_content_type("application/json"));
        assert!(!BfsCrawlerState::is_html_content_type("image/png"));
    }

    #[tokio::test]
    async fn test_crawler_config_default() {
        let config = BfsCrawlerConfig::default();
        assert_eq!(config.max_workers, 256);
        assert_eq!(config.timeout, 45);
        assert!(!config.ignore_robots);
        assert!(!config.enable_redis);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let dir = TempDir::new().unwrap();
        let config = BfsCrawlerConfig::default();
        let crawler = BfsCrawlerState::new("https://test.local".to_string(), dir.path(), config)
            .await
            .unwrap();

        let stats = crawler.get_stats().await;
        assert_eq!(stats.total_nodes, 0);
    }
}
