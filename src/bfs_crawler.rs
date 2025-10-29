use kanal::{bounded_async, AsyncReceiver, AsyncSender};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::config::Config;
use crate::network::{FetchError, HttpClient};
use crate::node_map::{CrawlData, NodeMap, NodeMapStats};
use crate::parser::extract_links;
use crate::rkyv_queue::{QueuedUrl, RkyvQueue};
use crate::robots::RobotsTxt;
use crate::sitemap_seeder::SitemapSeeder;
use crate::url_lock_manager::UrlLockManager;

pub const REQUEUE_SLEEP_MS: u64 = 10;
pub const PROGRESS_INTERVAL: usize = 100;
pub const PROGRESS_TIME_SECS: u64 = 60;

#[derive(Debug, Clone)]
struct DomainState {
    failures: usize,
    last_failure: std::time::Instant,
    backoff_until: std::time::Instant,
    active_requests: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DomainFailureTracker {
    domains: Arc<Mutex<HashMap<String, DomainState>>>,
    max_failures: usize,
    max_per_domain: usize,
}

impl DomainFailureTracker {
    fn new(max_failures: usize, max_per_domain: usize) -> Self {
        Self {
            domains: Arc::new(Mutex::new(HashMap::new())),
            max_failures,
            max_per_domain,
        }
    }

    fn record_failure(&self, domain: &str) {
        let mut domains = self.domains.lock();
        let now = std::time::Instant::now();
        let state = domains.entry(domain.to_string()).or_insert(DomainState {
            failures: 0,
            last_failure: now,
            backoff_until: now,
            active_requests: 0,
        });
        state.failures += 1;
        state.last_failure = now;

        let backoff_secs =
            (2_u32.pow(state.failures.min(Config::BACKOFF_MAX_EXP) as u32)).min(Config::BACKOFF_MAX_SECS) as u64;
        state.backoff_until = now + std::time::Duration::from_secs(backoff_secs);
    }

    fn should_skip(&self, domain: &str) -> bool {
        let domains = self.domains.lock();
        if let Some(state) = domains.get(domain) {
            let now = std::time::Instant::now();
            if state.failures >= self.max_failures {
                return true;
            }
            if now < state.backoff_until {
                return true;
            }
        }
        false
    }

    fn can_request(&self, domain: &str) -> bool {
        let domains = self.domains.lock();
        if let Some(state) = domains.get(domain) {
            state.active_requests < self.max_per_domain
        } else {
            true
        }
    }

    fn acquire_request(&self, domain: &str) -> bool {
        let mut domains = self.domains.lock();
        let now = std::time::Instant::now();
        let state = domains.entry(domain.to_string()).or_insert(DomainState {
            failures: 0,
            last_failure: now,
            backoff_until: now,
            active_requests: 0,
        });

        if state.active_requests < self.max_per_domain {
            state.active_requests += 1;
            true
        } else {
            false
        }
    }

    fn release_request(&self, domain: &str) {
        let mut domains = self.domains.lock();
        if let Some(state) = domains.get_mut(domain) {
            state.active_requests = state.active_requests.saturating_sub(1);
        }
    }

}

#[derive(Debug, Clone)]
pub struct BfsCrawlerConfig {
    pub max_workers: u32,
    pub rate_limit: u32,
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
            rate_limit: 200,
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
    pub queue: Arc<Mutex<RkyvQueue>>,
    pub sender: AsyncSender<QueuedUrl>,
    pub receiver: AsyncReceiver<QueuedUrl>,
    pub http: Arc<HttpClient>,
    pub limiter: Arc<Semaphore>,
    pub robots: Option<RobotsTxt>,
    pub start_url: String,
    pub running: Arc<Mutex<bool>>,
    pub(crate) failures: DomainFailureTracker,
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

        let limiter = Arc::new(Semaphore::new(config.rate_limit as usize));

        let max_queue = config.max_workers as usize * Config::QUEUE_MULTIPLIER;

        let node_map = Arc::new(NodeMap::new(&data_dir, Config::NODES_IN_MEMORY)?);
        let queue = Arc::new(Mutex::new(RkyvQueue::new(&data_dir, max_queue)?));

        let capacity = (config.max_workers as usize * Config::CHANNEL_MULTIPLIER).max(Config::CHANNEL_MIN_SIZE);
        let (sender, receiver) = bounded_async(capacity);

        let failures = DomainFailureTracker::new(Config::DOMAIN_MAX_FAILURES, Config::DOMAIN_MAX_REQUESTS);

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
            queue,
            sender,
            receiver,
            http,
            limiter,
            robots: None,
            start_url,
            running: Arc::new(Mutex::new(false)),
            failures,
            lock_manager,
        })
    }

    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Pre-seed from sitemaps if robots.txt allows
        if !self.config.ignore_robots {
            let seeder = SitemapSeeder::new(self.config.user_agent.clone());
            let seed_urls = seeder.seed(&self.start_url).await;

            for url in seed_urls {
                if self.node_map.add_url(url.clone(), 0, None).is_ok() {
                    let queued = QueuedUrl::new(url, 0, None);
                    {
                        let mut q = self.queue.lock();
                        let _ = q.push_back(queued.clone());
                    }
                    let _ = self.sender.send(queued).await;
                }
            }
        }

        // Always add start URL
        self.node_map.add_url(self.start_url.clone(), 0, None)?;

        let queued = QueuedUrl::new(self.start_url.clone(), 0, None);
        {
            let mut q = self.queue.lock();
            q.push_back(queued.clone())?;
        }
        self.sender
            .send(queued)
            .await
            .map_err(|e| format!("Failed to queue: {}", e))?;

        if !self.config.ignore_robots {
            self.robots = self.fetch_robots().await;
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

        let mut handles = Vec::new();
        for id in 0..self.config.max_workers {
            handles.push(self.spawn_worker(id).await);
        }

        for h in handles {
            let _ = h.await?;
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
            processed: stats.total_nodes,
            duration_secs: elapsed.as_secs(),
            stats: stats.clone(),
        };

        Ok(result)
    }

    fn spawn_auto_save_task(&self) -> Option<JoinHandle<()>> {
        if self.config.save_interval == 0 {
            return None;
        }

        let interval = Duration::from_secs(self.config.save_interval);
        let node_map = Arc::clone(&self.node_map);
        let url_queue = Arc::clone(&self.queue);
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

    async fn spawn_worker(&self, _worker_id: u32) -> tokio::task::JoinHandle<Result<(), String>> {
        let node_map = Arc::clone(&self.node_map);
        let receiver = self.receiver.clone();
        let sender = self.sender.clone();
        let http = Arc::clone(&self.http);
        let limiter = Arc::clone(&self.limiter);
        let robots = self.robots.clone();
        let user_agent = self.config.user_agent.clone();
        let running = Arc::clone(&self.running);
        let start_url_domain = self.get_domain(&self.start_url);
        let failures = self.failures.clone();
        let lock_manager = self.lock_manager.clone();

        tokio::spawn(async move {
            let mut processed_count = 0;
            let mut last_progress_report = std::time::Instant::now();

            loop {
                {
                    let running = running.lock();
                    if !*running {
                        break;
                    }
                }

                let next_url_item = (receiver.recv().await).ok();

                match next_url_item {
                    Some(queued_url) => {
                        let url = queued_url.url.clone();
                        let depth = queued_url.depth;

                        let url_domain = url::Url::parse(&url)
                            .ok()
                            .and_then(|u| u.host_str().map(|s| s.to_string()))
                            .unwrap_or_default();

                        if failures.should_skip(&url_domain) {
                            continue;
                        }

                        if !failures.can_request(&url_domain) {
                            tokio::time::sleep(Duration::from_millis(REQUEUE_SLEEP_MS)).await;

                            let _ = sender.send(queued_url).await;
                            continue;
                        }

                        if !failures.acquire_request(&url_domain) {
                            tokio::time::sleep(Duration::from_millis(REQUEUE_SLEEP_MS)).await;
                            let _ = sender.send(queued_url).await;
                            continue;
                        }

                        let redis_lock_acquired = if let Some(lock_manager) = &lock_manager {
                            let mut manager = lock_manager.lock().await;
                            match manager.try_acquire_url(&url).await {
                                Ok(true) => true,
                                Ok(false) => {
                                    failures.release_request(&url_domain);
                                    continue;
                                }
                                Err(e) => {
                                    eprintln!("Redis lock error for {}: {}. Proceeding without lock", url, e);
                                    false
                                }
                            }
                        } else {
                            false
                        };

                        let _permit = limiter.acquire().await.unwrap();

                        processed_count += 1;

                        let node_map_bg = node_map.clone();
                        let http_bg = http.clone();
                        let robots_bg = robots.clone();
                        let user_agent_bg = user_agent.clone();
                        let sender_bg = sender.clone();
                        let start_url_domain_bg = start_url_domain.clone();
                        let failures_bg = failures.clone();
                        let url_bg = url.clone();
                        let url_domain_bg = url_domain.clone();
                        let lock_manager_bg = lock_manager.clone();

                        tokio::spawn(async move {
                            let result =
                                Self::process_url(&url_bg, &http_bg, &robots_bg, &user_agent_bg)
                                    .await;

                            match result {
                                Ok(Some(fetch_result)) => {
                                    let links = extract_links(&fetch_result.content);

                                    let title = Self::extract_title(&fetch_result.content);

                                    if let Err(e) = node_map_bg.update_node(
                                        &url_bg,
                                        CrawlData {
                                            status_code: fetch_result.status_code,
                                            content_type: fetch_result.content_type.clone(),
                                            content_length: Some(fetch_result.content_length),
                                            title,
                                            links: links.clone(),
                                            response_time_ms: Some(fetch_result.response_time_ms),
                                        },
                                    ) {
                                        eprintln!("Failed to update node map for {}: {}", url_bg, e);
                                    }

                                    let mut new_urls_added = 0;
                                    let mut urls_to_add = Vec::new();

                                    for link in links {
                                        if let Ok(absolute_url) =
                                            Self::convert_to_absolute_url(&link, &url_bg)
                                        {
                                            if Self::is_same_domain(
                                                &absolute_url,
                                                &start_url_domain_bg,
                                            ) && Self::should_crawl_url(&absolute_url)
                                            {
                                                urls_to_add.push(absolute_url);
                                            }
                                        }
                                    }

                                    if !urls_to_add.is_empty() {
                                        let mut urls_to_queue = Vec::new();

                                        for absolute_url in urls_to_add {
                                            if !node_map_bg.contains(&absolute_url) {
                                                if let Ok(added) = node_map_bg.add_url(
                                                    absolute_url.clone(),
                                                    depth + 1,
                                                    Some(url_bg.clone()),
                                                ) {
                                                    if added {
                                                        urls_to_queue.push(absolute_url);
                                                    }
                                                }
                                            }
                                        }

                                        for absolute_url in urls_to_queue {
                                            let queued_url = QueuedUrl::new(
                                                absolute_url,
                                                depth + 1,
                                                Some(url_bg.clone()),
                                            );
                                            if sender_bg.send(queued_url).await.is_ok() {
                                                new_urls_added += 1;
                                            }
                                        }
                                    }

                                }
                                Ok(None) => {}
                                Err(e) => {
                                    if !url_domain_bg.is_empty() {
                                        failures_bg.record_failure(&url_domain_bg);
                                    }

                                    let error_msg = if e.contains("Connection refused") {
                                        "Connection refused".to_string()
                                    } else if e.contains("timeout") || e.contains("timed out") {
                                        "Timeout".to_string()
                                    } else if e.contains("DNS") {
                                        "DNS error".to_string()
                                    } else if e.contains("SSL") || e.contains("TLS") {
                                        "SSL/TLS error".to_string()
                                    } else if e.contains("Content too large") {
                                        "Content too large".to_string()
                                    } else if e.contains("Network error")
                                        || e.contains("record overflow")
                                    {
                                        "Network error".to_string()
                                    } else {
                                        e.to_string()
                                    };

                                    eprintln!("{} - {}", url_domain_bg, error_msg);
                                }
                            }

                            if redis_lock_acquired {
                                if let Some(lock_manager) = &lock_manager_bg {
                                    let mut manager = lock_manager.lock().await;
                                    if let Err(e) = manager.release_url(&url_bg).await {
                                        eprintln!("Failed to release Redis lock for {}: {}", url_bg, e);
                                    }
                                }
                            }

                            failures_bg.release_request(&url_domain_bg);
                        });

                        if processed_count % PROGRESS_INTERVAL == 0
                            || last_progress_report.elapsed().as_secs() >= PROGRESS_TIME_SECS
                        {

                            last_progress_report = std::time::Instant::now();
                        }
                    }
                    None => {
                        sleep(Duration::from_millis(Config::EMPTY_QUEUE_SLEEP_MS)).await;

                        if receiver.is_disconnected() || receiver.is_empty() {
                            sleep(Duration::from_millis(Config::EMPTY_QUEUE_CHECK_MS)).await;
                            if receiver.is_disconnected() || receiver.is_empty() {
                                println!("Worker {}: No more URLs to process, exiting", worker_id);
                                break;
                            }
                        }
                    }
                }
            }

            println!(
                "Worker {} completed (processed {} pages)",
                worker_id, processed_count
            );
            Ok(())
        })
    }

    /// Check robots.txt, fetch URL, and return result or None if blocked/failed
    async fn process_url(
        url: &str,
        http: &HttpClient,
        robots: &Option<RobotsTxt>,
        user_agent: &str,
    ) -> Result<Option<crate::network::FetchResult>, String> {
        if let Some(robots) = robots {
            if !robots.is_allowed(url, user_agent) {
                return Ok(None);
            }
        }

        if !Self::should_crawl_url(url) {
            return Ok(None);
        }
        match http.fetch(url).await {
            Ok(result) => {
                if result.status_code == 200 {
                    if let Some(content_type) = result.content_type.as_ref() {
                        if Self::is_html_content_type(content_type) {
                            Ok(Some(result))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(Some(result))
                    }
                } else {
                    Ok(None)
                }
            }
            Err(FetchError::Timeout) => Err("Timeout".to_string()),
            Err(FetchError::NetworkError(e)) => Err(format!("Network error: {}", e)),
            Err(FetchError::ContentTooLarge(size, max)) => {
                Err(format!("Content too large: {} bytes (max: {})", size, max))
            }
            Err(e) => Err(format!("Fetch error: {}", e)),
        }
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

    async fn fetch_robots(&self) -> Option<RobotsTxt> {
        let parsed_url = url::Url::parse(&self.start_url).ok()?;
        let host = parsed_url.host_str()?;
        let robots_url = format!("{}://{}/robots.txt", parsed_url.scheme(), host);

        match self.http.fetch(&robots_url).await {
            Ok(result) if result.status_code == 200 => {
                Some(RobotsTxt::new(&result.content, &self.config.user_agent))
            }
            _ => None,
        }
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
        {
            let mut queue = self.queue.lock();
            queue.force_flush()?;
            queue.clear()?;
        }
        Ok(())
    }

    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.node_map.export_to_jsonl(output_path)?;
        Ok(())
    }

    pub async fn get_all_nodes(&self) -> Result<Vec<crate::node_map::SitemapNode>, Box<dyn std::error::Error>> {
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

        // test parent traversal
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url(
                "/admission/visit/../default.aspx",
                "https://test.local"
            )
            .unwrap(),
            "https://test.local/admission/default.aspx"
        );
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url(
                "../default.aspx",
                "https://test.local/admission/visit/"
            )
            .unwrap(),
            "https://test.local/admission/default.aspx"
        );
        assert_eq!(
            BfsCrawlerState::convert_to_absolute_url(
                "../../home.html",
                "https://test.local/a/b/c/"
            )
            .unwrap(),
            "https://test.local/a/home.html"
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
        assert_eq!(config.rate_limit, 200);
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
