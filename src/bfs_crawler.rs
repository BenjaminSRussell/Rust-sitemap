use kanal::{bounded_async, AsyncReceiver, AsyncSender};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::network::{FetchError, HttpClient};
use crate::node_map::{NodeMap, NodeMapStats};
use crate::parser::extract_links;
use crate::rkyv_queue::{QueuedUrl, RkyvQueue};
use crate::robots::RobotsTxt;
use crate::url_lock_manager::UrlLockManager;

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

        let backoff_secs = (2_u32.pow(state.failures.min(8) as u32)).min(300) as u64;
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

    fn get_failure_count(&self, domain: &str) -> usize {
        let domains = self.domains.lock();
        domains.get(domain).map(|s| s.failures).unwrap_or(0)
    }

    fn get_backoff_remaining(&self, domain: &str) -> Option<u64> {
        let domains = self.domains.lock();
        if let Some(state) = domains.get(domain) {
            let now = std::time::Instant::now();
            if now < state.backoff_until {
                return Some(state.backoff_until.duration_since(now).as_secs());
            }
        }
        None
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

        let max_nodes = 100_000;
        let max_queue = config.max_workers as usize * 500;

        let node_map = Arc::new(NodeMap::new(&data_dir, max_nodes)?);
        let queue = Arc::new(Mutex::new(RkyvQueue::new(&data_dir, max_queue)?));

        let capacity = (config.max_workers as usize * 100).max(50_000);
        let (sender, receiver) = bounded_async(capacity);

        let failures = DomainFailureTracker::new(5, 5);

        let lock_manager = if config.enable_redis {
            if let Some(url) = &config.redis_url {
                match UrlLockManager::new(url, Some(config.lock_ttl)).await {
                    Ok(mgr) => {
                        println!("Redis initialized (TTL: {}s)", config.lock_ttl);
                        Some(Arc::new(tokio::sync::Mutex::new(mgr)))
                    }
                    Err(e) => {
                        eprintln!("WARNING: Redis failed: {}. Continuing without locks.", e);
                        None
                    }
                }
            } else {
                eprintln!("WARNING: Redis enabled but no URL provided.");
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
        println!("Initializing crawler");
        println!("  URL: {}", self.start_url);
        println!("  Workers: {}", self.config.max_workers);
        println!("  Rate: {} req/s", self.config.rate_limit);
        println!("  Memory: {} GB", self.config.max_memory / (1024 * 1024 * 1024));

        self.node_map.add_url(self.start_url.clone(), 0, None)?;

        let queued = QueuedUrl::new(self.start_url.clone(), 0, None);
        {
            let mut q = self.queue.lock();
            q.push_back(queued.clone())?;
        }
        self.sender.send(queued).await
            .map_err(|e| format!("Failed to queue: {}", e))?;

        if !self.config.ignore_robots {
            self.robots = self.fetch_robots().await;
        }

        println!("Crawler ready");
        Ok(())
    }

    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start = SystemTime::now();
        {
            let mut r = self.running.lock();
            *r = true;
        }

        println!("Starting crawl...");

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
                    println!("WARNING: Save task error: {}", e);
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

        println!("Crawl complete!");
        println!("  Discovered: {}", result.discovered);
        println!("  Processed: {}", result.processed);
        println!("  Duration: {}s", result.duration_secs);
        println!("  Stats: {}", stats);

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

                match node_map.force_flush().await {
                    Ok(_) => {
                        consecutive_errors = 0;
                        let stats = node_map.stats();

                        let queue_stats = {
                            let queue_guard = url_queue.lock();
                            queue_guard.stats()
                        };

                        println!(
                            "Checkpoint saved: {} | Queue: {}",
                            stats, queue_stats
                        );
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        eprintln!("WARNING: Checkpoint failed (attempt {}): {}", consecutive_errors, e);

                        if consecutive_errors >= 3 {
                            eprintln!("ERROR: Checkpoint failed 3 times. Stopping auto-save to prevent data corruption.");
                            break;
                        }
                    }
                }
            }
        }))
    }

    /// Spawn a worker task
    async fn spawn_worker(&self, worker_id: u32) -> tokio::task::JoinHandle<Result<(), String>> {
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
            println!("Worker {} started", worker_id);
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

                        
                        let url_domain = if let Ok(parsed_url) = url::Url::parse(&url) {
                            parsed_url.host_str().unwrap_or("").to_string()
                        } else {
                            String::new()
                        };

                        
                        if failures.should_skip(&url_domain) {
                            let failure_count = failures.get_failure_count(&url_domain);
                            if let Some(_backoff_secs) =
                                failures.get_backoff_remaining(&url_domain)
                            {
                                
                                continue;
                            } else if failure_count >= 5 {
                                
                                println!(
                                    "Worker {}: Skipping domain {} (failed {} times, permanently blocked)",
                                    worker_id, url_domain, failure_count
                                );
                                continue;
                            }
                            continue;
                        }

                        
                        if !failures.can_request(&url_domain) {
                            
                            
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            
                            let _ = sender.send(queued_url).await;
                            continue;
                        }

                        
                        if !failures.acquire_request(&url_domain) {
                            
                            tokio::time::sleep(Duration::from_millis(10)).await;
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
                                    
                                    eprintln!(
                                        "WARNING: Redis lock error for {}: {}. Continuing without lock.",
                                        url, e
                                    );
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
                            // Process URL in background (slow pages won't block workers)
                            let result = Self::process_url(
                                &url_bg,
                                &http_bg,
                                &robots_bg,
                                &user_agent_bg,
                            )
                            .await;

                            
                            match result {
                                Ok(Some(fetch_result)) => {
                                    
                                    let links = extract_links(&fetch_result.content);

                                    
                                    let title = Self::extract_title(&fetch_result.content);

                                    
                                    if let Err(e) = node_map_bg.update_node(
                                        &url_bg,
                                        fetch_result.status_code,
                                        fetch_result.content_type.clone(),
                                        title,
                                        links.clone(),
                                    ) {
                                        eprintln!(
                                            "Background task: Failed to update node map for {}: {}",
                                            url_bg, e
                                        );
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

                                    if new_urls_added > 0 {
                                        println!(
                                            "Processed {} - added {} new URLs (depth: {})",
                                            url_bg, new_urls_added, depth
                                        );
                                    }
                                }
                                Ok(None) => {
                                    // URL was skipped (robots.txt or non-200 response)
                                }
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
                                        format!("{}", e)
                                    };

                                    println!("{} - {}", url_domain_bg, error_msg);
                                }
                            }

                            
                            if redis_lock_acquired {
                                if let Some(lock_manager) = &lock_manager_bg {
                                    let mut manager = lock_manager.lock().await;
                                    if let Err(e) = manager.release_url(&url_bg).await {
                                        eprintln!(
                                            "WARNING: Failed to release Redis lock for {}: {}",
                                            url_bg, e
                                        );
                                    }
                                }
                            }

                            
                            failures_bg.release_request(&url_domain_bg);
                        });

                        
                        if processed_count % 100 == 0
                            || last_progress_report.elapsed().as_secs() >= 60
                        {
                            let stats = node_map.stats();
                            let channel_len = receiver.len();
                            println!(
                                "Worker {}: Spawned {} tasks, {}, Channel queue: {}",
                                worker_id, processed_count, stats, channel_len
                            );

                            last_progress_report = std::time::Instant::now();
                        }

                        
                        
                    }
                    None => {
                        
                        
                        sleep(Duration::from_millis(100)).await;

                        
                        if receiver.is_disconnected() || receiver.is_empty() {
                            
                            sleep(Duration::from_millis(500)).await;
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

    /// Process a single URL
    async fn process_url(
        url: &str,
        http: &HttpClient,
        robots: &Option<RobotsTxt>,
        user_agent: &str,
    ) -> Result<Option<crate::network::FetchResult>, String> {
        // Check robots.txt compliance
        if let Some(robots) = robots {
            if !robots.is_allowed(url, user_agent) {
                return Ok(None); // Skip this URL
            }
        }

        // Check if URL should be crawled
        if !Self::should_crawl_url(url) {
            return Ok(None); // Skip this URL
        }

        // Fetch the page
        match http.fetch(url).await {
            Ok(result) => {
                if result.status_code == 200 {
                    if let Some(content_type) = result.content_type.as_ref() {
                        if !Self::is_html_content_type(content_type) {
                            println!("Skipping non-HTML content at {} ({})", url, content_type);
                            return Ok(None);
                        }
                    }
                    Ok(Some(result))
                } else {
                    Ok(None) // Skip non-200 responses
                }
            }
            Err(FetchError::Timeout) => Err("Timeout fetching URL".to_string()),
            Err(FetchError::NetworkError(e)) => Err(format!("Network error: {}", e)),
            Err(FetchError::ContentTooLarge(size, max)) => Err(format!(
                "Content too large: {} bytes (max: {} bytes)",
                size, max
            )),
            Err(e) => Err(format!("Fetch error: {}", e)),
        }
    }

    /// Determine if a URL should be crawled
    fn should_crawl_url(url: &str) -> bool {
        // Basic URL validation
        if let Ok(parsed_url) = url::Url::parse(url) {
            // Only crawl HTTP/HTTPS URLs
            if !matches!(parsed_url.scheme(), "http" | "https") {
                return false;
            }

            // Skip fragment-only URLs
            if parsed_url.fragment().is_some()
                && parsed_url.path() == "/"
                && parsed_url.query().is_none()
            {
                return false;
            }

            // Skip common non-content URLs
            let path = parsed_url.path().to_lowercase();
            const DISALLOWED_EXTENSIONS: &[&str] = &[
                ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4",
                ".avi", ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
                ".tar", ".gz", ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso",
                ".apk",
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

            return true;
        }

        false
    }

    fn is_html_content_type(content_type: &str) -> bool {
        let lower = content_type.to_ascii_lowercase();
        lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml")
    }

    /// Convert relative URL to absolute URL
    fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
        if link.starts_with("http://") || link.starts_with("https://") {
            return Ok(link.to_string());
        }

        let base = url::Url::parse(base_url).map_err(|e| e.to_string())?;

        if link.starts_with("//") {
            Ok(format!("{}:{}", base.scheme(), link))
        } else if link.starts_with('/') {
            Ok(format!(
                "{}://{}{}",
                base.scheme(),
                base.host_str().unwrap_or(""),
                link
            ))
        } else if link.starts_with('#') {
            Ok(base_url.to_string())
        } else {
            let base_path = base.path();
            let base_dir = if base_path.ends_with('/') {
                base_path
            } else {
                base_path
                    .rsplit_once('/')
                    .map(|(dir, _)| dir)
                    .unwrap_or("/")
            };

            let resolved_path = if base_dir.ends_with('/') {
                format!("{}{}", base_dir, link)
            } else {
                format!("{}/{}", base_dir, link)
            };

            Ok(format!(
                "{}://{}{}",
                base.scheme(),
                base.host_str().unwrap_or(""),
                resolved_path
            ))
        }
    }

    /// Get domain from URL
    fn get_domain(&self, url: &str) -> String {
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                return host.to_string();
            }
        }
        String::new()
    }

    /// Check if two URLs are from the same domain (including subdomains)
    fn is_same_domain(url: &str, base_domain: &str) -> bool {
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                return host == base_domain
                    || host.ends_with(&format!(".{}", base_domain))
                    || base_domain.ends_with(&format!(".{}", host));
            }
        }
        false
    }

    /// Extract title from HTML content
    fn extract_title(content: &str) -> Option<String> {
        // Simple regex-based title extraction
        if let Some(start) = content.find("<title>") {
            if let Some(end) = content[start..].find("</title>") {
                let title = &content[start + 7..start + end];
                return Some(title.trim().to_string());
            }
        }
        None
    }

    /// Fetch robots.txt from the start URL
    async fn fetch_robots(&self) -> Option<RobotsTxt> {
        if let Ok(parsed_url) = url::Url::parse(&self.start_url) {
            if let Some(host) = parsed_url.host_str() {
                let robots_url = format!("{}://{}/robots.txt", parsed_url.scheme(), host);

                match self.http.fetch(&robots_url).await {
                    Ok(result) => {
                        if result.status_code == 200 {
                            println!("Fetched robots.txt from {}", robots_url);
                            Some(RobotsTxt::new(&result.content, &self.config.user_agent))
                        } else {
                            println!("robots.txt not found (status: {})", result.status_code);
                            None
                        }
                    }
                    Err(e) => {
                        println!("Failed to fetch robots.txt: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Stop the crawler
    pub async fn stop(&self) {
        let mut running = self.running.lock();
        *running = false;
        println!("BFS Crawler stopped");
    }

    /// Get current crawler statistics (LOCK-FREE!)
    pub async fn get_stats(&self) -> NodeMapStats {
        self.node_map.stats()
    }

    /// Save crawler state to disk and CLEANUP queue
    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Save node map (LOCK-FREE!)
        self.node_map.force_flush().await?;

        // Flush and DELETE persistent queue (we only keep node_map)
        {
            let mut queue = self.queue.lock();
            queue.force_flush()?;
            // Clear queue files - only keep node_map as requested
            queue.clear()?;
        }

        println!("Node map saved, queue cleaned up");
        Ok(())
    }

    /// Export the final sitemap to JSONL (LOCK-FREE!)
    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Exporting sitemap to JSONL...");
        self.node_map.export_to_jsonl(output_path)?;
        println!("Sitemap exported successfully");
        Ok(())
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
        let crawler = BfsCrawlerState::new(
            "https://test.local".to_string(),
            temp_dir.path(),
            config
        ).await.unwrap();

        assert_eq!(crawler.start_url, "https://test.local");
        assert_eq!(crawler.config.max_workers, 256);
    }

    #[test]
    fn test_should_crawl_url() {
        assert!(BfsCrawlerState::should_crawl_url("https://test.local/page"));
        assert!(BfsCrawlerState::should_crawl_url("http://test.local/page"));
        assert!(!BfsCrawlerState::should_crawl_url("ftp://test.local/page"));
        assert!(!BfsCrawlerState::should_crawl_url("https://test.local/file.pdf"));
        assert!(!BfsCrawlerState::should_crawl_url("https://test.local/image.jpg"));
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
            BfsCrawlerState::new(
                "https://test.local".to_string(),
                dir.path(),
                config
            ).await.unwrap()
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
            BfsCrawlerState::convert_to_absolute_url("https://other.local/page", "https://test.local").unwrap(),
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
        assert!(BfsCrawlerState::is_html_content_type("text/html; charset=utf-8"));
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
        let crawler = BfsCrawlerState::new(
            "https://test.local".to_string(),
            dir.path(),
            config
        ).await.unwrap();

        let stats = crawler.get_stats().await;
        assert_eq!(stats.total_nodes, 0);
    }

}
