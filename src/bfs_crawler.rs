use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use std::collections::HashMap;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use kanal::{AsyncSender, AsyncReceiver, bounded_async};

use crate::network::{FetchError, HttpClient};
use crate::node_map::{NodeMap, NodeMapStats};
use crate::parser::extract_links;
use crate::rkyv_queue::{QueuedUrl, RkyvQueue};
use crate::robots::RobotsTxt;
use crate::url_lock_manager::UrlLockManager;

/// Track failures per domain to skip problematic subdomains with exponential backoff
#[derive(Debug, Clone)]
struct DomainState {
    failures: usize,
    last_failure: std::time::Instant,
    backoff_until: std::time::Instant,
    concurrent_requests: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DomainFailureTracker {
    domains: Arc<Mutex<HashMap<String, DomainState>>>,
    failure_threshold: usize,
    max_concurrent_per_domain: usize,
}

impl DomainFailureTracker {
    fn new(failure_threshold: usize, max_concurrent_per_domain: usize) -> Self {
        Self {
            domains: Arc::new(Mutex::new(HashMap::new())),
            failure_threshold,
            max_concurrent_per_domain,
        }
    }

    fn record_failure(&self, domain: &str) {
        let mut domains = self.domains.lock();
        let now = std::time::Instant::now();
        let state = domains.entry(domain.to_string()).or_insert(DomainState {
            failures: 0,
            last_failure: now,
            backoff_until: now,
            concurrent_requests: 0,
        });
        state.failures += 1;
        state.last_failure = now;
        
        // Exponential backoff: 2^failures seconds, max 300 seconds (5 minutes)
        let backoff_secs = (2_u32.pow(state.failures.min(8) as u32)).min(300) as u64;
        state.backoff_until = now + std::time::Duration::from_secs(backoff_secs);
    }

    fn should_skip(&self, domain: &str) -> bool {
        let domains = self.domains.lock();
        if let Some(state) = domains.get(domain) {
            let now = std::time::Instant::now();
            // Skip if too many failures or in backoff period
            if state.failures >= self.failure_threshold {
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
            state.concurrent_requests < self.max_concurrent_per_domain
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
            concurrent_requests: 0,
        });
        
        if state.concurrent_requests < self.max_concurrent_per_domain {
            state.concurrent_requests += 1;
            true
        } else {
            false
        }
    }

    fn release_request(&self, domain: &str) {
        let mut domains = self.domains.lock();
        if let Some(state) = domains.get_mut(domain) {
            state.concurrent_requests = state.concurrent_requests.saturating_sub(1);
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

/// BFS Crawler configuration
#[derive(Debug, Clone)]
pub struct BfsCrawlerConfig {
    pub max_workers: u32,
    pub rate_limit: u32,
    pub timeout: u32,
    pub user_agent: String,
    pub ignore_robots: bool,
    pub max_memory_bytes: usize,
    pub auto_save_interval: u64,
    pub redis_url: Option<String>,
    pub lock_ttl: u64,
    pub enable_redis_locking: bool,
}

impl Default for BfsCrawlerConfig {
    fn default() -> Self {
        Self {
            max_workers: 256, // HIGH CONCURRENCY: Allow many workers to process URLs simultaneously
            rate_limit: 200, // Increased rate limit to handle more concurrent requests
            timeout: 45, // Longer timeout to allow slow pages to load (45 seconds)
            user_agent: "Rust-Sitemap-BFS-Crawler/1.0".to_string(),
            ignore_robots: false,
            max_memory_bytes: 10 * 1024 * 1024 * 1024, // 10GB for high concurrency
            auto_save_interval: 300,                  // 5 minutes
            redis_url: None,                          // Optional Redis URL
            lock_ttl: 60,                             // 60 seconds default
            enable_redis_locking: false,              // Disabled by default
        }
    }
}

/// BFS Crawler state - **MOSTLY LOCK-FREE**
pub struct BfsCrawlerState {
    pub config: BfsCrawlerConfig,
    pub node_map: Arc<NodeMap>, // Lock-free! No mutex needed (DashMap internally)
    pub url_queue_persistent: Arc<Mutex<RkyvQueue>>, // For persistence only
    pub url_sender: AsyncSender<QueuedUrl>, // High-performance kanal channel
    pub url_receiver: AsyncReceiver<QueuedUrl>, // High-performance kanal channel
    pub http_client: Arc<HttpClient>,
    pub rate_limiter: Arc<Semaphore>,
    pub robots_txt: Option<RobotsTxt>,
    pub start_url: String,
    pub is_running: Arc<Mutex<bool>>,
    pub(crate) failure_tracker: DomainFailureTracker, // Track failing domains (pub(crate) to match type visibility)
    pub url_lock_manager: Option<Arc<tokio::sync::Mutex<UrlLockManager>>>, // Optional Redis-based URL lock manager
}

/// BFS Crawler result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfsCrawlerResult {
    pub start_url: String,
    pub total_discovered: usize,
    pub total_processed: usize,
    pub crawl_duration_seconds: u64,
    pub final_stats: NodeMapStats,
}

impl BfsCrawlerState {
    pub async fn new<P: AsRef<std::path::Path>>(
        start_url: String,
        data_dir: P,
        config: BfsCrawlerConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create HTTP client
        let http_client = Arc::new(HttpClient::new(
            config.user_agent.clone(),
            config.timeout as u64,
        ));

        // Create rate limiter
        let rate_limiter = Arc::new(Semaphore::new(config.rate_limit as usize));

        // Calculate memory allocation: optimized for M4 Max with high memory
        // Reserve memory based on actual max_memory_bytes configuration
        let max_memory_nodes = 100_000; // Increased to 100K nodes in memory
        let max_queue_memory = config.max_workers as usize * 500; // Scaled with workers

        // Create node map for duplicate detection and final sitemap (LOCK-FREE!)
        let node_map = Arc::new(NodeMap::new(&data_dir, max_memory_nodes)?);

        // Create persistent URL queue using rkyv (for disk storage and recovery)
        let url_queue_persistent = Arc::new(Mutex::new(RkyvQueue::new(&data_dir, max_queue_memory)?));

        // Create high-performance kanal channels for in-flight URLs
        // Reduced channel buffer to prevent queue bloat when domains timeout
        // Smaller buffer = faster feedback when domains are slow
        let channel_capacity = (config.max_workers as usize * 100).max(50_000);
        let (url_sender, url_receiver) = bounded_async(channel_capacity);

        // Create failure tracker with per-domain rate limiting
        // - Skip domains after 5 failures (reduced from 10)
        // - Max 5 concurrent requests per domain (prevents hammering slow domains)
        let failure_tracker = DomainFailureTracker::new(5, 5);

        // Initialize Redis-based URL lock manager if enabled
        let url_lock_manager = if config.enable_redis_locking {
            if let Some(redis_url) = &config.redis_url {
                match UrlLockManager::new(redis_url, Some(config.lock_ttl)).await {
                    Ok(manager) => {
                        println!("✅ Redis URL lock manager initialized (TTL: {}s)", config.lock_ttl);
                        Some(Arc::new(tokio::sync::Mutex::new(manager)))
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to connect to Redis: {}. Continuing without distributed locking.", e);
                        None
                    }
                }
            } else {
                eprintln!("⚠️ Redis locking enabled but no redis_url provided. Continuing without distributed locking.");
                None
            }
        } else {
            None
        };

        Ok(Self {
            config,
            node_map,
            url_queue_persistent,
            url_sender,
            url_receiver,
            http_client,
            rate_limiter,
            robots_txt: None,
            start_url,
            is_running: Arc::new(Mutex::new(false)),
            failure_tracker,
            url_lock_manager,
        })
    }

    /// Initialize the crawler with the start URL  
    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Initializing BFS Crawler");
        println!("  Start URL: {}", self.start_url);
        println!("  Max Workers: {}", self.config.max_workers);
        println!("  Max Depth: Unlimited");
        println!("  Rate Limit: {} req/s", self.config.rate_limit);
        println!(
            "  Max Memory: {} GB",
            self.config.max_memory_bytes / (1024 * 1024 * 1024)
        );

        // Add start URL to node map and queue (LOCK-FREE!)
        self.node_map.add_url(self.start_url.clone(), 0, None)?;

        // Add to both persistent queue and kanal channel
        let start_queued_url = QueuedUrl::new(self.start_url.clone(), 0, None);
        {
            let mut queue = self.url_queue_persistent.lock();
            queue.push_back(start_queued_url.clone())?;
        }
        // Send to kanal channel for fast processing
        self.url_sender.send(start_queued_url).await
            .map_err(|e| format!("Failed to send start URL: {}", e))?;

        // Fetch robots.txt unless ignore_robots is set
        if !self.config.ignore_robots {
            self.robots_txt = self.fetch_robots_txt().await;
        }

        println!("✅ BFS Crawler initialized successfully");
        Ok(())
    }

    /// Start the BFS crawling process
    pub async fn start_crawling(&self) -> Result<BfsCrawlerResult, Box<dyn std::error::Error>> {
        let start_time = SystemTime::now();
        {
            let mut is_running = self.is_running.lock();
            *is_running = true;
        }

        println!("🔄 Starting BFS crawling process...");

        let auto_save_handle = self.spawn_auto_save_task();

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..self.config.max_workers {
            let handle = self.spawn_worker(worker_id).await;
            handles.push(handle);
        }

        // Wait for all workers to complete
        for handle in handles {
            let _ = handle.await?;
        }

        let end_time = SystemTime::now();
        let duration = end_time.duration_since(start_time).unwrap_or_default();

        // Get final statistics
        let final_stats = self.get_stats().await;

        // Ensure auto-save loop exits cleanly
        self.stop().await;
        if let Some(handle) = auto_save_handle {
            handle.abort();
            if let Err(e) = handle.await {
                if !e.is_cancelled() {
                    println!("⚠️ Auto-save task ended with error: {}", e);
                }
            }
        }

        let result = BfsCrawlerResult {
            start_url: self.start_url.clone(),
            total_discovered: final_stats.total_nodes,
            total_processed: final_stats.total_nodes,
            crawl_duration_seconds: duration.as_secs(),
            final_stats: final_stats.clone(),
        };

        println!("🏁 BFS crawling completed!");
        println!("  Total discovered: {}", result.total_discovered);
        println!("  Total processed: {}", result.total_processed);
        println!("  Duration: {} seconds", result.crawl_duration_seconds);
        println!("  Final stats: {}", final_stats);

        Ok(result)
    }

    fn spawn_auto_save_task(&self) -> Option<JoinHandle<()>> {
        if self.config.auto_save_interval == 0 {
            return None;
        }

        let interval = Duration::from_secs(self.config.auto_save_interval);
        let node_map = Arc::clone(&self.node_map);
        let url_queue = Arc::clone(&self.url_queue_persistent);
        let is_running = Arc::clone(&self.is_running);

        Some(tokio::spawn(async move {
            loop {
                let should_continue = {
                    let guard = is_running.lock();
                    *guard
                };
                if !should_continue {
                    break;
                }

                sleep(interval).await;

                let should_continue = {
                    let guard = is_running.lock();
                    *guard
                };
                if !should_continue {
                    break;
                }

                let stats = node_map.stats(); // Lock-free!

                let queue_stats = {
                    let queue_guard = url_queue.lock();
                    queue_guard.stats()
                };

                println!("💾 Auto-checkpoint: {} | Queue (persistent): {}", stats, queue_stats);
            }
        }))
    }

    /// Spawn a worker task
    async fn spawn_worker(&self, worker_id: u32) -> tokio::task::JoinHandle<Result<(), String>> {
        let node_map = Arc::clone(&self.node_map);
        let url_receiver = self.url_receiver.clone(); // Clone kanal receiver
        let url_sender = self.url_sender.clone(); // Clone kanal sender
        let http_client = Arc::clone(&self.http_client);
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let robots_txt = self.robots_txt.clone();
        let user_agent = self.config.user_agent.clone();
        let is_running = Arc::clone(&self.is_running);
        let start_url_domain = self.get_domain(&self.start_url);
        let failure_tracker = self.failure_tracker.clone();
        let url_lock_manager = self.url_lock_manager.clone();

        tokio::spawn(async move {
            println!("👷 Worker {} started", worker_id);
            let mut processed_count = 0;
            let mut last_progress_report = std::time::Instant::now();

            loop {
                // Check if crawler is still running
                {
                    let running = is_running.lock();
                    if !*running {
                        break;
                    }
                }

                // Get next URL from kanal channel (zero-copy, lock-free)
                let next_url_item = match url_receiver.recv().await {
                    Ok(url) => Some(url),
                    Err(_) => None, // Channel closed
                };

                match next_url_item {
                    Some(queued_url) => {
                        let url = queued_url.url.clone();
                        let depth = queued_url.depth;

                        // Extract domain from URL for failure tracking
                        let url_domain = if let Ok(parsed_url) = url::Url::parse(&url) {
                            parsed_url.host_str().unwrap_or("").to_string()
                        } else {
                            String::new()
                        };

                        // Skip if domain has failed too many times or is in backoff
                        if failure_tracker.should_skip(&url_domain) {
                            let failure_count = failure_tracker.get_failure_count(&url_domain);
                            if let Some(_backoff_secs) = failure_tracker.get_backoff_remaining(&url_domain) {
                                // In backoff - silently skip
                                continue;
                            } else if failure_count >= 5 {
                                // Permanently skipped
                                println!(
                                    "⛔ Worker {}: Skipping domain {} (failed {} times, permanently blocked)",
                                    worker_id, url_domain, failure_count
                                );
                                continue;
                            }
                            continue;
                        }

                        // Check if we can make a request to this domain (per-domain rate limiting)
                        if !failure_tracker.can_request(&url_domain) {
                            // Domain is at max concurrent requests - put URL back in queue
                            // Add small delay to prevent tight loop
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            // Try to send back to queue for later processing
                            let _ = url_sender.send(queued_url).await;
                            continue;
                        }

                        // Try to acquire domain permit
                        if !failure_tracker.acquire_request(&url_domain) {
                            // Failed to acquire - put back in queue
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            let _ = url_sender.send(queued_url).await;
                            continue;
                        }

                        // Try to acquire Redis URL lock (if enabled)
                        let redis_lock_acquired = if let Some(lock_manager) = &url_lock_manager {
                            let mut manager = lock_manager.lock().await;
                            match manager.try_acquire_url(&url).await {
                                Ok(true) => true,  // Lock acquired
                                Ok(false) => {
                                    // Another worker is processing this URL
                                    // Release domain permit and skip this URL
                                    failure_tracker.release_request(&url_domain);
                                    continue;
                                }
                                Err(e) => {
                                    // Redis error - log and continue without lock
                                    eprintln!("⚠️ Redis lock error for {}: {}. Continuing without lock.", url, e);
                                    false
                                }
                            }
                        } else {
                            false  // Redis locking not enabled
                        };

                        // Acquire global rate limiter permit
                        let _permit = rate_limiter.acquire().await.unwrap();

                        // **NON-BLOCKING: Spawn background task to process URL**
                        // Worker immediately moves to next URL while page loads in background
                        processed_count += 1;
                        
                        let node_map_bg = node_map.clone();
                        let http_client_bg = http_client.clone();
                        let robots_txt_bg = robots_txt.clone();
                        let user_agent_bg = user_agent.clone();
                        let url_sender_bg = url_sender.clone();
                        let start_url_domain_bg = start_url_domain.clone();
                        let failure_tracker_bg = failure_tracker.clone();
                        let url_bg = url.clone();
                        let url_domain_bg = url_domain.clone();
                        let url_lock_manager_bg = url_lock_manager.clone();

                        tokio::spawn(async move {
                            // Process URL in background (slow pages won't block workers)
                            let result = Self::process_url(&url_bg, &http_client_bg, &robots_txt_bg, &user_agent_bg).await;
                            
                            // Process result
                            match result {
                                Ok(Some(fetch_result)) => {
                                    // Extract links from the page
                                    let links = extract_links(&fetch_result.content);

                                    // Extract title from content
                                    let title = Self::extract_title(&fetch_result.content);

                                    // Update node map with crawled data (LOCK-FREE!)
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

                                    // Add discovered links to queue - batch process for efficiency
                                    let mut new_urls_added = 0;
                                    let mut urls_to_add = Vec::new();
                                    
                                    for link in links {
                                        if let Ok(absolute_url) =
                                            Self::convert_to_absolute_url(&link, &url_bg)
                                        {
                                            // Only crawl URLs from the same domain
                                            if Self::is_same_domain(&absolute_url, &start_url_domain_bg)
                                                && Self::should_crawl_url(&absolute_url)
                                            {
                                                urls_to_add.push(absolute_url);
                                            }
                                        }
                                    }
                                    
                                    // Add discovered links - LOCK-FREE with DashMap!
                                    if !urls_to_add.is_empty() {
                                        let mut urls_to_queue = Vec::new();
                                        
                                        for absolute_url in urls_to_add {
                                            // Lock-free contains check (bloom + sled) - DEDUP BEFORE QUEUEING
                                            if !node_map_bg.contains(&absolute_url) {
                                                // Lock-free add_url (bloom + sled + dashmap)
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
                                        
                                        // Send to channel (lock-free kanal) - only URLs that passed dedup
                                        for absolute_url in urls_to_queue {
                                            let queued_url = QueuedUrl::new(
                                                absolute_url,
                                                depth + 1,
                                                Some(url_bg.clone()),
                                            );
                                            if url_sender_bg.send(queued_url).await.is_ok() {
                                                new_urls_added += 1;
                                            }
                                        }
                                    }

                                    if new_urls_added > 0 {
                                        println!(
                                            "✅ Processed {} - added {} new URLs (depth: {})",
                                            url_bg, new_urls_added, depth
                                        );
                                    }
                                }
                                Ok(None) => {
                                    // URL was skipped (robots.txt or non-200 response)
                                }
                                Err(e) => {
                                    // Record failure for this domain
                                    if !url_domain_bg.is_empty() {
                                        failure_tracker_bg.record_failure(&url_domain_bg);
                                    }
                                    
                                    // Print error message with better formatting
                                    let error_msg = if e.contains("Connection refused") {
                                        format!("❌ Connection refused")
                                    } else if e.contains("timeout") || e.contains("timed out") {
                                        format!("⏱️  Timeout")
                                    } else if e.contains("DNS") {
                                        format!("🌐 DNS error")
                                    } else if e.contains("SSL") || e.contains("TLS") {
                                        format!("🔒 SSL/TLS error")
                                    } else if e.contains("Content too large") {
                                        format!("📦 Content too large")
                                    } else if e.contains("Network error") || e.contains("record overflow") {
                                        format!("❌ Network error")
                                    } else {
                                        format!("❌ {}", e)
                                    };
                                    
                                    println!("{} - {}", url_domain_bg, error_msg);
                                }
                            }

                            // Always release Redis lock when done (success or failure)
                            if redis_lock_acquired {
                                if let Some(lock_manager) = &url_lock_manager_bg {
                                    let mut manager = lock_manager.lock().await;
                                    if let Err(e) = manager.release_url(&url_bg).await {
                                        eprintln!("⚠️ Failed to release Redis lock for {}: {}", url_bg, e);
                                    }
                                }
                            }

                            // Always release domain permit when done (success or failure)
                            failure_tracker_bg.release_request(&url_domain_bg);
                        });
                        
                        // Report progress periodically (all lock-free!)
                        if processed_count % 100 == 0
                            || last_progress_report.elapsed().as_secs() >= 60
                        {
                            let stats = node_map.stats(); // Lock-free!
                            let channel_len = url_receiver.len();
                            println!(
                                "Worker {}: Spawned {} tasks, {}, Channel queue: {}",
                                worker_id, processed_count, stats, channel_len
                            );

                            last_progress_report = std::time::Instant::now();
                        }

                        // REMOVED: Artificial sleep that was throttling throughput
                        // The rate limiter already controls request rate properly
                    }
                    None => {
                        // No more URLs to process - kanal channel is empty or closed
                        // Short sleep to avoid tight loop
                        sleep(Duration::from_millis(100)).await;

                        // Check if channel still has capacity (sender still alive)
                        if url_receiver.is_disconnected() || url_receiver.len() == 0 {
                            // Double-check after a shorter delay
                            sleep(Duration::from_millis(500)).await;
                            if url_receiver.is_disconnected() || url_receiver.len() == 0 {
                                println!("Worker {}: No more URLs to process, exiting", worker_id);
                                break;
                            }
                        }
                    }
                }
            }

            println!(
                "👷 Worker {} completed (processed {} pages)",
                worker_id, processed_count
            );
            Ok(())
        })
    }

    /// Process a single URL
    async fn process_url(
        url: &str,
        http_client: &HttpClient,
        robots_txt: &Option<RobotsTxt>,
        user_agent: &str,
    ) -> Result<Option<crate::network::FetchResult>, String> {
        // Check robots.txt compliance
        if let Some(robots) = robots_txt {
            if !robots.is_allowed(url, user_agent) {
                return Ok(None); // Skip this URL
            }
        }

        // Check if URL should be crawled
        if !Self::should_crawl_url(url) {
            return Ok(None); // Skip this URL
        }

        // Fetch the page
        match http_client.fetch(url).await {
            Ok(result) => {
                if result.status_code == 200 {
                    if let Some(content_type) = result.content_type.as_ref() {
                        if !Self::is_html_content_type(content_type) {
                            println!("📦 Skipping non-HTML content at {} ({})", url, content_type);
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
    async fn fetch_robots_txt(&self) -> Option<RobotsTxt> {
        if let Ok(parsed_url) = url::Url::parse(&self.start_url) {
            if let Some(host) = parsed_url.host_str() {
                let robots_url = format!("{}://{}/robots.txt", parsed_url.scheme(), host);

                match self.http_client.fetch(&robots_url).await {
                    Ok(result) => {
                        if result.status_code == 200 {
                            println!("📋 Fetched robots.txt from {}", robots_url);
                            Some(RobotsTxt::new(&result.content, &self.config.user_agent))
                        } else {
                            println!("📋 robots.txt not found (status: {})", result.status_code);
                            None
                        }
                    }
                    Err(e) => {
                        println!("📋 Failed to fetch robots.txt: {}", e);
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
        let mut is_running = self.is_running.lock();
        *is_running = false;
        println!("🛑 BFS Crawler stopped");
    }

    /// Get current crawler statistics (LOCK-FREE!)
    pub async fn get_stats(&self) -> NodeMapStats {
        self.node_map.stats()
    }

    /// Save crawler state to disk and CLEANUP queue
    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Save node map (LOCK-FREE!)
        self.node_map.force_flush()?;
        
        // Flush and DELETE persistent queue (we only keep node_map)
        {
            let mut queue = self.url_queue_persistent.lock();
            queue.force_flush()?;
            // Clear queue files - only keep node_map as requested
            queue.clear()?;
        }
        
        println!("✅ Node map saved, queue cleaned up");
        Ok(())
    }

    /// Export the final sitemap to JSONL (LOCK-FREE!)
    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(
        &self,
        output_path: P,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("📝 Exporting sitemap to JSONL...");
        self.node_map.export_to_jsonl(output_path)?;
        println!("✅ Sitemap exported successfully");
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
        let crawler =
            BfsCrawlerState::new("https://example.com".to_string(), temp_dir.path(), config)
                .await
                .unwrap();

        assert_eq!(crawler.start_url, "https://example.com");
        assert_eq!(crawler.config.max_workers, 4);
    }

    #[test]
    fn test_should_crawl_url() {
        assert!(BfsCrawlerState::should_crawl_url(
            "https://example.com/page"
        ));
        assert!(BfsCrawlerState::should_crawl_url("http://example.com/page"));
        assert!(!BfsCrawlerState::should_crawl_url("ftp://example.com/page"));
        assert!(!BfsCrawlerState::should_crawl_url(
            "https://example.com/file.pdf"
        ));
        assert!(!BfsCrawlerState::should_crawl_url(
            "https://example.com/image.jpg"
        ));
    }
}
