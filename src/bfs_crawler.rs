use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Duration};
use serde::{Serialize, Deserialize};
use std::time::SystemTime;

use crate::node_map::{NodeMap, NodeMapStats};
use crate::rkyv_queue::{RkyvQueue, QueuedUrl};
use crate::network::{HttpClient, FetchError};
use crate::parser::extract_links;
use crate::robots::RobotsTxt;

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
}

impl Default for BfsCrawlerConfig {
    fn default() -> Self {
        Self {
            max_workers: 4,
            rate_limit: 10,
            timeout: 30,
            user_agent: "Rust-Sitemap-BFS-Crawler/1.0".to_string(),
            ignore_robots: false,
            max_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            auto_save_interval: 300, // 5 minutes
        }
    }
}

/// BFS Crawler state
#[derive(Debug)]
pub struct BfsCrawlerState {
    pub config: BfsCrawlerConfig,
    pub node_map: Arc<Mutex<NodeMap>>,
    pub url_queue: Arc<Mutex<RkyvQueue>>,
    pub http_client: Arc<HttpClient>,
    pub rate_limiter: Arc<Semaphore>,
    pub robots_txt: Option<RobotsTxt>,
    pub start_url: String,
    pub is_running: Arc<Mutex<bool>>,
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
    pub fn new<P: AsRef<std::path::Path>>(
        start_url: String,
        data_dir: P,
        config: BfsCrawlerConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create HTTP client
        let http_client = Arc::new(HttpClient::new(config.user_agent.clone(), config.timeout as u64));
        
        // Create rate limiter
        let rate_limiter = Arc::new(Semaphore::new(config.rate_limit as usize));
        
        // Calculate memory allocation: aim to stay under 2GB total
        // Reserve ~500MB for node map, ~300MB for queue, rest for operations
        let max_memory_nodes = 50_000; // ~50K nodes in memory at once
        let max_queue_memory = config.max_workers as usize * 100; // 400 items for 4 workers
        
        // Create node map for duplicate detection and final sitemap
        let node_map = Arc::new(Mutex::new(NodeMap::new(
            &data_dir,
            max_memory_nodes,
        )?));
        
        // Create URL queue using rkyv
        let url_queue = Arc::new(Mutex::new(RkyvQueue::new(
            &data_dir,
            max_queue_memory,
        )?));
        
        Ok(Self {
            config,
            node_map,
            url_queue,
            http_client,
            rate_limiter,
            robots_txt: None,
            start_url,
            is_running: Arc::new(Mutex::new(false)),
        })
    }
    
    /// Initialize the crawler with the start URL
    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Initializing BFS Crawler");
        println!("  Start URL: {}", self.start_url);
        println!("  Max Workers: {}", self.config.max_workers);
        println!("  Max Depth: Unlimited");
        println!("  Rate Limit: {} req/s", self.config.rate_limit);
        println!("  Max Memory: {} GB", self.config.max_memory_bytes / (1024 * 1024 * 1024));
        
        // Add start URL to node map and queue
        {
            let mut node_map = self.node_map.lock().await;
            node_map.add_url(self.start_url.clone(), 0, None)?;
        }
        
        {
            let mut queue = self.url_queue.lock().await;
            queue.push_back(QueuedUrl::new(self.start_url.clone(), 0, None))?;
        }
        
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
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        drop(is_running);
        
        println!("🔄 Starting BFS crawling process...");
        
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
        let final_stats = {
            let node_map = self.node_map.lock().await;
            node_map.stats()
        };
        
        // Mark as stopped
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        
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
    
    /// Spawn a worker task
    async fn spawn_worker(&self, worker_id: u32) -> tokio::task::JoinHandle<Result<(), String>> {
        let node_map = Arc::clone(&self.node_map);
        let url_queue = Arc::clone(&self.url_queue);
        let http_client = Arc::clone(&self.http_client);
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let robots_txt = self.robots_txt.clone();
        let user_agent = self.config.user_agent.clone();
        let is_running = Arc::clone(&self.is_running);
        let start_url_domain = self.get_domain(&self.start_url);
        
        tokio::spawn(async move {
            println!("👷 Worker {} started", worker_id);
            let mut processed_count = 0;
            let mut last_progress_report = std::time::Instant::now();
            
            loop {
                // Check if crawler is still running
                {
                    let running = is_running.lock().await;
                    if !*running {
                        break;
                    }
                }
                
                // Get next URL from queue
                let next_url_item = {
                    let mut queue = url_queue.lock().await;
                    queue.pop_front()
                };
                
                match next_url_item {
                    Some(queued_url) => {
                        let url = queued_url.url;
                        let depth = queued_url.depth;
                        
                        // Acquire rate limiter permit
                        let _permit = rate_limiter.acquire().await.unwrap();
                        
                        // Process the URL
                        match Self::process_url(
                            &url,
                            &http_client,
                            &robots_txt,
                            &user_agent,
                        ).await {
                            Ok(Some(fetch_result)) => {
                                // Extract links from the page
                                let links = extract_links(&fetch_result.content);
                                
                                // Extract title from content
                                let title = Self::extract_title(&fetch_result.content);
                                
                                // Update node map with crawled data
                                {
                                    let mut node_map_guard = node_map.lock().await;
                                    if let Err(e) = node_map_guard.update_node(
                                        &url,
                                        fetch_result.status_code,
                                        fetch_result.content_type.clone(),
                                        title,
                                        links.clone(),
                                    ) {
                                        println!("Worker {}: Failed to update node map for {}: {}", worker_id, url, e);
                                    }
                                }
                                
                                // Add discovered links to queue
                                let mut new_urls_added = 0;
                                for link in links {
                                    if let Ok(absolute_url) = Self::convert_to_absolute_url(&link, &url) {
                                        // Only crawl URLs from the same domain
                                        if Self::is_same_domain(&absolute_url, &start_url_domain) &&
                                           Self::should_crawl_url(&absolute_url) {
                                            
                                            // Check if URL is already in node map
                                            let already_seen = {
                                                let node_map_guard = node_map.lock().await;
                                                node_map_guard.contains(&absolute_url)
                                            };
                                            
                                            if !already_seen {
                                                // Add to node map
                                                {
                                                    let mut node_map_guard = node_map.lock().await;
                                                    if let Ok(added) = node_map_guard.add_url(
                                                        absolute_url.clone(),
                                                        depth + 1,
                                                        Some(url.clone()),
                                                    ) {
                                                        if added {
                                                            // Add to queue
                                                            let mut queue = url_queue.lock().await;
                                                            if let Ok(_) = queue.push_back(QueuedUrl::new(
                                                                absolute_url,
                                                                depth + 1,
                                                                Some(url.clone()),
                                                            )) {
                                                                new_urls_added += 1;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                processed_count += 1;
                                
                                if new_urls_added > 0 {
                                    println!("Worker {}: Processed {} - added {} new URLs (depth: {})", 
                                             worker_id, url, new_urls_added, depth);
                                }
                                
                                // Report progress
                                if processed_count % 10 == 0 || last_progress_report.elapsed().as_secs() >= 30 {
                                    let stats = {
                                        let node_map_guard = node_map.lock().await;
                                        node_map_guard.stats()
                                    };
                                    let queue_stats = {
                                        let queue = url_queue.lock().await;
                                        queue.stats()
                                    };
                                    println!("Worker {}: Processed {} pages, {}, {}", 
                                             worker_id, processed_count, stats, queue_stats);
                                    
                                    last_progress_report = std::time::Instant::now();
                                }
                            }
                            Ok(None) => {
                                // URL was skipped (robots.txt or non-200 response)
                            }
                            Err(e) => {
                                println!("Worker {}: Failed to process {}: {}", worker_id, url, e);
                            }
                        }
                        
                        // Small delay to prevent overwhelming the server
                        sleep(Duration::from_millis(100)).await;
                    }
                    None => {
                        // No more URLs to process, check if we should continue
                        sleep(Duration::from_millis(500)).await;
                        
                        // Check if there are more URLs in the queue
                        let has_more = {
                            let queue = url_queue.lock().await;
                            !queue.is_empty()
                        };
                        
                        if !has_more {
                            // Double-check after a longer delay
                            sleep(Duration::from_millis(2000)).await;
                            let still_has_more = {
                                let queue = url_queue.lock().await;
                                !queue.is_empty()
                            };
                            
                            if !still_has_more {
                                println!("Worker {}: No more URLs to process, exiting", worker_id);
                                break;
                            }
                        }
                    }
                }
            }
            
            println!("👷 Worker {} completed (processed {} pages)", worker_id, processed_count);
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
                    Ok(Some(result))
                } else {
                    Ok(None) // Skip non-200 responses
                }
            }
            Err(FetchError::Timeout) => {
                Err("Timeout fetching URL".to_string())
            }
            Err(FetchError::NetworkError(e)) => {
                Err(format!("Network error: {}", e))
            }
            Err(FetchError::ContentTooLarge(size, max)) => {
                Err(format!("Content too large: {} bytes (max: {} bytes)", size, max))
            }
            Err(e) => {
                Err(format!("Fetch error: {}", e))
            }
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
            if parsed_url.fragment().is_some() && parsed_url.path() == "/" && parsed_url.query().is_none() {
                return false;
            }
            
            // Skip common non-content URLs
            let path = parsed_url.path().to_lowercase();
            if path.ends_with(".pdf") || path.ends_with(".jpg") || path.ends_with(".jpeg") || 
               path.ends_with(".png") || path.ends_with(".gif") || path.ends_with(".css") || 
               path.ends_with(".js") || path.ends_with(".xml") || path.ends_with(".zip") ||
               path.ends_with(".mp4") || path.ends_with(".avi") || path.ends_with(".mov") ||
               path.ends_with(".mp3") || path.ends_with(".wav") || path.ends_with(".doc") ||
               path.ends_with(".docx") || path.ends_with(".xls") || path.ends_with(".xlsx") {
                return false;
            }
            
            return true;
        }
        
        false
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
            Ok(format!("{}://{}{}", base.scheme(), base.host_str().unwrap_or(""), link))
        } else if link.starts_with('#') {
            Ok(base_url.to_string())
        } else {
            let base_path = base.path();
            let base_dir = if base_path.ends_with('/') {
                base_path
            } else {
                base_path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("/")
            };
            
            let resolved_path = if base_dir.ends_with('/') {
                format!("{}{}", base_dir, link)
            } else {
                format!("{}/{}", base_dir, link)
            };
            
            Ok(format!("{}://{}{}", base.scheme(), base.host_str().unwrap_or(""), resolved_path))
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
                return host == base_domain || 
                       host.ends_with(&format!(".{}", base_domain)) ||
                       base_domain.ends_with(&format!(".{}", host));
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
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        println!("🛑 BFS Crawler stopped");
    }
    
    /// Get current crawler statistics
    pub async fn get_stats(&self) -> NodeMapStats {
        let node_map = self.node_map.lock().await;
        node_map.stats()
    }
    
    /// Save crawler state to disk
    pub async fn save_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut node_map = self.node_map.lock().await;
            node_map.force_flush()?;
        }
        {
            let mut queue = self.url_queue.lock().await;
            queue.force_flush()?;
        }
        Ok(())
    }
    
    /// Export the final sitemap to JSONL
    pub async fn export_to_jsonl<P: AsRef<std::path::Path>>(&self, output_path: P) -> Result<(), Box<dyn std::error::Error>> {
        println!("📝 Exporting sitemap to JSONL...");
        let node_map = self.node_map.lock().await;
        node_map.export_to_jsonl(output_path)?;
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
        let crawler = BfsCrawlerState::new(
            "https://example.com".to_string(),
            temp_dir.path(),
            config,
        ).unwrap();
        
        assert_eq!(crawler.start_url, "https://example.com");
        assert_eq!(crawler.config.max_workers, 4);
    }

    #[test]
    fn test_should_crawl_url() {
        assert!(BfsCrawlerState::should_crawl_url("https://example.com/page"));
        assert!(BfsCrawlerState::should_crawl_url("http://example.com/page"));
        assert!(!BfsCrawlerState::should_crawl_url("ftp://example.com/page"));
        assert!(!BfsCrawlerState::should_crawl_url("https://example.com/file.pdf"));
        assert!(!BfsCrawlerState::should_crawl_url("https://example.com/image.jpg"));
    }
}
