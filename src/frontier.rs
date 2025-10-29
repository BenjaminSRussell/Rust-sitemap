use dashmap::DashMap;
use parking_lot::Mutex;
use robotstxt::DefaultMatcher;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::node_map::{NodeMap, SitemapNode};
use crate::rkyv_queue::{QueuedUrl, RkyvQueue};

/// Per-host state tracking for politeness and backoff
#[derive(Debug)]
struct HostQueue {
    /// URLs waiting to be crawled for this host
    queue: VecDeque<QueuedUrl>,
    /// Number of consecutive failures
    failures: u32,
    /// Time until we can retry after failures
    backoff_until: Instant,
    /// Crawl delay from robots.txt (default 0)
    crawl_delay: Duration,
    /// When the next request is allowed (for politeness)
    ready_at: Instant,
    /// Cached robots.txt content for this host
    robots_txt: Option<String>,
}

impl HostQueue {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            queue: VecDeque::new(),
            failures: 0,
            backoff_until: now,
            crawl_delay: Duration::from_secs(0),
            ready_at: now,
            robots_txt: None,
        }
    }

    /// Check if this host is ready to accept a request
    fn is_ready(&self) -> bool {
        let now = Instant::now();
        now >= self.ready_at && now >= self.backoff_until
    }

    /// Update the ready_at time after making a request
    fn mark_request_made(&mut self) {
        self.ready_at = Instant::now() + self.crawl_delay;
    }

    /// Record a failure and apply exponential backoff
    fn record_failure(&mut self) {
        self.failures += 1;
        let backoff_secs = (2_u32.pow(self.failures.min(8))).min(300);
        self.backoff_until = Instant::now() + Duration::from_secs(backoff_secs as u64);
    }

    /// Reset failure count (after successful request)
    fn reset_failures(&mut self) {
        self.failures = 0;
        self.backoff_until = Instant::now();
    }

    /// Set crawl delay from robots.txt
    fn set_crawl_delay(&mut self, delay: Duration) {
        self.crawl_delay = delay;
    }

    /// Cache robots.txt content
    fn set_robots_txt(&mut self, robots_txt: String) {
        self.robots_txt = Some(robots_txt);
    }

    /// Check if a URL is allowed by robots.txt
    fn is_allowed(&self, url: &str, user_agent: &str) -> bool {
        if let Some(ref robots_txt) = self.robots_txt {
            let mut matcher = DefaultMatcher::default();
            matcher.one_agent_allowed_by_robots(robots_txt, user_agent, url)
        } else {
            // No robots.txt means everything is allowed
            true
        }
    }
}

/// The main frontier that manages all per-host queues and scheduling
pub struct Frontier {
    /// Per-host queues (domain -> HostQueue)
    hosts: DashMap<String, Arc<Mutex<HostQueue>>>,
    /// Durable write-ahead log
    wal: Arc<Mutex<RkyvQueue>>,
    /// NodeMap for deduplication
    node_map: Arc<NodeMap>,
    /// Round-robin iterator state
    last_host_checked: Arc<Mutex<usize>>,
    /// User agent for robots.txt checking
    user_agent: String,
    /// Ignore robots.txt if true
    ignore_robots: bool,
}

impl Frontier {
    pub fn new(
        wal: Arc<Mutex<RkyvQueue>>,
        node_map: Arc<NodeMap>,
        user_agent: String,
        ignore_robots: bool,
    ) -> Self {
        Self {
            hosts: DashMap::new(),
            wal,
            node_map,
            last_host_checked: Arc::new(Mutex::new(0)),
            user_agent,
            ignore_robots,
        }
    }

    /// Extract host from URL
    fn extract_host(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
    }

    /// Add newly discovered URLs to the frontier
    pub async fn add_links(
        &self,
        links: Vec<(String, u32, Option<String>)>,
        start_url_domain: &str,
    ) -> usize {
        let mut added_count = 0;

        for (url, depth, parent_url) in links {
            // Normalize URL
            let normalized_url = SitemapNode::normalize_url(&url);

            // Check if already visited
            if self.node_map.contains(&normalized_url) {
                continue;
            }

            // Filter to same domain
            let url_domain = match Self::extract_host(&normalized_url) {
                Some(domain) => domain,
                None => continue,
            };

            if !Self::is_same_domain(&url_domain, start_url_domain) {
                continue;
            }

            // Add to node map
            match self
                .node_map
                .add_url(normalized_url.clone(), depth, parent_url.clone())
            {
                Ok(true) => {
                    // Successfully added to node map, now add to frontier
                    let queued_url = QueuedUrl::new(normalized_url.clone(), depth, parent_url);

                    // Write to WAL first (durability)
                    {
                        let mut wal = self.wal.lock();
                        if let Err(e) = wal.push_back(queued_url.clone()) {
                            eprintln!("WAL write error for {}: {}", normalized_url, e);
                            continue;
                        }
                    }

                    // Add to appropriate host queue
                    let host_queue = self
                        .hosts
                        .entry(url_domain)
                        .or_insert_with(|| Arc::new(Mutex::new(HostQueue::new())));

                    {
                        let mut queue = host_queue.lock();
                        queue.queue.push_back(queued_url);
                    }

                    added_count += 1;
                }
                Ok(false) => {
                    // Already existed, skip
                }
                Err(e) => {
                    eprintln!("Error adding URL to node map {}: {}", normalized_url, e);
                }
            }
        }

        added_count
    }

    /// Get the next URL to crawl (round-robin across hosts)
    pub async fn get_next_url(&self) -> Option<(String, String, u32, Option<String>)> {
        // Collect all host keys
        let host_keys: Vec<String> = self.hosts.iter().map(|entry| entry.key().clone()).collect();

        if host_keys.is_empty() {
            return None;
        }

        let num_hosts = host_keys.len();
        let mut last_checked = self.last_host_checked.lock();

        // Try each host once in round-robin fashion
        for _ in 0..num_hosts {
            let idx = *last_checked % num_hosts;
            *last_checked = (*last_checked + 1) % num_hosts;

            let host = &host_keys[idx];

            if let Some(host_queue_arc) = self.hosts.get(host) {
                let mut host_queue = host_queue_arc.lock();

                // Check if this host is ready
                if !host_queue.is_ready() {
                    continue;
                }

                // Try to get a URL from this host's queue
                if let Some(queued_url) = host_queue.queue.pop_front() {
                    // Check robots.txt if needed
                    if !self.ignore_robots {
                        if !host_queue.is_allowed(&queued_url.url, &self.user_agent) {
                            // Not allowed by robots.txt, skip this URL
                            continue;
                        }
                    }

                    // Mark request as made
                    host_queue.mark_request_made();

                    // Return (host, url, depth, parent_url)
                    return Some((
                        host.clone(),
                        queued_url.url,
                        queued_url.depth,
                        queued_url.parent_url,
                    ));
                }
            }
        }

        // No hosts were ready or had URLs
        None
    }

    /// Record a successful crawl for a host
    pub fn record_success(&self, host: &str) {
        if let Some(host_queue_arc) = self.hosts.get(host) {
            let mut host_queue = host_queue_arc.lock();
            host_queue.reset_failures();
        }
    }

    /// Record a failure for a host
    pub fn record_failure(&self, host: &str) {
        if let Some(host_queue_arc) = self.hosts.get(host) {
            let mut host_queue = host_queue_arc.lock();
            host_queue.record_failure();
        }
    }

    /// Set robots.txt content for a host
    pub fn set_robots_txt(&self, host: &str, robots_txt: String, crawl_delay: Option<Duration>) {
        let host_queue = self
            .hosts
            .entry(host.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(HostQueue::new())));

        let mut queue = host_queue.lock();
        queue.set_robots_txt(robots_txt);

        if let Some(delay) = crawl_delay {
            queue.set_crawl_delay(delay);
        }
    }

    /// Check if the frontier is empty (no more work to do)
    pub fn is_empty(&self) -> bool {
        // Check if all host queues are empty
        for entry in self.hosts.iter() {
            let host_queue = entry.value().lock();
            if !host_queue.queue.is_empty() {
                return false;
            }
        }
        true
    }

    /// Get statistics about the frontier
    pub fn stats(&self) -> FrontierStats {
        let mut total_queued = 0;
        let mut hosts_with_work = 0;
        let mut hosts_in_backoff = 0;

        for entry in self.hosts.iter() {
            let host_queue = entry.value().lock();
            let queue_len = host_queue.queue.len();
            total_queued += queue_len;

            if queue_len > 0 {
                hosts_with_work += 1;
            }

            if !host_queue.is_ready() {
                hosts_in_backoff += 1;
            }
        }

        FrontierStats {
            total_hosts: self.hosts.len(),
            total_queued,
            hosts_with_work,
            hosts_in_backoff,
        }
    }

    /// Check if two domains are the same (handles subdomains)
    fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
        url_domain == base_domain
            || url_domain.ends_with(&format!(".{}", base_domain))
            || base_domain.ends_with(&format!(".{}", url_domain))
    }
}

#[derive(Debug, Clone)]
pub struct FrontierStats {
    pub total_hosts: usize,
    pub total_queued: usize,
    pub hosts_with_work: usize,
    pub hosts_in_backoff: usize,
}

impl std::fmt::Display for FrontierStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Frontier: {} hosts, {} queued URLs, {} with work, {} in backoff",
            self.total_hosts, self.total_queued, self.hosts_with_work, self.hosts_in_backoff
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_host_queue_ready() {
        let mut queue = HostQueue::new();
        assert!(queue.is_ready());

        queue.set_crawl_delay(Duration::from_secs(1));
        queue.mark_request_made();
        assert!(!queue.is_ready());
    }

    #[test]
    fn test_host_queue_backoff() {
        let mut queue = HostQueue::new();
        queue.record_failure();
        assert!(!queue.is_ready());
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(
            Frontier::extract_host("https://example.com/path"),
            Some("example.com".to_string())
        );
        assert_eq!(Frontier::extract_host("invalid"), None);
    }

    #[tokio::test]
    async fn test_frontier_basic() {
        let dir = TempDir::new().unwrap();
        let node_map = Arc::new(NodeMap::new(dir.path(), 1000).unwrap());
        let wal = Arc::new(Mutex::new(RkyvQueue::new(dir.path(), 100).unwrap()));

        let frontier = Frontier::new(wal, node_map, "TestBot/1.0".to_string(), false);

        let links = vec![(
            "https://example.com/page1".to_string(),
            1,
            Some("https://example.com".to_string()),
        )];

        let added = frontier.add_links(links, "example.com").await;
        assert_eq!(added, 1);

        let stats = frontier.stats();
        assert_eq!(stats.total_queued, 1);
    }
}