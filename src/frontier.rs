use dashmap::DashMap;
use robotstxt::DefaultMatcher;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::bfs_crawler::StateWriteMessage;
use crate::state::{CrawlerState, HostState, SitemapNode};
use crate::url_utils;

// In-memory politeness window per host
#[derive(Debug)]
struct HostTracking {
    /// When the next request is allowed (for politeness)
    ready_at: Instant,
}

/// Frontier that manages per-host queues and scheduling
pub struct Frontier {
    /// Read-only access to crawler state
    state: Arc<CrawlerState>,
    /// MPSC sender for state writes
    state_writer_tx: mpsc::UnboundedSender<StateWriteMessage>,
    /// In-memory host tracking for politeness
    host_tracking: DashMap<String, HostTracking>,
    /// User agent for robots.txt checking
    user_agent: String,
    /// Ignore robots.txt if true
    ignore_robots: bool,
}

impl Frontier {
    pub fn new(
        state: Arc<CrawlerState>,
        state_writer_tx: mpsc::UnboundedSender<StateWriteMessage>,
        user_agent: String,
        ignore_robots: bool,
    ) -> Self {
        Self {
            state,
            state_writer_tx,
            host_tracking: DashMap::new(),
            user_agent,
            ignore_robots,
        }
    }

    /// Extract host from URL
    fn extract_host(url: &str) -> Option<String> {
        url_utils::extract_host(url)
    }

    /// Add newly discovered URLs to the frontier
    pub async fn add_links(
        &self,
        links: Vec<(String, u32, Option<String>)>,
        start_url_domain: &str,
    ) -> usize {
        let mut added_count = 0;

        for (url, depth, parent_url) in links {
            // Normalize before dedup checks
            let normalized_url = SitemapNode::normalize_url(&url);

            // Skip URLs already stored
            if self.state.contains_url(&normalized_url).unwrap_or(false) {
                continue;
            }

            // Enforce domain filter
            let url_domain = match Self::extract_host(&normalized_url) {
                Some(domain) => domain,
                None => continue,
            };

            if !Self::is_same_domain(&url_domain, start_url_domain) {
                continue;
            }

            // Build node for persistence
            let node = SitemapNode::new(
                url.clone(),
                normalized_url.clone(),
                depth,
                parent_url.clone(),
                None,
            );

            // Persist node via writer
            if self.state_writer_tx.send(StateWriteMessage::AddNode(node)).is_err() {
                eprintln!("Failed to send AddNode message for {}", normalized_url);
                continue;
            }

            // Queue URL for crawling
            if self.state_writer_tx.send(StateWriteMessage::EnqueueUrl {
                url: normalized_url.clone(),
                depth,
                parent_url,
            }).is_err() {
                eprintln!("Failed to send EnqueueUrl message for {}", normalized_url);
                continue;
            }

            added_count += 1;
        }

        added_count
    }

    /// Get the next URL to crawl
    pub async fn get_next_url(&self) -> Option<(String, String, u32, Option<String>)> {
        // Pull the next URL from state
        loop {
            let url_str = match self.state.dequeue_url() {
                Ok(Some(url)) => url,
                Ok(None) => return None, // Queue is empty
                Err(e) => {
                    eprintln!("Error dequeuing URL: {}", e);
                    return None;
                }
            };

            // Parse host for politeness tracking
            let host = match Self::extract_host(&url_str) {
                Some(h) => h,
                None => continue, // Invalid URL, try next
            };

            // Respect per-host delay
            let mut tracking = self.host_tracking.entry(host.clone()).or_insert_with(|| HostTracking {
                ready_at: Instant::now(),
            });

            let now = Instant::now();
            if now < tracking.ready_at {
                // Host not ready yet; TODO: consider re-queueing instead of skipping
                continue;
            }

            // Fetch host state from storage
            let host_state = match self.state.get_host_state(&host) {
                Ok(Some(state)) => state,
                Ok(None) => HostState::new(host.clone()),
                Err(e) => {
                    eprintln!("Error getting host state for {}: {}", host, e);
                    HostState::new(host.clone())
                }
            };

            // Skip hosts still in backoff
            if !host_state.is_ready() {
                continue;
            }

            // Verify robots.txt when enabled
            if !self.ignore_robots {
                if let Some(ref robots_txt) = host_state.robots_txt {
                    let mut matcher = DefaultMatcher::default();
                    if !matcher.one_agent_allowed_by_robots(robots_txt, &self.user_agent, &url_str) {
                        continue; // Robots disallow this URL
                    }
                }
            }

            // Schedule next allowed crawl time
            let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);
            tracking.ready_at = Instant::now() + crawl_delay;

            // TODO: store depth and parent metadata directly in the queue
            return Some((host, url_str, 0, None));
        }
    }

    /// Record a successful crawl for a host
    pub fn record_success(&self, host: &str) {
        let _ = self.state_writer_tx.send(StateWriteMessage::UpdateHostState {
            host: host.to_string(),
            robots_txt: None,
            crawl_delay_secs: None,
            success: Some(true),
        });
    }

    /// Record a failure for a host
    pub fn record_failure(&self, host: &str) {
        let _ = self.state_writer_tx.send(StateWriteMessage::UpdateHostState {
            host: host.to_string(),
            robots_txt: None,
            crawl_delay_secs: None,
            success: Some(false),
        });
    }

    /// Set robots.txt content for a host
    pub fn set_robots_txt(&self, host: &str, robots_txt: String) {
        // Parse crawl delay from robots.txt
        let crawl_delay_secs = Self::extract_crawl_delay(&robots_txt, &self.user_agent);

        let _ = self.state_writer_tx.send(StateWriteMessage::UpdateHostState {
            host: host.to_string(),
            robots_txt: Some(robots_txt),
            crawl_delay_secs,
            success: None,
        });
    }

    /// Check if we have saved state to resume from
    pub fn has_saved_state(&self) -> bool {
        self.state.get_queue_size().unwrap_or(0) > 0
    }

    /// Check if the frontier is empty (no more work to do)
    pub fn is_empty(&self) -> bool {
        self.state.get_queue_size().unwrap_or(0) == 0
    }

    /// Get statistics about the frontier
    pub fn stats(&self) -> FrontierStats {
        let total_queued = self.state.get_queue_size().unwrap_or(0);
        let total_hosts = self.state.get_all_hosts().unwrap_or_default().len();

        FrontierStats {
            total_hosts,
            total_queued,
            hosts_with_work: 0, // TODO: derive by scanning host queues
            hosts_in_backoff: 0, // TODO: derive from host state snapshots
        }
    }

    /// Check if two domains are the same (handles subdomains)
    fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
        url_utils::is_same_domain(url_domain, base_domain)
    }

    /// Extract crawl delay from robots.txt for a specific user agent
    fn extract_crawl_delay(robots_txt: &str, user_agent: &str) -> Option<u64> {
        let mut in_matching_agent = false;
        let mut crawl_delay = None;

        for line in robots_txt.lines() {
            let line = line.trim();

            // Check for User-agent directive
            if line.to_lowercase().starts_with("user-agent:") {
                let agent = line[11..].trim();
                in_matching_agent = agent == "*" || agent.eq_ignore_ascii_case(user_agent);
            }

            // Check for Crawl-delay directive
            if in_matching_agent && line.to_lowercase().starts_with("crawl-delay:") {
                if let Some(delay_str) = line[12..].trim().split_whitespace().next() {
                    if let Ok(delay) = delay_str.parse::<f64>() {
                        crawl_delay = Some(delay.ceil() as u64);
                    }
                }
            }
        }

        crawl_delay
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
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());
        let (tx, mut rx) = mpsc::unbounded_channel();

        let frontier = Frontier::new(state.clone(), tx, "TestBot/1.0".to_string(), false);

        let links = vec![(
            "https://example.com/page1".to_string(),
            1,
            Some("https://example.com".to_string()),
        )];

        let added = frontier.add_links(links, "example.com").await;
        assert_eq!(added, 1);

        // Process messages from the channel to update state
        while let Ok(msg) = rx.try_recv() {
            match msg {
                StateWriteMessage::AddNode(node) => {
                    state.add_node(&node).unwrap();
                }
                StateWriteMessage::EnqueueUrl { url, depth, parent_url } => {
                    let queued = crate::state::QueuedUrl {
                        url,
                        depth,
                        parent_url,
                        priority: 0,
                    };
                    state.enqueue_url(&queued).unwrap();
                }
                _ => {}
            }
        }

        let stats = frontier.stats();
        assert!(stats.total_queued >= 1);
    }
}
