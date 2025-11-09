// Central configuration values so tuning lives in one place.

/// Memory allocator choice for runtime performance tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Allocator {
    None,
}

/// Runtime configuration for the sitemap crawler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub heartbeat: bool,
    pub allocator: Allocator,
    pub robots_ttl_hours: u64,
}

impl Config {
    // Crawl timing limits so periodic tasks stay coordinated across modules.
    // Note: Used in main.rs binary, not lib.rs (Python bindings)
    #[allow(dead_code)]
    pub const SAVE_INTERVAL_SECS: u64 = 300;

    // HTTP settings so the client adheres to shared resource constraints.
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // Cap at 10MB to avoid buffering enormous responses.
    pub const POOL_IDLE_PER_HOST: usize = 64; // Increased to support high concurrency
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 90; // Keep connections alive longer

    // Event processing settings for coordinating state updates.
    pub const EVENT_CHANNEL_BUFFER_SIZE: usize = 10_000; // Buffer for state events before backpressure kicks in

    // Polling and coordination delays to avoid tight loops.
    pub const LOOP_YIELD_DELAY_MS: u64 = 10; // Yield delay when no work available
    pub const WORK_STEALING_CHECK_INTERVAL_MS: u64 = 500; // How often to check for work stealing opportunities

    // Shutdown and cleanup delays.
    // Note: Used in main.rs binary, not lib.rs (Python bindings)
    #[allow(dead_code)]
    pub const SHUTDOWN_GRACE_PERIOD_SECS: u64 = 2; // Time to wait for graceful shutdown
    pub const FRONTIER_CRAWL_DELAY_MS: u64 = 50; // Default crawl delay for rate limiting

    // Bloom filter settings for URL deduplication.
    pub const BLOOM_FILTER_EXPECTED_ITEMS: usize = 10_000_000; // 10M URLs to match frontier.rs warning threshold

    // Memory limits for bounded collections to prevent OOM crashes
    pub const MAX_HOST_QUEUE_SIZE: usize = 10_000; // Max URLs queued per host
    pub const MAX_HOST_CACHE_SIZE: usize = 100_000; // Max hosts in LRU cache
    pub const MAX_PENDING_URLS: usize = 1_000_000; // Max pending URLs before cleanup

    // Link extraction limits to prevent queue explosion
    pub const MAX_LINKS_PER_PAGE: usize = 50; // Maximum links to extract per page (prevents discovery explosion)
    pub const MAX_CRAWL_DEPTH: usize = 5; // Maximum link depth from start URL (prevents exponential growth)

    // Adaptive link extraction thresholds
    pub const QUEUE_SIZE_HIGH_THRESHOLD: usize = 5_000; // Reduce link extraction when queue is large
    pub const QUEUE_SIZE_LOW_THRESHOLD: usize = 1_000; // Normal link extraction when queue is small
    pub const LINKS_PER_PAGE_LOW_QUEUE: usize = 50; // Links when queue < LOW_THRESHOLD
    pub const LINKS_PER_PAGE_MED_QUEUE: usize = 20; // Links when LOW <= queue < HIGH
    pub const LINKS_PER_PAGE_HIGH_QUEUE: usize = 5; // Links when queue >= HIGH_THRESHOLD
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat: false,
            allocator: Allocator::None,
            robots_ttl_hours: 24, // Follow Google's 24h caching recommendation.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(!config.heartbeat);
        assert_eq!(config.allocator, Allocator::None);
        assert_eq!(config.robots_ttl_hours, 24);
    }
}
