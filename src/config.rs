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
    pub const SAVE_INTERVAL_SECS: u64 = 300;

    // HTTP settings so the client adheres to shared resource constraints.
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // Cap at 10MB to avoid buffering enormous responses.
    pub const POOL_IDLE_PER_HOST: usize = 16;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;
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
