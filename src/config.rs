// Central configuration values

pub struct Config;

impl Config {
    // Crawl timing limits
    pub const SAVE_INTERVAL_SECS: u64 = 300;
    pub const LOCK_TTL_SECS: u64 = 60;

    // Sitemap seeding settings
    pub const SITEMAP_TIMEOUT_SECS: u64 = 120;
    pub const ROBOTS_TIMEOUT_SECS: u64 = 30;

    // HTTP settings
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // 10MB
    pub const MAX_RETRIES: u32 = 2;
    pub const RETRY_BACKOFF_MS: u64 = 500;
    pub const POOL_IDLE_PER_HOST: usize = 16;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;

    // Queue sizing
    pub const NODES_IN_MEMORY: usize = 100_000;
    pub const QUEUE_MULTIPLIER: usize = 500;
}
