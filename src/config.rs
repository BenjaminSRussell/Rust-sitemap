// Global configuration constants - single source of truth

pub struct Config;

impl Config {
    // Crawler timing
    pub const SAVE_INTERVAL_SECS: u64 = 300;
    pub const LOCK_TTL_SECS: u64 = 60;
    pub const DEFAULT_TIMEOUT_SECS: u64 = 45;

    // Sitemap seeding
    pub const SITEMAP_TIMEOUT_SECS: u64 = 120;
    pub const ROBOTS_TIMEOUT_SECS: u64 = 30;

    // HTTP/Network config
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // 10MB
    pub const MAX_RETRIES: u32 = 2;
    pub const RETRY_BACKOFF_MS: u64 = 500;
    pub const POOL_IDLE_PER_HOST: usize = 16;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;

    // Domain failure tracking
    pub const DOMAIN_MAX_FAILURES: usize = 5;
    pub const DOMAIN_MAX_REQUESTS: usize = 5;
    pub const BACKOFF_MAX_EXP: usize = 8;
    pub const BACKOFF_MAX_SECS: u32 = 300;

    // Queue and memory
    pub const NODES_IN_MEMORY: usize = 100_000;
    pub const QUEUE_MULTIPLIER: usize = 500;
    pub const CHANNEL_MULTIPLIER: usize = 100;
    pub const CHANNEL_MIN_SIZE: usize = 50_000;

    // Worker scheduling
    pub const BACKOFF_RESCHEDULE_MS: u64 = 250;
    pub const EMPTY_QUEUE_SLEEP_MS: u64 = 100;
    pub const EMPTY_QUEUE_CHECK_MS: u64 = 500;
}
