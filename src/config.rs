// Central configuration values so tuning lives in one place.

pub struct Config;

impl Config {
    // Crawl timing limits so periodic tasks stay coordinated across modules.
    pub const SAVE_INTERVAL_SECS: u64 = 300;

    // HTTP settings so the client adheres to shared resource constraints.
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // Cap at 10MB to avoid buffering enormous responses.
    pub const POOL_IDLE_PER_HOST: usize = 16;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;
}
