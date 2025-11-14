pub struct Config;

impl Config {
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024;
    pub const POOL_IDLE_PER_HOST: usize = 64;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 90;

    pub const EVENT_CHANNEL_BUFFER_SIZE: usize = 10_000;
    pub const WORK_STEALING_CHECK_INTERVAL_MS: u64 = 500;
    pub const FRONTIER_CRAWL_DELAY_MS: u64 = 10;
    pub const LOOP_YIELD_DELAY_MS: u64 = 10;

    pub const BLOOM_FILTER_EXPECTED_ITEMS: usize = 10_000_000;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_constants() {
        assert!(Config::MAX_CONTENT_SIZE > 0);
        assert!(Config::POOL_IDLE_PER_HOST > 0);
    }
}
