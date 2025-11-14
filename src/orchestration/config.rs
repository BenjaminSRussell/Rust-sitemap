//! Config building and presets.

use crate::bfs_crawler::BfsCrawlerConfig;

/// Presets override CLI args. "ben" = max throughput.
pub fn apply_preset(
    preset_name: &str,
    workers: &mut usize,
    timeout: &mut u64,
    ignore_robots: &mut bool,
    max_urls: &mut Option<usize>,
) {
    match preset_name {
        "ben" => {
            eprintln!("Applying 'ben' preset: Maximum throughput mode");
            *workers = 1024;
            *timeout = 60;
            *ignore_robots = true;
            *max_urls = None;
            eprintln!("   Workers: {}", workers);
            eprintln!("   Timeout: {}s", timeout);
            eprintln!("   Ignoring robots.txt: true");
            eprintln!("   Max URLs: unlimited");
        }
        _ => {
            eprintln!("Warning: Unknown preset '{}', using default settings", preset_name);
        }
    }
}

pub fn build_crawler_config(
    workers: usize,
    timeout: u64,
    user_agent: String,
    ignore_robots: bool,
    enable_redis: bool,
    redis_url: String,
    lock_ttl: u64,
    save_interval: u64,
    max_urls: Option<usize>,
    duration: Option<u64>,
) -> BfsCrawlerConfig {
    assert!(
        timeout < u32::MAX as u64,
        "Timeout value exceeds u32::MAX and will be truncated."
    );
    BfsCrawlerConfig {
        max_workers: u32::try_from(workers).unwrap_or(u32::MAX),
        timeout: timeout as u32,
        user_agent,
        ignore_robots,
        save_interval,
        redis_url: if enable_redis { Some(redis_url) } else { None },
        lock_ttl,
        enable_redis,
        max_urls,
        duration_secs: duration,
    }
}
