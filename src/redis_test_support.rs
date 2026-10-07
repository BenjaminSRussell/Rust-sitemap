//! Shared helpers for Redis-backed integration tests (#40).
//!
//! Tests call [`redis_url_or_skip`]. With no Redis reachable they skip cleanly,
//! unless the `redis-tests` feature is on (CI), in which case they fail loudly
//! so a broken service container can't make the suite silently green.

use std::time::Duration;

/// Redis URL used by integration tests (`REDIS_URL`, default localhost).
pub fn test_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn host_port(url: &str) -> Option<String> {
    let rest = url.split("://").nth(1)?;
    let rest = rest.rsplit('@').next()?;
    let hp = rest.split('/').next()?;
    if hp.contains(':') {
        Some(hp.to_string())
    } else {
        Some(format!("{hp}:6379"))
    }
}

/// Probe the port first: ConnectionManager retries for a long time when Redis
/// is down, which would hang the test binary.
pub fn redis_reachable(url: &str) -> bool {
    use std::net::ToSocketAddrs;
    let Some(hp) = host_port(url) else {
        return false;
    };
    let Ok(mut addrs) = hp.to_socket_addrs() else {
        return false;
    };
    addrs.any(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_millis(300)).is_ok())
}

/// Returns the Redis URL, or None (test should return early) when Redis is
/// absent and strict mode is off.
pub fn redis_url_or_skip(test: &str) -> Option<String> {
    let url = test_redis_url();
    if redis_reachable(&url) {
        return Some(url);
    }
    if cfg!(feature = "redis-tests") {
        panic!("{test}: Redis not reachable at {url} but feature `redis-tests` is enabled");
    }
    println!("{test}: Redis not available at {url}, skipping");
    None
}

/// Unique key/URL prefix so concurrent test binaries never collide.
pub fn unique_prefix(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{tag}-{}-{nanos}", std::process::id())
}
