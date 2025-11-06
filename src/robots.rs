//! Robots.txt fetching and crawl-delay helpers so the crawler honors site policies.

use crate::network::HttpClient;
use crate::url_utils;
use std::time::SystemTime;

/// Robots.txt cache TTL in hours (24 hours by default per RFC 9309)
/// Used by is_stale() function for cache validation.
#[allow(dead_code)]
const ROBOTS_TTL_HOURS: u64 = 24;

/// Fetch robots.txt content for the domain so we can cache directives by hostname.
///
/// # Note
/// TODO: Caller should re-fetch when `is_stale(SystemTime::now(), fetched_at)` returns true.
/// Cache entries should include a `fetched_at: SystemTime` field and be revalidated after
/// ROBOTS_TTL_HOURS (24 hours by default).
pub async fn fetch_robots_txt(http: &HttpClient, domain: &str) -> Option<String> {
    let robots_url = format!("https://{}/robots.txt", domain);

    match http.fetch(&robots_url).await {
        Ok(result) if result.status_code == 200 => Some(result.content),
        _ => None,
    }
}

/// Fetch robots.txt for the host derived from a URL so seeders can stay compliant.
///
/// # Note
/// TODO: Caller should re-fetch when `is_stale(SystemTime::now(), fetched_at)` returns true.
/// Cache entries should include a `fetched_at: SystemTime` field and be revalidated after
/// ROBOTS_TTL_HOURS (24 hours by default).
pub async fn fetch_robots_txt_from_url(http: &HttpClient, start_url: &str) -> Option<String> {
    let robots_url = url_utils::robots_url(start_url)?;

    match http.fetch(&robots_url).await {
        Ok(result) if result.status_code == 200 => Some(result.content),
        _ => None,
    }
}

/// Check if a cached robots.txt entry is stale based on fetch time.
/// Returns true if the entry should be re-fetched.
/// Currently used in tests; available for cache invalidation logic.
#[allow(dead_code)]
fn is_stale(now: SystemTime, fetched_at: SystemTime) -> bool {
    // If fetched_at is in the future (clock skew), treat as stale
    let elapsed = match now.duration_since(fetched_at) {
        Ok(duration) => duration,
        Err(_) => return true, // Future timestamp, treat as stale
    };

    let ttl_duration = std::time::Duration::from_secs(ROBOTS_TTL_HOURS * 3600);
    elapsed >= ttl_duration
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_is_stale_fresh() {
        let now = SystemTime::now();
        let fetched_at = now;
        assert!(
            !is_stale(now, fetched_at),
            "Just-fetched entry should not be stale"
        );
    }

    #[test]
    fn test_is_stale_expired() {
        let now = SystemTime::now();
        let fetched_at = now - Duration::from_secs((ROBOTS_TTL_HOURS + 1) * 3600);
        assert!(is_stale(now, fetched_at), "Old entry should be stale");
    }

    #[test]
    fn test_is_stale_future() {
        let now = SystemTime::now();
        let fetched_at = now + Duration::from_secs(3600);
        assert!(
            is_stale(now, fetched_at),
            "Future timestamp should be treated as stale"
        );
    }

    #[test]
    fn test_ttl_constant_within_bounds() {
        assert!(ROBOTS_TTL_HOURS <= 48, "TTL should not exceed 48 hours");
        assert!(ROBOTS_TTL_HOURS > 0, "TTL must be positive");
    }
}
