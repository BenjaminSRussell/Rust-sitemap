//! Robots.txt fetching and crawl-delay helpers so the crawler honors site policies.

use crate::network::HttpClient;
use crate::url_utils;
use std::time::SystemTime;

/// Cache TTL for robots.txt content. Google and major crawlers typically cache robots.txt
/// for ~24 hours; this mirrors that common practice.
const ROBOTS_TTL_HOURS: u64 = 24;

/// Check whether a cached robots.txt entry is stale and should be re-fetched.
///
/// # Arguments
/// * `now` - Current system time
/// * `fetched_at` - Time when the robots.txt was originally fetched
///
/// # Returns
/// `true` if the elapsed time exceeds ROBOTS_TTL_HOURS, `false` otherwise.
///
/// # Panics
/// May panic in debug builds if system time calculations fail.
fn is_stale(now: SystemTime, fetched_at: SystemTime) -> bool {
    debug_assert!(ROBOTS_TTL_HOURS <= 48, "TTL should not exceed 48 hours");

    if let Ok(elapsed) = now.duration_since(fetched_at) {
        elapsed.as_secs() > ROBOTS_TTL_HOURS * 3600
    } else {
        // If fetched_at is in the future (clock skew), treat as stale to be safe.
        true
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_is_stale_fresh() {
        let now = SystemTime::now();
        let fetched_at = now;
        assert!(!is_stale(now, fetched_at), "Just-fetched entry should not be stale");
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
        assert!(is_stale(now, fetched_at), "Future timestamp should be treated as stale");
    }

    #[test]
    fn test_ttl_constant_within_bounds() {
        assert!(ROBOTS_TTL_HOURS <= 48, "TTL should not exceed 48 hours");
        assert!(ROBOTS_TTL_HOURS > 0, "TTL must be positive");
    }
}
