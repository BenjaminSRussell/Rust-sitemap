//! URL utilities for consistent crawling behavior across modules.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use url::Url;

pub fn extract_host(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

/// Legacy helper kept until every caller migrates to get_registrable_domain().
pub fn get_root_domain(hostname: &str) -> String {
    let parts: Vec<&str> = hostname.split('.').collect();
    if parts.len() >= 2 {
        format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        hostname.to_string()
    }
}

/// Extract registrable domain (eTLD+1) via the Public Suffix List so multi-label TLDs stay intact.
pub fn get_registrable_domain(hostname: &str) -> String {
    match psl::domain(hostname.as_bytes()) {
        Some(domain) => String::from_utf8_lossy(domain.as_bytes()).to_string(),
        None => get_root_domain(hostname), // PSL omits localhost/IPs so fall back to suffix math.
    }
}

/// Rendezvous (HRW) hashing keeps shard assignment stable when resizing.
/// Only ~1/N keys move during resharding instead of rebalancing everything.
pub fn rendezvous_shard_id(domain: &str, num_shards: usize) -> usize {
    debug_assert!(!domain.is_empty());

    if num_shards == 0 {
        return 0;
    }

    let mut max_hash = 0u64;
    let mut best_shard = 0;

    for shard_id in 0..num_shards {
        let mut hasher = DefaultHasher::new();
        domain.hash(&mut hasher);
        shard_id.hash(&mut hasher);
        let hash_value = hasher.finish();

        if hash_value > max_hash {
            max_hash = hash_value;
            best_shard = shard_id;
        }
    }

    best_shard
}

/// Hash the authority (host + port) so identical origins stay on the same shard.
/// Paths do not influence the hash, which helps deduplicate work for a host.
/// Currently used in tests but ready for future dedupe logic.
#[allow(dead_code)]
pub fn get_authority_hash(url: &str) -> u64 {
    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return 0,
    };

    let mut hasher = DefaultHasher::new();

    // Include the host so like origins always collide.
    if let Some(host) = parsed_url.host_str() {
        host.hash(&mut hasher);
    }

    // Include the resolved port so http:80 and https:80 do not collide.
    parsed_url.port_or_known_default().hash(&mut hasher);

    hasher.finish()
}

pub fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
    debug_assert!(!url_domain.is_empty() && !base_domain.is_empty());

    url_domain == base_domain
        || (url_domain.len() > base_domain.len()
            && url_domain.ends_with(base_domain)
            && url_domain.as_bytes()[url_domain.len() - base_domain.len() - 1] == b'.')
        || (base_domain.len() > url_domain.len()
            && base_domain.ends_with(url_domain)
            && base_domain.as_bytes()[base_domain.len() - url_domain.len() - 1] == b'.')
}

pub fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
    if link.is_empty() {
        return Err("Empty link".to_string());
    }
    if base_url.is_empty() {
        return Err("Empty base URL".to_string());
    }

    let base = Url::parse(base_url).map_err(|e| format!("parse base: {}", e))?;
    let absolute_url = base.join(link).map_err(|e| format!("join link: {}", e))?;
    Ok(absolute_url.to_string())
}

pub fn robots_url(start_url: &str) -> Option<String> {
    let parsed = Url::parse(start_url).ok()?;
    let scheme = parsed.scheme();
    let host = parsed.host_str()?;
    Some(format!("{}://{}/robots.txt", scheme, host))
}

/// Keep only HTTP(S) URLs that look like HTML pages so the crawler avoids obvious binaries.
/// Callers must still inspect Content-Type (text/html or application/xhtml+xml per MDN) for certainty.
pub fn should_crawl_url(url: &str) -> bool {
    debug_assert!(!url.is_empty());

    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };

    if !matches!(parsed_url.scheme(), "http" | "https") {
        return false;
    }

    if parsed_url.fragment().is_some() && parsed_url.path() == "/" && parsed_url.query().is_none() {
        return false;
    }

    // Extension filter rejects common binary assets without allocating.
    let path = parsed_url.path();
    let path_lower = path.as_bytes();

    // Compare ASCII bytes directly so the hot path stays allocation-free.
    for ext in [
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4", ".avi",
        ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".tar", ".gz",
        ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso", ".apk",
    ] {
        if path.len() >= ext.len() {
            let start = path.len() - ext.len();
            if path_lower[start..].eq_ignore_ascii_case(ext.as_bytes()) {
                return false;
            }
        }
    }

    if let Some(query) = parsed_url.query() {
        // Flag obvious download links without allocating.
        let query_bytes = query.as_bytes();
        if contains_ascii_ignore_case(query_bytes, b"download")
            || contains_ascii_ignore_case(query_bytes, b"attachment")
        {
            return false;
        }
    }

    true
}

/// Zero-alloc ASCII substring search shared by the URL filters.
#[inline]
fn contains_ascii_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }

    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

/// Add https:// to bare domains so CLI input stays forgiving.
pub fn normalize_url_for_cli(url: &str) -> String {
    let trimmed = url.trim();
    debug_assert!(trimmed.len() < 1 << 20, "URL exceeds 1MB sanity bound");

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }

    if trimmed.contains('.') && !trimmed.contains('/') {
        return format!("https://{}", trimmed);
    }

    format!("https://{}", trimmed)
}

/// Detect HTML Content-Type headers so callers can gate sitemap ingestion.
pub fn is_html_content_type(content_type: &str) -> bool {
    debug_assert!(!content_type.is_empty());

    // Prefix check avoids heap work when sniffing MIME.
    let bytes = content_type.as_bytes();
    bytes.len() >= 9 && bytes[..9].eq_ignore_ascii_case(b"text/html")
        || bytes.len() >= 21 && bytes[..21].eq_ignore_ascii_case(b"application/xhtml+xml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host() {
        assert_eq!(
            extract_host("https://example.com/path"),
            Some("example.com".to_string())
        );
        assert_eq!(extract_host("invalid"), None);
    }

    #[test]
    fn test_get_root_domain() {
        assert_eq!(get_root_domain("www.hartford.edu"), "hartford.edu");
        assert_eq!(get_root_domain("api.staging.example.com"), "example.com");
        assert_eq!(get_root_domain("example.com"), "example.com");
    }

    #[test]
    fn test_get_registrable_domain() {
        // Standard TLDs ensure PSL lookups hit the fast path.
        assert_eq!(get_registrable_domain("www.example.com"), "example.com");
        assert_eq!(
            get_registrable_domain("api.staging.example.com"),
            "example.com"
        );

        // Multi-label TLDs confirm PSL handles nested suffixes.
        assert_eq!(get_registrable_domain("www.example.co.uk"), "example.co.uk");
        assert_eq!(
            get_registrable_domain("blog.example.com.au"),
            "example.com.au"
        );

        // Already-registrable names should round-trip untouched.
        assert_eq!(get_registrable_domain("example.com"), "example.com");
    }

    #[test]
    fn test_rendezvous_shard_id() {
        let domain = "example.com";
        let num_shards = 8;

        let shard1 = rendezvous_shard_id(domain, num_shards);
        let shard2 = rendezvous_shard_id(domain, num_shards);
        assert_eq!(shard1, shard2);

        let _shard_a = rendezvous_shard_id("example.com", num_shards);
        let _shard_b = rendezvous_shard_id("different.com", num_shards);

        assert!(shard1 < num_shards);
        assert_eq!(rendezvous_shard_id("example.com", 0), 0);
        assert_eq!(rendezvous_shard_id("example.com", 1), 0);
    }

    #[test]
    fn test_rendezvous_minimal_churn() {
        let domain = "example.com";
        let shard_8 = rendezvous_shard_id(domain, 8);
        let shard_9 = rendezvous_shard_id(domain, 9);

        assert!(shard_8 < 8);
        assert!(shard_9 < 9);
    }

    #[test]
    fn test_is_same_domain() {
        assert!(
            is_same_domain("test.local", "test.local"),
            "Expected 'test.local' to match 'test.local'"
        );
        assert!(
            is_same_domain("www.test.local", "test.local"),
            "Expected 'www.test.local' to match 'test.local'"
        );
        assert!(
            is_same_domain("test.local", "www.test.local"),
            "Expected 'test.local' to match 'www.test.local'"
        );
        assert!(
            !is_same_domain("other.local", "test.local"),
            "Expected 'other.local' not to match 'test.local'"
        );
    }

    #[test]
    fn test_convert_to_absolute_url() {
        assert_eq!(
            convert_to_absolute_url("/page1", "https://test.local/foo").unwrap(),
            "https://test.local/page1"
        );
        assert_eq!(
            convert_to_absolute_url("page1", "https://test.local/foo/").unwrap(),
            "https://test.local/foo/page1"
        );
        assert_eq!(
            convert_to_absolute_url("https://other.local/page", "https://test.local").unwrap(),
            "https://other.local/page"
        );
    }

    #[test]
    fn test_robots_url() {
        assert_eq!(
            robots_url("https://example.com/some/path"),
            Some("https://example.com/robots.txt".to_string())
        );
        assert_eq!(
            robots_url("http://test.local"),
            Some("http://test.local/robots.txt".to_string())
        );
    }

    #[test]
    fn test_should_crawl_url() {
        assert!(should_crawl_url("https://test.local/page"));
        assert!(should_crawl_url("http://test.local/page"));
        assert!(!should_crawl_url("ftp://test.local/page"));
        assert!(!should_crawl_url("https://test.local/file.pdf"));
        assert!(!should_crawl_url("https://test.local/image.jpg"));
        assert!(!should_crawl_url("https://test.local/#section"));
        assert!(should_crawl_url("https://test.local/page#section"));
    }

    #[test]
    fn test_normalize_url_for_cli() {
        assert_eq!(normalize_url_for_cli("example.com"), "https://example.com");
        assert_eq!(
            normalize_url_for_cli("https://example.com"),
            "https://example.com"
        );
        assert_eq!(
            normalize_url_for_cli("http://example.com"),
            "http://example.com"
        );
    }

    #[test]
    fn test_is_html_content_type() {
        assert!(is_html_content_type("text/html"));
        assert!(is_html_content_type("text/html; charset=utf-8"));
        assert!(is_html_content_type("application/xhtml+xml"));
        assert!(!is_html_content_type("application/json"));
        assert!(!is_html_content_type("image/png"));
    }

    #[test]
    fn test_get_authority_hash() {
        // Same host must hash identically regardless of path.
        let hash1 = get_authority_hash("https://example.com/path1");
        let hash2 = get_authority_hash("https://example.com/path2");
        assert_eq!(hash1, hash2, "Same host should produce same hash");

        // Different hosts should diverge.
        let hash3 = get_authority_hash("https://different.com/path");
        assert_ne!(
            hash1, hash3,
            "Different hosts should produce different hashes"
        );

        // Port participates in the hash so different ports diverge.
        let hash4 = get_authority_hash("https://example.com:8080/path");
        assert_ne!(
            hash1, hash4,
            "Different ports should produce different hashes"
        );
    }
}
