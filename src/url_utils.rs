//! URL utilities for consistent crawling behavior across modules.

use url::Url;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn extract_host(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

/// DEPRECATED: Use get_registrable_domain() for PSL-aware extraction.
pub fn get_root_domain(hostname: &str) -> String {
    let parts: Vec<&str> = hostname.split('.').collect();
    if parts.len() >= 2 {
        format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        hostname.to_string()
    }
}

/// Extract registrable domain (eTLD+1) using Public Suffix List.
/// Handles multi-label TLDs: www.example.co.uk → example.co.uk
pub fn get_registrable_domain(hostname: &str) -> String {
    match psl::domain(hostname.as_bytes()) {
        Some(domain) => String::from_utf8_lossy(domain.as_bytes()).to_string(),
        None => get_root_domain(hostname),  // Fallback for localhost, IPs
    }
}

/// Rendezvous (HRW) hashing for consistent shard assignment.
/// Minimizes key movement on reshard: ~1/N keys move vs ~100% with modulo.
pub fn rendezvous_shard_id(domain: &str, num_shards: usize) -> usize {
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

pub fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
    url_domain == base_domain
        || url_domain.ends_with(&format!(".{}", base_domain))
        || base_domain.ends_with(&format!(".{}", url_domain))
}

pub fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
    let base = Url::parse(base_url).map_err(|e| e.to_string())?;
    let absolute_url = base.join(link).map_err(|e| e.to_string())?;
    Ok(absolute_url.to_string())
}

pub fn robots_url(start_url: &str) -> Option<String> {
    let parsed = Url::parse(start_url).ok()?;
    let scheme = parsed.scheme();
    let host = parsed.host_str()?;
    Some(format!("{}://{}/robots.txt", scheme, host))
}

/// Filter URLs: HTTP(S) only, skip binaries/assets/fragment-only.
pub fn should_crawl_url(url: &str) -> bool {
    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };

    if !matches!(parsed_url.scheme(), "http" | "https") {
        return false;
    }

    if parsed_url.fragment().is_some()
        && parsed_url.path() == "/"
        && parsed_url.query().is_none()
    {
        return false;
    }

    let path = parsed_url.path().to_lowercase();
    const DISALLOWED_EXTENSIONS: &[&str] = &[
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4",
        ".avi", ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".tar", ".gz", ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso", ".apk",
    ];
    if DISALLOWED_EXTENSIONS.iter().any(|ext| path.ends_with(ext)) {
        return false;
    }

    if let Some(query) = parsed_url.query() {
        let query_lower = query.to_ascii_lowercase();
        if query_lower.contains("download") || query_lower.contains("attachment") {
            return false;
        }
    }

    true
}

/// Add https:// prefix for bare domains (CLI convenience).
pub fn normalize_url_for_cli(url: &str) -> String {
    let trimmed = url.trim();

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }

    if trimmed.contains('.') && !trimmed.contains('/') {
        return format!("https://{}", trimmed);
    }

    format!("https://{}", trimmed)
}

pub fn is_html_content_type(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml")
}

/// Hash URL authority (host + port) for consistent shard routing.
pub fn get_authority_hash(url: &str) -> u64 {
    if let Ok(parsed) = Url::parse(url) {
        let mut hasher = DefaultHasher::new();

        if let Some(host) = parsed.host_str() {
            host.hash(&mut hasher);
        }

        if let Some(port) = parsed.port() {
            port.hash(&mut hasher);
        }

        hasher.finish()
    } else {
        let mut hasher = DefaultHasher::new();
        url.hash(&mut hasher);
        hasher.finish()
    }
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
        // Standard TLDs
        assert_eq!(get_registrable_domain("www.example.com"), "example.com");
        assert_eq!(get_registrable_domain("api.staging.example.com"), "example.com");

        // Multi-label TLDs (Public Suffix List aware)
        assert_eq!(get_registrable_domain("www.example.co.uk"), "example.co.uk");
        assert_eq!(get_registrable_domain("blog.example.com.au"), "example.com.au");

        // Already registrable
        assert_eq!(get_registrable_domain("example.com"), "example.com");
    }

    #[test]
    fn test_rendezvous_shard_id() {
        let domain = "example.com";
        let num_shards = 8;

        let shard1 = rendezvous_shard_id(domain, num_shards);
        let shard2 = rendezvous_shard_id(domain, num_shards);
        assert_eq!(shard1, shard2);

        let shard_a = rendezvous_shard_id("example.com", num_shards);
        let shard_b = rendezvous_shard_id("different.com", num_shards);

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
        assert!(is_same_domain("test.local", "test.local"));
        assert!(is_same_domain("www.test.local", "test.local"));
        assert!(is_same_domain("test.local", "www.test.local"));
        assert!(!is_same_domain("other.local", "test.local"));
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
        assert_eq!(
            normalize_url_for_cli("example.com"),
            "https://example.com"
        );
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
        // Same URL should produce same hash
        let hash1 = get_authority_hash("https://example.com/path1");
        let hash2 = get_authority_hash("https://example.com/path2");
        assert_eq!(hash1, hash2, "Same host should produce same hash");

        // Different hosts should produce different hashes
        let hash3 = get_authority_hash("https://different.com/path");
        assert_ne!(hash1, hash3, "Different hosts should produce different hashes");

        // Same host with different ports should produce different hashes
        let hash4 = get_authority_hash("https://example.com:8080/path");
        assert_ne!(hash1, hash4, "Different ports should produce different hashes");
    }
}
