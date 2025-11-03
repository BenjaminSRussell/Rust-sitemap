//! URL helper functions used throughout the crawler so behaviors stay consistent across modules.

use url::Url;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Extract the host portion of a URL so we can compare or group by host.
pub fn extract_host(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

/// Return the root domain using a simple last-two-label heuristic so seeders can target registrable domains.
pub fn get_root_domain(hostname: &str) -> String {
    let parts: Vec<&str> = hostname.split('.').collect();

    // Prefer the last two labels so subdomains collapse to their registrable domain.
    if parts.len() >= 2 {
        format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        hostname.to_string()
    }
}

/// Check whether two domains match, including subdomain variants, so same-site checks are flexible.
pub fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
    url_domain == base_domain
        || url_domain.ends_with(&format!(".{}", base_domain))
        || base_domain.ends_with(&format!(".{}", url_domain))
}

/// Resolve a link against the provided base URL so relative paths become absolute.
pub fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
    let base = Url::parse(base_url).map_err(|e| e.to_string())?;
    let absolute_url = base.join(link).map_err(|e| e.to_string())?;
    Ok(absolute_url.to_string())
}

/// Build the robots.txt URL for the given start URL so we know where to fetch directives.
pub fn robots_url(start_url: &str) -> Option<String> {
    let parsed = Url::parse(start_url).ok()?;
    let scheme = parsed.scheme();
    let host = parsed.host_str()?;
    Some(format!("{}://{}/robots.txt", scheme, host))
}

/// Determine whether a URL is eligible for crawling so we avoid wasting time on unsupported schemes and assets.
pub fn should_crawl_url(url: &str) -> bool {
    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };

    // Require HTTP(S) so we ignore unsupported protocols.
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return false;
    }

    // Skip fragment-only URLs to avoid redundant entries that add no content.
    if parsed_url.fragment().is_some()
        && parsed_url.path() == "/"
        && parsed_url.query().is_none()
    {
        return false;
    }

    // Skip unwanted file extensions so we do not crawl binary assets.
    let path = parsed_url.path().to_lowercase();
    const DISALLOWED_EXTENSIONS: &[&str] = &[
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4",
        ".avi", ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".tar", ".gz", ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso", ".apk",
    ];
    if DISALLOWED_EXTENSIONS.iter().any(|ext| path.ends_with(ext)) {
        return false;
    }

    // Skip download-style query parameters to avoid pulling attachments.
    if let Some(query) = parsed_url.query() {
        let query_lower = query.to_ascii_lowercase();
        if query_lower.contains("download") || query_lower.contains("attachment") {
            return false;
        }
    }

    true
}

/// Normalize CLI input by adding https:// when no scheme is provided so users can type bare domains.
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

/// Check if a content type represents HTML so the crawler only streams parseable pages.
pub fn is_html_content_type(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml")
}

/// Hash a URL's authority (host + optional port) for shard routing.
/// This enables consistent sharding of URLs to the same worker thread.
pub fn get_authority_hash(url: &str) -> u64 {
    if let Ok(parsed) = Url::parse(url) {
        let mut hasher = DefaultHasher::new();

        // Hash the host so identical domains map to the same shard.
        if let Some(host) = parsed.host_str() {
            host.hash(&mut hasher);
        }

        // Hash the port if present so different services on the same host route separately.
        if let Some(port) = parsed.port() {
            port.hash(&mut hasher);
        }

        hasher.finish()
    } else {
        // Fall back to hashing the entire string so we still produce a stable bucket.
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
        assert!(should_crawl_url("https://test.local/page#section")); // Allow fragments when a path exists because anchors do not change content.
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
