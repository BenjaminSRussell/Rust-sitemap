//! URL helper functions used throughout the crawler

use url::Url;

/// Extract the host portion of a URL
pub fn extract_host(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

/// Return the root domain using a simple last-two-label heuristic
pub fn get_root_domain(hostname: &str) -> String {
    let parts: Vec<&str> = hostname.split('.').collect();

    // Use the last two labels when available
    if parts.len() >= 2 {
        format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        hostname.to_string()
    }
}

/// Check whether two domains match, including subdomain variants
pub fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
    url_domain == base_domain
        || url_domain.ends_with(&format!(".{}", base_domain))
        || base_domain.ends_with(&format!(".{}", url_domain))
}

/// Resolve a link against the provided base URL
pub fn convert_to_absolute_url(link: &str, base_url: &str) -> Result<String, String> {
    let base = Url::parse(base_url).map_err(|e| e.to_string())?;
    let absolute_url = base.join(link).map_err(|e| e.to_string())?;
    Ok(absolute_url.to_string())
}

/// Remove the fragment portion from a URL
pub fn normalize_url(url: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        parsed.set_fragment(None);
        parsed.to_string()
    } else {
        url.to_string()
    }
}

/// Extract the fragment (including '#') from a URL
pub fn extract_fragment(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.fragment().map(|f| format!("#{}", f)))
}

/// Build the robots.txt URL for the given start URL
pub fn robots_url(start_url: &str) -> Option<String> {
    let parsed = Url::parse(start_url).ok()?;
    let scheme = parsed.scheme();
    let host = parsed.host_str()?;
    Some(format!("{}://{}/robots.txt", scheme, host))
}

/// Determine whether a URL is eligible for crawling
pub fn should_crawl_url(url: &str) -> bool {
    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };

    // Require HTTP(S)
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return false;
    }

    // Skip fragment-only URLs
    if parsed_url.fragment().is_some()
        && parsed_url.path() == "/"
        && parsed_url.query().is_none()
    {
        return false;
    }

    // Skip unwanted file extensions
    let path = parsed_url.path().to_lowercase();
    const DISALLOWED_EXTENSIONS: &[&str] = &[
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".xml", ".zip", ".mp4",
        ".avi", ".mov", ".mp3", ".wav", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".tar", ".gz", ".tgz", ".bz2", ".7z", ".rar", ".exe", ".msi", ".dmg", ".iso", ".apk",
    ];
    if DISALLOWED_EXTENSIONS.iter().any(|ext| path.ends_with(ext)) {
        return false;
    }

    // Skip download-style query parameters
    if let Some(query) = parsed_url.query() {
        let query_lower = query.to_ascii_lowercase();
        if query_lower.contains("download") || query_lower.contains("attachment") {
            return false;
        }
    }

    true
}

/// Normalize CLI input by adding https:// when no scheme is provided
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

/// Check if a content type represents HTML
pub fn is_html_content_type(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml")
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
    fn test_normalize_url() {
        assert_eq!(
            normalize_url("https://example.com/page#section"),
            "https://example.com/page"
        );
        assert_eq!(
            normalize_url("https://example.com/page"),
            "https://example.com/page"
        );
    }

    #[test]
    fn test_extract_fragment() {
        assert_eq!(
            extract_fragment("https://example.com/page#section1"),
            Some("#section1".to_string())
        );
        assert_eq!(extract_fragment("https://example.com/page"), None);
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
        assert!(should_crawl_url("https://test.local/page#section")); // Allow fragments when a path exists
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
}
