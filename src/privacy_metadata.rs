//! Privacy and tracking metadata extraction for web pages.
//!
//! This module provides functionality to detect and analyze:
//! - Third-party tracking cookies (session, persistent, analytics)
//! - Tracking-related HTTP headers (ETag, P3P, CSP-Report-Only)
//! - Third-party tracking scripts (Google Analytics, Facebook Pixel, etc.)
//! - Tracking pixels and beacons (1x1 invisible images/iframes)

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Privacy and tracking metadata extracted from HTTP responses and HTML content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyMetadata {
    /// All cookies set by the server (Set-Cookie headers)
    pub cookies: Vec<CookieInfo>,

    /// Tracking-related HTTP headers
    pub tracking_headers: TrackingHeaders,

    /// Third-party tracking scripts detected in HTML
    pub tracking_scripts: Vec<String>,

    /// Tracking pixels and beacons detected in HTML
    pub tracking_pixels: Vec<String>,
}

/// Information about a cookie set by the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CookieInfo {
    /// Raw Set-Cookie header value
    pub raw_header: String,

    /// Cookie name (extracted from raw header)
    pub name: Option<String>,

    /// Cookie classification based on name and attributes
    pub cookie_type: CookieType,

    /// Max-Age or Expires attribute (indicates persistence)
    pub max_age_secs: Option<i64>,

    /// Whether this cookie is HttpOnly
    pub http_only: bool,

    /// Whether this cookie is Secure
    pub secure: bool,

    /// SameSite attribute
    pub same_site: Option<String>,
}

/// Classification of cookie types based on common patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CookieType {
    /// Session identifier cookies
    Session,
    /// Analytics/tracking cookies (Google Analytics, etc.)
    Analytics,
    /// Advertising/marketing cookies
    Advertising,
    /// Persistent user tracking cookies
    Tracking,
    /// Functional cookies (preferences, etc.)
    Functional,
    /// Unknown/unclassified
    Unknown,
}

/// Tracking-related HTTP headers
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrackingHeaders {
    /// ETag header (can be used for tracking)
    pub etag: Option<String>,

    /// P3P (Platform for Privacy Preferences) header
    pub p3p: Option<String>,

    /// Content-Security-Policy-Report-Only header
    pub csp_report_only: Option<String>,

    /// X-Frame-Options header
    pub x_frame_options: Option<String>,
}

/// Well-known tracking domains (blocklist)
/// This is a curated list of common tracking/analytics providers
pub fn get_tracker_domains() -> HashSet<&'static str> {
    [
        // Google tracking
        "google-analytics.com",
        "googletagmanager.com",
        "googleadservices.com",
        "googlesyndication.com",
        "doubleclick.net",

        // Facebook tracking
        "connect.facebook.net",
        "facebook.com",
        "fbcdn.net",

        // Analytics providers
        "hotjar.com",
        "mixpanel.com",
        "segment.com",
        "segment.io",
        "amplitude.com",
        "heap.io",
        "fullstory.com",

        // Advertising networks
        "amazon-adsystem.com",
        "adsrvr.org",
        "adnxs.com",
        "advertising.com",
        "criteo.com",
        "pubmatic.com",

        // CDN-based trackers
        "cloudflareinsights.com",
        "newrelic.com",
        "nr-data.net",

        // Social media widgets
        "twitter.com",
        "linkedin.com",
        "pinterest.com",
        "instagram.com",

        // Other common trackers
        "mouseflow.com",
        "crazy​egg.com",
        "optimizely.com",
        "quantserve.com",
        "scorecardresearch.com",
    ]
    .iter()
    .copied()
    .collect()
}

/// Common tracking cookie name patterns
const TRACKING_COOKIE_PATTERNS: &[&str] = &[
    "_ga",      // Google Analytics
    "_gid",     // Google Analytics
    "_gat",     // Google Analytics
    "_fbp",     // Facebook Pixel
    "_fbc",     // Facebook Click ID
    "__utma",   // Google Analytics (legacy)
    "__utmb",   // Google Analytics (legacy)
    "__utmc",   // Google Analytics (legacy)
    "__utmz",   // Google Analytics (legacy)
    "_hjid",    // Hotjar
    "ajs_",     // Segment
    "mp_",      // Mixpanel
    "amplitude", // Amplitude
    "intercom", // Intercom
];

/// Session cookie name patterns
const SESSION_COOKIE_PATTERNS: &[&str] = &[
    "session",
    "sess",
    "sid",
    "sessionid",
    "jsessionid",
    "phpsessid",
    "aspsessionid",
    "csrf",
    "xsrf",
];

impl PrivacyMetadata {
    /// Create empty privacy metadata
    pub fn empty() -> Self {
        Self {
            cookies: Vec::new(),
            tracking_headers: TrackingHeaders::default(),
            tracking_scripts: Vec::new(),
            tracking_pixels: Vec::new(),
        }
    }

    /// Extract privacy metadata from HTTP response headers
    pub fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let mut metadata = Self::empty();

        // Extract cookies
        metadata.cookies = Self::extract_cookies(headers);

        // Extract tracking headers
        metadata.tracking_headers = Self::extract_tracking_headers(headers);

        metadata
    }

    /// Extract all Set-Cookie headers and parse them
    fn extract_cookies(headers: &reqwest::header::HeaderMap) -> Vec<CookieInfo> {
        headers
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|h| h.to_str().ok())
            .map(|cookie_str| Self::parse_cookie(cookie_str))
            .collect()
    }

    /// Parse a single Set-Cookie header value
    fn parse_cookie(raw_header: &str) -> CookieInfo {
        let parts: Vec<&str> = raw_header.split(';').map(|s| s.trim()).collect();

        // Extract cookie name from first part (name=value)
        let name = parts.first()
            .and_then(|first| first.split('=').next())
            .map(|s| s.to_string());

        // Parse attributes
        let mut max_age_secs = None;
        let mut http_only = false;
        let mut secure = false;
        let mut same_site = None;

        for part in &parts[1..] {
            let lower = part.to_lowercase();

            if lower.starts_with("max-age=") {
                max_age_secs = part
                    .split('=')
                    .nth(1)
                    .and_then(|s| s.parse::<i64>().ok());
            } else if lower.starts_with("expires=") {
                // Parse expiration date and calculate max-age
                // For simplicity, we'll just mark it as persistent (2 years)
                max_age_secs = Some(63072000); // 2 years in seconds
            } else if lower == "httponly" {
                http_only = true;
            } else if lower == "secure" {
                secure = true;
            } else if lower.starts_with("samesite=") {
                same_site = part.split('=').nth(1).map(|s| s.to_string());
            }
        }

        // Classify cookie type
        let cookie_type = Self::classify_cookie(name.as_deref(), max_age_secs);

        CookieInfo {
            raw_header: raw_header.to_string(),
            name,
            cookie_type,
            max_age_secs,
            http_only,
            secure,
            same_site,
        }
    }

    /// Classify a cookie based on its name and attributes
    fn classify_cookie(name: Option<&str>, max_age_secs: Option<i64>) -> CookieType {
        let name = match name {
            Some(n) => n.to_lowercase(),
            None => return CookieType::Unknown,
        };

        // Check against known tracking patterns
        for pattern in TRACKING_COOKIE_PATTERNS {
            if name.contains(pattern) {
                // Google Analytics cookies
                if name.starts_with("_ga") || name.starts_with("__utm") {
                    return CookieType::Analytics;
                }
                // Facebook cookies
                if name.starts_with("_fb") {
                    return CookieType::Advertising;
                }
                // Generic tracking
                return CookieType::Tracking;
            }
        }

        // Check for session cookies
        for pattern in SESSION_COOKIE_PATTERNS {
            if name.contains(pattern) {
                return CookieType::Session;
            }
        }

        // Check persistence (cookies with long max-age are likely tracking)
        if let Some(max_age) = max_age_secs {
            // Cookies that last more than 1 year are likely tracking
            if max_age > 31536000 {
                return CookieType::Tracking;
            }
        }

        CookieType::Functional
    }

    /// Extract tracking-related HTTP headers
    fn extract_tracking_headers(headers: &reqwest::header::HeaderMap) -> TrackingHeaders {
        TrackingHeaders {
            etag: headers
                .get(reqwest::header::ETAG)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string()),
            p3p: headers
                .get("P3P")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string()),
            csp_report_only: headers
                .get("Content-Security-Policy-Report-Only")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string()),
            x_frame_options: headers
                .get("X-Frame-Options")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string()),
        }
    }

    /// Add tracking scripts found in HTML content
    pub fn add_tracking_scripts(&mut self, scripts: Vec<String>) {
        self.tracking_scripts.extend(scripts);
    }

    /// Add tracking pixels found in HTML content
    pub fn add_tracking_pixels(&mut self, pixels: Vec<String>) {
        self.tracking_pixels.extend(pixels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_google_analytics_cookie() {
        let cookie_type = PrivacyMetadata::classify_cookie(Some("_ga"), Some(63072000));
        assert_eq!(cookie_type, CookieType::Analytics);
    }

    #[test]
    fn test_classify_facebook_pixel_cookie() {
        let cookie_type = PrivacyMetadata::classify_cookie(Some("_fbp"), Some(7776000));
        assert_eq!(cookie_type, CookieType::Advertising);
    }

    #[test]
    fn test_classify_session_cookie() {
        let cookie_type = PrivacyMetadata::classify_cookie(Some("sessionid"), None);
        assert_eq!(cookie_type, CookieType::Session);
    }

    #[test]
    fn test_parse_cookie_with_attributes() {
        let raw = "_ga=GA1.2.123456789.1234567890; Max-Age=63072000; Path=/; HttpOnly; Secure; SameSite=Lax";
        let cookie = PrivacyMetadata::parse_cookie(raw);

        assert_eq!(cookie.name, Some("_ga".to_string()));
        assert_eq!(cookie.cookie_type, CookieType::Analytics);
        assert_eq!(cookie.max_age_secs, Some(63072000));
        assert!(cookie.http_only);
        assert!(cookie.secure);
        assert_eq!(cookie.same_site, Some("Lax".to_string()));
    }
}
