//! Privacy and tracking metadata extraction for web pages.
//!
//! This module provides functionality to detect and analyze:
//! - Third-party tracking cookies (session, persistent, analytics)
//! - Tracking-related HTTP headers (ETag, P3P, CSP-Report-Only)
//! - Third-party API calls for data exfiltration
//! - Cross-origin resource loading patterns
//!
//! # JavaScript Analysis Limitations
//!
//! **WARNING**: The JavaScript API call extraction uses regex-based pattern matching,
//! which has significant limitations:
//!
//! - **High false negative rate**: Misses variable assignments (`var u = "..."; fetch(u)`),
//!   string concatenation (`"http" + "s://..."`), template literals, and any dynamic URL construction
//! - **Cannot handle minified/obfuscated code**: Breaks on whitespace changes and short variable names
//! - **Context-insensitive**: The 500-char window heuristic for data detection is arbitrary
//! - **No semantic understanding**: Cannot track control flow or variable scope
//!
//! For production-grade JavaScript analysis, use a proper AST parser like `swc` or `oxc`.
//! This regex-based approach is suitable only for detecting obvious, unobfuscated tracking patterns.

use serde::{Deserialize, Serialize};
use std::collections::{HashSet, HashMap};
use lazy_static::lazy_static;
use regex::Regex;
use smallvec::SmallVec;
use std::sync::Mutex;
use scraper::Selector;

/// Privacy and tracking metadata extracted from HTTP responses and HTML content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyMetadata {
    /// All cookies set by the server (Set-Cookie headers)
    pub cookies: Vec<CookieInfo>,

    /// Tracking-related HTTP headers
    pub tracking_headers: TrackingHeaders,

    /// Third-party API endpoints detected in HTML (fetch/XHR/beacon)
    pub third_party_apis: Vec<ApiCallInfo>,

    /// External resource loads (scripts, images, iframes from third-party domains)
    pub external_resources: Vec<ExternalResource>,
}

/// SameSite policy enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SameSitePolicy {
    Strict,
    Lax,
    None,
    Invalid,
}

impl SameSitePolicy {
    fn from_string(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "STRICT" => SameSitePolicy::Strict,
            "LAX" => SameSitePolicy::Lax,
            "NONE" => SameSitePolicy::None,
            _ => SameSitePolicy::Invalid,
        }
    }
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

    /// SameSite attribute (validated)
    pub same_site: Option<SameSitePolicy>,

    /// Domain scope of cookie (RFC 6265)
    pub domain: Option<String>,

    /// Path scope of cookie (RFC 6265)
    pub path: Option<String>,

    /// Whether cookie is high-risk for tracking (SameSite=None + Secure + persistent)
    pub high_risk_tracking: bool,
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

/// Information about a third-party API call detected in JavaScript
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCallInfo {
    /// The API endpoint URL
    pub endpoint: String,

    /// Method used (fetch, XMLHttpRequest, sendBeacon, etc.)
    pub method: ApiCallMethod,

    /// Domain classification (analytics, advertising, unknown)
    pub domain_type: DomainType,

    /// Potential data being sent (detected from code patterns)
    pub suspected_data: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ApiCallMethod {
    Fetch,
    XmlHttpRequest,
    SendBeacon,
    FormPost,
    ImagePixel,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DomainType {
    Analytics,
    Advertising,
    SocialMedia,
    CDN,
    Unknown,
}

/// External resource loaded from third-party domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalResource {
    /// Resource URL
    pub url: String,

    /// Resource type (script, img, iframe, etc.)
    pub resource_type: ResourceType,

    /// Domain classification
    pub domain_type: DomainType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ResourceType {
    Script,
    Image,
    Iframe,
    Video,
    Audio,
    Link,
    Unknown,
}

lazy_static! {
    /// Pre-compiled regex patterns for API call detection
    /// Note: Rust's regex crate doesn't support backreferences, so we match any quote character
    static ref FETCH_PATTERN: Regex = Regex::new(
        r#"fetch\s*\(\s*(?:['"`])([^'"`]{1,2048}?)(?:['"`])(?:\s*[,)])"#
    ).expect("Invalid fetch regex");

    static ref XHR_PATTERN: Regex = Regex::new(
        r#"\.open\s*\(\s*(?:['"`])(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)(?:['"`])\s*,\s*(?:['"`])([^'"`]{1,2048}?)(?:['"`])"#
    ).expect("Invalid XHR regex");

    static ref BEACON_PATTERN: Regex = Regex::new(
        r#"sendBeacon\s*\(\s*(?:['"`])([^'"`]{1,2048}?)(?:['"`])"#
    ).expect("Invalid beacon regex");

    static ref SERVICE_WORKER_PATTERN: Regex = Regex::new(
        r#"serviceWorker\s*\.\s*register\s*\(\s*(?:['"`])([^'"`]{1,2048}?)(?:['"`])"#
    ).expect("Invalid service worker regex");

    static ref WEBRTC_PATTERN: Regex = Regex::new(
        r#"(?:new\s+)?RTCPeerConnection|createDataChannel"#
    ).expect("Invalid WebRTC regex");
    
    /// Well-known tracking domains (blocklist)
    static ref TRACKER_DOMAINS: HashSet<&'static str> = {
        vec![
            "google-analytics.com",
            "googletagmanager.com",
            "googleadservices.com",
            "googlesyndication.com",
            "doubleclick.net",
            "connect.facebook.net",
            "facebook.com",
            "fbcdn.net",
            "hotjar.com",
            "mixpanel.com",
            "segment.com",
            "segment.io",
            "amplitude.com",
            "heap.io",
            "fullstory.com",
            "amazon-adsystem.com",
            "adsrvr.org",
            "adnxs.com",
            "advertising.com",
            "criteo.com",
            "pubmatic.com",
            "cloudflareinsights.com",
            "newrelic.com",
            "nr-data.net",
            "twitter.com",
            "linkedin.com",
            "pinterest.com",
            "instagram.com",
            "mouseflow.com",
            "crazyegg.com",
            "optimizely.com",
            "quantserve.com",
            "scorecardresearch.com",
        ].into_iter().collect()
    };
    
    /// Pre-compiled CSS selectors for resource extraction
    static ref SCRIPT_SELECTOR: Selector = Selector::parse("script[src]").expect("Invalid script selector");
    static ref IMG_SELECTOR: Selector = Selector::parse("img[src]").expect("Invalid img selector");
    static ref IFRAME_SELECTOR: Selector = Selector::parse("iframe[src]").expect("Invalid iframe selector");
    
    /// Domain classification cache (URL domain -> DomainType)
    static ref DOMAIN_CACHE: Mutex<HashMap<String, DomainType>> = Mutex::new(HashMap::new());
}

lazy_static! {
    /// Pre-compiled tracking cookie name patterns
    static ref TRACKING_COOKIE_PATTERNS: HashSet<&'static str> = vec![
        "_ga", "_gid", "_gat", "_fbp", "_fbc",
        "__utma", "__utmb", "__utmc", "__utmz",
        "_hjid", "ajs_", "mp_", "amplitude", "intercom",
    ].into_iter().collect();

    /// Pre-compiled session cookie name patterns
    static ref SESSION_COOKIE_PATTERNS: HashSet<&'static str> = vec![
        "session", "sess", "sid", "sessionid", "jsessionid",
        "phpsessid", "aspsessionid", "csrf", "xsrf",
    ].into_iter().collect();
}

impl PrivacyMetadata {
    /// Create empty privacy metadata
    pub fn empty() -> Self {
        Self {
            cookies: Vec::new(),
            tracking_headers: TrackingHeaders::default(),
            third_party_apis: Vec::new(),
            external_resources: Vec::new(),
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
        let mut cookies: SmallVec<[CookieInfo; 6]> = SmallVec::new();
        for h in headers.get_all(reqwest::header::SET_COOKIE).iter() {
            if let Ok(cookie_str) = h.to_str() {
                cookies.push(Self::parse_cookie(cookie_str));
            }
        }
        cookies.into_vec()
    }

    /// Parse a single Set-Cookie header value
    fn parse_cookie(raw_header: &str) -> CookieInfo {
        let parts: Vec<&str> = raw_header.split(';').collect();

        let name = parts.first()
            .and_then(|first| first.split('=').next())
            .map(|s| s.trim().to_string());

        let mut max_age_secs = None;
        let mut http_only = false;
        let mut secure = false;
        let mut same_site = None;
        let mut domain = None;
        let mut path = None;

        for part_ref in &parts[1..] {
            let part = part_ref.trim();
            let part_lower = part.to_ascii_lowercase();

            if part_lower.starts_with("max-age=") {
                max_age_secs = part[8..].parse::<i64>().ok().filter(|&v| v >= 0);
            } else if part_lower.starts_with("expires=") {
                if max_age_secs.is_none() {
                    max_age_secs = Self::parse_expires_header(&part[8..]);
                }
            } else if part_lower == "httponly" {
                http_only = true;
            } else if part_lower == "secure" {
                secure = true;
            } else if part_lower.starts_with("samesite=") {
                same_site = Some(SameSitePolicy::from_string(&part[9..]));
            } else if part_lower.starts_with("domain=") {
                domain = Some(part[7..].to_string());
            } else if part_lower.starts_with("path=") {
                path = Some(part[5..].to_string());
            }
        }

        let cookie_type = Self::classify_cookie(name.as_deref(), max_age_secs);
        
        let high_risk_tracking = same_site == Some(SameSitePolicy::None) 
            && secure 
            && max_age_secs.map(|v| v > 31536000).unwrap_or(false);

        CookieInfo {
            raw_header: raw_header.to_string(),
            name,
            cookie_type,
            max_age_secs,
            http_only,
            secure,
            same_site,
            domain,
            path,
            high_risk_tracking,
        }
    }
    
    fn parse_expires_header(expires_str: &str) -> Option<i64> {
        use chrono::DateTime;
        
        if let Ok(dt) = DateTime::parse_from_rfc2822(expires_str) {
            let now = chrono::Utc::now();
            let duration = dt.with_timezone(&chrono::Utc) - now;
            if duration.num_seconds() > 0 {
                return Some(duration.num_seconds());
            }
        }
        None
    }

    /// Classify a cookie based on its name and attributes
    fn classify_cookie(name: Option<&str>, max_age_secs: Option<i64>) -> CookieType {
        let name = match name {
            Some(n) => n,
            None => return CookieType::Unknown,
        };

        let name_lower = name.to_ascii_lowercase();
        
        if name_lower.starts_with("_ga") || name_lower.starts_with("__utm") {
            return CookieType::Analytics;
        }
        if name_lower.starts_with("_fb") {
            return CookieType::Advertising;
        }

        for pattern in TRACKING_COOKIE_PATTERNS.iter() {
            if name_lower.contains(pattern) {
                return CookieType::Tracking;
            }
        }

        for pattern in SESSION_COOKIE_PATTERNS.iter() {
            if name_lower.contains(pattern) {
                return CookieType::Session;
            }
        }

        if let Some(max_age) = max_age_secs {
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

    /// Add third-party API calls detected in JavaScript
    pub fn add_api_calls(&mut self, apis: Vec<ApiCallInfo>) {
        self.third_party_apis.extend(apis);
    }

    /// Add external resources detected in HTML
    pub fn add_external_resources(&mut self, resources: Vec<ExternalResource>) {
        self.external_resources.extend(resources);
    }

    // [Zencoder Task Doc]
    // WHAT: Extracts third-party API calls from JavaScript by matching fetch, XHR, beacon, and service worker patterns.
    // USED_BY: src/bfs_crawler.rs (crawl_single_url)

    /// Extract API calls from JavaScript code using regex-based pattern matching.
    ///
    /// # Limitations
    ///
    /// This is a **naive, regex-based approach** with severe limitations:
    /// - High false negative rate (misses dynamic URLs, variables, concatenation)
    /// - Breaks on minified code
    /// - Cannot handle template literals or modern JS syntax reliably
    ///
    /// Use only for detecting simple, obvious tracking patterns. For comprehensive
    /// analysis, use a proper JavaScript AST parser.
    pub fn extract_api_calls(script: &str, page_domain: &str) -> Vec<ApiCallInfo> {
        let mut apis = Vec::with_capacity(16);
        let mut seen: HashSet<(String, ApiCallMethod)> = HashSet::with_capacity(16);

        for api in Self::extract_fetch_calls(script, page_domain) {
            let key = (api.endpoint.clone(), api.method);
            if seen.insert(key) {
                apis.push(api);
            }
        }

        for api in Self::extract_xhr_calls(script, page_domain) {
            let key = (api.endpoint.clone(), api.method);
            if seen.insert(key) {
                apis.push(api);
            }
        }

        for api in Self::extract_beacon_calls(script, page_domain) {
            let key = (api.endpoint.clone(), api.method);
            if seen.insert(key) {
                apis.push(api);
            }
        }

        for api in Self::extract_service_worker_calls(script, page_domain) {
            let key = (api.endpoint.clone(), api.method);
            if seen.insert(key) {
                apis.push(api);
            }
        }

        apis
    }

    fn extract_api_calls_by_pattern(
        script: &str,
        page_domain: &str,
        pattern: &Regex,
        method: ApiCallMethod,
        service_worker: bool,
    ) -> Vec<ApiCallInfo> {
        let mut calls: SmallVec<[ApiCallInfo; 4]> = SmallVec::new();

        for cap in pattern.captures_iter(script) {
            // Changed from cap.get(2) to cap.get(1) since we removed the quote capture group
            if let Some(url_match) = cap.get(1) {
                let url = url_match.as_str();
                if Self::is_third_party_url(url, page_domain) {
                    let suspected_data = if service_worker {
                        vec!["serviceWorkerInterception".to_string()]
                    } else {
                        Self::extract_suspected_data(script, url)
                    };

                    calls.push(ApiCallInfo {
                        endpoint: url.to_string(),
                        method,
                        domain_type: Self::classify_domain(url),
                        suspected_data,
                    });
                }
            }
        }

        calls.into_vec()
    }

    fn extract_fetch_calls(script: &str, page_domain: &str) -> Vec<ApiCallInfo> {
        Self::extract_api_calls_by_pattern(script, page_domain, &FETCH_PATTERN, ApiCallMethod::Fetch, false)
    }

    fn extract_xhr_calls(script: &str, page_domain: &str) -> Vec<ApiCallInfo> {
        Self::extract_api_calls_by_pattern(script, page_domain, &XHR_PATTERN, ApiCallMethod::XmlHttpRequest, false)
    }

    fn extract_beacon_calls(script: &str, page_domain: &str) -> Vec<ApiCallInfo> {
        Self::extract_api_calls_by_pattern(script, page_domain, &BEACON_PATTERN, ApiCallMethod::SendBeacon, false)
    }

    fn extract_service_worker_calls(script: &str, page_domain: &str) -> Vec<ApiCallInfo> {
        Self::extract_api_calls_by_pattern(script, page_domain, &SERVICE_WORKER_PATTERN, ApiCallMethod::Unknown, true)
    }

    fn is_third_party_url(url: &str, page_domain: &str) -> bool {
        if url.starts_with('/') || url.starts_with("./") || url.starts_with("../") {
            return false;
        }

        if let Some(domain) = crate::url_utils::extract_host(url) {
            !crate::url_utils::is_same_domain(&domain, page_domain)
        } else {
            false
        }
    }

    fn classify_domain(url: &str) -> DomainType {
        if let Some(host) = crate::url_utils::extract_host(url) {
            let registrable = crate::url_utils::get_registrable_domain(&host);
            
            if let Ok(mut cache) = DOMAIN_CACHE.lock() {
                if let Some(cached_type) = cache.get(registrable.as_str()) {
                    return *cached_type;
                }
                
                let result = if TRACKER_DOMAINS.contains(registrable.as_str()) {
                    if registrable.contains("analytics") 
                        || registrable.contains("mixpanel")
                        || registrable.contains("segment") 
                        || registrable.contains("amplitude") {
                        DomainType::Analytics
                    } else if registrable.contains("facebook") 
                        || registrable.contains("twitter")
                        || registrable.contains("linkedin") {
                        DomainType::SocialMedia
                    } else if registrable.contains("doubleclick") 
                        || registrable.contains("adsrvr")
                        || registrable.contains("criteo") {
                        DomainType::Advertising
                    } else if registrable.contains("cloudflare") 
                        || registrable.contains("newrelic") {
                        DomainType::CDN
                    } else {
                        DomainType::Analytics
                    }
                } else {
                    DomainType::Unknown
                };
                
                cache.insert(registrable, result);
                result
            } else {
                DomainType::Unknown
            }
        } else {
            DomainType::Unknown
        }
    }

    // [Zencoder Task Doc]
    // WHAT: Analyzes JavaScript context around an API endpoint to detect what kind of data might be sent (cookies, storage, fingerprints, etc.).
    // USED_BY: src/privacy_metadata.rs (extract_api_calls_by_pattern)

    // TODO: This regex-based heuristic is unreliable, produces high false negatives, and will be broken by any code minification. This entire approach must be replaced with a proper JavaScript AST parser (e.g., swc or rslint) to be effective.

    fn extract_suspected_data(script: &str, url: &str) -> Vec<String> {
        let url_pos = match script.find(url) {
            Some(pos) => pos,
            None => return Vec::new(),
        };

        let context_start = url_pos.saturating_sub(500);
        let context_end = (url_pos + 500).min(script.len());
        let context = &script[context_start..context_end];

        let mut suspected: SmallVec<[String; 16]> = SmallVec::new();

        if context.contains("document.cookie") || context.contains(".cookie") {
            suspected.push("cookies".to_string());
        }
        if context.contains("localStorage") {
            suspected.push("storage".to_string());
        }
        if context.contains("navigator.userAgent") || context.contains("userAgent") {
            suspected.push("userAgent".to_string());
        }
        if context.contains("screen.width") || context.contains("screen.height") {
            suspected.push("screenSize".to_string());
        }
        if context.contains("navigator.language") {
            suspected.push("language".to_string());
        }
        if context.contains("location.href") || context.contains("window.location") {
            suspected.push("currentURL".to_string());
        }
        if context.contains("referrer") {
            suspected.push("referrer".to_string());
        }
        if context.contains("navigator.platform") {
            suspected.push("platform".to_string());
        }
        if context.contains("navigator.plugins") {
            suspected.push("plugins".to_string());
        }
        if context.contains("navigator.connection") {
            suspected.push("networkInfo".to_string());
        }
        if context.contains("hardwareConcurrency") {
            suspected.push("cpuCores".to_string());
        }
        if context.contains("performance.memory") {
            suspected.push("heapSize".to_string());
        }
        if context.contains("performance.") || context.contains("timing") {
            suspected.push("performanceData".to_string());
        }
        if context.contains("canvas") {
            suspected.push("canvasFingerprint".to_string());
        }
        if context.contains("AudioContext") {
            suspected.push("audioFingerprint".to_string());
        }
        if WEBRTC_PATTERN.is_match(context) {
            suspected.push("webrtcIPLeak".to_string());
        }
        if context.contains("getContext") && context.contains("webgl") {
            suspected.push("webglFingerprint".to_string());
        }

        suspected.into_vec()
    }

    /// Extract external resources from HTML
    pub fn extract_external_resources(html: &str, page_domain: &str) -> Vec<ExternalResource> {
        use scraper::Html;

        let document = Html::parse_document(html);
        let mut resources: SmallVec<[ExternalResource; 12]> = SmallVec::new();
        let mut seen_urls: HashSet<String> = HashSet::with_capacity(16);

        Self::extract_resources_by_selector(&document, &SCRIPT_SELECTOR, ResourceType::Script, 
                                          page_domain, &mut resources, &mut seen_urls);
        Self::extract_resources_by_selector(&document, &IMG_SELECTOR, ResourceType::Image, 
                                          page_domain, &mut resources, &mut seen_urls);
        Self::extract_resources_by_selector(&document, &IFRAME_SELECTOR, ResourceType::Iframe, 
                                          page_domain, &mut resources, &mut seen_urls);

        resources.into_vec()
    }

    fn extract_resources_by_selector(
        document: &scraper::Html,
        selector: &Selector,
        resource_type: ResourceType,
        page_domain: &str,
        resources: &mut SmallVec<[ExternalResource; 12]>,
        seen_urls: &mut HashSet<String>,
    ) {
        for el in document.select(selector) {
            if let Some(src) = el.value().attr("src") {
                if !seen_urls.contains(src) && Self::is_third_party_url(src, page_domain) {
                    seen_urls.insert(src.to_string());
                    resources.push(ExternalResource {
                        url: src.to_string(),
                        resource_type,
                        domain_type: Self::classify_domain(src),
                    });
                }
            }
        }
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
        let raw = "_ga=GA1.2.123456789.1234567890; Max-Age=63072000; Path=/; Domain=.example.com; HttpOnly; Secure; SameSite=Lax";
        let cookie = PrivacyMetadata::parse_cookie(raw);

        assert_eq!(cookie.name, Some("_ga".to_string()));
        assert_eq!(cookie.cookie_type, CookieType::Analytics);
        assert_eq!(cookie.max_age_secs, Some(63072000));
        assert!(cookie.http_only);
        assert!(cookie.secure);
        assert_eq!(cookie.same_site, Some(SameSitePolicy::Lax));
        assert_eq!(cookie.path, Some("/".to_string()));
        assert_eq!(cookie.domain, Some(".example.com".to_string()));
        assert!(!cookie.high_risk_tracking);
    }

    #[test]
    fn test_high_risk_cookie_detection() {
        let raw = "_ga=123; Max-Age=63072000; Secure; SameSite=None";
        let cookie = PrivacyMetadata::parse_cookie(raw);
        assert!(cookie.high_risk_tracking);
    }

    #[test]
    fn test_negative_max_age_filtered() {
        let raw = "id=val; Max-Age=-1";
        let cookie = PrivacyMetadata::parse_cookie(raw);
        assert_eq!(cookie.max_age_secs, None);
    }
}
