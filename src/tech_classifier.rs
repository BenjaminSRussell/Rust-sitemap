//! Technology stack classification for web pages.
//!
//! This module identifies the technology platform a site is built with
//! (e.g., Shopify, Next.js, WordPress) through header analysis and HTML patterns.
//! This information can be used to:
//! - Target platform-specific APIs for richer data extraction
//! - Adjust crawling strategies per platform
//! - Generate analytics about tech stack distribution

use serde::{Serialize, Deserialize};

/// Technology platform classification for a web page
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TechProfile {
    /// Shopify e-commerce platform
    Shopify,
    /// Next.js React framework
    NextJs,
    /// WordPress CMS
    WordPress,
    /// Wix website builder
    Wix,
    /// Squarespace website builder
    Squarespace,
    /// WooCommerce (WordPress e-commerce)
    WooCommerce,
    /// Magento e-commerce platform
    Magento,
    /// BigCommerce e-commerce platform
    BigCommerce,
    /// Drupal CMS
    Drupal,
    /// Joomla CMS
    Joomla,
    /// Ghost CMS
    Ghost,
    /// Nuxt.js (Vue.js framework)
    NuxtJs,
    /// Gatsby static site generator
    Gatsby,
    /// React (detected but not framework-specific)
    React,
    /// Angular framework
    Angular,
    /// Vue.js framework
    Vue,
    /// Platform could not be identified
    Unknown,
}

impl TechProfile {
    /// Convert TechProfile to a string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            TechProfile::Shopify => "Shopify",
            TechProfile::NextJs => "Next.js",
            TechProfile::WordPress => "WordPress",
            TechProfile::Wix => "Wix",
            TechProfile::Squarespace => "Squarespace",
            TechProfile::WooCommerce => "WooCommerce",
            TechProfile::Magento => "Magento",
            TechProfile::BigCommerce => "BigCommerce",
            TechProfile::Drupal => "Drupal",
            TechProfile::Joomla => "Joomla",
            TechProfile::Ghost => "Ghost",
            TechProfile::NuxtJs => "Nuxt.js",
            TechProfile::Gatsby => "Gatsby",
            TechProfile::React => "React",
            TechProfile::Angular => "Angular",
            TechProfile::Vue => "Vue.js",
            TechProfile::Unknown => "Unknown",
        }
    }
}

impl std::fmt::Display for TechProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Classify the technology stack of a web page based on HTTP headers and HTML content
///
/// # Arguments
/// * `html` - The HTML content of the page
/// * `headers` - HTTP response headers
///
/// # Returns
/// The detected technology profile, or `TechProfile::Unknown` if not identified
///
/// # Classification Strategy
/// 1. Header checks (fastest and most reliable)
/// 2. HTML meta tags and script sources
/// 3. HTML content patterns (slowest but catches more cases)
pub fn classify(html: &str, headers: &reqwest::header::HeaderMap) -> TechProfile {
    // Phase 1: Header Checks (Fastest and most reliable)
    if let Some(profile) = classify_from_headers(headers) {
        return profile;
    }

    // Phase 2: HTML Content Checks
    classify_from_html(html)
}

/// Classify based on HTTP response headers
fn classify_from_headers(headers: &reqwest::header::HeaderMap) -> Option<TechProfile> {
    // Shopify detection
    if headers.get("x-shopify-request-id").is_some()
        || headers.get("x-shopid").is_some()
        || headers.get("x-shardid").is_some() {
        return Some(TechProfile::Shopify);
    }

    // Next.js detection
    if let Some(powered_by) = headers.get("x-powered-by").and_then(|v| v.to_str().ok()) {
        if powered_by.contains("Next.js") {
            return Some(TechProfile::NextJs);
        }
        if powered_by.contains("PHP") {
            // Might be WordPress, but need HTML to confirm
            return None;
        }
    }

    // Check for common CMS headers
    if headers.get("x-drupal-cache").is_some() || headers.get("x-drupal-dynamic-cache").is_some() {
        return Some(TechProfile::Drupal);
    }

    if headers.get("x-ghost-cache-status").is_some() {
        return Some(TechProfile::Ghost);
    }

    None
}

/// Classify based on HTML content patterns
fn classify_from_html(html: &str) -> TechProfile {
    // E-commerce platforms (check these first as they're more specific)

    // Shopify (very distinctive patterns)
    if html.contains("cdn.shopify.com")
        || html.contains("Shopify.theme")
        || html.contains("shopify-digital-wallet")
        || html.contains("shopify-features") {
        return TechProfile::Shopify;
    }

    // BigCommerce
    if html.contains("bigcommerce.com/assets") || html.contains("cdn11.bigcommerce.com") {
        return TechProfile::BigCommerce;
    }

    // Magento
    if html.contains("Mage.Cookies")
        || html.contains("/static/version")
        || html.contains("var BLANK_URL")
        || html.contains("Magento_") {
        return TechProfile::Magento;
    }

    // WooCommerce (WordPress + WooCommerce)
    if (html.contains("/wp-content/") || html.contains("/wp-includes/"))
        && (html.contains("woocommerce") || html.contains("WC_Price_Filter")) {
        return TechProfile::WooCommerce;
    }

    // Website builders

    // Wix (very distinctive)
    if html.contains("static.wixstatic.com")
        || html.contains("parastorage.com")
        || html.contains("wix-code-public-path") {
        return TechProfile::Wix;
    }

    // Squarespace
    if html.contains("static1.squarespace.com")
        || html.contains("squarespace.com/static")
        || html.contains("Static.SQUARESPACE_CONTEXT") {
        return TechProfile::Squarespace;
    }

    // CMS platforms

    // WordPress (most common CMS)
    if html.contains("/wp-content/")
        || html.contains("/wp-includes/")
        || html.contains("wp-json")
        || html.contains("wordpress") {
        return TechProfile::WordPress;
    }

    // Drupal
    if html.contains("Drupal.settings")
        || html.contains("/sites/default/files/")
        || html.contains("drupal.js") {
        return TechProfile::Drupal;
    }

    // Joomla
    if html.contains("/media/jui/")
        || html.contains("Joomla!")
        || html.contains("/components/com_") {
        return TechProfile::Joomla;
    }

    // Ghost
    if html.contains("ghost.io") || html.contains("ghost-sdk") {
        return TechProfile::Ghost;
    }

    // JavaScript frameworks and static site generators

    // Next.js (most distinctive check)
    if html.contains(r#"id="__NEXT_DATA__""#)
        || html.contains(r#"id="__next""#)
        || html.contains("/_next/static/") {
        return TechProfile::NextJs;
    }

    // Nuxt.js
    if html.contains(r#"id="__NUXT__""#)
        || html.contains("window.__NUXT__")
        || html.contains("/_nuxt/") {
        return TechProfile::NuxtJs;
    }

    // Gatsby
    if html.contains(r#"id="___gatsby""#)
        || html.contains("gatsby-plugin")
        || html.contains("webpack-runtime-") {
        return TechProfile::Gatsby;
    }

    // Angular (check for Angular-specific patterns)
    if html.contains("ng-version")
        || html.contains("ng-app")
        || html.contains("<app-root") {
        return TechProfile::Angular;
    }

    // Vue.js (generic Vue, not Nuxt)
    if html.contains("data-v-")
        || html.contains("[v-cloak]")
        || html.contains("v-app") {
        return TechProfile::Vue;
    }

    // React (generic React detection, least specific)
    if html.contains("react") || html.contains("data-reactroot") || html.contains("data-reactid") {
        return TechProfile::React;
    }

    TechProfile::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shopify_detection_from_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-shopify-request-id", "test-id".parse().unwrap());

        let profile = classify("", &headers);
        assert_eq!(profile, TechProfile::Shopify);
    }

    #[test]
    fn test_shopify_detection_from_html() {
        let html = r#"<script src="https://cdn.shopify.com/s/files/1/test.js"></script>"#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::Shopify);
    }

    #[test]
    fn test_nextjs_detection_from_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-powered-by", "Next.js".parse().unwrap());

        let profile = classify("", &headers);
        assert_eq!(profile, TechProfile::NextJs);
    }

    #[test]
    fn test_nextjs_detection_from_html() {
        let html = r#"<script id="__NEXT_DATA__" type="application/json">{"props":{}}</script>"#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::NextJs);
    }

    #[test]
    fn test_wordpress_detection() {
        let html = r#"<link rel='stylesheet' href='/wp-content/themes/test/style.css'>"#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::WordPress);
    }

    #[test]
    fn test_woocommerce_detection() {
        let html = r#"
            <link rel='stylesheet' href='/wp-content/plugins/woocommerce/style.css'>
            <script src='/wp-includes/js/jquery.js'></script>
        "#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::WooCommerce);
    }

    #[test]
    fn test_wix_detection() {
        let html = r#"<img src="https://static.wixstatic.com/media/test.jpg">"#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::Wix);
    }

    #[test]
    fn test_unknown_detection() {
        let html = r#"<html><body>Hello World</body></html>"#;
        let headers = reqwest::header::HeaderMap::new();

        let profile = classify(html, &headers);
        assert_eq!(profile, TechProfile::Unknown);
    }
}
