//! Shopify data extraction parser.
//!
//! Shopify exposes structured product data through two main mechanisms:
//!
//! 1. **`.json` endpoints**: Every Shopify product page has a corresponding
//!    JSON endpoint at `{url}.json` that returns the complete product data
//! 2. **Embedded JSON**: Some Shopify themes embed product data in script tags
//!
//! This parser prioritizes the `.json` endpoint as it's the most reliable
//! source of truth.
//!
//! # Example URLs
//!
//! - HTML: `https://store.myshopify.com/products/widget`
//! - JSON: `https://store.myshopify.com/products/widget.json`
//!
//! The JSON endpoint returns complete product information including:
//! - Title, description, vendor
//! - Variants, pricing, inventory
//! - Images, tags, options
//! - All metadata

use scraper::{Html, Selector};
use serde_json::Value;

/// Constructs a Shopify JSON endpoint URL from a product page URL.
///
/// # Examples
///
/// ```
/// # use rust_sitemap::parsing_modules::shopify_parser::build_json_url;
/// let url = "https://store.myshopify.com/products/cool-widget";
/// let json_url = build_json_url(url);
/// assert_eq!(json_url, Some("https://store.myshopify.com/products/cool-widget.json".to_string()));
/// ```
///
/// ```
/// # use rust_sitemap::parsing_modules::shopify_parser::build_json_url;
/// let url = "https://store.myshopify.com/collections/all";
/// let json_url = build_json_url(url);
/// assert!(json_url.is_none()); // Not a product page
/// ```
pub fn build_json_url(url: &str) -> Option<String> {
    // Only works for product pages
    if !url.contains("/products/") {
        return None;
    }

    // Remove any existing query parameters or fragments
    let clean_url = url.split('?').next()?.split('#').next()?;

    // Add .json extension
    Some(format!("{}.json", clean_url))
}

/// Extracts embedded Shopify product JSON from HTML.
///
/// Many Shopify themes embed product data in a script tag like:
/// ```html
/// <script type="application/json" data-product-json>
/// {"id": 123, "title": "Product", ...}
/// </script>
/// ```
///
/// Or in a meta tag:
/// ```html
/// <script type="application/ld+json">
/// {"@type": "Product", "name": "...", ...}
/// </script>
/// ```
///
/// This function attempts to extract that embedded data.
pub fn extract_embedded_product_json(html: &str) -> Option<String> {
    let document = Html::parse_document(html);

    // Try common Shopify product JSON selectors
    let selectors = [
        r#"script[type="application/json"][data-product-json]"#,
        r#"script[type="application/json"]#ProductJson"#,
        r#"script[type="application/json"].product-json"#,
    ];

    for selector_str in &selectors {
        if let Ok(selector) = Selector::parse(selector_str) {
            if let Some(element) = document.select(&selector).next() {
                let json_text = element.text().collect::<String>();
                // Validate it's actually JSON
                if serde_json::from_str::<Value>(&json_text).is_ok() {
                    return Some(json_text);
                }
            }
        }
    }

    None
}

/// Extracts the product ID from Shopify HTML.
///
/// This can be useful for constructing API URLs or tracking products.
/// The ID is often embedded in meta tags or data attributes.
pub fn extract_product_id(html: &str) -> Option<String> {
    let document = Html::parse_document(html);

    // Try meta tag first
    if let Ok(selector) = Selector::parse(r#"meta[property="product:id"]"#) {
        if let Some(element) = document.select(&selector).next() {
            if let Some(content) = element.value().attr("content") {
                return Some(content.to_string());
            }
        }
    }

    // Try data attribute on product form
    if let Ok(selector) = Selector::parse(r#"form[data-product-id]"#) {
        if let Some(element) = document.select(&selector).next() {
            if let Some(id) = element.value().attr("data-product-id") {
                return Some(id.to_string());
            }
        }
    }

    None
}

/// Information about a Shopify page for fetching structured data
#[derive(Debug, Clone)]
pub struct ShopifyPageInfo {
    /// The JSON endpoint URL if this is a product page
    pub json_url: Option<String>,
    /// The product ID if found
    pub product_id: Option<String>,
    /// Embedded JSON if found in the HTML
    pub embedded_json: Option<String>,
}

/// Analyzes a Shopify page and extracts all available structured data sources.
///
/// This provides multiple ways to access the data, ordered by reliability:
/// 1. JSON endpoint URL (most reliable, requires network request)
/// 2. Embedded JSON (fast, already in HTML)
/// 3. Product ID (can be used for API calls)
pub fn analyze_page(url: &str, html: &str) -> ShopifyPageInfo {
    ShopifyPageInfo {
        json_url: build_json_url(url),
        product_id: extract_product_id(html),
        embedded_json: extract_embedded_product_json(html),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_json_url_product_page() {
        let url = "https://example.myshopify.com/products/test-product";
        let json_url = build_json_url(url);
        assert_eq!(
            json_url,
            Some("https://example.myshopify.com/products/test-product.json".to_string())
        );
    }

    #[test]
    fn test_build_json_url_with_query() {
        let url = "https://example.myshopify.com/products/test-product?variant=123";
        let json_url = build_json_url(url);
        assert_eq!(
            json_url,
            Some("https://example.myshopify.com/products/test-product.json".to_string())
        );
    }

    #[test]
    fn test_build_json_url_not_product() {
        let url = "https://example.myshopify.com/collections/all";
        let json_url = build_json_url(url);
        assert!(json_url.is_none());
    }

    #[test]
    fn test_extract_embedded_json() {
        let html = r#"
            <html>
            <script type="application/json" data-product-json>
            {
                "id": 123456789,
                "title": "Cool Widget",
                "price": "29.99"
            }
            </script>
            </html>
        "#;

        let result = extract_embedded_product_json(html);
        assert!(result.is_some());

        let json = result.unwrap();
        assert!(json.contains("Cool Widget"));
        assert!(json.contains("123456789"));
    }

    #[test]
    fn test_extract_product_id_meta() {
        let html = r#"
            <html>
            <head>
                <meta property="product:id" content="987654321">
            </head>
            </html>
        "#;

        let result = extract_product_id(html);
        assert_eq!(result, Some("987654321".to_string()));
    }

    #[test]
    fn test_extract_product_id_form() {
        let html = r#"
            <html>
            <form data-product-id="111222333">
                <button>Add to Cart</button>
            </form>
            </html>
        "#;

        let result = extract_product_id(html);
        assert_eq!(result, Some("111222333".to_string()));
    }

    #[test]
    fn test_analyze_page() {
        let url = "https://store.myshopify.com/products/widget";
        let html = r#"
            <html>
            <meta property="product:id" content="999">
            <script type="application/json" data-product-json>
            {"id": 999, "title": "Widget"}
            </script>
            </html>
        "#;

        let info = analyze_page(url, html);
        assert!(info.json_url.is_some());
        assert_eq!(info.product_id, Some("999".to_string()));
        assert!(info.embedded_json.is_some());
    }
}
