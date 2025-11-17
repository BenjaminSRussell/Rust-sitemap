//! Next.js data extraction parser.
//!
//! Next.js embeds all page props and data in a `<script id="__NEXT_DATA__">` tag
//! for client-side hydration. This parser extracts that JSON directly, bypassing
//! the need to parse the rendered HTML.
//!
//! # Why this works
//!
//! Next.js pages follow this pattern:
//! ```html
//! <script id="__NEXT_DATA__" type="application/json">
//! {
//!   "props": {
//!     "pageProps": { ... your data here ... },
//!     ...
//!   },
//!   ...
//! }
//! </script>
//! ```
//!
//! This is the **source of truth** - the exact data the React components use.
//! It's much more reliable than CSS selectors and never breaks with layout changes.

use scraper::{Html, Selector};
use serde_json::Value;

/// Extracts the `__NEXT_DATA__` JSON from a Next.js page.
///
/// # Returns
///
/// - `Some(json_string)` if the `__NEXT_DATA__` script is found and contains valid JSON
/// - `None` if the script is not found or the JSON is invalid
///
/// # Example
///
/// ```ignore
/// let html = r#"<script id="__NEXT_DATA__">{"props":{"pageProps":{...}}}</script>"#;
/// let data = extract_next_data(html);
/// assert!(data.is_some());
/// ```
pub fn extract_next_data(html: &str) -> Option<String> {
    let document = Html::parse_document(html);

    // Parse the selector for the __NEXT_DATA__ script tag
    let selector = Selector::parse(r#"script[id="__NEXT_DATA__"]"#).ok()?;

    // Find the first matching element
    let element = document.select(&selector).next()?;

    // Extract the text content (the JSON)
    let json_text = element.text().collect::<String>();

    // Validate that it's actually JSON before returning
    // This prevents returning garbage if the script tag is malformed
    serde_json::from_str::<Value>(&json_text).ok()?;

    Some(json_text)
}

/// Extracts the `pageProps` object from `__NEXT_DATA__`.
///
/// This is often where the most useful data lives in Next.js applications.
///
/// # Returns
///
/// - `Some(json_string)` containing just the pageProps object
/// - `None` if extraction fails or pageProps doesn't exist
pub fn extract_page_props(html: &str) -> Option<String> {
    let next_data = extract_next_data(html)?;
    let parsed: Value = serde_json::from_str(&next_data).ok()?;

    // Navigate to props.pageProps
    let page_props = parsed
        .get("props")?
        .get("pageProps")?;

    serde_json::to_string(page_props).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_next_data_valid() {
        let html = r#"
            <html>
            <head>
                <script id="__NEXT_DATA__" type="application/json">
                {
                    "props": {
                        "pageProps": {
                            "product": {
                                "name": "Test Product",
                                "price": 29.99
                            }
                        }
                    }
                }
                </script>
            </head>
            </html>
        "#;

        let result = extract_next_data(html);
        assert!(result.is_some());

        let json = result.unwrap();
        assert!(json.contains("pageProps"));
        assert!(json.contains("product"));
    }

    #[test]
    fn test_extract_next_data_missing() {
        let html = r#"<html><body>No Next.js data here</body></html>"#;
        let result = extract_next_data(html);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_next_data_invalid_json() {
        let html = r#"
            <html>
            <script id="__NEXT_DATA__">not valid json</script>
            </html>
        "#;

        let result = extract_next_data(html);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_page_props() {
        let html = r#"
            <html>
            <script id="__NEXT_DATA__" type="application/json">
            {
                "props": {
                    "pageProps": {
                        "title": "Test Page",
                        "data": [1, 2, 3]
                    }
                }
            }
            </script>
            </html>
        "#;

        let result = extract_page_props(html);
        assert!(result.is_some());

        let json = result.unwrap();
        assert!(json.contains("Test Page"));
        assert!(!json.contains("props")); // Should only have pageProps content
    }
}
