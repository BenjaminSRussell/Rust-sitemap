//! Rich metadata extraction from HTML documents.
//!
//! This module extracts structured metadata from web pages using multiple standards:
//! - JSON-LD (Schema.org structured data)
//! - OpenGraph (Facebook/social media metadata)
//! - Twitter Cards
//! - Schema.org microdata
//! - Standard HTML meta tags
//! - Content extraction via readability algorithm

use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Comprehensive metadata extracted from a web page
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PageMetadata {
    /// Page title (from <title> tag)
    pub title: Option<String>,

    /// Meta description
    pub description: Option<String>,

    /// Canonical URL
    pub canonical_url: Option<String>,

    /// Author information
    pub author: Option<String>,

    /// Publication date (ISO 8601 format)
    pub date_published: Option<String>,

    /// Last modified date (ISO 8601 format)
    pub date_modified: Option<String>,

    /// OpenGraph metadata
    pub opengraph: OpenGraphMetadata,

    /// Twitter Card metadata
    pub twitter: TwitterCardMetadata,

    /// JSON-LD structured data (raw JSON objects)
    pub json_ld: Vec<serde_json::Value>,

    /// Schema.org microdata items
    pub microdata: Vec<MicrodataItem>,

    /// Main article content (extracted via readability)
    pub article_text: Option<String>,

    /// Article text length in characters
    pub article_text_length: Option<usize>,

    /// Language code (from html lang attribute or meta tags)
    pub language: Option<String>,

    /// Keywords from meta tags
    pub keywords: Vec<String>,

    /// Images found in the page
    pub images: Vec<ImageMetadata>,
}

/// OpenGraph metadata (og: tags)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenGraphMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub image: Option<String>,
    pub site_name: Option<String>,
    pub locale: Option<String>,
    pub og_type: Option<String>,
    pub video: Option<String>,
    pub audio: Option<String>,
    /// Additional OpenGraph properties not captured above
    pub extra: HashMap<String, String>,
}

/// Twitter Card metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TwitterCardMetadata {
    pub card: Option<String>,
    pub site: Option<String>,
    pub creator: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    /// Additional Twitter properties
    pub extra: HashMap<String, String>,
}

/// Schema.org microdata item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrodataItem {
    pub item_type: Option<String>,
    pub properties: HashMap<String, Vec<String>>,
}

/// Image metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub src: String,
    pub alt: Option<String>,
    pub width: Option<String>,
    pub height: Option<String>,
}

impl PageMetadata {
    /// Extract all metadata from an HTML document
    pub fn extract(html: &str, page_url: &str) -> Self {
        let document = Html::parse_document(html);

        let mut metadata = PageMetadata::default();

        // Extract basic metadata
        metadata.title = Self::extract_title(&document);
        metadata.description = Self::extract_meta_content(&document, "name", "description");
        metadata.canonical_url = Self::extract_canonical(&document);
        metadata.author = Self::extract_author(&document);
        metadata.language = Self::extract_language(&document);
        metadata.keywords = Self::extract_keywords(&document);

        // Extract social metadata
        metadata.opengraph = Self::extract_opengraph(&document);
        metadata.twitter = Self::extract_twitter(&document);

        // Extract structured data
        metadata.json_ld = Self::extract_json_ld(&document);
        metadata.microdata = Self::extract_microdata(&document);

        // Extract article content using readability
        if let Some((text, text_len)) = Self::extract_article_content(html, page_url) {
            metadata.article_text = Some(text);
            metadata.article_text_length = Some(text_len);
        }

        // Extract images
        metadata.images = Self::extract_images(&document);

        // Extract dates from JSON-LD if available
        if !metadata.json_ld.is_empty() {
            if let Some(date) = Self::extract_date_from_json_ld(&metadata.json_ld, "datePublished") {
                metadata.date_published = Some(date);
            }
            if let Some(date) = Self::extract_date_from_json_ld(&metadata.json_ld, "dateModified") {
                metadata.date_modified = Some(date);
            }
        }

        metadata
    }

    fn extract_title(document: &Html) -> Option<String> {
        let selector = Selector::parse("title").unwrap();
        document
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn extract_canonical(document: &Html) -> Option<String> {
        let selector = Selector::parse("link[rel='canonical']").ok()?;
        document
            .select(&selector)
            .next()
            .and_then(|el| el.value().attr("href"))
            .map(|s| s.to_string())
    }

    fn extract_author(document: &Html) -> Option<String> {
        // Try multiple author meta tags
        Self::extract_meta_content(document, "name", "author")
            .or_else(|| Self::extract_meta_content(document, "property", "article:author"))
            .or_else(|| Self::extract_meta_content(document, "name", "article:author"))
    }

    fn extract_language(document: &Html) -> Option<String> {
        // Try html lang attribute first
        let html_selector = Selector::parse("html").unwrap();
        if let Some(lang) = document
            .select(&html_selector)
            .next()
            .and_then(|el| el.value().attr("lang"))
        {
            return Some(lang.to_string());
        }

        // Fall back to meta tags
        Self::extract_meta_content(document, "http-equiv", "content-language")
            .or_else(|| Self::extract_meta_content(document, "name", "language"))
    }

    fn extract_keywords(document: &Html) -> Vec<String> {
        Self::extract_meta_content(document, "name", "keywords")
            .map(|keywords| {
                keywords
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn extract_meta_content(document: &Html, attr_name: &str, attr_value: &str) -> Option<String> {
        let selector_str = format!("meta[{}='{}']", attr_name, attr_value);
        let selector = Selector::parse(&selector_str).ok()?;
        document
            .select(&selector)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
    }

    fn extract_opengraph(document: &Html) -> OpenGraphMetadata {
        let mut og = OpenGraphMetadata::default();

        // Parse all og: properties
        let selector = Selector::parse("meta[property^='og:']").unwrap();
        for element in document.select(&selector) {
            if let (Some(property), Some(content)) = (
                element.value().attr("property"),
                element.value().attr("content"),
            ) {
                let key = property.strip_prefix("og:").unwrap_or(property);
                let content = content.to_string();

                match key {
                    "title" => og.title = Some(content),
                    "description" => og.description = Some(content),
                    "url" => og.url = Some(content),
                    "image" => og.image = Some(content),
                    "site_name" => og.site_name = Some(content),
                    "locale" => og.locale = Some(content),
                    "type" => og.og_type = Some(content),
                    "video" => og.video = Some(content),
                    "audio" => og.audio = Some(content),
                    _ => {
                        og.extra.insert(key.to_string(), content);
                    }
                }
            }
        }

        og
    }

    fn extract_twitter(document: &Html) -> TwitterCardMetadata {
        let mut twitter = TwitterCardMetadata::default();

        // Parse all twitter: properties
        let selector = Selector::parse("meta[name^='twitter:']").unwrap();
        for element in document.select(&selector) {
            if let (Some(name), Some(content)) = (
                element.value().attr("name"),
                element.value().attr("content"),
            ) {
                let key = name.strip_prefix("twitter:").unwrap_or(name);
                let content = content.to_string();

                match key {
                    "card" => twitter.card = Some(content),
                    "site" => twitter.site = Some(content),
                    "creator" => twitter.creator = Some(content),
                    "title" => twitter.title = Some(content),
                    "description" => twitter.description = Some(content),
                    "image" => twitter.image = Some(content),
                    _ => {
                        twitter.extra.insert(key.to_string(), content);
                    }
                }
            }
        }

        twitter
    }

    fn extract_json_ld(document: &Html) -> Vec<serde_json::Value> {
        let mut json_ld_items = Vec::new();

        let selector = Selector::parse("script[type='application/ld+json']").unwrap();
        for element in document.select(&selector) {
            let text = element.text().collect::<String>();

            // Try to parse as JSON
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                // Handle both single objects and arrays of objects
                match value {
                    serde_json::Value::Array(arr) => json_ld_items.extend(arr),
                    obj => json_ld_items.push(obj),
                }
            }
        }

        json_ld_items
    }

    fn extract_microdata(document: &Html) -> Vec<MicrodataItem> {
        let mut items = Vec::new();

        let selector = Selector::parse("[itemscope]").unwrap();
        for element in document.select(&selector) {
            let item_type = element.value().attr("itemtype").map(|s| s.to_string());
            let mut properties: HashMap<String, Vec<String>> = HashMap::new();

            // Find all itemprop descendants (but not nested itemscope)
            let prop_selector = Selector::parse("[itemprop]").unwrap();
            for prop_element in element.select(&prop_selector) {
                // Skip if this belongs to a nested itemscope
                if prop_element.value().attr("itemscope").is_some() {
                    continue;
                }

                if let Some(prop_name) = prop_element.value().attr("itemprop") {
                    let value = prop_element
                        .value()
                        .attr("content")
                        .or_else(|| prop_element.value().attr("href"))
                        .or_else(|| prop_element.value().attr("src"))
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| prop_element.text().collect::<String>().trim().to_string());

                    properties
                        .entry(prop_name.to_string())
                        .or_default()
                        .push(value);
                }
            }

            if !properties.is_empty() || item_type.is_some() {
                items.push(MicrodataItem {
                    item_type,
                    properties,
                });
            }
        }

        items
    }

    fn extract_article_content(html: &str, page_url: &str) -> Option<(String, usize)> {
        // Use readability to extract main content
        match readability::extractor::extract(
            &mut html.as_bytes(),
            &url::Url::parse(page_url).ok()?,
        ) {
            Ok(product) => {
                let text = product.text;
                let text_len = text.len();

                // Only return if we extracted meaningful content (> 100 chars)
                if text_len > 100 {
                    Some((text, text_len))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    fn extract_images(document: &Html) -> Vec<ImageMetadata> {
        let mut images = Vec::new();

        let selector = Selector::parse("img[src]").unwrap();
        for element in document.select(&selector) {
            if let Some(src) = element.value().attr("src") {
                images.push(ImageMetadata {
                    src: src.to_string(),
                    alt: element.value().attr("alt").map(|s| s.to_string()),
                    width: element.value().attr("width").map(|s| s.to_string()),
                    height: element.value().attr("height").map(|s| s.to_string()),
                });
            }
        }

        // Limit to first 20 images to avoid bloat
        images.truncate(20);
        images
    }

    fn extract_date_from_json_ld(json_ld: &[serde_json::Value], field_name: &str) -> Option<String> {
        for item in json_ld {
            if let Some(date) = item.get(field_name).and_then(|v| v.as_str()) {
                return Some(date.to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_opengraph() {
        let html = r#"
            <html>
            <head>
                <meta property="og:title" content="Test Title">
                <meta property="og:description" content="Test Description">
                <meta property="og:image" content="https://example.com/image.jpg">
            </head>
            </html>
        "#;

        let document = Html::parse_document(html);
        let og = PageMetadata::extract_opengraph(&document);

        assert_eq!(og.title, Some("Test Title".to_string()));
        assert_eq!(og.description, Some("Test Description".to_string()));
        assert_eq!(og.image, Some("https://example.com/image.jpg".to_string()));
    }

    #[test]
    fn test_extract_json_ld() {
        let html = r#"
            <html>
            <head>
                <script type="application/ld+json">
                {
                    "@context": "https://schema.org",
                    "@type": "Article",
                    "headline": "Test Article",
                    "datePublished": "2024-01-01"
                }
                </script>
            </head>
            </html>
        "#;

        let document = Html::parse_document(html);
        let json_ld = PageMetadata::extract_json_ld(&document);

        assert_eq!(json_ld.len(), 1);
        assert_eq!(json_ld[0]["headline"], "Test Article");
    }

    #[test]
    fn test_extract_twitter_cards() {
        let html = r#"
            <html>
            <head>
                <meta name="twitter:card" content="summary_large_image">
                <meta name="twitter:title" content="Tweet Title">
                <meta name="twitter:creator" content="@username">
            </head>
            </html>
        "#;

        let document = Html::parse_document(html);
        let twitter = PageMetadata::extract_twitter(&document);

        assert_eq!(twitter.card, Some("summary_large_image".to_string()));
        assert_eq!(twitter.title, Some("Tweet Title".to_string()));
        assert_eq!(twitter.creator, Some("@username".to_string()));
    }
}
