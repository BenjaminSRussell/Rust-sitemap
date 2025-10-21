use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a page node in the web crawler's data structure
/// This struct contains all the information about a crawled web page
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct PageNode {
    /// The URL of the page
    pub url: String,

    /// HTTP status code returned when accessing the page
    pub status_code: u16,

    /// Title of the page (from <title> tag)
    pub title: Option<String>,

    /// Raw HTML content of the page
    pub content: String,

    /// Timestamp when the page was crawled (Unix timestamp)
    pub crawled_at: u64,

    /// Links found on this page (URLs)
    pub links: Vec<String>,

    /// Metadata extracted from the page (meta tags, etc.)
    pub metadata: HashMap<String, String>,

    /// Depth level from the starting URL
    pub depth: u32,

    /// Parent URL that led to this page
    pub parent_url: Option<String>,

    /// Size of the page content in bytes
    pub content_size: u64,

    /// Content type of the page (e.g., "text/html")
    pub content_type: Option<String>,

    /// Whether this page has been processed
    pub processed: bool,
}

impl PartialEq for PageNode {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.status_code == other.status_code
            && self.title == other.title
            && self.content == other.content
            && self.crawled_at == other.crawled_at
            && self.links == other.links
            && self.metadata == other.metadata
            && self.depth == other.depth
            && self.parent_url == other.parent_url
            && self.content_size == other.content_size
            && self.content_type == other.content_type
            && self.processed == other.processed
    }
}

impl PageNode {
    /// Create a new PageNode with the given URL and depth
    pub fn new(url: String, depth: u32) -> Self {
        Self {
            url,
            status_code: 0,
            title: None,
            content: String::new(),
            crawled_at: 0,
            links: Vec::new(),
            metadata: HashMap::new(),
            depth,
            parent_url: None,
            content_size: 0,
            content_type: None,
            processed: false,
        }
    }

    /// Set the crawled timestamp to current time
    pub fn set_crawled_now(&mut self) {
        self.crawled_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Add a link to this page's link list
    pub fn add_link(&mut self, link: String) {
        self.links.push(link);
    }

    /// Add metadata to this page
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Set the content and update the content size
    pub fn set_content(&mut self, content: String) {
        self.content_size = content.len() as u64;
        self.content = content;
    }

    /// Mark this page as processed
    pub fn mark_processed(&mut self) {
        self.processed = true;
    }

    /// Set the content and metadata for this page
    pub fn set_content_and_metadata(
        &mut self,
        content: String,
        status_code: u16,
        content_type: Option<String>,
    ) {
        self.set_content(content);
        self.status_code = status_code;
        self.content_type = content_type;
        self.set_crawled_now();
    }
}
