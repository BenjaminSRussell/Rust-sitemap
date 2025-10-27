use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

/// page node
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct PageNode {
    /// url
    pub url: String,

    /// status code
    pub status_code: u16,

    /// page title
    pub title: Option<String>,

    /// page html
    pub content: String,

    /// crawl timestamp
    pub crawled_at: u64,

    /// outbound links
    pub links: Vec<String>,

    /// metadata
    pub metadata: HashMap<String, String>,

    /// crawl depth
    pub depth: u32,

    /// parent url
    pub parent_url: Option<String>,

    /// content bytes
    pub content_size: u64,

    /// content type
    pub content_type: Option<String>,

    /// processed flag
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
    /// new page node
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

    /// set crawl time
    pub fn set_crawled_now(&mut self) {
        self.crawled_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// add link
    pub fn add_link(&mut self, link: String) {
        self.links.push(link);
    }

    /// add metadata
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// set content
    pub fn set_content(&mut self, content: String) {
        self.content_size = content.len() as u64;
        self.content = content;
    }

    /// mark processed
    pub fn mark_processed(&mut self) {
        self.processed = true;
    }

    /// set fetch data
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
