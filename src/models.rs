use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

/// page node representing a crawled document
/// stores crawl metadata and content for a page
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct PageNode {
    /// url of the page
    pub url: String,

    /// http status code returned for the fetch
    pub status_code: u16,

    /// page title from the <title> tag
    pub title: Option<String>,

    /// html body of the page
    pub content: String,

    /// unix timestamp recording when the page was crawled
    pub crawled_at: u64,

    /// outbound links discovered on this page
    pub links: Vec<String>,

    /// metadata extracted from the page
    pub metadata: HashMap<String, String>,

    /// crawl depth from the starting url
    pub depth: u32,

    /// parent url that referenced this page
    pub parent_url: Option<String>,

    /// byte size of the page content
    pub content_size: u64,

    /// declared content type
    pub content_type: Option<String>,

    /// whether post processing is done
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
    /// create a new page node with the given url and depth
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

    /// set crawled timestamp to now
    pub fn set_crawled_now(&mut self) {
        self.crawled_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// append a link to this page
    pub fn add_link(&mut self, link: String) {
        self.links.push(link);
    }

    /// insert metadata entry
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// set the content and update size
    pub fn set_content(&mut self, content: String) {
        self.content_size = content.len() as u64;
        self.content = content;
    }

    /// mark this page as processed
    pub fn mark_processed(&mut self) {
        self.processed = true;
    }

    /// set content fields after fetching
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
