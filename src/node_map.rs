use bloomfilter::Bloom;
use dashmap::DashMap;
use parking_lot::RwLock;
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Error, Debug)]
pub enum NodeMapError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Parameters for updating crawl data
pub struct CrawlData {
    pub status_code: u16,
    pub content_type: Option<String>,
    pub content_length: Option<usize>,
    pub title: Option<String>,
    pub links: Vec<String>,
    pub response_time_ms: Option<u64>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
pub struct SitemapNode {
    pub url: String,
    pub url_normalized: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub fragments: Vec<String>,
    pub discovered_at: u64,
    pub queued_at: u64,
    pub crawled_at: Option<u64>,
    pub response_time_ms: Option<u64>,
    pub status_code: Option<u16>,
    pub content_type: Option<String>,
    pub content_length: Option<usize>,
    pub title: Option<String>,
    pub links: Vec<String>,
}

impl SitemapNode {
    pub fn new(url: String, depth: u32, parent_url: Option<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let url_normalized = Self::normalize_url(&url);
        let fragment = Self::extract_fragment(&url);
        let fragments = if let Some(frag) = fragment {
            vec![frag]
        } else {
            Vec::new()
        };

        let parent_url_normalized = parent_url.map(|p| Self::normalize_url(&p));

        Self {
            url,
            url_normalized,
            depth,
            parent_url: parent_url_normalized,
            fragments,
            discovered_at: now,
            queued_at: now,
            crawled_at: None,
            response_time_ms: None,
            status_code: None,
            content_type: None,
            content_length: None,
            title: None,
            links: Vec::new(),
        }
    }

    fn normalize_url(url: &str) -> String {
        if let Ok(mut parsed) = url::Url::parse(url) {
            parsed.set_fragment(None);
            parsed.to_string()
        } else {
            url.to_string()
        }
    }

    fn extract_fragment(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.fragment().map(|f| format!("#{}", f)))
    }

    pub fn set_crawled_data(
        &mut self,
        status_code: u16,
        content_type: Option<String>,
        content_length: Option<usize>,
        title: Option<String>,
        links: Vec<String>,
        response_time_ms: Option<u64>,
    ) {
        self.status_code = Some(status_code);
        self.content_type = content_type;
        self.content_length = content_length;
        self.title = title;
        self.links = links;
        self.response_time_ms = response_time_ms;

        self.crawled_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
    }

    pub fn add_fragment(&mut self, fragment: String) {
        if !self.fragments.contains(&fragment) {
            self.fragments.push(fragment);
        }
    }
}

pub struct NodeMap {
    nodes: DashMap<String, SitemapNode>,
    bloom: RwLock<Bloom<String>>,
    dedup_db: Option<sled::Db>,
    node_map_file: std::path::PathBuf,
    total_nodes: AtomicUsize,
}

impl NodeMap {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_memory_nodes: usize) -> Result<Self, NodeMapError> {
        let data_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;

        let node_map_file = data_path.join("node_map.rkyv");
        let dedup_db_path = data_path.join("nodemap_dedup");
        let dedup_db = sled::open(&dedup_db_path).ok();
        let bloom = RwLock::new(Bloom::new_for_fp_rate(10_000_000, 0.0001));

        let map = Self {
            nodes: DashMap::with_capacity(max_memory_nodes),
            bloom,
            dedup_db,
            node_map_file,
            total_nodes: AtomicUsize::new(0),
        };

        Ok(map)
    }

    pub fn add_url(
        &self,
        url: String,
        depth: u32,
        parent_url: Option<String>,
    ) -> Result<bool, NodeMapError> {
        let url_normalized = SitemapNode::normalize_url(&url);
        let fragment = SitemapNode::extract_fragment(&url);

        if self.bloom.read().check(&url_normalized) {
            if let Some(ref db) = self.dedup_db {
                if db.contains_key(url_normalized.as_bytes()).unwrap_or(false) {
                    if let Some(frag) = fragment {
                        if let Some(mut node) = self.nodes.get_mut(&url_normalized) {
                            node.add_fragment(frag);
                        }
                    }
                    return Ok(false);
                }
            } else {
                if let Some(frag) = fragment {
                    if let Some(mut node) = self.nodes.get_mut(&url_normalized) {
                        node.add_fragment(frag);
                    }
                }
                return Ok(false);
            }
        }

        if let Some(ref db) = self.dedup_db {
            let _ = db.insert(url_normalized.as_bytes(), &[]);
        }

        self.bloom.write().set(&url_normalized);

        let node = SitemapNode::new(url.clone(), depth, parent_url);
        self.nodes.insert(url_normalized, node);
        self.total_nodes.fetch_add(1, Ordering::Relaxed);

        Ok(true)
    }

    pub fn update_node(&self, url: &str, crawl_data: CrawlData) -> Result<(), NodeMapError> {
        let url_normalized = SitemapNode::normalize_url(url);

        if let Some(mut node) = self.nodes.get_mut(&url_normalized) {
            node.set_crawled_data(
                crawl_data.status_code,
                crawl_data.content_type,
                crawl_data.content_length,
                crawl_data.title,
                crawl_data.links,
                crawl_data.response_time_ms,
            );
        }
        Ok(())
    }

    pub fn contains(&self, url: &str) -> bool {
        let url_normalized = SitemapNode::normalize_url(url);

        if !self.bloom.read().check(&url_normalized) {
            return false;
        }

        if let Some(ref db) = self.dedup_db {
            return db.contains_key(url_normalized.as_bytes()).unwrap_or(false);
        }

        true
    }
    pub fn stats(&self) -> NodeMapStats {
        let unique_urls = if let Some(ref db) = self.dedup_db {
            db.len()
        } else {
            self.total_nodes.load(Ordering::Relaxed)
        };

        NodeMapStats {
            total_nodes: self.total_nodes.load(Ordering::Relaxed),
            memory_nodes: self.nodes.len(),
            unique_urls,
        }
    }

    async fn flush_to_disk(&self) -> Result<(), NodeMapError> {
        if self.nodes.is_empty() {
            return Ok(());
        }

        let temp_file = self.node_map_file.with_extension("tmp");

        let result = async {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_file)
                .await?;

            let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);

            for entry in self.nodes.iter() {
                let node = entry.value();
                if let Ok(bytes) = rkyv::to_bytes::<_, 2048>(node) {
                    let len_bytes = (bytes.len() as u32).to_le_bytes();
                    writer.write_all(&len_bytes).await?;
                    writer.write_all(&bytes).await?;
                }
            }

            writer.flush().await?;
            drop(writer);

            tokio::fs::rename(&temp_file, &self.node_map_file).await?;
            Ok(())
        }
        .await;

        if result.is_err() && temp_file.exists() {
            let _ = tokio::fs::remove_file(&temp_file).await;
        }

        result
    }

    pub async fn force_flush(&self) -> Result<(), NodeMapError> {
        self.flush_to_disk().await?;
        Ok(())
    }

    pub fn export_to_jsonl<P: AsRef<Path>>(&self, output_path: P) -> Result<(), NodeMapError> {
        let all_nodes = self.read_all_nodes_from_disk()?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?;

        for node in all_nodes {
            let json_line = serde_json::to_string(&node).map_err(|e| {
                NodeMapError::Serialization(format!("Failed to serialize to JSON: {}", e))
            })?;
            writeln!(file, "{}", json_line)?;
        }

        file.flush()?;
        Ok(())
    }

    pub fn get_all_nodes(&self) -> Result<Vec<SitemapNode>, NodeMapError> {
        self.read_all_nodes_from_disk()
    }

    fn read_all_nodes_from_disk(&self) -> Result<Vec<SitemapNode>, NodeMapError> {
        let mut nodes = Vec::new();

        for entry in self.nodes.iter() {
            nodes.push(entry.value().clone());
        }

        if !self.node_map_file.exists() {
            return Ok(nodes);
        }

        let mut file = OpenOptions::new().read(true).open(&self.node_map_file)?;

        loop {
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;

                    let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&item_bytes) }
                        .map_err(|e| {
                            NodeMapError::Serialization(format!("Failed to deserialize: {}", e))
                        })?;

                    nodes.push(node);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(nodes)
    }
}

#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub struct NodeMapStats {
    pub total_nodes: usize,
    pub memory_nodes: usize,
    pub unique_urls: usize,
}

impl std::fmt::Display for NodeMapStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeMap: {} total, {} in memory, {} unique",
            self.total_nodes, self.memory_nodes, self.unique_urls
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_node_map_creation() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();
        assert_eq!(map.nodes.len(), 0);
    }

    #[test]
    fn test_add_url() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        let added = map
            .add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        assert!(added);

        let added_again = map
            .add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        assert!(!added_again);
    }

    #[test]
    fn test_contains() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        map.add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        assert!(map.contains("https://test.local"));
        assert!(!map.contains("https://other.local"));
    }

    #[test]
    fn test_update_node() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        map.add_url("https://test.local".to_string(), 0, None)
            .unwrap();

        let links = vec!["https://test.local/page1".to_string()];
        map.update_node(
            "https://test.local",
            CrawlData {
                status_code: 200,
                content_type: Some("text/html".to_string()),
                content_length: Some(1024),
                title: Some("Test Page".to_string()),
                links,
                response_time_ms: Some(250),
            },
        )
        .unwrap();

        // Use normalized URL for lookup
        let url_normalized = SitemapNode::normalize_url("https://test.local");
        let node = map.nodes.get(&url_normalized).unwrap();
        assert_eq!(node.status_code, Some(200));
        assert_eq!(node.title, Some("Test Page".to_string()));
        assert_eq!(node.content_length, Some(1024));
        assert_eq!(node.response_time_ms, Some(250));
    }

    #[test]
    fn test_stats() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        for i in 0..5 {
            map.add_url(format!("https://test.local/{}", i), i, None)
                .unwrap();
        }

        let stats = map.stats();
        assert_eq!(stats.total_nodes, 5);
        assert_eq!(stats.memory_nodes, 5);
    }

    #[tokio::test]
    async fn test_force_flush() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        map.add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        map.force_flush().await.unwrap();

        assert!(dir.path().join("node_map.rkyv").exists());
    }

    #[test]
    fn test_export_to_jsonl() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        map.add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        map.update_node(
            "https://test.local",
            CrawlData {
                status_code: 200,
                content_type: Some("text/html".to_string()),
                content_length: Some(512),
                title: Some("Test".to_string()),
                links: vec![],
                response_time_ms: Some(100),
            },
        )
        .unwrap();

        let output = dir.path().join("output.jsonl");
        map.export_to_jsonl(&output).unwrap();

        assert!(output.exists());
        let content = std::fs::read_to_string(&output).unwrap();
        assert!(content.contains("https://test.local"));
    }

    #[test]
    fn test_multiple_urls_different_depths() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        map.add_url("https://test.local".to_string(), 0, None)
            .unwrap();
        map.add_url(
            "https://test.local/page1".to_string(),
            1,
            Some("https://test.local".to_string()),
        )
        .unwrap();
        map.add_url(
            "https://test.local/page2".to_string(),
            1,
            Some("https://test.local".to_string()),
        )
        .unwrap();

        let stats = map.stats();
        assert_eq!(stats.total_nodes, 3);
    }

    #[test]
    fn test_sitemap_node_structure() {
        let node = SitemapNode {
            url: "https://test.local".to_string(),
            url_normalized: "https://test.local".to_string(),
            parent_url: None,
            depth: 0,
            fragments: vec![],
            discovered_at: 0,
            queued_at: 0,
            crawled_at: Some(100),
            response_time_ms: Some(250),
            status_code: Some(200),
            title: Some("Test".to_string()),
            content_type: Some("text/html".to_string()),
            content_length: Some(1024),
            links: vec![],
        };

        assert_eq!(node.url, "https://test.local");
        assert_eq!(node.url_normalized, "https://test.local");
        assert_eq!(node.depth, 0);
        assert_eq!(node.status_code, Some(200));
    }

    #[test]
    fn test_url_fragment_normalization() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        // add fragment url
        let added1 = map
            .add_url("https://test.local/page#section1".to_string(), 0, None)
            .unwrap();
        assert!(added1);

        // add second fragment
        let added2 = map
            .add_url("https://test.local/page#section2".to_string(), 0, None)
            .unwrap();
        assert!(!added2);

        // fragments recorded
        let node = map.nodes.get("https://test.local/page").unwrap();
        assert_eq!(node.fragments.len(), 2);
        assert!(node.fragments.contains(&"#section1".to_string()));
        assert!(node.fragments.contains(&"#section2".to_string()));
    }

    #[test]
    fn test_parent_url_normalization() {
        let dir = TempDir::new().unwrap();
        let map = NodeMap::new(dir.path(), 100).unwrap();

        // add child with fragment parent
        map.add_url(
            "https://test.local/child".to_string(),
            1,
            Some("https://test.local/parent#section".to_string()),
        )
        .unwrap();

        // parent normalized
        let node = map.nodes.get("https://test.local/child").unwrap();
        assert_eq!(
            node.parent_url,
            Some("https://test.local/parent".to_string())
        );
    }
}
