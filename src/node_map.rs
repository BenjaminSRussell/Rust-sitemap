use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::fs::OpenOptions;
use std::io::{Write, Read};
use rkyv::{Archive, Serialize, Deserialize};
use serde::{Serialize as SerdeSerialize, Deserialize as SerdeDeserialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeMapError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Represents a node in the sitemap with all its relationships and metadata
#[derive(Debug, Clone, Archive, Serialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
pub struct SitemapNode {
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub discovered_at: u64,
    pub status_code: Option<u16>,
    pub content_type: Option<String>,
    pub title: Option<String>,
    pub links: Vec<String>,
}

impl SitemapNode {
    pub fn new(url: String, depth: u32, parent_url: Option<String>) -> Self {
        Self {
            url,
            depth,
            parent_url,
            discovered_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            status_code: None,
            content_type: None,
            title: None,
            links: Vec::new(),
        }
    }

    pub fn set_crawled_data(&mut self, status_code: u16, content_type: Option<String>, title: Option<String>, links: Vec<String>) {
        self.status_code = Some(status_code);
        self.content_type = content_type;
        self.title = title;
        self.links = links;
    }
}

/// Persistent node map that stores all discovered URLs and their relationships
/// Uses rkyv for efficient disk storage and memory management
#[derive(Debug)]
pub struct NodeMap {
    // In-memory map for fast lookups (URL -> Node)
    nodes: HashMap<String, SitemapNode>,
    // Track all seen URLs for duplicate prevention
    seen_urls: HashSet<String>,
    // Persistence
    node_map_file: std::path::PathBuf,
    // Memory management
    max_memory_nodes: usize,
    // Statistics
    total_nodes: usize,
}

impl NodeMap {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_memory_nodes: usize) -> Result<Self, NodeMapError> {
        let data_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;
        
        let node_map_file = data_path.join("node_map.rkyv");
        
        let mut map = Self {
            nodes: HashMap::new(),
            seen_urls: HashSet::new(),
            node_map_file,
            max_memory_nodes,
            total_nodes: 0,
        };
        
        // Try to load existing map
        map.load_from_disk()?;
        
        Ok(map)
    }

    /// Add a new URL to the node map (returns false if already exists)
    pub fn add_url(&mut self, url: String, depth: u32, parent_url: Option<String>) -> Result<bool, NodeMapError> {
        // Check for duplicates
        if self.seen_urls.contains(&url) {
            return Ok(false);
        }
        
        // If at capacity, flush to disk
        if self.nodes.len() >= self.max_memory_nodes {
            self.flush_to_disk()?;
            // Keep seen_urls in memory but clear nodes to save RAM
            self.nodes.clear();
        }
        
        let node = SitemapNode::new(url.clone(), depth, parent_url);
        self.nodes.insert(url.clone(), node);
        self.seen_urls.insert(url);
        self.total_nodes += 1;
        
        Ok(true)
    }

    /// Update a node with crawled data
    pub fn update_node(&mut self, url: &str, status_code: u16, content_type: Option<String>, title: Option<String>, links: Vec<String>) -> Result<(), NodeMapError> {
        if let Some(node) = self.nodes.get_mut(url) {
            node.set_crawled_data(status_code, content_type, title, links);
        } else {
            // Node may have been flushed to disk, load it back
            self.load_from_disk()?;
            if let Some(node) = self.nodes.get_mut(url) {
                node.set_crawled_data(status_code, content_type, title, links);
            }
        }
        Ok(())
    }

    /// Check if a URL has been seen
    pub fn contains(&self, url: &str) -> bool {
        self.seen_urls.contains(url)
    }

    /// Get total number of nodes
    pub fn len(&self) -> usize {
        self.total_nodes
    }

    /// Get statistics
    pub fn stats(&self) -> NodeMapStats {
        NodeMapStats {
            total_nodes: self.total_nodes,
            memory_nodes: self.nodes.len(),
            unique_urls: self.seen_urls.len(),
        }
    }

    /// Flush current nodes to disk
    fn flush_to_disk(&self) -> Result<(), NodeMapError> {
        if self.nodes.is_empty() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.node_map_file)?;
        
        let mut writer = std::io::BufWriter::new(file);
        
        // Serialize each node and append to file
        for node in self.nodes.values() {
            let bytes = rkyv::to_bytes::<_, 2048>(node)
                .map_err(|e| NodeMapError::Serialization(format!("Failed to serialize: {}", e)))?;
            // Write length prefix followed by data
            let len_bytes = (bytes.len() as u32).to_le_bytes();
            writer.write_all(&len_bytes)?;
            writer.write_all(&bytes)?;
        }
        writer.flush()?;
        
        Ok(())
    }

    /// Load nodes from disk (only loads seen URLs for duplicate prevention)
    fn load_from_disk(&mut self) -> Result<(), NodeMapError> {
        if !self.node_map_file.exists() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.node_map_file)?;
        
        // Read nodes and populate seen_urls set
        loop {
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    
                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;
                    
                    let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&item_bytes) }
                        .map_err(|e| NodeMapError::Serialization(format!("Failed to deserialize: {}", e)))?;
                    
                    self.seen_urls.insert(node.url.clone());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        
        Ok(())
    }

    /// Force flush all pending nodes to disk
    pub fn force_flush(&mut self) -> Result<(), NodeMapError> {
        self.flush_to_disk()?;
        Ok(())
    }

    /// Export all nodes to JSONL format
    pub fn export_to_jsonl<P: AsRef<Path>>(&self, output_path: P) -> Result<(), NodeMapError> {
        // First, read all nodes from disk
        let all_nodes = self.read_all_nodes_from_disk()?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?;
        
        // Write each node as a JSON line
        for node in all_nodes {
            let json_line = serde_json::to_string(&node)
                .map_err(|e| NodeMapError::Serialization(format!("Failed to serialize to JSON: {}", e)))?;
            writeln!(file, "{}", json_line)?;
        }
        
        file.flush()?;
        Ok(())
    }

    /// Read all nodes from disk (for final export)
    fn read_all_nodes_from_disk(&self) -> Result<Vec<SitemapNode>, NodeMapError> {
        let mut nodes = Vec::new();
        
        // Add in-memory nodes first
        for node in self.nodes.values() {
            nodes.push(node.clone());
        }
        
        if !self.node_map_file.exists() {
            return Ok(nodes);
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.node_map_file)?;
        
        loop {
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    
                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;
                    
                    let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&item_bytes) }
                        .map_err(|e| NodeMapError::Serialization(format!("Failed to deserialize: {}", e)))?;
                    
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

    /// Clear all data (memory and disk)
    pub fn clear(&mut self) -> Result<(), NodeMapError> {
        self.nodes.clear();
        self.seen_urls.clear();
        if self.node_map_file.exists() {
            std::fs::remove_file(&self.node_map_file)?;
        }
        self.total_nodes = 0;
        Ok(())
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
        write!(f, "NodeMap: {} total, {} in memory, {} unique", 
               self.total_nodes, self.memory_nodes, self.unique_urls)
    }
}

