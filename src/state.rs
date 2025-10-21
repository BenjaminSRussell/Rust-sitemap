use crate::models::PageNode;
use sled::Db;
use std::path::Path;
use std::collections::HashSet;
use rkyv::{to_bytes, archived_root, Deserialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateError {
    #[error("Database error: {0}")]
    Database(#[from] sled::Error),
    #[error("Serialization error")]
    Serialization,
    #[error("Deserialization error")]
    Deserialization,
}

// Make the error Send + Sync
unsafe impl Send for StateError {}
unsafe impl Sync for StateError {}

/// Manages the persistent state of the crawler using Sled database
pub struct CrawlerState {
    db: Db,
    batch_size: usize,
    pending_writes: Vec<(String, PageNode)>,
}

impl CrawlerState {
    /// Initialize a new CrawlerState with a Sled database at the given directory
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self, StateError> {
        let db = sled::open(data_dir)?;
        Ok(Self { 
            db,
            batch_size: 100, // Batch writes for better performance
            pending_writes: Vec::new(),
        })
    }

    /// Load an existing CrawlerState from the given directory
    #[allow(dead_code)]
    pub fn load<P: AsRef<Path>>(data_dir: P) -> Result<Self, StateError> {
        let db = sled::open(data_dir)?;
        Ok(Self { 
            db,
            batch_size: 100,
            pending_writes: Vec::new(),
        })
    }

    /// Insert or update a PageNode in the state (batched for performance)
    pub fn insert(&mut self, url: String, node: PageNode) -> Result<(), StateError> {
        self.pending_writes.push((url, node));
        
        // Flush when batch is full
        if self.pending_writes.len() >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }
    
    /// Flush pending writes to disk
    pub fn flush_batch(&mut self) -> Result<(), StateError> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }
        
        let mut batch = sled::Batch::default();
        for (url, node) in self.pending_writes.drain(..) {
            let serialized = to_bytes::<_, 8192>(&node).map_err(|_| StateError::Serialization)?;
            batch.insert(url.as_bytes(), serialized.as_ref());
        }
        
        self.db.apply_batch(batch)?;
        Ok(())
    }

    /// Get a PageNode by URL
    #[allow(dead_code)]
    pub fn get(&self, url: &str) -> Result<Option<PageNode>, StateError> {
        match self.db.get(url.as_bytes())? {
            Some(data) => {
                let archived = unsafe { archived_root::<PageNode>(&data) };
                let node = archived.deserialize(&mut rkyv::Infallible).map_err(|_| StateError::Deserialization)?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    /// Check if a URL exists in the state
    #[allow(dead_code)]
    pub fn contains(&self, url: &str) -> Result<bool, StateError> {
        Ok(self.db.contains_key(url.as_bytes())?)
    }

    /// Get all URLs in the state
    #[allow(dead_code)]
    pub fn get_all_urls(&self) -> Result<HashSet<String>, StateError> {
        let mut urls = HashSet::new();
        for result in self.db.iter() {
            let (key, _) = result?;
            if let Ok(url) = String::from_utf8(key.to_vec()) {
                urls.insert(url);
            }
        }
        Ok(urls)
    }

    /// Get all unprocessed URLs
    #[allow(dead_code)]
    pub fn get_unprocessed_urls(&self) -> Result<HashSet<String>, StateError> {
        let mut unprocessed = HashSet::new();
        for result in self.db.iter() {
            let (key, value) = result?;
            if let Ok(url) = String::from_utf8(key.to_vec()) {
                let archived = unsafe { archived_root::<PageNode>(&value) };
                if !archived.processed {
                    unprocessed.insert(url);
                }
            }
        }
        Ok(unprocessed)
    }

    /// Get the count of total pages
    pub fn count(&self) -> Result<usize, StateError> {
        Ok(self.db.len())
    }

    /// Get the count of processed pages
    pub fn processed_count(&self) -> Result<usize, StateError> {
        let mut count = 0;
        for result in self.db.iter() {
            let (_, value) = result?;
            let archived = unsafe { archived_root::<PageNode>(&value) };
            if archived.processed {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Remove a URL from the state
    #[allow(dead_code)]
    pub fn remove(&mut self, url: &str) -> Result<Option<PageNode>, StateError> {
        match self.db.remove(url.as_bytes())? {
            Some(data) => {
                let archived = unsafe { archived_root::<PageNode>(&data) };
                let node = archived.deserialize(&mut rkyv::Infallible).map_err(|_| StateError::Deserialization)?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    /// Clear all data from the state
    #[allow(dead_code)]
    pub fn clear(&mut self) -> Result<(), StateError> {
        self.db.clear()?;
        Ok(())
    }

    /// Flush any pending writes to disk
    pub fn flush(&mut self) -> Result<(), StateError> {
        self.flush_batch()?;
        self.db.flush()?;
        Ok(())
    }
}

impl Drop for CrawlerState {
    fn drop(&mut self) {
        // Ensure data is flushed to disk when the state is dropped
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_state() -> (TempDir, CrawlerState) {
        let temp_dir = TempDir::new().unwrap();
        let state = CrawlerState::new(temp_dir.path()).unwrap();
        (temp_dir, state)
    }

    #[test]
    fn test_create_and_insert() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url = "https://example.com".to_string();
        let mut node = PageNode::new(url.clone(), 0);
        node.set_content_and_metadata("Hello World".to_string(), 200, Some("text/html".to_string()));
        node.add_link("https://example.com/page1".to_string());
        
        state.insert(url.clone(), node.clone()).unwrap();
        
        let retrieved = state.get(&url).unwrap().unwrap();
        assert_eq!(retrieved.url, node.url);
        assert_eq!(retrieved.depth, node.depth);
        assert_eq!(retrieved.content, node.content);
        assert_eq!(retrieved.status_code, node.status_code);
        assert_eq!(retrieved.content_type, node.content_type);
        assert_eq!(retrieved.links, node.links);
    }

    #[test]
    fn test_persistence_on_drop() {
        let temp_dir = TempDir::new().unwrap();
        let url = "https://example.com".to_string();
        let node = PageNode::new(url.clone(), 0);
        
        {
            let mut state = CrawlerState::new(temp_dir.path()).unwrap();
            state.insert(url.clone(), node).unwrap();
            // State is dropped here, should flush to disk
        }
        
        // Verify the data persists after drop
        let state = CrawlerState::load(temp_dir.path()).unwrap();
        let retrieved = state.get(&url).unwrap().unwrap();
        assert_eq!(retrieved.url, url);
    }

    #[test]
    fn test_load_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        let url = "https://example.com".to_string();
        let mut node = PageNode::new(url.clone(), 1);
        node.set_content_and_metadata("Test Content".to_string(), 200, Some("text/html".to_string()));
        
        // Create and insert data
        {
            let mut state = CrawlerState::new(temp_dir.path()).unwrap();
            state.insert(url.clone(), node.clone()).unwrap();
        }
        
        // Load from disk and verify
        let state = CrawlerState::load(temp_dir.path()).unwrap();
        let retrieved = state.get(&url).unwrap().unwrap();
        assert_eq!(retrieved.url, node.url);
        assert_eq!(retrieved.depth, node.depth);
        assert_eq!(retrieved.content, node.content);
        assert_eq!(retrieved.status_code, node.status_code);
    }

    #[test]
    fn test_update_existing_node() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url = "https://example.com".to_string();
        let mut node1 = PageNode::new(url.clone(), 0);
        node1.set_content_and_metadata("Original Content".to_string(), 200, Some("text/html".to_string()));
        
        let mut node2 = PageNode::new(url.clone(), 1);
        node2.set_content_and_metadata("Updated Content".to_string(), 200, Some("text/html".to_string()));
        node2.add_link("https://example.com/new".to_string());
        
        // Insert first node
        state.insert(url.clone(), node1).unwrap();
        
        // Update with second node
        state.insert(url.clone(), node2.clone()).unwrap();
        
        // Verify the update
        let retrieved = state.get(&url).unwrap().unwrap();
        assert_eq!(retrieved.url, node2.url);
        assert_eq!(retrieved.depth, node2.depth);
        assert_eq!(retrieved.content, node2.content);
        assert_eq!(retrieved.links, node2.links);
    }

    #[test]
    fn test_contains_and_count() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url1 = "https://example.com".to_string();
        let url2 = "https://example.org".to_string();
        
        assert!(!state.contains(&url1).unwrap());
        assert_eq!(state.count().unwrap(), 0);
        
        state.insert(url1.clone(), PageNode::new(url1.clone(), 0)).unwrap();
        state.insert(url2.clone(), PageNode::new(url2.clone(), 0)).unwrap();
        
        assert!(state.contains(&url1).unwrap());
        assert!(state.contains(&url2).unwrap());
        assert_eq!(state.count().unwrap(), 2);
    }

    #[test]
    fn test_get_all_urls() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url1 = "https://example.com".to_string();
        let url2 = "https://example.org".to_string();
        
        state.insert(url1.clone(), PageNode::new(url1.clone(), 0)).unwrap();
        state.insert(url2.clone(), PageNode::new(url2.clone(), 0)).unwrap();
        
        let urls = state.get_all_urls().unwrap();
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&url1));
        assert!(urls.contains(&url2));
    }

    #[test]
    fn test_get_unprocessed_urls() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url1 = "https://example.com".to_string();
        let url2 = "https://example.org".to_string();
        
        let mut node1 = PageNode::new(url1.clone(), 0);
        node1.mark_processed();
        
        state.insert(url1.clone(), node1).unwrap();
        state.insert(url2.clone(), PageNode::new(url2.clone(), 0)).unwrap();
        
        let unprocessed = state.get_unprocessed_urls().unwrap();
        assert_eq!(unprocessed.len(), 1);
        assert!(unprocessed.contains(&url2));
        assert!(!unprocessed.contains(&url1));
    }

    #[test]
    fn test_remove() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url = "https://example.com".to_string();
        let node = PageNode::new(url.clone(), 0);
        
        state.insert(url.clone(), node.clone()).unwrap();
        assert!(state.contains(&url).unwrap());
        
        let removed = state.remove(&url).unwrap().unwrap();
        assert_eq!(removed.url, node.url);
        assert!(!state.contains(&url).unwrap());
    }

    #[test]
    fn test_clear() {
        let (_temp_dir, mut state) = create_temp_state();
        
        let url1 = "https://example.com".to_string();
        let url2 = "https://example.org".to_string();
        
        state.insert(url1.clone(), PageNode::new(url1.clone(), 0)).unwrap();
        state.insert(url2.clone(), PageNode::new(url2.clone(), 0)).unwrap();
        
        assert_eq!(state.count().unwrap(), 2);
        
        state.clear().unwrap();
        assert_eq!(state.count().unwrap(), 0);
    }
}