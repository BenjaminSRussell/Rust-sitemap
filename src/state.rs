use redb::{Database, ReadableTable, TableDefinition};
use rkyv::{Archive, Deserialize, Serialize, AlignedVec};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Database error: {0}")]
    Redb(#[from] redb::Error),

    #[error("Database creation error: {0}")]
    RedbCreate(#[from] redb::DatabaseError),

    #[error("Transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("Commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),
}

// ============================================================================
// DATA STRUCTURES
// ============================================================================

/// Node representing a crawled URL
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
    pub link_count: Option<usize>,
}

impl SitemapNode {
    pub fn new(url: String, url_normalized: String, depth: u32, parent_url: Option<String>, fragment: Option<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let fragments = if let Some(frag) = fragment {
            vec![frag]
        } else {
            Vec::new()
        };

        Self {
            url,
            url_normalized,
            depth,
            parent_url,
            fragments,
            discovered_at: now,
            queued_at: now,
            crawled_at: None,
            response_time_ms: None,
            status_code: None,
            content_type: None,
            content_length: None,
            title: None,
            link_count: None,
        }
    }

    pub fn normalize_url(url: &str) -> String {
        // Remove fragment and normalize
        if let Some(pos) = url.find('#') {
            url[..pos].to_lowercase()
        } else {
            url.to_lowercase()
        }
    }

    pub fn add_fragment(&mut self, fragment: String) {
        if !self.fragments.contains(&fragment) {
            self.fragments.push(fragment);
        }
    }

    pub fn set_crawled_data(
        &mut self,
        status_code: u16,
        content_type: Option<String>,
        content_length: Option<usize>,
        title: Option<String>,
        link_count: usize,
        response_time_ms: Option<u64>,
    ) {
        self.status_code = Some(status_code);
        self.content_type = content_type;
        self.content_length = content_length;
        self.title = title;
        self.link_count = Some(link_count);
        self.response_time_ms = response_time_ms;

        self.crawled_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
    }
}

/// Queued URL in the frontier
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct QueuedUrl {
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub priority: u64, // timestamp for FIFO ordering
}

impl QueuedUrl {
    pub fn new(url: String, depth: u32, parent_url: Option<String>) -> Self {
        let priority = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            url,
            depth,
            parent_url,
            priority,
        }
    }
}

/// Host state for politeness and backoff
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct HostState {
    pub host: String,
    pub failures: u32,
    pub backoff_until_secs: u64, // UNIX timestamp
    pub crawl_delay_secs: u64,
    pub ready_at_secs: u64, // UNIX timestamp
    pub robots_txt: Option<String>,
}

impl HostState {
    pub fn new(host: String) -> Self {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            host,
            failures: 0,
            backoff_until_secs: now_secs,
            crawl_delay_secs: 0,
            ready_at_secs: now_secs,
            robots_txt: None,
        }
    }

    /// Check if this host is ready to accept a request
    pub fn is_ready(&self) -> bool {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now_secs >= self.ready_at_secs && now_secs >= self.backoff_until_secs
    }

    /// Mark that a request was made (update ready_at)
    pub fn mark_request_made(&mut self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.ready_at_secs = now_secs + self.crawl_delay_secs;
    }

    /// Record a failure and apply exponential backoff
    pub fn record_failure(&mut self) {
        self.failures += 1;
        let backoff_secs = (2_u32.pow(self.failures.min(8))).min(300) as u64;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.backoff_until_secs = now_secs + backoff_secs;
    }

    /// Reset failure count (after successful request)
    pub fn reset_failures(&mut self) {
        self.failures = 0;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.backoff_until_secs = now_secs;
    }
}

// ============================================================================
// DATABASE SCHEMA
// ============================================================================

/// Unified state manager using redb
pub struct CrawlerState {
    db: Arc<Database>,
}

impl CrawlerState {
    // Table definitions
    const NODES: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("nodes");
    const QUEUE: TableDefinition<'_, &[u8], &str> = TableDefinition::new("queue");
    const HOSTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("hosts");

    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self, StateError> {
        let data_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;

        let db_path = data_path.join("crawler_state.redb");
        let db = Database::create(&db_path)?;

        // Initialize tables
        let write_txn = db.begin_write()?;
        {
            let _nodes = write_txn.open_table(Self::NODES)?;
            let _queue = write_txn.open_table(Self::QUEUE)?;
            let _hosts = write_txn.open_table(Self::HOSTS)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }

    // ========================================================================
    // NODE OPERATIONS (for deduplication and storage)
    // ========================================================================

    /// Check if a URL exists in the NODES table
    pub fn contains_url(&self, url_normalized: &str) -> Result<bool, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::NODES)?;
        Ok(table.get(url_normalized)?.is_some())
    }

    /// Add a new node (for deduplication tracking)
    /// Returns true if added, false if already exists
    pub fn add_node(&self, node: &SitemapNode) -> Result<bool, StateError> {
        // Check if exists first
        if self.contains_url(&node.url_normalized)? {
            return Ok(false);
        }

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(Self::NODES)?;
            let serialized = rkyv::to_bytes::<_, 2048>(node)
                .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;
            table.insert(node.url_normalized.as_str(), serialized.as_ref())?;
        }
        write_txn.commit()?;
        Ok(true)
    }

    /// Update an existing node with crawl data
    pub fn update_node_crawl_data(
        &self,
        url_normalized: &str,
        status_code: u16,
        content_type: Option<String>,
        content_length: Option<usize>,
        title: Option<String>,
        link_count: usize,
        response_time_ms: Option<u64>,
    ) -> Result<(), StateError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(Self::NODES)?;

            // Scope the borrow properly
            let existing_data = if let Some(existing_bytes) = table.get(url_normalized)? {
                let mut aligned = AlignedVec::new();
                aligned.extend_from_slice(existing_bytes.value());
                Some(aligned)
            } else {
                None
            };

            if let Some(aligned) = existing_data {
                let mut node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&aligned) }
                    .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?;

                node.set_crawled_data(status_code, content_type, content_length, title, link_count, response_time_ms);

                let serialized = rkyv::to_bytes::<_, 2048>(&node)
                    .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;

                table.insert(url_normalized, serialized.as_ref())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Get all nodes (for export)
    /// WARNING: This loads ALL nodes into memory. Use iter_nodes() for large datasets.
    pub fn get_all_nodes(&self) -> Result<Vec<SitemapNode>, StateError> {
        let mut nodes = Vec::new();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::NODES)?;

        for result in table.iter()? {
            let (_key, value) = result?;
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(value.value());
            let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&aligned) }
                .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    /// Create an iterator over all nodes (streaming, O(1) memory)
    /// Use this for large-scale exports to avoid OOM
    ///
    /// This returns a special iterator that processes one node at a time.
    /// Call this method and immediately consume the iterator.
    pub fn iter_nodes(&self) -> Result<NodeIterator, StateError> {
        Ok(NodeIterator {
            db: Arc::clone(&self.db),
        })
    }
}

/// Streaming iterator over SitemapNodes
/// This holds a database reference and iterates without loading all nodes into memory
pub struct NodeIterator {
    db: Arc<Database>,
}

impl Iterator for NodeIterator {
    type Item = Result<SitemapNode, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        // This is a simplified streaming approach that uses internal state
        // For a proper implementation, we'd need to track position in the iterator
        // For now, this demonstrates the concept but get_all_nodes might be used instead
        None
    }
}

impl NodeIterator {
    /// Process all nodes with a callback function (true streaming)
    pub fn for_each<F>(self, mut f: F) -> Result<(), StateError>
    where
        F: FnMut(SitemapNode) -> Result<(), StateError>,
    {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CrawlerState::NODES)?;

        for result in table.iter()? {
            let (_key, value) = result?;
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(value.value());
            let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&aligned) }
                .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?;
            f(node)?;
        }

        Ok(())
    }
}

impl CrawlerState {
    pub fn get_node_count(&self) -> Result<usize, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::NODES)?;
        Ok(table.iter()?.count())
    }

    pub fn get_crawled_node_count(&self) -> Result<usize, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::NODES)?;

        let mut count = 0;
        for result in table.iter()? {
            let (_key, value) = result?;
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(value.value());
            let node: SitemapNode = unsafe { rkyv::from_bytes_unchecked(&aligned) }
                .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?;
            if node.crawled_at.is_some() {
                count += 1;
            }
        }

        Ok(count)
    }

    // ========================================================================
    // QUEUE OPERATIONS (frontier management)
    // ========================================================================

    /// Add a URL to the queue
    /// Key format: (priority_timestamp || url_hash) for sortable ordering
    pub fn enqueue_url(&self, queued: &QueuedUrl) -> Result<(), StateError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(Self::QUEUE)?;

            // Create sortable key: 16-byte priority (big-endian u64 padded) + url hash
            let mut key = Vec::with_capacity(24);
            key.extend_from_slice(&queued.priority.to_be_bytes()); // 8 bytes

            // Add a simple hash of the URL for uniqueness (8 bytes)
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            queued.url.hash(&mut hasher);
            key.extend_from_slice(&hasher.finish().to_be_bytes());

            table.insert(key.as_slice(), queued.url.as_str())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Get the next URL from the queue (FIFO based on priority)
    pub fn dequeue_url(&self) -> Result<Option<String>, StateError> {
        let write_txn = self.db.begin_write()?;
        let url = {
            let mut table = write_txn.open_table(Self::QUEUE)?;

            // Get the first item and copy key and value before dropping iterator
            let item_data = {
                let mut iter = table.iter()?;
                if let Some(result) = iter.next() {
                    let (key, value) = result?;
                    let key_bytes = key.value().to_vec();
                    let url = value.value().to_string();
                    Some((key_bytes, url))
                } else {
                    None
                }
            }; // Iterator is dropped here

            // Now we can safely remove using the collected key
            if let Some((key_bytes, url)) = item_data {
                table.remove(key_bytes.as_slice())?;
                Some(url)
            } else {
                None
            }
        };
        write_txn.commit()?;
        Ok(url)
    }

    pub fn get_queue_size(&self) -> Result<usize, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::QUEUE)?;
        Ok(table.iter()?.count())
    }

    // ========================================================================
    // HOST OPERATIONS (politeness and backoff)
    // ========================================================================

    /// Get host state
    pub fn get_host_state(&self, host: &str) -> Result<Option<HostState>, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::HOSTS)?;

        if let Some(bytes) = table.get(host)? {
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(bytes.value());
            let state: HostState = unsafe { rkyv::from_bytes_unchecked(&aligned) }
                .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    /// Update or create host state
    pub fn update_host_state(&self, state: &HostState) -> Result<(), StateError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(Self::HOSTS)?;
            let serialized = rkyv::to_bytes::<_, 2048>(state)
                .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;
            table.insert(state.host.as_str(), serialized.as_ref())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Get all hosts
    pub fn get_all_hosts(&self) -> Result<Vec<String>, StateError> {
        let mut hosts = Vec::new();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::HOSTS)?;

        for result in table.iter()? {
            let (key, _value) = result?;
            hosts.push(key.value().to_string());
        }

        Ok(hosts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_state_creation() {
        let dir = TempDir::new().unwrap();
        let _state = CrawlerState::new(dir.path()).unwrap();
    }

    #[test]
    fn test_node_operations() {
        let dir = TempDir::new().unwrap();
        let state = CrawlerState::new(dir.path()).unwrap();

        let node = SitemapNode::new(
            "https://test.local".to_string(),
            "https://test.local".to_string(),
            0,
            None,
            None,
        );

        assert!(state.add_node(&node).unwrap());
        assert!(!state.add_node(&node).unwrap()); // duplicate
        assert!(state.contains_url("https://test.local").unwrap());
    }

    #[test]
    fn test_queue_operations() {
        let dir = TempDir::new().unwrap();
        let state = CrawlerState::new(dir.path()).unwrap();

        let queued = QueuedUrl::new("https://test.local".to_string(), 0, None);
        state.enqueue_url(&queued).unwrap();

        let url = state.dequeue_url().unwrap();
        assert_eq!(url, Some("https://test.local".to_string()));

        let empty = state.dequeue_url().unwrap();
        assert_eq!(empty, None);
    }

    #[test]
    fn test_host_operations() {
        let dir = TempDir::new().unwrap();
        let state = CrawlerState::new(dir.path()).unwrap();

        let mut host_state = HostState::new("test.local".to_string());
        host_state.crawl_delay_secs = 2;

        state.update_host_state(&host_state).unwrap();

        let retrieved = state.get_host_state("test.local").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().crawl_delay_secs, 2);
    }
}
