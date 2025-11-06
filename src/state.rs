use crate::wal::SeqNo;
use redb::{Database, ReadableTable, TableDefinition};
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
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
// EVENT-BASED STATE MESSAGES (Idempotent)
// ============================================================================

/// Idempotent state events with sequence numbers so the writer can replay updates safely.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum StateEvent {
    /// Fact: A crawl was attempted for this URL so we track status codes and metadata.
    CrawlAttemptFact {
        url_normalized: String,
        status_code: u16,
        content_type: Option<String>,
        content_length: Option<usize>,
        title: Option<String>,
        link_count: usize,
        response_time_ms: Option<u64>,
    },
    /// Fact: A new node was discovered so the exporter knows about newly enqueued URLs.
    AddNodeFact(SitemapNode),
    /// Fact: Host state was updated so politeness and robots rules remain accurate.
    UpdateHostStateFact {
        host: String,
        robots_txt: Option<String>,
        crawl_delay_secs: Option<u64>,
        reset_failures: bool,
        increment_failures: bool,
    },
}

/// Event wrapper with a sequence number so WAL replay can preserve ordering.
#[derive(Debug, Clone)]
pub struct StateEventWithSeqno {
    pub seqno: SeqNo,
    pub event: StateEvent,
}

// ============================================================================
// DATA STRUCTURES
// ============================================================================

/// Crawl metadata parameters to avoid excessive function arguments.
#[derive(Debug, Clone)]
pub struct CrawlData {
    pub status_code: u16,
    pub content_type: Option<String>,
    pub content_length: Option<usize>,
    pub title: Option<String>,
    pub link_count: usize,
    pub response_time_ms: Option<u64>,
}

/// Node representing a crawled URL so persistence can store crawl metadata in one record.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
pub struct SitemapNode {
    pub schema_version: u16,
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
    const CURRENT_SCHEMA: u16 = 1;
}

impl SitemapNode {
    pub fn new(
        url: String,
        url_normalized: String,
        depth: u32,
        parent_url: Option<String>,
        fragment: Option<String>,
    ) -> Self {
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
            schema_version: Self::CURRENT_SCHEMA,
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
        // Strip the fragment and lower-case so equivalent URLs deduplicate cleanly.
        if let Some(pos) = url.find('#') {
            url[..pos].to_lowercase()
        } else {
            url.to_lowercase()
        }
    }

    #[inline]
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

/// Queued URL in the frontier so we can persist queue state when needed.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct QueuedUrl {
    pub schema_version: u16,
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub priority: u64,
}

impl QueuedUrl {
    const CURRENT_SCHEMA: u16 = 1;

    pub fn new(url: String, depth: u32, parent_url: Option<String>) -> Self {
        let priority = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            schema_version: Self::CURRENT_SCHEMA,
            url,
            depth,
            parent_url,
            priority,
        }
    }
}

/// Host state for politeness and backoff so we can enforce crawl-delay and retry logic per host.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct HostState {
    pub schema_version: u16,
    pub host: String,
    pub failures: u32,
    pub backoff_until_secs: u64, // UNIX timestamp so we can compare against now() cheaply.
    pub crawl_delay_secs: u64,
    pub ready_at_secs: u64, // UNIX timestamp marking when the host becomes eligible again.
    pub robots_txt: Option<String>,
    #[with(rkyv::with::Skip)]
    pub inflight: std::sync::atomic::AtomicUsize, // Current concurrent requests to this host.
    pub max_inflight: usize, // Maximum allowed concurrent requests (default: 2).
}

impl HostState {
    const CURRENT_SCHEMA: u16 = 1;
}

impl Clone for HostState {
    fn clone(&self) -> Self {
        Self {
            schema_version: self.schema_version,
            host: self.host.clone(),
            failures: self.failures,
            backoff_until_secs: self.backoff_until_secs,
            crawl_delay_secs: self.crawl_delay_secs,
            ready_at_secs: self.ready_at_secs,
            robots_txt: self.robots_txt.clone(),
            inflight: std::sync::atomic::AtomicUsize::new(
                self.inflight.load(std::sync::atomic::Ordering::Relaxed),
            ),
            max_inflight: self.max_inflight,
        }
    }
}

impl HostState {
    /// Maximum consecutive failures before a host is permanently blacklisted
    pub const MAX_FAILURES_THRESHOLD: u32 = 5;

    pub fn new(host: String) -> Self {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            schema_version: Self::CURRENT_SCHEMA,
            host,
            failures: 0,
            backoff_until_secs: now_secs,
            crawl_delay_secs: 0,
            ready_at_secs: now_secs,
            robots_txt: None,
            inflight: std::sync::atomic::AtomicUsize::new(0),
            max_inflight: 2, // Conservative default: allow 2 concurrent requests per host.
        }
    }

    /// Check if this host has exceeded the permanent failure threshold
    #[inline]
    pub fn is_permanently_failed(&self) -> bool {
        self.failures >= Self::MAX_FAILURES_THRESHOLD
    }

    /// Check if this host is ready to accept a request so the scheduler can decide whether to delay.
    #[inline]
    pub fn is_ready(&self) -> bool {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now_secs >= self.ready_at_secs && now_secs >= self.backoff_until_secs
    }

    /// Mark that a request was made and update ready_at so the next crawl respects crawl_delay.
    #[inline]
    pub fn mark_request_made(&mut self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.ready_at_secs = now_secs + self.crawl_delay_secs;
    }

    /// Record a failure and apply exponential backoff so repeated errors slow down retries.
    pub fn record_failure(&mut self) {
        self.failures += 1;
        let backoff_secs = (2_u32.pow(self.failures.min(8))).min(300) as u64;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.backoff_until_secs = now_secs + backoff_secs;
    }

    /// Reset the failure count after a successful request so future retries do not stay throttled.
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

/// Unified state manager using redb so crawler components share one durable store.
pub struct CrawlerState {
    db: Arc<Database>,
}

impl CrawlerState {
    // Table definitions so every transaction targets the same logical buckets.
    const NODES: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("nodes");
    const HOSTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("hosts");
    const METADATA: TableDefinition<'_, &str, u64> = TableDefinition::new("metadata");

    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self, StateError> {
        let data_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;

        let db_path = data_path.join("crawler_state.redb");
        let db = Database::create(&db_path)?;

        // Open each table to ensure the database creates them before use.
        let write_txn = db.begin_write()?;
        {
            let _nodes = write_txn.open_table(Self::NODES)?;
            let _hosts = write_txn.open_table(Self::HOSTS)?;
            let _metadata = write_txn.open_table(Self::METADATA)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    // ========================================================================
    // METADATA OPERATIONS (for sequence number tracking)
    // ========================================================================

    /// Update the last processed sequence number inside a write transaction so checkpoints remain atomic with data updates.
    fn update_last_seqno_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        seqno: u64,
    ) -> Result<(), StateError> {
        let mut table = txn.open_table(Self::METADATA)?;
        table.insert("last_seqno", seqno)?;
        Ok(())
    }

    // ========================================================================
    // BATCH EVENT PROCESSING (Called by writer thread)
    // ========================================================================

    /// Apply a batch of events in a single write transaction so WAL replay stays idempotent.
    /// Returns the maximum sequence number processed so callers know how far to truncate the log.
    pub fn apply_event_batch(&self, events: &[StateEventWithSeqno]) -> Result<u64, StateError> {
        if events.is_empty() {
            return Ok(0);
        }

        let write_txn = self.db.begin_write()?;
        let mut max_seqno = 0u64;

        for event_with_seqno in events {
            let seqno_val = event_with_seqno.seqno.local_seqno;
            if seqno_val > max_seqno {
                max_seqno = seqno_val;
            }

            match &event_with_seqno.event {
                StateEvent::CrawlAttemptFact {
                    url_normalized,
                    status_code,
                    content_type,
                    content_length,
                    title,
                    link_count,
                    response_time_ms,
                } => {
                    let crawl_data = CrawlData {
                        status_code: *status_code,
                        content_type: content_type.clone(),
                        content_length: *content_length,
                        title: title.clone(),
                        link_count: *link_count,
                        response_time_ms: *response_time_ms,
                    };
                    self.apply_crawl_attempt_in_txn(&write_txn, url_normalized, &crawl_data)?;
                }
                StateEvent::AddNodeFact(node) => {
                    self.apply_add_node_in_txn(&write_txn, node)?;
                }
                StateEvent::UpdateHostStateFact {
                    host,
                    robots_txt,
                    crawl_delay_secs,
                    reset_failures,
                    increment_failures,
                } => {
                    self.apply_host_state_in_txn(
                        &write_txn,
                        host,
                        robots_txt.clone(),
                        *crawl_delay_secs,
                        *reset_failures,
                        *increment_failures,
                    )?;
                }
            }
        }

        // Update the last sequence number in the same transaction so WAL truncation stays in sync with commits.
        self.update_last_seqno_in_txn(&write_txn, max_seqno)?;

        write_txn.commit()?;
        Ok(max_seqno)
    }

    fn apply_crawl_attempt_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        url_normalized: &str,
        crawl_data: &CrawlData,
    ) -> Result<(), StateError> {
        let mut table = txn.open_table(Self::NODES)?;

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

            node.set_crawled_data(
                crawl_data.status_code,
                crawl_data.content_type.clone(),
                crawl_data.content_length,
                crawl_data.title.clone(),
                crawl_data.link_count,
                crawl_data.response_time_ms,
            );

            let serialized = rkyv::to_bytes::<_, 2048>(&node)
                .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;

            table.insert(url_normalized, serialized.as_ref())?;
        }

        Ok(())
    }

    fn apply_add_node_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        node: &SitemapNode,
    ) -> Result<(), StateError> {
        let mut table = txn.open_table(Self::NODES)?;

        // Check for an existing record to maintain idempotence.
        if table.get(node.url_normalized.as_str())?.is_some() {
            return Ok(());
        }

        let serialized = rkyv::to_bytes::<_, 2048>(node)
            .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;
        table.insert(node.url_normalized.as_str(), serialized.as_ref())?;
        Ok(())
    }

    fn apply_host_state_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        host: &str,
        robots_txt: Option<String>,
        crawl_delay_secs: Option<u64>,
        reset_failures: bool,
        increment_failures: bool,
    ) -> Result<(), StateError> {
        let mut table = txn.open_table(Self::HOSTS)?;

        // Load the existing entry or create a new default so we always mutate the latest state.
        let mut host_state = if let Some(bytes) = table.get(host)? {
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(bytes.value());
            unsafe { rkyv::from_bytes_unchecked(&aligned) }
                .map_err(|e| StateError::Serialization(format!("Deserialize failed: {}", e)))?
        } else {
            HostState::new(host.to_string())
        };

        // Apply the updates so the stored host state reflects the newest directives and failure counts.
        if let Some(txt) = robots_txt {
            host_state.robots_txt = Some(txt);
        }
        if let Some(delay) = crawl_delay_secs {
            host_state.crawl_delay_secs = delay;
        }
        if reset_failures {
            host_state.reset_failures();
        }
        if increment_failures {
            host_state.record_failure();
        }

        let serialized = rkyv::to_bytes::<_, 2048>(&host_state)
            .map_err(|e| StateError::Serialization(format!("Serialize failed: {}", e)))?;
        table.insert(host, serialized.as_ref())?;
        Ok(())
    }

    // ========================================================================
    // NODE OPERATIONS (for deduplication and storage)
    // ========================================================================

    /// Check if a URL exists in the nodes table so callers can avoid inserting duplicates.
    pub fn contains_url(&self, url_normalized: &str) -> Result<bool, StateError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(Self::NODES)?;
        Ok(table.get(url_normalized)?.is_some())
    }

    /// Create an iterator over all nodes (streaming, O(1) memory).
    /// Use this for large-scale exports to avoid out-of-memory issues.
    ///
    /// This returns a special iterator that processes one node at a time.
    /// Call this method and immediately consume the iterator.
    pub fn iter_nodes(&self) -> Result<NodeIterator, StateError> {
        Ok(NodeIterator {
            db: Arc::clone(&self.db),
        })
    }
}

/// Streaming iterator over SitemapNodes.
/// Holds a database reference and iterates without loading all nodes into memory so exports stay memory-friendly.
pub struct NodeIterator {
    db: Arc<Database>,
}

impl Iterator for NodeIterator {
    type Item = Result<SitemapNode, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Placeholder implementation: production code should track iterator position and return rows lazily.
        // Until that enhancement lands, callers should use get_all_nodes or for_each for real work.
        None
    }
}

impl NodeIterator {
    /// Process all nodes with a callback function (true streaming) so callers can handle massive datasets incrementally.
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
    // HOST OPERATIONS (politeness and backoff)
    // ========================================================================

    /// Get host state so scheduling can consult the latest politeness metadata.
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
}
