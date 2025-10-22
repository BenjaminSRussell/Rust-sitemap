use parking_lot::Mutex;
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
pub struct QueuedUrl {
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub added_at: u64,
}

impl QueuedUrl {
    pub fn new(url: String, depth: u32, parent_url: Option<String>) -> Self {
        Self {
            url,
            depth,
            parent_url,
            added_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

/// Rkyv-based persistent queue for high performance
/// Note: Deduplication is handled by NodeMap, queue is just a queue
#[derive(Debug)]
pub struct RkyvQueue {
    memory_queue: VecDeque<QueuedUrl>,
    max_memory_size: usize,
    queue_file_path: std::path::PathBuf,
    total_count: usize,
}

impl RkyvQueue {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_memory_size: usize) -> Result<Self, QueueError> {
        let data_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;

        let queue_file_path = data_path.join("queue.rkyv");

        let mut queue = Self {
            memory_queue: VecDeque::new(),
            max_memory_size,
            queue_file_path,
            total_count: 0,
        };

        // Try to load existing queue
        queue.load_from_disk()?;

        Ok(queue)
    }

    pub fn push_back(&mut self, item: QueuedUrl) -> Result<(), QueueError> {
        // Note: No dedup here - NodeMap handles that
        // Queue is just a queue, not a dedup structure

        // If we're at capacity, flush to disk first
        if self.memory_queue.len() >= self.max_memory_size {
            self.flush_to_disk()?;
            self.memory_queue.clear();
        }

        self.memory_queue.push_back(item);
        self.total_count += 1;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn pop_front(&mut self) -> Option<QueuedUrl> {
        if let Some(item) = self.memory_queue.pop_front() {
            return Some(item);
        }

        // If memory queue is empty, try to load from disk
        if let Ok(loaded_items) = self.load_from_disk() {
            if !loaded_items.is_empty() {
                self.memory_queue.extend(loaded_items);
                // Clear the disk file after loading items to prevent re-loading the same items
                if self.queue_file_path.exists() {
                    let _ = std::fs::remove_file(&self.queue_file_path);
                }
                return self.memory_queue.pop_front();
            }
        }

        None
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.memory_queue.len() + self.disk_count().unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.memory_queue.is_empty() && self.disk_count().unwrap_or(0) == 0
    }

    #[allow(dead_code)]
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    #[allow(dead_code)]
    pub fn contains(&self, url: &str) -> bool {
        // Queue doesn't track seen URLs - NodeMap does that
        self.memory_queue.iter().any(|item| item.url == url)
    }

    fn flush_to_disk(&self) -> Result<(), QueueError> {
        if self.memory_queue.is_empty() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.queue_file_path)?;

        let mut writer = std::io::BufWriter::new(file);

        // Serialize each item individually and append to file
        for item in &self.memory_queue {
            let bytes = rkyv::to_bytes::<_, 1024>(item)
                .map_err(|e| QueueError::Serialization(format!("Failed to serialize: {}", e)))?;
            // Write length prefix followed by data
            let len_bytes = (bytes.len() as u32).to_le_bytes();
            writer.write_all(&len_bytes)?;
            writer.write_all(&bytes)?;
        }
        writer.flush()?;

        Ok(())
    }

    fn load_from_disk(&mut self) -> Result<Vec<QueuedUrl>, QueueError> {
        if !self.queue_file_path.exists() {
            return Ok(Vec::new());
        }

        let mut file = OpenOptions::new().read(true).open(&self.queue_file_path)?;

        let mut items = Vec::new();

        // Read items one by one using length-prefixed format
        loop {
            // Read length prefix (4 bytes)
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    // Read the item data
                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;

                    // Deserialize the item
                    let item: QueuedUrl = unsafe { rkyv::from_bytes_unchecked(&item_bytes) }
                        .map_err(|e| {
                            QueueError::Serialization(format!("Failed to deserialize: {}", e))
                        })?;

                    items.push(item);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // No need to track URLs - NodeMap handles dedup
        Ok(items)
    }

    fn disk_count(&self) -> Result<usize, QueueError> {
        if !self.queue_file_path.exists() {
            return Ok(0);
        }

        let mut file = OpenOptions::new().read(true).open(&self.queue_file_path)?;

        let mut count = 0;

        // Count items by reading length prefixes
        loop {
            // Read length prefix (4 bytes)
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    // Skip the item data
                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;

                    count += 1;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(count)
    }

    /// Force flush all pending items to disk
    pub fn force_flush(&mut self) -> Result<(), QueueError> {
        self.flush_to_disk()?;
        Ok(())
    }

    /// Clear all data (memory and disk)
    pub fn clear(&mut self) -> Result<(), QueueError> {
        self.memory_queue.clear();
        if self.queue_file_path.exists() {
            std::fs::remove_file(&self.queue_file_path)?;
        }
        self.total_count = 0;
        Ok(())
    }

    /// Get statistics about the queue
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            memory_count: self.memory_queue.len(),
            disk_count: self.disk_count().unwrap_or(0),
            total_count: self.total_count,
            max_memory_size: self.max_memory_size,
            unique_urls: 0, // Queue doesn't track unique URLs - NodeMap does
        }
    }

    /// Get combined statistics (alias for stats for compatibility)
    #[allow(dead_code)]
    pub fn combined_stats(&self) -> QueueStats {
        self.stats()
    }
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub memory_count: usize,
    pub disk_count: usize,
    pub total_count: usize,
    pub max_memory_size: usize,
    pub unique_urls: usize,
}

impl std::fmt::Display for QueueStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Queue: {} memory, {} disk, {} total, {} unique (max: {})",
            self.memory_count,
            self.disk_count,
            self.total_count,
            self.unique_urls,
            self.max_memory_size
        )
    }
}

/// Thread-safe wrapper for RkyvQueue
#[allow(dead_code)]
pub type SharedRkyvQueue = Arc<Mutex<RkyvQueue>>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_rkyv_queue_basic() {
        let temp_dir = TempDir::new().unwrap();
        let mut queue = RkyvQueue::new(temp_dir.path(), 3).unwrap();

        // Add items
        queue
            .push_back(QueuedUrl::new("https://example.com".to_string(), 0, None))
            .unwrap();
        queue
            .push_back(QueuedUrl::new(
                "https://example.com/page1".to_string(),
                1,
                Some("https://example.com".to_string()),
            ))
            .unwrap();

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.total_count(), 2);

        // Pop item
        let item = queue.pop_front().unwrap();
        assert_eq!(item.url, "https://example.com");

        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_rkyv_queue_overflow() {
        let temp_dir = TempDir::new().unwrap();
        let mut queue = RkyvQueue::new(temp_dir.path(), 2).unwrap();

        // Add 5 items (should trigger disk storage after 2)
        for i in 0..5 {
            queue
                .push_back(QueuedUrl::new(
                    format!("https://example.com/page{}", i),
                    i,
                    None,
                ))
                .unwrap();
        }

        // Should have 1 in memory, 4 on disk
        let stats = queue.stats();
        println!(
            "Debug: memory_count={}, disk_count={}, total_count={}, unique_urls={}",
            stats.memory_count, stats.disk_count, stats.total_count, stats.unique_urls
        );
        assert_eq!(stats.memory_count, 1);
        assert_eq!(stats.disk_count, 4);
        assert_eq!(stats.total_count, 5);

        // Pop all items
        let mut popped_urls = Vec::new();
        while let Some(item) = queue.pop_front() {
            popped_urls.push(item.url);
        }

        assert_eq!(popped_urls.len(), 5);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_rkyv_performance() {
        let temp_dir = TempDir::new().unwrap();
        let mut queue = RkyvQueue::new(temp_dir.path(), 1000).unwrap();

        let start = std::time::Instant::now();

        // Add many items
        for i in 0..10000 {
            queue
                .push_back(QueuedUrl::new(
                    format!("https://example.com/page{}", i),
                    i % 10,
                    None,
                ))
                .unwrap();
        }

        let add_time = start.elapsed();
        println!("Added 10000 items in {:?}", add_time);

        // Force flush to test disk performance
        queue.force_flush().unwrap();

        let flush_time = start.elapsed() - add_time;
        println!("Flushed to disk in {:?}", flush_time);

        // Clear memory and reload
        queue.memory_queue.clear();
        let reload_start = std::time::Instant::now();
        queue.load_from_disk().unwrap();
        let reload_time = reload_start.elapsed();
        println!("Reloaded from disk in {:?}", reload_time);

        let stats = queue.stats();
        assert_eq!(stats.total_count, 10000);
    }
}
