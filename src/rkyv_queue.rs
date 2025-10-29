use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
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

        queue.load_from_disk()?;
        Ok(queue)
    }

    pub fn push_back(&mut self, item: QueuedUrl) -> Result<(), QueueError> {
        if self.memory_queue.len() >= self.max_memory_size {
            self.flush_to_disk()?;
            self.memory_queue.clear();
        }

        self.memory_queue.push_back(item);
        self.total_count += 1;
        Ok(())
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

        for item in &self.memory_queue {
            let bytes = rkyv::to_bytes::<_, 1024>(item)
                .map_err(|e| QueueError::Serialization(format!("Failed to serialize: {}", e)))?;
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

        loop {
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    let mut item_bytes = vec![0u8; len];
                    file.read_exact(&mut item_bytes)?;

                    let item: QueuedUrl = unsafe { rkyv::from_bytes_unchecked(&item_bytes) }
                        .map_err(|e| {
                            QueueError::Serialization(format!("Failed to deserialize: {}", e))
                        })?;

                    items.push(item);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_queue_creation() {
        let dir = TempDir::new().unwrap();
        let queue = RkyvQueue::new(dir.path(), 10).unwrap();
        assert_eq!(queue.memory_queue.len(), 0);
    }

    #[test]
    fn test_push_back() {
        let dir = TempDir::new().unwrap();
        let mut queue = RkyvQueue::new(dir.path(), 10).unwrap();

        let url = QueuedUrl::new("https://test.local".to_string(), 0, None);
        queue.push_back(url).unwrap();

        assert_eq!(queue.memory_queue.len(), 1);
        assert_eq!(queue.total_count, 1);
    }

    #[test]
    fn test_queued_url_creation() {
        let url = QueuedUrl::new(
            "https://test.local".to_string(),
            5,
            Some("https://parent.local".to_string()),
        );
        assert_eq!(url.url, "https://test.local");
        assert_eq!(url.depth, 5);
        assert_eq!(url.parent_url, Some("https://parent.local".to_string()));
    }
}
