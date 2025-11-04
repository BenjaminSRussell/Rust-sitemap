use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Sequence number for idempotent replay so events remain ordered across instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeqNo {
    pub instance_id: u64,
    pub local_seqno: u64,
}

impl SeqNo {
    pub fn new(instance_id: u64, local_seqno: u64) -> Self {
        Self {
            instance_id,
            local_seqno,
        }
    }

    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.instance_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.local_seqno.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WalError> {
        if bytes.len() != 16 {
            return Err(WalError::CorruptRecord("Invalid seqno length".to_string()));
        }
        let instance_id = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let local_seqno = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        Ok(Self {
            instance_id,
            local_seqno,
        })
    }
}

#[derive(Error, Debug)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corrupt record: {0}")]
    CorruptRecord(String),
}

/// WAL record format used for persistence.
/// [u32 len][u32 crc32c][u128 seqno][payload] so the reader knows how to validate entries.
pub struct WalRecord {
    pub seqno: SeqNo,
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Encode the record into bytes.
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = self.payload.len();
        let seqno_bytes = self.seqno.to_bytes();

        // Calculate the CRC of the sequence number and payload without intermediate allocation.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&seqno_bytes);
        hasher.update(&self.payload);
        let crc = hasher.finalize();

        // Build the record as [len][crc][seqno][payload].
        let total_len = 4 + 4 + 16 + payload_len;
        let mut record = Vec::with_capacity(total_len);
        record.extend_from_slice(&(payload_len as u32).to_le_bytes());
        record.extend_from_slice(&crc.to_le_bytes());
        record.extend_from_slice(&seqno_bytes);
        record.extend_from_slice(&self.payload);

        record
    }

    /// Decode a record from the reader.
    /// Returns None on torn writes (EOF during record processing).
    pub fn decode<R: Read>(reader: &mut R) -> Result<Option<Self>, WalError> {
        // Read the length.
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(WalError::Io(e)),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read the CRC.
        let mut crc_buf = [0u8; 4];
        match reader.read_exact(&mut crc_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(WalError::Io(e)),
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        // Read the sequence number.
        let mut seqno_buf = [0u8; 16];
        match reader.read_exact(&mut seqno_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(WalError::Io(e)),
        }
        let seqno = SeqNo::from_bytes(&seqno_buf)?;

        // Read the payload.
        let mut payload = vec![0u8; payload_len];
        match reader.read_exact(&mut payload) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(WalError::Io(e)),
        }

        // Verify the CRC without intermediate allocation.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&seqno_buf);
        hasher.update(&payload);
        let actual_crc = hasher.finalize();

        if actual_crc != expected_crc {
            return Err(WalError::CorruptRecord(format!(
                "CRC mismatch: expected {}, got {}",
                expected_crc, actual_crc
            )));
        }

        Ok(Some(WalRecord { seqno, payload }))
    }
}

/// WAL writer with fsync batching.
pub struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    sidecar_path: PathBuf,
    current_offset: u64,
    last_fsync: std::time::Instant,
    fsync_interval_ms: u64,
}

impl WalWriter {
    pub fn new(data_dir: &std::path::Path, fsync_interval_ms: u64) -> Result<Self, WalError> {
        let path = data_dir.join("wal.log");
        let sidecar_path = data_dir.join("wal.offset");

        // Open or create the WAL file in append mode.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        // Capture the current offset.
        let current_offset = file.metadata()?.len();

        Ok(Self {
            file: BufWriter::new(file),
            path,
            sidecar_path,
            current_offset,
            last_fsync: std::time::Instant::now(),
            fsync_interval_ms,
        })
    }

    /// Append a record to the WAL so new events become durable.
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let encoded = record.encode();
        self.file.write_all(&encoded)?;
        self.current_offset += encoded.len() as u64;

        // Trigger fsync when the interval elapses.
        if self.last_fsync.elapsed().as_millis() as u64 >= self.fsync_interval_ms {
            self.fsync()?;
        }

        Ok(())
    }

    /// Force fsync so buffered data reaches disk immediately.
    pub fn fsync(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        self.last_fsync = std::time::Instant::now();
        Ok(())
    }

    /// Truncate the WAL to the given offset after a successful commit so replay stops at the latest checkpoint.
    pub fn truncate(&mut self, offset: u64) -> Result<(), WalError> {
        // Flush and sync before truncating.
        self.fsync()?;

        // Write the truncation point to the sidecar.
        let mut sidecar = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.sidecar_path)?;
        sidecar.write_all(&offset.to_le_bytes())?;
        sidecar.sync_all()?;

        // Truncate the WAL file.
        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)?;
        file.set_len(offset)?;
        file.sync_all()?;

        // Fsync the parent directory to make file metadata durable.
        if let Some(parent) = self.path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }

        // Reopen the file in append mode.
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.file = BufWriter::new(new_file);
        self.current_offset = offset;

        Ok(())
    }

    pub fn get_offset(&self) -> u64 {
        self.current_offset
    }
}

/// WAL reader for replay.
pub struct WalReader {
    path: PathBuf,
}

impl WalReader {
    pub fn new(data_dir: &std::path::Path) -> Self {
        let path = data_dir.join("wal.log");
        Self { path }
    }

    /// Replay all records from the WAL so state can rebuild after a crash.
    pub fn replay<F>(&self, mut callback: F) -> Result<u64, WalError>
    where
        F: FnMut(WalRecord) -> Result<(), WalError>,
    {
        if !self.path.exists() {
            return Ok(0);
        }

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut max_seqno = SeqNo::new(0, 0);

        while let Some(record) = WalRecord::decode(&mut reader)? {
            if record.seqno > max_seqno {
                max_seqno = record.seqno;
            }
            callback(record)?;
        }

        Ok(max_seqno.local_seqno)
    }
}

/// Shared WAL writer for async context.
pub type SharedWalWriter = Arc<Mutex<WalWriter>>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_seqno_roundtrip() {
        let seqno = SeqNo::new(42, 1337);
        let bytes = seqno.to_bytes();
        let decoded = SeqNo::from_bytes(&bytes).unwrap();
        assert_eq!(seqno, decoded);
    }

    #[test]
    fn test_record_encode_decode() {
        let record = WalRecord {
            seqno: SeqNo::new(1, 100),
            payload: b"hello world".to_vec(),
        };

        let encoded = record.encode();
        let mut cursor = std::io::Cursor::new(encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();

        assert_eq!(record.seqno, decoded.seqno);
        assert_eq!(record.payload, decoded.payload);
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::new(dir.path(), 100).unwrap();

        // Write a few records so the replay test has data to read.
        for i in 0..10 {
            let record = WalRecord {
                seqno: SeqNo::new(1, i),
                payload: format!("record_{}", i).into_bytes(),
            };
            writer.append(&record).unwrap();
        }
        writer.fsync().unwrap();

        // Replay the records to ensure we get back what we wrote.
        let reader = WalReader::new(dir.path());
        let mut count = 0;
        reader
            .replay(|record| {
                assert_eq!(
                    record.payload,
                    format!("record_{}", count).into_bytes()
                );
                count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(count, 10);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::new(dir.path(), 100).unwrap();

        // Write records to the WAL so we can test truncation.
        for i in 0..5 {
            let record = WalRecord {
                seqno: SeqNo::new(1, i),
                payload: format!("record_{}", i).into_bytes(),
            };
            writer.append(&record).unwrap();
        }
        writer.fsync().unwrap();

        let offset_before_last = writer.get_offset();

        // Write one more record to create a truncation target.
        let record = WalRecord {
            seqno: SeqNo::new(1, 5),
            payload: b"last".to_vec(),
        };
        writer.append(&record).unwrap();
        writer.fsync().unwrap();

        // Truncate to the offset before the last record to simulate checkpointing.
        writer.truncate(offset_before_last).unwrap();

        // Replay should only see the first five records to confirm truncation worked.
        let reader = WalReader::new(dir.path());
        let mut count = 0;
        reader.replay(|_| {
            count += 1;
            Ok(())
        }).unwrap();

        assert_eq!(count, 5);
    }
}
