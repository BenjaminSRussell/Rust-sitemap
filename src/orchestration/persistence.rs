//! State database and WAL setup.

use crate::state::{CrawlerState, StateEvent, StateEventWithSeqno};
use crate::wal::{WalReader, WalWriter, WalError};
use std::sync::Arc;

// [Zencoder Task Doc]
// WHAT: Initializes the state database, WAL writer, and generates a unique instance ID for this crawler run.
// USED_BY: src/orchestration/builder.rs (build_crawler)

/// Creates state DB, WAL writer, and instance ID.
/// Returns unwrapped WalWriter for direct ownership by WriterThread.
pub fn initialize_persistence<P: AsRef<std::path::Path>>(
    data_dir: P,
) -> Result<
    (
        Arc<CrawlerState>,
        WalWriter,
        u64, // instance_id
    ),
    Box<dyn std::error::Error>,
> {
    let state = Arc::new(CrawlerState::new(&data_dir)?);
    let wal_writer = WalWriter::new(data_dir.as_ref(), 100)?;

    let instance_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            eprintln!("Time error: {}. Using default duration.", e);
            std::time::Duration::from_secs(0)
        })
        .as_nanos() as u64;

    Ok((state, wal_writer, instance_id))
}

/// Replays WAL to recover state from previous run.
pub fn replay_wal_if_needed<P: AsRef<std::path::Path>>(
    data_dir: P,
    state: &Arc<CrawlerState>,
) -> Result<(u64, usize), Box<dyn std::error::Error>> {
    let wal_reader = WalReader::new(data_dir.as_ref());
    let mut replayed_count = 0usize;

    let max_seqno = wal_reader.replay(|record| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unsafe { rkyv::archived_root::<StateEvent>(&record.payload) }
        }));

        let archived = match result {
            Ok(a) => a,
            Err(_) => {
                eprintln!(
                    "FATAL: Corrupted WAL record at seqno {:?} - deserialization panicked. Aborting replay.",
                    record.seqno
                );
                return Err(WalError::CorruptRecord(format!(
                    "Deserialization panic at seqno {:?}",
                    record.seqno
                )));
            }
        };

        let event: StateEvent = match rkyv::Deserialize::deserialize(archived, &mut rkyv::Infallible) {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!(
                    "FATAL: Corrupted WAL record at seqno {:?} - deserialization failed: {:?}. Aborting replay.",
                    record.seqno, e
                );
                return Err(WalError::CorruptRecord(format!(
                    "Deserialization error at seqno {:?}: {:?}",
                    record.seqno, e
                )));
            }
        };

        let event_with_seqno = StateEventWithSeqno {
            seqno: record.seqno,
            event,
        };
        let _ = state.apply_event_batch(&[event_with_seqno]);
        replayed_count += 1;

        Ok(())
    })?;

    if replayed_count > 0 {
        eprintln!("Recovered {} events from previous crawl", replayed_count);
    }

    Ok((max_seqno, replayed_count))
}
