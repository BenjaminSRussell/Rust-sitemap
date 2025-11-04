use dashmap::DashMap;
use fastbloom::BloomFilter;
use robotstxt::DefaultMatcher;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use crate::state::{CrawlerState, HostState, SitemapNode, StateEvent};
use crate::url_utils;
use crate::writer_thread::WriterThread;

const FP_CHECK_SEMAPHORE_LIMIT: usize = 32;
const GLOBAL_FRONTIER_SIZE_LIMIT: usize = 1_000_000;

type UrlReceiver = tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>;
type ControlReceiver = tokio::sync::mpsc::UnboundedReceiver<ShardMsg>;
type ControlSender = tokio::sync::mpsc::UnboundedSender<ShardMsg>;
type WorkItem = (String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit);
type FrontierDispatcherNew = (
    FrontierDispatcher,
    Vec<UrlReceiver>,
    Vec<ControlReceiver>,
    Vec<ControlSender>,
    Arc<AtomicUsize>,
    Arc<tokio::sync::Semaphore>,
);

#[derive(Debug)]
pub(crate) struct QueuedUrl {
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Message to report crawl completion back to the shard
#[derive(Debug, Clone)]
pub enum ShardMsg {
    Finished {
        host: String,
        ok: bool,
    },
}

/// Min-heap by ready_at for politeness scheduling.
#[derive(Debug, Clone)]
struct ReadyHost {
    host: String,
    ready_at: Instant,
}

impl PartialEq for ReadyHost {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at == other.ready_at
    }
}

impl Eq for ReadyHost {}

impl PartialOrd for ReadyHost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadyHost {
    fn cmp(&self, other: &Self) -> Ordering {
        other.ready_at.cmp(&self.ready_at)  // Min-heap
    }
}

/// Routes URLs to shards via eTLD+1 + rendezvous hashing.
/// Applies global backpressure when frontier > 1M URLs.
pub struct FrontierDispatcher {
    shard_senders: Vec<tokio::sync::mpsc::UnboundedSender<QueuedUrl>>,
    num_shards: usize,
    global_frontier_size: Arc<AtomicUsize>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
}

impl FrontierDispatcher {
    pub(crate) fn new(num_shards: usize) -> FrontierDispatcherNew {
        let mut senders = Vec::with_capacity(num_shards);
        let mut receivers = Vec::with_capacity(num_shards);
        let mut control_senders = Vec::with_capacity(num_shards);
        let mut control_receivers = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);

            let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel();
            control_senders.push(ctrl_tx);
            control_receivers.push(ctrl_rx);
        }

        let global_frontier_size = Arc::new(AtomicUsize::new(0));
        let backpressure_semaphore = Arc::new(tokio::sync::Semaphore::new(GLOBAL_FRONTIER_SIZE_LIMIT));

        let dispatcher = Self {
            shard_senders: senders,
            num_shards,
            global_frontier_size: global_frontier_size.clone(),
            backpressure_semaphore: backpressure_semaphore.clone(),
        };

        (dispatcher, receivers, control_receivers, control_senders, global_frontier_size, backpressure_semaphore)
    }

    /// Route a URL to the appropriate shard based on registrable domain + rendezvous hashing.
    /// This ensures all subdomains of a site land on the same shard for connection reuse.
    /// Applies global backpressure when frontier size exceeds limits.
    pub async fn add_links(
        &self,
        links: Vec<(String, u32, Option<String>)>,
    ) -> usize {
        let mut added_count = 0;

        for (url, depth, parent_url) in links {
            // Acquire permit but don't drop it - pass ownership to QueuedUrl
            let permit = match self.backpressure_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("Failed to acquire backpressure permit");
                    continue;
                }
            };

            let normalized_url = SitemapNode::normalize_url(&url);

            let host = match url_utils::extract_host(&normalized_url) {
                Some(h) => h,
                None => {
                    eprintln!("Failed to extract host from URL: {}", normalized_url);
                    continue;
                }
            };

            let registrable_domain = url_utils::get_registrable_domain(&host);
            let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

            let queued = QueuedUrl {
                url: normalized_url,
                depth,
                parent_url,
                permit: Some(permit),
            };

            if let Err(e) = self.shard_senders[shard_id].send(queued) {
                eprintln!("Failed to send URL to shard {}: {}", shard_id, e);
                continue;
            }

            self.global_frontier_size.fetch_add(1, AtomicOrdering::Relaxed);
            added_count += 1;
        }

        added_count
    }
}

/// Per-shard frontier - owns host_queues, ready_heap, dedup_filter.
/// Runs on single LocalSet thread, no global locks on hot path.
pub struct FrontierShard {
    shard_id: usize,
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,

    host_queues: DashMap<String, parking_lot::Mutex<VecDeque<QueuedUrl>>>,
    ready_heap: BinaryHeap<ReadyHost>,
    url_filter: BloomFilter,
    pending_urls: DashMap<String, ()>,

    host_state_cache: DashMap<String, HostState>,
    user_agent: String,
    ignore_robots: bool,

    url_receiver: UrlReceiver,
    control_receiver: ControlReceiver,
    fp_check_semaphore: Arc<tokio::sync::Semaphore>,

    global_frontier_size: Arc<AtomicUsize>,
}

impl FrontierShard {
    pub(crate) fn new(
        shard_id: usize,
        state: Arc<CrawlerState>,
        writer_thread: Arc<WriterThread>,
        user_agent: String,
        ignore_robots: bool,
        url_receiver: UrlReceiver,
        control_receiver: ControlReceiver,
        global_frontier_size: Arc<AtomicUsize>,
        _backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let url_filter = BloomFilter::with_false_pos(0.01).expected_items(10_000_000);

        Self {
            shard_id,
            state,
            writer_thread,
            host_queues: DashMap::new(),
            ready_heap: BinaryHeap::new(),
            url_filter,
            pending_urls: DashMap::new(),
            host_state_cache: DashMap::new(),
            user_agent,
            ignore_robots,
            url_receiver,
            control_receiver,
            fp_check_semaphore: Arc::new(tokio::sync::Semaphore::new(FP_CHECK_SEMAPHORE_LIMIT)),
            global_frontier_size,
        }
    }

    /// Process incoming control messages to update host state.
    /// This runs in the shard's LocalSet.
    pub async fn process_control_messages(&mut self) {
        let batch_size = 100;

        for _ in 0..batch_size {
            match self.control_receiver.try_recv() {
                Ok(ShardMsg::Finished { host, ok }) => {
                    if let Some(mut cached) = self.host_state_cache.get_mut(&host) {
                        // Decrement inflight counter
                        let _prev = cached.inflight.fetch_sub(1, AtomicOrdering::Relaxed);

                        // Update success/failure state
                        if ok {
                            cached.reset_failures();
                        } else {
                            cached.record_failure();
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    eprintln!("Shard {}: Control receiver disconnected", self.shard_id);
                    break;
                }
            }
        }
    }

    /// Process incoming URLs from the dispatcher and add them to local queues.
    /// This runs in the shard's LocalSet.
    pub async fn process_incoming_urls(&mut self, start_url_domain: &str) -> usize {
        let mut added_count = 0;
        let batch_size = 100;

        // Drain up to batch_size URLs from the receiver
        for _ in 0..batch_size {
            match self.url_receiver.try_recv() {
                Ok(queued) => {
                    if self.add_url_to_local_queue(queued, start_url_domain).await {
                        added_count += 1;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break, // No more URLs available
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    eprintln!("Shard {}: URL receiver disconnected", self.shard_id);
                    break;
                }
            }
        }

        added_count
    }

    /// Add a single URL to this shard's local queue (with deduplication).
    async fn add_url_to_local_queue(&mut self, queued: QueuedUrl, start_url_domain: &str) -> bool {
        let normalized_url = queued.url.clone();

        // Check pending set first
        if self.pending_urls.contains_key(&normalized_url) {
            return false;
        }

        let bloom_hit = self.url_filter.contains(&normalized_url);

        if bloom_hit {
            let _permit = match self.fp_check_semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    eprintln!("Shard {}: Failed to acquire semaphore for FP check", self.shard_id);
                    return false;
                }
            };

            let state_arc = Arc::clone(&self.state);
            let url_clone = normalized_url.clone();

            match tokio::task::spawn_blocking(move || {
                state_arc.contains_url(&url_clone)
            }).await {
                Ok(Ok(true)) => return false,
                Ok(Ok(false)) => {},
                Ok(Err(e)) => {
                    eprintln!("Shard {}: DB error checking URL: {}. Fail-open", self.shard_id, e);
                }
                Err(e) => {
                    eprintln!("Shard {}: Join error: {}", self.shard_id, e);
                    return false;
                }
            }
        } else {
            self.url_filter.insert(&normalized_url);
        }

        self.pending_urls.insert(normalized_url.clone(), ());

        let url_domain = match Self::extract_host(&normalized_url) {
            Some(domain) => domain,
            None => {
                self.pending_urls.remove(&normalized_url);
                return false;
            }
        };

        if !Self::is_same_domain(&url_domain, start_url_domain) {
            self.pending_urls.remove(&normalized_url);
            return false;
        }

        let node = SitemapNode::new(
            queued.url.clone(),
            normalized_url.clone(),
            queued.depth,
            queued.parent_url.clone(),
            None,
        );

        if let Err(e) = self.writer_thread.send_event_async(StateEvent::AddNodeFact(node)).await {
            eprintln!("Shard {}: Failed to send AddNodeFact: {}", self.shard_id, e);
            self.pending_urls.remove(&normalized_url);
            return false;
        }

        let host = url_domain;
        {
            let queue_mutex = self.host_queues
                .entry(host.clone())
                .or_insert_with(|| parking_lot::Mutex::new(VecDeque::new()));
            let mut queue = queue_mutex.lock();
            queue.push_back(queued);
        }

        let now = Instant::now();
        self.ready_heap.push(ReadyHost {
            host: host.clone(),
            ready_at: now,
        });

        true
    }
    pub async fn get_next_url(&mut self) -> Option<WorkItem> {
        loop {
            let ready_host = self.ready_heap.pop()?;

            let now = Instant::now();
            if now < ready_host.ready_at {
                self.ready_heap.push(ready_host);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let url_data = {
                if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                    let mut queue = queue_mutex.lock();
                    queue.pop_front()
                } else {
                    None
                }
            };

            if let Some(queued) = url_data {
                let host_state = if let Some(cached) = self.host_state_cache.get(&ready_host.host) {
                    cached.clone()
                } else {
                    let state_arc = Arc::clone(&self.state);
                    let host_clone = ready_host.host.clone();
                    let cache_clone = self.host_state_cache.clone();

                    tokio::task::spawn_blocking(move || {
                        if let Ok(Some(state)) = state_arc.get_host_state(&host_clone) {
                            cache_clone.insert(host_clone, state);
                        }
                    });

                    HostState::new(ready_host.host.clone())
                };

                self.pending_urls.remove(&queued.url);
                self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);

                // Permit is now owned by queued and will be returned to caller
                // Check if host is in backoff period
                if !host_state.is_ready() {
                    self.pending_urls.insert(queued.url.clone(), ());

                    if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                        let mut queue = queue_mutex.lock();
                        queue.push_front(queued);
                    }

                    self.global_frontier_size.fetch_add(1, AtomicOrdering::Relaxed);

                    let backoff_secs = host_state.backoff_until_secs.saturating_sub(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                    );
                    let backoff_until = Instant::now() + Duration::from_secs(backoff_secs);

                    self.ready_heap.push(ReadyHost {
                        host: ready_host.host,
                        ready_at: backoff_until,
                    });
                    continue;
                }

                // Check per-host concurrency limit to prevent thrashing
                let current_inflight = host_state.inflight.load(std::sync::atomic::Ordering::Relaxed);
                if current_inflight >= host_state.max_inflight {
                    // Host at capacity, requeue for later
                    self.pending_urls.insert(queued.url.clone(), ());

                    if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                        let mut queue = queue_mutex.lock();
                        queue.push_front(queued);
                    }

                    self.global_frontier_size.fetch_add(1, AtomicOrdering::Relaxed);

                    // Check again soon (50ms)
                    self.ready_heap.push(ReadyHost {
                        host: ready_host.host,
                        ready_at: Instant::now() + Duration::from_millis(50),
                    });
                    continue;
                }

                if !self.ignore_robots {
                    if let Some(ref robots_txt) = host_state.robots_txt {
                        let mut matcher = DefaultMatcher::default();
                        if !matcher.one_agent_allowed_by_robots(robots_txt, &self.user_agent, &queued.url) {
                            continue;
                        }
                    }
                }

                // Increment inflight counter before returning URL
                host_state.inflight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Schedule next crawl time
                let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);

                // Update cache with incremented inflight count
                self.host_state_cache.insert(ready_host.host.clone(), host_state);
                let next_ready = Instant::now() + crawl_delay;

                // Check if host has more URLs
                let host_has_more = self.host_queues
                    .get(&ready_host.host)
                    .is_some_and(|q_mutex| !q_mutex.lock().is_empty());

                if host_has_more {
                    self.ready_heap.push(ReadyHost {
                        host: ready_host.host.clone(),
                        ready_at: next_ready,
                    });
                }

                // Extract the permit from queued and return it
                let permit = match queued.permit {
                    Some(p) => p,
                    None => {
                        eprintln!("Shard {}: QueuedUrl missing permit, skipping", self.shard_id);
                        continue;
                    }
                };
                return Some((ready_host.host, queued.url, queued.depth, queued.parent_url, permit));
            } else {
                continue;
            }
        }
    }

    /// Extract the host from a URL
    fn extract_host(url: &str) -> Option<String> {
        url_utils::extract_host(url)
    }

    /// Check if two domains are the same
    fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
        url_utils::is_same_domain(url_domain, base_domain)
    }
}

/// Wrapper around sharded frontier that provides a unified interface compatible with legacy Frontier.
/// Uses work-stealing from shards via thread-local channels.
pub struct ShardedFrontier {
    dispatcher: Arc<FrontierDispatcher>,
    work_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<WorkItem>>>,
    work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>,
    control_senders: Vec<ControlSender>,
}

impl ShardedFrontier {
    pub fn new(dispatcher: FrontierDispatcher, control_senders: Vec<ControlSender>) -> Self {
        let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            dispatcher: Arc::new(dispatcher),
            work_rx: Arc::new(tokio::sync::Mutex::new(work_rx)),
            work_tx,
            control_senders,
        }
    }

    pub async fn add_links(&self, links: Vec<(String, u32, Option<String>)>) -> usize {
        self.dispatcher.add_links(links).await
    }

    pub async fn get_next_url(&self) -> Option<WorkItem> {
        let mut rx = self.work_rx.lock().await;
        rx.recv().await
    }

    pub fn record_success(&self, host: &str, latency_ms: u64) {
        self.send_finish_message(host, true, latency_ms);
    }

    pub fn record_failure(&self, host: &str, latency_ms: u64) {
        self.send_finish_message(host, false, latency_ms);
    }

    /// Send a finish message to the appropriate shard based on host
    fn send_finish_message(&self, host: &str, ok: bool, _latency_ms: u64) {
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.control_senders.len());

        let msg = ShardMsg::Finished {
            host: host.to_string(),
            ok,
        };

        if let Err(e) = self.control_senders[shard_id].send(msg) {
            eprintln!("Failed to send finish message to shard {}: {}", shard_id, e);
        }
    }

    pub fn set_robots_txt(&self, _host: &str, _robots_txt: String) {
        // Shards handle this internally via their own host_state_cache
    }

    pub fn is_empty(&self) -> bool {
        self.dispatcher.global_frontier_size.load(AtomicOrdering::Relaxed) == 0
    }

    pub fn stats(&self) -> FrontierStats {
        let total_queued = self.dispatcher.global_frontier_size.load(AtomicOrdering::Relaxed);
        FrontierStats {
            total_hosts: 0, // TODO: aggregate from shards
            total_queued,
            hosts_with_work: 0,
            hosts_in_backoff: 0,
        }
    }

    pub fn work_tx(&self) -> tokio::sync::mpsc::UnboundedSender<WorkItem> {
        self.work_tx.clone()
    }
}

/// Legacy type alias for backward compatibility with lib.rs exports.
/// All functionality has been migrated to ShardedFrontier.
#[deprecated(note = "Use ShardedFrontier instead")]
pub type Frontier = ShardedFrontier;

#[derive(Debug, Clone)]
pub struct FrontierStats {
    pub total_hosts: usize,
    pub total_queued: usize,
    pub hosts_with_work: usize,
    pub hosts_in_backoff: usize,
}

impl std::fmt::Display for FrontierStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Frontier: {} hosts, {} queued URLs, {} with work, {} in backoff",
            self.total_hosts, self.total_queued, self.hosts_with_work, self.hosts_in_backoff
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ready_host_ordering() {
        let now = Instant::now();
        let host1 = ReadyHost {
            host: "a.com".to_string(),
            ready_at: now,
        };
        let host2 = ReadyHost {
            host: "b.com".to_string(),
            ready_at: now + Duration::from_secs(1),
        };

        // host1 should precede host2 because it is ready sooner.
        assert!(host1 > host2); // In a min-heap, "greater" means higher priority.
    }
}
