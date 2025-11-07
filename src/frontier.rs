use dashmap::DashMap;
use fastbloom::BloomFilter;
use robotstxt::DefaultMatcher;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::network::HttpClient;
use crate::robots;
use crate::state::{CrawlerState, HostState, SitemapNode, StateEvent};
use crate::url_utils;
use crate::writer_thread::WriterThread;

const FP_CHECK_SEMAPHORE_LIMIT: usize = 32;
const GLOBAL_FRONTIER_SIZE_LIMIT: usize = 1_000_000;
const READY_HEAP_SIZE_LIMIT: usize = 100_000;  // Max hosts in ready heap
const BLOOM_FP_RATE: f64 = 0.01; // 1% FP acceptable for dedup

type UrlReceiver = tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>;
type ControlReceiver = tokio::sync::mpsc::UnboundedReceiver<ShardMsg>;
type ControlSender = tokio::sync::mpsc::UnboundedSender<ShardMsg>;
type WorkItem = (
    String,
    String,
    u32,
    Option<String>,
    tokio::sync::OwnedSemaphorePermit,
);
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
    Finished { host: String, ok: bool },
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
        other.ready_at.cmp(&self.ready_at) // Min-heap
    }
}

/// Routes URLs to shards and applies global backpressure.
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
        let backpressure_semaphore =
            Arc::new(tokio::sync::Semaphore::new(GLOBAL_FRONTIER_SIZE_LIMIT));

        let dispatcher = Self {
            shard_senders: senders,
            num_shards,
            global_frontier_size: global_frontier_size.clone(),
            backpressure_semaphore: backpressure_semaphore.clone(),
        };

        (
            dispatcher,
            receivers,
            control_receivers,
            control_senders,
            global_frontier_size,
            backpressure_semaphore,
        )
    }

    /// Routes a URL to the appropriate shard, ensuring subdomains land on the same shard.
    pub async fn add_links(&self, links: Vec<(String, u32, Option<String>)>) -> usize {
        let mut added_count = 0;

        for (url, depth, parent_url) in links {
            // Check available permits and warn if approaching limit
            let available = self.backpressure_semaphore.available_permits();
            if available < GLOBAL_FRONTIER_SIZE_LIMIT / 10 {
                eprintln!(
                    "WARNING: Frontier approaching capacity limit ({} of {} permits available)",
                    available, GLOBAL_FRONTIER_SIZE_LIMIT
                );
            }

            // Acquire permit with timeout to prevent indefinite blocking
            let permit = match tokio::time::timeout(
                Duration::from_secs(30),
                self.backpressure_semaphore.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => {
                    eprintln!("Failed to acquire backpressure permit: semaphore closed");
                    continue;
                }
                Err(_) => {
                    eprintln!("Timeout acquiring backpressure permit after 30s (frontier may be full)");
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
                // Note: The permit in the failed queued URL (inside e.0) will be dropped here,
                // which correctly returns it to the semaphore.
                // We don't increment global_frontier_size since the URL never entered the frontier.
                continue;
            }

            self.global_frontier_size
                .fetch_add(1, AtomicOrdering::Relaxed);
            added_count += 1;
        }

        added_count
    }
}

/// Per-shard frontier that runs on a single thread.
pub struct FrontierShard {
    shard_id: usize,
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,
    http: Arc<HttpClient>,

    host_queues: DashMap<String, parking_lot::Mutex<VecDeque<QueuedUrl>>>,
    ready_heap: BinaryHeap<ReadyHost>,
    url_filter: BloomFilter,
    url_filter_count: AtomicUsize,  // Track bloom filter insertions to detect overflow
    pending_urls: DashMap<String, ()>,

    host_state_cache: DashMap<String, HostState>,
    hosts_fetching_robots: DashMap<String, std::time::Instant>, // Track when fetch started for timeout
    user_agent: String,
    ignore_robots: bool,

    url_receiver: UrlReceiver,
    control_receiver: ControlReceiver,
    work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>, // Send work to crawler
    fp_check_semaphore: Arc<tokio::sync::Semaphore>,

    global_frontier_size: Arc<AtomicUsize>,
}

impl FrontierShard {
    pub(crate) fn new(
        shard_id: usize,
        state: Arc<CrawlerState>,
        writer_thread: Arc<WriterThread>,
        http: Arc<HttpClient>,
        user_agent: String,
        ignore_robots: bool,
        url_receiver: UrlReceiver,
        control_receiver: ControlReceiver,
        work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>,
        global_frontier_size: Arc<AtomicUsize>,
        _backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let url_filter = BloomFilter::with_false_pos(BLOOM_FP_RATE).expected_items(10_000_000);

        Self {
            shard_id,
            state,
            writer_thread,
            http,
            host_queues: DashMap::new(),
            ready_heap: BinaryHeap::new(),
            url_filter,
            url_filter_count: AtomicUsize::new(0),
            pending_urls: DashMap::new(),
            host_state_cache: DashMap::new(),
            hosts_fetching_robots: DashMap::new(),
            user_agent,
            ignore_robots,
            url_receiver,
            control_receiver,
            work_tx,
            fp_check_semaphore: Arc::new(tokio::sync::Semaphore::new(FP_CHECK_SEMAPHORE_LIMIT)),
            global_frontier_size,
        }
    }

    /// Safely push to ready_heap with bounds checking to prevent unbounded growth
    fn push_ready_host(&mut self, ready_host: ReadyHost) {
        if self.ready_heap.len() >= READY_HEAP_SIZE_LIMIT {
            eprintln!(
                "WARNING: Shard {} ready_heap at capacity ({} hosts), dropping host {}",
                self.shard_id,
                READY_HEAP_SIZE_LIMIT,
                ready_host.host
            );
            // Drop the host - extreme backpressure situation
            return;
        }
        self.ready_heap.push(ready_host);
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

        // Collect URLs to process
        let mut urls_to_process = Vec::new();
        for _ in 0..batch_size {
            match self.url_receiver.try_recv() {
                Ok(queued) => urls_to_process.push(queued),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    eprintln!("Shard {}: URL receiver disconnected", self.shard_id);
                    break;
                }
            }
        }

        if urls_to_process.is_empty() {
            return 0;
        }

        // Batch check URLs that hit the Bloom filter
        let mut urls_needing_check = Vec::new();
        let mut url_indices = Vec::new(); // Track which URLs need checking

        for (idx, queued) in urls_to_process.iter().enumerate() {
            let normalized_url = &queued.url;

            // Skip if already pending
            if self.pending_urls.contains_key(normalized_url) {
                continue;
            }

            if self.url_filter.contains(normalized_url) {
                // Bloom filter hit - need to check DB
                urls_needing_check.push(normalized_url.clone());
                url_indices.push(idx);
            }
        }

        // Batch check all URLs that need verification in a single spawn_blocking call
        let checked_urls_set: std::collections::HashSet<String> = if !urls_needing_check.is_empty() {
            let _permit = match self.fp_check_semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    eprintln!(
                        "Shard {}: Failed to acquire semaphore for batch FP check",
                        self.shard_id
                    );
                    // Reject all URLs if we can't acquire the semaphore
                    for _queued in urls_to_process {
                        self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
                    }
                    return 0;
                }
            };

            let state_arc = Arc::clone(&self.state);
            let urls_to_check = urls_needing_check.clone();

            match tokio::task::spawn_blocking(move || {
                let mut exists = std::collections::HashSet::new();
                for url in urls_to_check {
                    if let Ok(true) = state_arc.contains_url(&url) {
                        exists.insert(url);
                    }
                }
                exists
            }).await {
                Ok(set) => set,
                Err(e) => {
                    eprintln!("Shard {}: Batch check join error: {}", self.shard_id, e);
                    std::collections::HashSet::new()
                }
            }
        } else {
            std::collections::HashSet::new()
        };

        // Process each URL
        for (idx, queued) in urls_to_process.into_iter().enumerate() {
            // Check if this URL was found to already exist in the DB
            if checked_urls_set.contains(&queued.url) {
                // URL already exists - reject it
                self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
                continue;
            }

            // Add to bloom filter if not already there
            if !url_indices.contains(&idx) {
                self.url_filter.insert(&queued.url);

                // Track bloom filter usage
                let count = self.url_filter_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                const BLOOM_FILTER_CAPACITY: usize = 10_000_000;
                const WARN_THRESHOLD: usize = (BLOOM_FILTER_CAPACITY * 9) / 10;

                if count == WARN_THRESHOLD {
                    eprintln!(
                        "WARNING: Bloom filter approaching capacity ({} of {} items, 90%)",
                        count, BLOOM_FILTER_CAPACITY
                    );
                } else if count == BLOOM_FILTER_CAPACITY {
                    eprintln!(
                        "CRITICAL: Bloom filter at capacity ({} items)",
                        BLOOM_FILTER_CAPACITY
                    );
                } else if count > BLOOM_FILTER_CAPACITY && count % 1_000_000 == 0 {
                    eprintln!(
                        "WARNING: Bloom filter overflow ({} items, capacity {})",
                        count, BLOOM_FILTER_CAPACITY
                    );
                }
            }

            if self.add_url_to_local_queue_unchecked(queued, start_url_domain).await {
                added_count += 1;
            }
        }

        added_count
    }

    /// Add URL to queue without bloom filter/DB check (used after batch checking).
    async fn add_url_to_local_queue_unchecked(&mut self, queued: QueuedUrl, start_url_domain: &str) -> bool {
        // C3: Reject incoming URLs lacking a permit at queue entrance
        if queued.permit.is_none() {
            eprintln!(
                "Shard {}: Rejecting URL without permit at queue entrance: {}",
                self.shard_id, queued.url
            );
            self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
            return false;
        }

        let normalized_url = queued.url.clone();

        // Check pending set (shouldn't happen with batch processing, but safe to check)
        if self.pending_urls.contains_key(&normalized_url) {
            self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
            return false;
        }

        self.pending_urls.insert(normalized_url.clone(), ());

        let url_domain = match Self::extract_host(&normalized_url) {
            Some(domain) => domain,
            None => {
                self.pending_urls.remove(&normalized_url);
                self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
                return false;
            }
        };

        if !Self::is_same_domain(&url_domain, start_url_domain) {
            self.pending_urls.remove(&normalized_url);
            self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
            return false;
        }

        let node = SitemapNode::new(
            queued.url.clone(),
            normalized_url.clone(),
            queued.depth,
            queued.parent_url.clone(),
            None,
        );

        if let Err(e) = self
            .writer_thread
            .send_event_async(StateEvent::AddNodeFact(node))
            .await
        {
            eprintln!("Shard {}: Failed to send AddNodeFact: {}", self.shard_id, e);
            self.pending_urls.remove(&normalized_url);
            self.global_frontier_size.fetch_sub(1, AtomicOrdering::Relaxed);
            return false;
        }

        let host = url_domain;
        {
            let queue_mutex = self
                .host_queues
                .entry(host.clone())
                .or_insert_with(|| parking_lot::Mutex::new(VecDeque::new()));
            let mut queue = queue_mutex.lock();
            queue.push_back(queued);
        }

        let now = Instant::now();
        self.push_ready_host(ReadyHost {
            host: host.clone(),
            ready_at: now,
        });

        true
    }

    pub async fn get_next_url(&mut self) -> Option<()> {
        loop {
            let ready_host = self.ready_heap.pop()?;

            let now = Instant::now();
            if now < ready_host.ready_at {
                self.push_ready_host(ready_host);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let url_data = {
                if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                    let mut queue = queue_mutex.lock();
                    let result = queue.pop_front();

                    // Check if queue is now empty after pop
                    let is_empty = queue.is_empty();
                    drop(queue); // Release lock before removing from map

                    // Remove empty queue to prevent memory leak
                    if is_empty {
                        self.host_queues.remove(&ready_host.host);
                    }

                    result
                } else {
                    None
                }
            };

            if let Some(mut queued) = url_data {
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
                // C2: Don't decrement counter here - wait until successful send

                // Check if host has permanently failed (exceeded max failure threshold)
                if host_state.is_permanently_failed() {
                    // Drop this URL - don't requeue it
                    eprintln!(
                        "Skipping URL from permanently failed host {} (failures: {})",
                        ready_host.host, host_state.failures
                    );
                    // CRITICAL: Decrement counter since URL is being permanently dropped
                    self.global_frontier_size
                        .fetch_sub(1, AtomicOrdering::Relaxed);
                    // Don't add back to ready_heap - this host is blacklisted
                    continue;
                }

                // Permit is now owned by queued and will be returned to caller
                // Check if host is in backoff period
                if !host_state.is_ready() {
                    self.pending_urls.insert(queued.url.clone(), ());

                    if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                        let mut queue = queue_mutex.lock();
                        queue.push_front(queued);
                    }

                    // C2: No need to restore counter - we never decremented it

                    let backoff_secs = host_state.backoff_until_secs.saturating_sub(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    );
                    let backoff_until = Instant::now() + Duration::from_secs(backoff_secs);

                    self.push_ready_host(ReadyHost {
                        host: ready_host.host,
                        ready_at: backoff_until,
                    });
                    continue;
                }

                // Check per-host concurrency limit to prevent thrashing
                let current_inflight = host_state
                    .inflight
                    .load(std::sync::atomic::Ordering::Relaxed);
                if current_inflight >= host_state.max_inflight {
                    // Host at capacity, requeue for later
                    self.pending_urls.insert(queued.url.clone(), ());

                    if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                        let mut queue = queue_mutex.lock();
                        queue.push_front(queued);
                    }

                    // C2: No need to restore counter - we never decremented it

                    // Check again soon (50ms)
                    self.push_ready_host(ReadyHost {
                        host: ready_host.host,
                        ready_at: Instant::now() + Duration::from_millis(50),
                    });
                    continue;
                }

                if !self.ignore_robots {
                    match &host_state.robots_txt {
                        Some(robots_txt) => {
                            // We have robots.txt, check if URL is allowed
                            let mut matcher = DefaultMatcher::default();
                            if !matcher.one_agent_allowed_by_robots(
                                robots_txt,
                                &self.user_agent,
                                &queued.url,
                            ) {
                                // URL is disallowed by robots.txt, skip it permanently
                                eprintln!(
                                    "Shard {}: URL {} blocked by robots.txt",
                                    self.shard_id, queued.url
                                );
                                // CRITICAL: Decrement counter since URL is being permanently dropped
                                self.global_frontier_size
                                    .fetch_sub(1, AtomicOrdering::Relaxed);
                                continue;
                            }
                        }
                        None => {
                            // We don't have robots.txt yet, check if we should fetch it
                            const ROBOTS_FETCH_TIMEOUT_SECS: u64 = 30;

                            let should_fetch = match self
                                .hosts_fetching_robots
                                .get(&ready_host.host)
                            {
                                Some(entry) => {
                                    // Check if the fetch has timed out
                                    let elapsed = entry.value().elapsed().as_secs();
                                    if elapsed > ROBOTS_FETCH_TIMEOUT_SECS {
                                        eprintln!(
                                            "Shard {}: robots.txt fetch for {} timed out after {}s, retrying",
                                            self.shard_id, ready_host.host, elapsed
                                        );
                                        // Remove the stale entry and allow retry
                                        drop(entry);
                                        self.hosts_fetching_robots.remove(&ready_host.host);
                                        true
                                    } else {
                                        false // Fetch in progress, don't retry
                                    }
                                }
                                None => true, // No fetch in progress, start one
                            };

                            if should_fetch {
                                // Insert with current timestamp
                                if self
                                    .hosts_fetching_robots
                                    .insert(ready_host.host.clone(), Instant::now())
                                    .is_none()
                                {
                                    eprintln!(
                                        "Shard {}: Host {} missing robots.txt, fetching in background (allowing crawl)...",
                                        self.shard_id, ready_host.host
                                    );

                                    let http_clone = Arc::clone(&self.http);
                                    let host_clone = ready_host.host.clone();
                                    let writer_clone = Arc::clone(&self.writer_thread);
                                    let hosts_fetching_clone = self.hosts_fetching_robots.clone();
                                    let cache_clone = self.host_state_cache.clone();

                                    // Spawn with timeout and panic guard to prevent memory leaks
                                    tokio::task::spawn(async move {
                                        // Use a defer-like pattern to ensure cleanup even on panic
                                        struct CleanupGuard {
                                            hosts_fetching: dashmap::DashMap<String, std::time::Instant>,
                                            host: String,
                                        }
                                        impl Drop for CleanupGuard {
                                            fn drop(&mut self) {
                                                self.hosts_fetching.remove(&self.host);
                                            }
                                        }
                                        let _cleanup = CleanupGuard {
                                            hosts_fetching: hosts_fetching_clone.clone(),
                                            host: host_clone.clone(),
                                        };

                                        let robots_result = tokio::time::timeout(
                                            Duration::from_secs(ROBOTS_FETCH_TIMEOUT_SECS),
                                            robots::fetch_robots_txt(&http_clone, &host_clone),
                                        )
                                        .await;

                                        let robots_txt = match robots_result {
                                            Ok(result) => result,
                                            Err(_) => {
                                                eprintln!(
                                                    "robots.txt fetch timeout for {} after {}s",
                                                    host_clone, ROBOTS_FETCH_TIMEOUT_SECS
                                                );
                                                None
                                            }
                                        };

                                        // Store the result (even if None) so future requests respect it
                                        let event = StateEvent::UpdateHostStateFact {
                                            host: host_clone.clone(),
                                            robots_txt: robots_txt.clone(),
                                            crawl_delay_secs: None,
                                            reset_failures: false,
                                            increment_failures: false,
                                        };

                                        if let Err(e) = writer_clone.send_event_async(event).await {
                                            eprintln!(
                                                "Failed to store robots.txt for {}: {}",
                                                host_clone, e
                                            );
                                        }

                                        // Update the cache immediately so subsequent URLs are checked
                                        if let Some(mut cached) = cache_clone.get_mut(&host_clone) {
                                            cached.robots_txt = robots_txt;
                                        }

                                        // Cleanup happens automatically via CleanupGuard drop
                                    });
                                }
                            }
                            // Proceed with crawling - don't block on robots.txt
                        }
                    }
                }

                // Increment inflight counter before returning URL
                host_state
                    .inflight
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Schedule next crawl time
                let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);

                // Update cache with incremented inflight count
                self.host_state_cache
                    .insert(ready_host.host.clone(), host_state);
                let next_ready = Instant::now() + crawl_delay;

                // Check if host has more URLs
                let host_has_more = self
                    .host_queues
                    .get(&ready_host.host)
                    .is_some_and(|q_mutex| !q_mutex.lock().is_empty());

                if host_has_more {
                    self.push_ready_host(ReadyHost {
                        host: ready_host.host.clone(),
                        ready_at: next_ready,
                    });
                }

                // Note: This check should now be redundant due to C3 validation at queue entrance
                debug_assert!(queued.permit.is_some());

                // Send the work item to the crawler via work_tx
                let work_item = (
                    ready_host.host.clone(),
                    queued.url.clone(),
                    queued.depth,
                    queued.parent_url.clone(),
                    queued.permit.take().unwrap(),
                );
                eprintln!(
                    "Shard {}: Sending work item: {} (depth {})",
                    self.shard_id, queued.url, queued.depth
                );
                match self.work_tx.send(work_item) {
                    Ok(_) => {
                        // C2: Successfully sent - now decrement the counter
                        self.global_frontier_size
                            .fetch_sub(1, AtomicOrdering::Relaxed);
                        return Some(());
                    }
                    Err(e) => {
                        eprintln!("Shard {}: Failed to send work item: {}", self.shard_id, e);

                        // CRITICAL FIX: Extract the work_item from SendError to recover the permit
                        // The permit is inside e.0 (the failed work_item tuple) at position 4
                        let (_host, _url, _depth, _parent, permit) = e.0;

                        // Restore the permit to queued so it won't be dropped on the next attempt
                        queued.permit = Some(permit);

                        // C2: Send failed - no need to restore counter since we never decremented it
                        // Unwind state: decrement inflight counter, re-add to pending, re-queue URL
                        if let Some(cached) = self.host_state_cache.get(&ready_host.host) {
                            cached
                                .inflight
                                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        // Re-add URL to pending set
                        self.pending_urls.insert(queued.url.clone(), ());
                        // Re-queue the URL for this host with its permit restored
                        if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                            let mut queue = queue_mutex.lock();
                            queue.push_front(queued);
                        }
                        continue;
                    }
                }
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

    /// Check if this shard has any queued URLs (waiting for politeness delays/backoff)
    pub fn has_queued_urls(&self) -> bool {
        !self.host_queues.is_empty()
    }

    /// Get the next ready time (when the next URL will be available)
    /// Returns None if no URLs are queued
    pub fn next_ready_time(&self) -> Option<Instant> {
        self.ready_heap.peek().map(|ready_host| ready_host.ready_at)
    }
}

/// A unified interface for the sharded frontier.
pub struct ShardedFrontier {
    dispatcher: Arc<FrontierDispatcher>,
    work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>,
    control_senders: Vec<ControlSender>,
}

/// Receiver for work items from the frontier - must be used directly in tokio::select!
/// to avoid LocalSet deadlock
pub type WorkReceiver = tokio::sync::mpsc::UnboundedReceiver<WorkItem>;

impl ShardedFrontier {
    pub fn new(
        dispatcher: FrontierDispatcher,
        control_senders: Vec<ControlSender>,
    ) -> (Self, WorkReceiver) {
        let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
        let frontier = Self {
            dispatcher: Arc::new(dispatcher),
            work_tx,
            control_senders,
        };
        (frontier, work_rx)
    }

    pub async fn add_links(&self, links: Vec<(String, u32, Option<String>)>) -> usize {
        self.dispatcher.add_links(links).await
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
        let shard_id =
            url_utils::rendezvous_shard_id(&registrable_domain, self.control_senders.len());

        let msg = ShardMsg::Finished {
            host: host.to_string(),
            ok,
        };

        if let Err(e) = self.control_senders[shard_id].send(msg) {
            eprintln!("Failed to send finish message to shard {}: {}", shard_id, e);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dispatcher
            .global_frontier_size
            .load(AtomicOrdering::Relaxed)
            == 0
    }

    pub fn stats(&self) -> FrontierStats {
        let total_queued = self
            .dispatcher
            .global_frontier_size
            .load(AtomicOrdering::Relaxed);
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

    #[test]
    fn test_ready_host_min_heap_property() {
        let now = Instant::now();
        let host_early = ReadyHost {
            host: "early.com".to_string(),
            ready_at: now,
        };
        let host_middle = ReadyHost {
            host: "middle.com".to_string(),
            ready_at: now + Duration::from_secs(5),
        };
        let host_late = ReadyHost {
            host: "late.com".to_string(),
            ready_at: now + Duration::from_secs(10),
        };

        let mut heap = BinaryHeap::new();
        heap.push(host_middle.clone());
        heap.push(host_late.clone());
        heap.push(host_early.clone());

        // When popped, they should come out in min-heap order (earliest ready_at first)
        assert_eq!(heap.pop().unwrap().host, host_early.host);
        assert_eq!(heap.pop().unwrap().host, host_middle.host);
        assert_eq!(heap.pop().unwrap().host, host_late.host);
        assert!(heap.is_empty());
    }}
