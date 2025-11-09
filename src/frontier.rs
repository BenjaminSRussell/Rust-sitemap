use dashmap::DashMap;
use fastbloom::BloomFilter;
use robotstxt::DefaultMatcher;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::network::HttpClient;
use crate::robots;
use crate::state::{CrawlerState, HostState, SitemapNode, StateEvent};
use crate::url_utils;
use crate::writer_thread::WriterThread;

// Default permit limits - can be overridden via environment variables
// Optimized for high-volume crawling based on stress testing (see stress_tests/README.md)
const DEFAULT_FP_CHECK_SEMAPHORE_LIMIT: usize = 512;  // Was 32 - increased 16x for high URL volume
const DEFAULT_ROBOTS_FETCH_SEMAPHORE_LIMIT: usize = 64;  // Was 6 - increased 10x for many hosts
const DEFAULT_GLOBAL_FRONTIER_SIZE_LIMIT: usize = 5_000_000;  // Was 1M - increased 5x for large crawls
const DEFAULT_READY_HEAP_SIZE_LIMIT: usize = 500_000;  // Was 100k - increased 5x for many hosts
const BLOOM_FP_RATE: f64 = 0.01; // 1% FP keeps dedup fast without wasting memory.

// Helper function to get permit limits from environment or defaults
fn get_fp_check_limit() -> usize {
    std::env::var("FP_CHECK_SEMAPHORE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_FP_CHECK_SEMAPHORE_LIMIT)
}

fn get_robots_fetch_limit() -> usize {
    std::env::var("ROBOTS_FETCH_SEMAPHORE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROBOTS_FETCH_SEMAPHORE_LIMIT)
}

fn get_global_frontier_size_limit() -> usize {
    std::env::var("GLOBAL_FRONTIER_SIZE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_GLOBAL_FRONTIER_SIZE_LIMIT)
}

fn get_ready_heap_size_limit() -> usize {
    std::env::var("READY_HEAP_SIZE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_READY_HEAP_SIZE_LIMIT)
}

type UrlReceiver = tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>;
type WorkItem = (String, String, u32, Option<String>, FrontierPermit);
type FrontierDispatcherNew = (
    FrontierDispatcher,
    Vec<UrlReceiver>,
    Arc<AtomicUsize>,
    Arc<tokio::sync::Semaphore>,
);

#[derive(Debug)]
pub(crate) struct QueuedUrl {
    pub url: String,
    pub depth: u32,
    pub parent_url: Option<String>,
    pub permit: FrontierPermit,
}

/// Manages a permit from the global frontier semaphore, decrementing the counter when dropped.
pub(crate) struct FrontierPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    global_frontier_size: Arc<AtomicUsize>,
}

impl FrontierPermit {
    /// Creates a new `FrontierPermit`.
    pub(crate) fn new(
        permit: tokio::sync::OwnedSemaphorePermit,
        global_frontier_size: Arc<AtomicUsize>,
    ) -> Self {
        // INCREMENT the counter when creating a permit (was missing - caused underflow!)
        global_frontier_size.fetch_add(1, AtomicOrdering::Relaxed);
        Self {
            _permit: permit,
            global_frontier_size,
        }
    }
}

impl std::fmt::Debug for FrontierPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontierPermit").finish()
    }
}

impl Drop for FrontierPermit {
    fn drop(&mut self) {
        self.global_frontier_size
            .fetch_sub(1, AtomicOrdering::Relaxed);
    }
}


/// A host ready for crawling, ordered by `ready_at` time.
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

/// Dispatches URLs to shards and manages global backpressure.
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

        for _ in 0..num_shards {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);
        }

        let global_frontier_size = Arc::new(AtomicUsize::new(0));
        let frontier_size_limit = get_global_frontier_size_limit();
        let backpressure_semaphore =
            Arc::new(tokio::sync::Semaphore::new(frontier_size_limit));

        let dispatcher = Self {
            shard_senders: senders,
            num_shards,
            global_frontier_size: global_frontier_size.clone(),
            backpressure_semaphore: backpressure_semaphore.clone(),
        };

        (
            dispatcher,
            receivers,
            global_frontier_size,
            backpressure_semaphore,
        )
    }

    /// Adds links to the frontier, routing them to appropriate shards.
    pub async fn add_links(&self, links: Vec<(String, u32, Option<String>)>) -> usize {
        let add_links_start = std::time::Instant::now();
        let mut added_count = 0;
        let total_links = links.len();

        let frontier_size_limit = get_global_frontier_size_limit();
        for (url, depth, parent_url) in links {
            // Check available permits and warn if approaching limit
            let available = self.backpressure_semaphore.available_permits();
            if available < frontier_size_limit / 10 {
                eprintln!(
                    "WARNING: Frontier approaching capacity limit ({} of {} permits available)",
                    available, frontier_size_limit
                );
            }

            // Acquire permit with timeout to prevent indefinite blocking
            let owned_permit = match tokio::time::timeout(
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
                    eprintln!(
                        "Timeout acquiring backpressure permit after 30s (frontier may be full)"
                    );
                    continue;
                }
            };

            let permit = FrontierPermit::new(owned_permit, Arc::clone(&self.global_frontier_size));

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
                permit,
            };

            if let Err(e) = self.shard_senders[shard_id].send(queued) {
                eprintln!("Failed to send URL to shard {}: {}", shard_id, e);
                // The permit in the failed queued URL will be dropped here, decrementing the counter.
                continue;
            }

            added_count += 1;
        }

        // Removed timing debug output for production
        added_count
    }
}

/// A single shard of the frontier, processing URLs for a subset of hosts.
pub struct FrontierShard {
    shard_id: usize,
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,
    http: Arc<HttpClient>,

    host_queues: DashMap<String, parking_lot::Mutex<VecDeque<QueuedUrl>>>,
    ready_heap: BinaryHeap<ReadyHost>,
    /// CRITICAL FIX: Track which hosts are currently in ready_heap to prevent duplicates
    hosts_in_heap: std::collections::HashSet<String>,
    url_filter: BloomFilter,
    url_filter_count: AtomicUsize, // Track bloom filter insertions to detect overflow
    pending_urls: DashMap<String, ()>,

    /// CRITICAL FIX: Shared reference to host state cache for direct access from ShardedFrontier
    host_state_cache: Arc<DashMap<String, HostState>>,
    hosts_fetching_robots: DashMap<String, std::time::Instant>, // Track when fetch started for timeout
    user_agent: String,
    ignore_robots: bool,

    url_receiver: UrlReceiver,
    work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>, // Send work to crawler
    fp_check_semaphore: Arc<tokio::sync::Semaphore>,
    robots_fetch_semaphore: Arc<tokio::sync::Semaphore>,

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
        work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>,
        global_frontier_size: Arc<AtomicUsize>,
        _backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let url_filter = BloomFilter::with_false_pos(BLOOM_FP_RATE)
            .expected_items(Config::BLOOM_FILTER_EXPECTED_ITEMS);

        Self {
            shard_id,
            state,
            writer_thread,
            http,
            host_queues: DashMap::new(),
            ready_heap: BinaryHeap::new(),
            hosts_in_heap: std::collections::HashSet::new(),
            url_filter,
            url_filter_count: AtomicUsize::new(0),
            pending_urls: DashMap::new(),
            host_state_cache: Arc::new(DashMap::new()),
            hosts_fetching_robots: DashMap::new(),
            user_agent,
            ignore_robots,
            url_receiver,
            work_tx,
            fp_check_semaphore: Arc::new(tokio::sync::Semaphore::new(get_fp_check_limit())),
            robots_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(
                get_robots_fetch_limit(),
            )),
            global_frontier_size,
        }
    }

    /// Returns a reference to the host state cache for direct access (bypasses control channels)
    pub fn get_host_state_cache(&self) -> Arc<DashMap<String, HostState>> {
        Arc::clone(&self.host_state_cache)
    }

    /// Pushes a `ReadyHost` to the ready heap, with a size limit.
    fn push_ready_host(&mut self, ready_host: ReadyHost) {
        // CRITICAL FIX: Check if host is already in heap to prevent duplicates
        if self.hosts_in_heap.contains(&ready_host.host) {
            // Host already in heap, don't add duplicate
            return;
        }

        let ready_heap_limit = get_ready_heap_size_limit();
        if self.ready_heap.len() >= ready_heap_limit {
            eprintln!(
                "WARNING: Shard {} ready_heap at capacity ({} hosts), dropping host {}",
                self.shard_id, ready_heap_limit, ready_host.host
            );
            // Drop the host - extreme backpressure situation
            return;
        }

        self.hosts_in_heap.insert(ready_host.host.clone());
        self.ready_heap.push(ready_host);
    }

    /// Processes incoming control messages to update host states.
    /// NOTE: This is a no-op now - control channels are bypassed in favor of direct state access.
    /// Kept for API compatibility with main.rs worker loops.
    pub async fn process_control_messages(&mut self) {
        // No-op: Direct state access via ShardedFrontier methods (record_success, record_failed, record_completed)
        // replaces the broken async control channel pattern.
    }

    /// Processes incoming URLs from the dispatcher and adds them to local queues.
    pub async fn process_incoming_urls(&mut self, start_url_domain: &str) -> usize {
        let process_start = std::time::Instant::now();
        let mut added_count = 0;
        let batch_size = 100;

        // Pull a batch so we amortize database lookups and semaphore work.
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

        // Track Bloom-filter hits so we only touch storage for likely duplicates.
        let mut urls_needing_check = Vec::new();
        let mut url_indices = Vec::new(); // Record indexes so we can drop confirmed duplicates.

        for (idx, queued) in urls_to_process.iter().enumerate() {
            let normalized_url = &queued.url;

            // Pending URLs already live in local queues, so skip duplicates immediately.
            if self.pending_urls.contains_key(normalized_url) {
                continue;
            }

            if self.url_filter.contains(normalized_url) {
                // Bloom hits need a database check to confirm the duplicate.
                urls_needing_check.push(normalized_url.clone());
                url_indices.push(idx);
            }
        }

        // Verify all suspected duplicates in one blocking call to keep Tokio responsive.
        let checked_urls_set: std::collections::HashSet<String> = if !urls_needing_check.is_empty()
        {
            let _permit = match self.fp_check_semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    eprintln!(
                        "Shard {}: Failed to acquire semaphore for batch FP check",
                        self.shard_id
                    );
                    // Without the semaphore we cannot safely verify, so drop the batch.
                    for _queued in urls_to_process {
                        self.global_frontier_size
                            .fetch_sub(1, AtomicOrdering::Relaxed);
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
            })
            .await
            {
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
                // The permit will be dropped here, decrementing the counter.
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

            if self
                .add_url_to_local_queue_unchecked(queued, start_url_domain)
                .await
            {
                added_count += 1;
            }
        }

        // Removed timing debug output for production
        added_count
    }

    /// Adds a URL to the local queue without bloom filter/DB checks.
    async fn add_url_to_local_queue_unchecked(
        &mut self,
        queued: QueuedUrl,
        start_url_domain: &str,
    ) -> bool {
        let normalized_url = queued.url.clone();

        // Check pending set (shouldn't happen with batch processing, but safe to check)
        if self.pending_urls.contains_key(&normalized_url) {
            // The permit will be dropped here, decrementing the counter.
            return false;
        }

        self.pending_urls.insert(normalized_url.clone(), ());

        let url_domain = match Self::extract_host(&normalized_url) {
            Some(domain) => domain,
            None => {
                self.pending_urls.remove(&normalized_url);
                // The permit will be dropped here, decrementing the counter.
                return false;
            }
        };

        if !Self::is_same_domain(&url_domain, start_url_domain) {
            self.pending_urls.remove(&normalized_url);
            // The permit will be dropped here, decrementing the counter.
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
            // The permit will be dropped here, decrementing the counter.
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
            if self.ready_heap.is_empty() {
                return None;
            }
            let ready_host = self.ready_heap.pop()?;
            // CRITICAL FIX: Remove host from tracking set when popping
            self.hosts_in_heap.remove(&ready_host.host);

            let now = Instant::now();
            if now < ready_host.ready_at {
                self.push_ready_host(ready_host);
                tokio::time::sleep(Duration::from_millis(Config::LOOP_YIELD_DELAY_MS)).await;
                continue;
            }

            let url_data = {
                // Extract result and cleanup flag from the queue
                let (result_opt, should_remove) = if let Some(queue_mutex) =
                    self.host_queues.get(&ready_host.host)
                {
                    // Try to lock with timeout to avoid indefinite blocking
                    let mut attempts = 0;
                    let result = loop {
                        if let Some(mut guard) = queue_mutex.try_lock() {
                            let result = guard.pop_front();
                            let is_empty = guard.is_empty();
                            drop(guard); // Release lock before breaking
                            break (result, is_empty);
                        }

                        attempts += 1;
                        if attempts >= 100 {
                            eprintln!(
                                "Shard {}: TIMEOUT waiting for lock on host {} after {} attempts",
                                self.shard_id, ready_host.host, attempts
                            );
                            break (None, false);
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                    };

                    // Drop the DashMap reference BEFORE trying to remove
                    drop(queue_mutex);
                    result
                } else {
                    (None, false)
                };

                // Remove empty queue to prevent memory leak (now safe because we dropped the reference)
                if should_remove {
                    self.host_queues.remove(&ready_host.host);
                }

                result_opt
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
                // C2: Don't decrement counter here - wait until successful send

                // Check if host has permanently failed (exceeded max failure threshold)
                if host_state.is_permanently_failed() {
                    // Drop this URL - don't requeue it. The permit will be automatically returned.
                    eprintln!(
                        "[BLOCKED] Permanently skipping {} from failed host {} ({} consecutive failures)",
                        queued.url, ready_host.host, host_state.failures
                    );
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

                    // Check again soon
                    self.push_ready_host(ReadyHost {
                        host: ready_host.host,
                        ready_at: Instant::now()
                            + Duration::from_millis(Config::FRONTIER_CRAWL_DELAY_MS),
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
                                continue;
                            }
                        }
                        None => {
                            // We don't have robots.txt yet, check if we should fetch it
                            const ROBOTS_FETCH_TIMEOUT_SECS: u64 = 3;

                            let should_fetch = match self
                                .hosts_fetching_robots
                                .get(&ready_host.host)
                            {
                                Some(_entry) => {
                                    // Fetch already attempted or in progress, don't retry
                                    false
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
                                    let http_clone = Arc::clone(&self.http);
                                    let host_clone = ready_host.host.clone();
                                    let writer_clone = Arc::clone(&self.writer_thread);
                                    let hosts_fetching_clone = self.hosts_fetching_robots.clone();
                                    let cache_clone = self.host_state_cache.clone();
                                    let robots_sem = Arc::clone(&self.robots_fetch_semaphore);

                                    // Spawn with timeout and panic guard to prevent memory leaks
                                    tokio::task::spawn(async move {
                                        // Acquire semaphore permit to limit concurrent robots.txt fetches
                                        let _robots_permit = match robots_sem.acquire().await {
                                            Ok(permit) => permit,
                                            Err(_) => {
                                                eprintln!("Failed to acquire robots fetch semaphore for {}", host_clone);
                                                hosts_fetching_clone.remove(&host_clone);
                                                return;
                                            }
                                        };
                                        // Use a defer-like pattern to ensure cleanup even on panic
                                        struct CleanupGuard {
                                            hosts_fetching:
                                                dashmap::DashMap<String, std::time::Instant>,
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
                                            Err(_) => None,
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

                // Schedule next crawl time
                let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);
                let next_ready = Instant::now() + crawl_delay;

                // CRITICAL FIX: Atomically get-or-insert host and increment inflight
                // Use entry API to atomically get-or-insert, then increment inflight
                {
                    let entry = self.host_state_cache.entry(ready_host.host.clone()).or_insert(host_state);
                    let _prev = entry.inflight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } // entry dropped here, releasing borrow

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

                // Send the work item to the crawler via work_tx
                let work_item = (
                    ready_host.host.clone(),
                    queued.url.clone(),
                    queued.depth,
                    queued.parent_url.clone(),
                    queued.permit,
                );
                match self.work_tx.send(work_item) {
                    Ok(_) => {
                        // C2: Successfully sent - now the FrontierPermit will be dropped when the work_item is processed
                        return Some(());
                    }
                    Err(e) => {
                        eprintln!("Shard {}: Failed to send work item: {}", self.shard_id, e);

                        // Extract the failed work_item tuple so we can recover the permit and re-queue.
                        let (_host_w, url_w, depth_w, parent_w, permit) = e.0;

                        // Unwind state: decrement inflight counter, re-add to pending, re-queue URL using the permit returned by SendError
                        if let Some(cached) = self.host_state_cache.get(&ready_host.host) {
                            cached
                                .inflight
                                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        // Re-add URL to pending set
                        self.pending_urls.insert(url_w.clone(), ());
                        // Re-queue the URL for this host with its permit restored
                        let new_queued = QueuedUrl {
                            url: url_w,
                            depth: depth_w,
                            parent_url: parent_w,
                            permit,
                        };
                        if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
                            let mut queue = queue_mutex.lock();
                            queue.push_front(new_queued);
                        }
                        continue;
                    }
                }
            } else {
                continue;
            }
        }
    }

    /// Extracts the host from a URL.
    fn extract_host(url: &str) -> Option<String> {
        url_utils::extract_host(url)
    }

    /// Checks if two domains are the same.
    fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
        url_utils::is_same_domain(url_domain, base_domain)
    }

    /// Checks if the shard has any queued URLs.
    pub fn has_queued_urls(&self) -> bool {
        !self.host_queues.is_empty()
    }

    /// Gets the next ready time for a URL to be available.
    pub fn next_ready_time(&self) -> Option<Instant> {
        self.ready_heap.peek().map(|ready_host| ready_host.ready_at)
    }
}

/// Provides a unified interface to the sharded frontier.
pub struct ShardedFrontier {
    dispatcher: Arc<FrontierDispatcher>,
    _work_tx: tokio::sync::mpsc::UnboundedSender<WorkItem>,
    /// CRITICAL FIX: Direct access to host state caches for bypassing broken control channels
    host_state_caches: Vec<Arc<DashMap<String, HostState>>>,
    num_shards: usize,
}

/// Receiver for work items from the frontier.
pub type WorkReceiver = tokio::sync::mpsc::UnboundedReceiver<WorkItem>;

impl ShardedFrontier {
    pub fn new(
        dispatcher: FrontierDispatcher,
        host_state_caches: Vec<Arc<DashMap<String, HostState>>>,
    ) -> (Self, WorkReceiver) {
        let (work_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
        let num_shards = host_state_caches.len();
        let frontier = Self {
            dispatcher: Arc::new(dispatcher),
            _work_tx: work_tx,
            host_state_caches,
            num_shards,
        };
        (frontier, work_rx)
    }

    /// Adds a list of links to the frontier.
    pub async fn add_links(&self, links: Vec<(String, u32, Option<String>)>) -> usize {
        self.dispatcher.add_links(links).await
    }

    /// Records a successful crawl for a given host.
    pub fn record_success(&self, host: &str, _latency_ms: u64) {
        // CRITICAL FIX: Directly decrement inflight counter instead of using broken control channels
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id) {
            if let Some(mut cached) = host_state_cache.get_mut(host) {
                let _prev = cached.inflight.fetch_sub(1, AtomicOrdering::Relaxed);
                cached.reset_failures();
            }
        }
    }

    /// Records a failed crawl for a given host.
    pub fn record_failed(&self, host: &str, _latency_ms: u64) {
        // CRITICAL FIX: Directly decrement inflight counter instead of using broken control channels
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id) {
            if let Some(mut cached) = host_state_cache.get_mut(host) {
                let _prev = cached.inflight.fetch_sub(1, AtomicOrdering::Relaxed);
                cached.record_failure();
            }
        }
    }

    /// CRITICAL FIX: Records task completion (decrements inflight) without incrementing failure count.
    /// Used for transient errors (timeouts, network errors) that shouldn't count towards host blocking.
    pub fn record_completed(&self, host: &str, _latency_ms: u64) {
        // CRITICAL FIX: Directly decrement inflight counter without touching failure count
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id) {
            if let Some(cached) = host_state_cache.get_mut(host) {
                let _prev = cached.inflight.fetch_sub(1, AtomicOrdering::Relaxed);
                // Don't touch failure count - that's the whole point of this method
            }
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

        assert_eq!(heap.pop().unwrap().host, host_early.host);
        assert_eq!(heap.pop().unwrap().host, host_middle.host);
        assert_eq!(heap.pop().unwrap().host, host_late.host);
        assert!(heap.is_empty());
    }

}