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

const BLOOM_FP_RATE: f64 = 0.01; // 1% false positive rate.

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
        // INCREMENT the counter when creating a permit with overflow protection
        let _ = global_frontier_size.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |val| val.checked_add(1)
        );
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
        // Decrement with underflow protection
        let _ = self.global_frontier_size.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |val| val.checked_sub(1)
        );
    }
}


/// A host ready for crawling, ordered by `ready_at` time.
#[derive(Debug, Clone)]
pub struct ReadyHost {
    pub host: String,
    pub ready_at: Instant,
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
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..num_shards {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);
        }

        let global_frontier_size = Arc::new(AtomicUsize::new(0));
        // No artificial limit - use a very large semaphore
        let backpressure_semaphore =
            Arc::new(tokio::sync::Semaphore::new(usize::MAX));

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
        let mut added_count = 0;
        for (url, depth, parent_url) in links {
            // Acquire permit - no limits
            let owned_permit = match self.backpressure_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("Failed to acquire backpressure permit: semaphore closed");
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
    pub shard_id: usize,
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
    shared_stats: SharedFrontierStats,
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
        shared_stats: SharedFrontierStats,
    ) -> Self {
        // Initialize fresh frontier state
        let host_queues = DashMap::new();
        let ready_heap = BinaryHeap::new();
        let url_filter = BloomFilter::with_false_pos(BLOOM_FP_RATE)
            .expected_items(Config::BLOOM_FILTER_EXPECTED_ITEMS);
        let url_filter_count = AtomicUsize::new(0);
        let hosts_in_heap = std::collections::HashSet::new();

        Self {
            shard_id,
            state,
            writer_thread,
            http,
            host_queues,
            ready_heap,
            hosts_in_heap,
            url_filter,
            url_filter_count,
            pending_urls: DashMap::new(),
            host_state_cache: Arc::new(DashMap::new()),
            hosts_fetching_robots: DashMap::new(),
            user_agent,
            ignore_robots,
            url_receiver,
            work_tx,
            fp_check_semaphore: Arc::new(tokio::sync::Semaphore::new(512)),
            robots_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(64)),
            global_frontier_size,
            shared_stats,
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

        self.hosts_in_heap.insert(ready_host.host.clone());
        self.ready_heap.push(ready_host);

        // Update shared stats: increment hosts_with_work with overflow protection
        let _ = self.shared_stats.hosts_with_work.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |val| val.checked_add(1)
        );
    }

    // Control channels have been removed in favor of direct state access via ShardedFrontier methods.
    // See record_success, record_failed, and record_completed in ShardedFrontier for direct state updates.

    /// Processes incoming URLs from the dispatcher and adds them to local queues.
    pub async fn process_incoming_urls(&mut self, start_url_domain: &str) -> usize {
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
                        let _ = self.global_frontier_size.fetch_update(
                            AtomicOrdering::Relaxed,
                            AtomicOrdering::Relaxed,
                            |val| val.checked_sub(1)
                        );
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
                self.url_filter_count.fetch_add(1, AtomicOrdering::Relaxed);
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

        // Update shared stats: increment total_hosts if this is a new host with overflow protection
        if !self.host_state_cache.contains_key(&host) {
            let _ = self.shared_stats.total_hosts.fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |val| val.checked_add(1)
            );
        }

        self.push_ready_host(ReadyHost {
            host: host.clone(),
            ready_at: now,
        });

        true
    }

    /// Pop a host from the ready heap and update tracking
    fn pop_ready_host(&mut self) -> Option<ReadyHost> {
        let ready_host = self.ready_heap.pop()?;

        // Remove host from tracking set when popping
        self.hosts_in_heap.remove(&ready_host.host);

        // Update shared stats: decrement hosts_with_work
        let _ = self.shared_stats.hosts_with_work.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |val| val.checked_sub(1)
        );

        Some(ready_host)
    }

    /// Check if host is ready (passed politeness delay)
    async fn check_host_ready(&mut self, ready_host: ReadyHost) -> Result<ReadyHost, ReadyHost> {
        let now = Instant::now();
        if now < ready_host.ready_at {
            self.push_ready_host(ready_host.clone());
            tokio::time::sleep(Duration::from_millis(Config::LOOP_YIELD_DELAY_MS)).await;
            Err(ready_host)
        } else {
            Ok(ready_host)
        }
    }

    /// Pop URL from host queue with timeout handling
    async fn pop_url_from_host_queue(&mut self, host: &str) -> Option<QueuedUrl> {
        let (result_opt, should_remove) = if let Some(queue_mutex) = self.host_queues.get(host) {
            let mut attempts = 0;
            let result = loop {
                if let Some(mut guard) = queue_mutex.try_lock() {
                    let result = guard.pop_front();
                    let is_empty = guard.is_empty();
                    drop(guard);
                    break (result, is_empty);
                }

                attempts += 1;
                if attempts >= 100 {
                    eprintln!(
                        "Shard {}: TIMEOUT waiting for lock on host {} after {} attempts",
                        self.shard_id, host, attempts
                    );
                    break (None, false);
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            };

            drop(queue_mutex);
            result
        } else {
            (None, false)
        };

        // Remove empty queue to prevent memory leak
        if should_remove {
            self.host_queues.remove(host);
        }

        result_opt
    }

    /// Get or load host state from cache
    fn get_or_load_host_state(&self, host: &str) -> HostState {
        if let Some(cached) = self.host_state_cache.get(host) {
            cached.clone()
        } else {
            let state_arc = Arc::clone(&self.state);
            let host_clone = host.to_string();
            let cache_clone = self.host_state_cache.clone();

            tokio::task::spawn_blocking(move || {
                if let Ok(Some(state)) = state_arc.get_host_state(&host_clone) {
                    cache_clone.insert(host_clone, state);
                }
            });

            HostState::new(host.to_string())
        }
    }

    /// Check if host is permanently failed and should be skipped
    fn check_permanently_failed(&mut self, host_state: &HostState, ready_host: &ReadyHost, queued: &QueuedUrl) -> bool {
        if !host_state.is_permanently_failed() {
            return false;
        }

        eprintln!(
            "[BLOCKED] Permanently skipping {} from failed host {} ({} consecutive failures)",
            queued.url, ready_host.host, host_state.failures
        );

        // Check if host has more URLs and re-push before continuing
        let host_has_more = self
            .host_queues
            .get(&ready_host.host)
            .is_some_and(|q_mutex| !q_mutex.lock().is_empty());

        if host_has_more {
            self.push_ready_host(ReadyHost {
                host: ready_host.host.clone(),
                ready_at: Instant::now(),
            });
        }

        true
    }

    /// Check if host is in backoff period and requeue if needed
    fn check_backoff(&mut self, host_state: &HostState, ready_host: &ReadyHost, queued: QueuedUrl) -> Result<(), ()> {
        if host_state.is_ready() {
            return Ok(());
        }

        self.pending_urls.insert(queued.url.clone(), ());

        if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
            let mut queue = queue_mutex.lock();
            queue.push_front(queued);
        }

        let backoff_secs = host_state.backoff_until_secs.saturating_sub(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
        let backoff_until = Instant::now() + Duration::from_secs(backoff_secs);

        self.push_ready_host(ReadyHost {
            host: ready_host.host.clone(),
            ready_at: backoff_until,
        });

        Err(())
    }

    /// Check per-host concurrency limit
    fn check_concurrency_limit(&mut self, host_state: &HostState, ready_host: &ReadyHost, queued: QueuedUrl) -> Result<(), ()> {
        let current_inflight = host_state.inflight.load(std::sync::atomic::Ordering::Relaxed);
        if current_inflight < host_state.max_inflight {
            return Ok(());
        }

        // Host at capacity, requeue for later
        self.pending_urls.insert(queued.url.clone(), ());

        if let Some(queue_mutex) = self.host_queues.get(&ready_host.host) {
            let mut queue = queue_mutex.lock();
            queue.push_front(queued);
        }

        // Check again soon
        self.push_ready_host(ReadyHost {
            host: ready_host.host.clone(),
            ready_at: Instant::now() + Duration::from_millis(Config::FRONTIER_CRAWL_DELAY_MS),
        });

        Err(())
    }

    fn check_robots_allowed(&self, robots_txt: &str, url: &str) -> Result<bool, String> {
        if robots_txt.trim().is_empty() {
            return Ok(true);
        }

        let is_allowed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut matcher = DefaultMatcher::default();
            matcher.one_agent_allowed_by_robots(robots_txt, &self.user_agent, url)
        }));

        match is_allowed {
            Ok(allowed) => Ok(allowed),
            Err(_) => {
                eprintln!("Shard {}: robots.txt parser panicked for URL {}, allowing by default", self.shard_id, url);
                Ok(true)
            }
        }
    }

    /// Handle robots.txt checking logic with TTL validation per RFC 9309
    fn handle_robots_check(&mut self, host_state: &HostState, ready_host: &ReadyHost, queued: &QueuedUrl) -> Result<(), ()> {
        if self.ignore_robots {
            return Ok(());
        }

        // Check if robots.txt is stale (>24 hours old or never fetched)
        if host_state.is_robots_txt_stale() {
            // Stale or missing robots.txt - trigger re-fetch
            self.spawn_robots_fetch_if_needed(&ready_host.host);

            // If we have an old cached version, use it while fetching fresh one (fail-graceful)
            // If no cache at all, allow URL (fail-open to avoid blocking on first fetch)
            match &host_state.robots_txt {
                Some(robots_txt) => {
                    // Use stale cache while re-fetching (better than blocking)
                    match self.check_robots_allowed(robots_txt, &queued.url) {
                        Ok(true) => Ok(()),
                        Ok(false) => {
                            eprintln!(
                                "Shard {}: URL {} blocked by robots.txt (using stale cache, re-fetching)",
                                self.shard_id, queued.url
                            );

                            let host_has_more = self
                                .host_queues
                                .get(&ready_host.host)
                                .is_some_and(|q_mutex| !q_mutex.lock().is_empty());

                            if host_has_more {
                                self.push_ready_host(ReadyHost {
                                    host: ready_host.host.clone(),
                                    ready_at: Instant::now(),
                                });
                            }
                            Err(())
                        }
                        Err(panic_info) => {
                            eprintln!(
                                "Shard {}: robotstxt library panicked for {} on {}, allowing URL (fail-open). Panic: {}",
                                self.shard_id, queued.url, ready_host.host, panic_info
                            );
                            Ok(())
                        }
                    }
                }
                None => {
                    // No cache at all - allow URL while fetching (fail-open)
                    Ok(())
                }
            }
        } else {
            // Fresh cache available - use it
            match &host_state.robots_txt {
                Some(robots_txt) => {
                    match self.check_robots_allowed(robots_txt, &queued.url) {
                        Ok(true) => Ok(()),
                        Ok(false) => {
                            eprintln!(
                                "Shard {}: URL {} blocked by robots.txt",
                                self.shard_id, queued.url
                            );

                            let host_has_more = self
                                .host_queues
                                .get(&ready_host.host)
                                .is_some_and(|q_mutex| !q_mutex.lock().is_empty());

                            if host_has_more {
                                self.push_ready_host(ReadyHost {
                                    host: ready_host.host.clone(),
                                    ready_at: Instant::now(),
                                });
                            }
                            Err(())
                        }
                        Err(panic_info) => {
                            eprintln!(
                                "Shard {}: robotstxt library panicked for {} on {}, allowing URL (fail-open). Panic: {}",
                                self.shard_id, queued.url, ready_host.host, panic_info
                            );
                            Ok(())
                        }
                    }
                }
                None => {
                    // This shouldn't happen (is_robots_txt_stale returns true for None), but fail-open just in case
                    self.spawn_robots_fetch_if_needed(&ready_host.host);
                    Ok(())
                }
            }
        }
    }

    /// Spawn robots.txt fetch task if not already in progress
    fn spawn_robots_fetch_if_needed(&self, host: &str) {
        const ROBOTS_FETCH_TIMEOUT_SECS: u64 = 3;

        let should_fetch = match self.hosts_fetching_robots.get(host) {
            Some(_entry) => false,
            None => true,
        };

        if !should_fetch {
            return;
        }

        if self.hosts_fetching_robots.insert(host.to_string(), Instant::now()).is_none() {
            let http_clone = Arc::clone(&self.http);
            let host_clone = host.to_string();
            let writer_clone = Arc::clone(&self.writer_thread);
            let hosts_fetching_clone = self.hosts_fetching_robots.clone();
            let cache_clone = self.host_state_cache.clone();
            let robots_sem = Arc::clone(&self.robots_fetch_semaphore);

            tokio::task::spawn(async move {
                let _robots_permit = match robots_sem.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        eprintln!("Failed to acquire robots fetch semaphore for {}", host_clone);
                        hosts_fetching_clone.remove(&host_clone);
                        return;
                    }
                };

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
                    Err(_) => None,
                };

                let event = StateEvent::UpdateHostStateFact {
                    host: host_clone.clone(),
                    robots_txt: robots_txt.clone(),
                    crawl_delay_secs: None,
                    reset_failures: false,
                    increment_failures: false,
                };

                if let Err(e) = writer_clone.send_event_async(event).await {
                    eprintln!("Failed to store robots.txt for {}: {}", host_clone, e);
                }

                if let Some(mut cached) = cache_clone.get_mut(&host_clone) {
                    cached.robots_txt = robots_txt;
                }
            });
        }
    }

    /// Prepare host for next crawl and update cache
    fn prepare_host_for_crawl(&mut self, ready_host: &ReadyHost, host_state: HostState) -> Duration {
        let entry = self.host_state_cache.entry(ready_host.host.clone()).or_insert(host_state.clone());
        let _ = entry.inflight.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |val| val.checked_add(1)
        );

        Duration::from_secs(host_state.crawl_delay_secs)
    }

    /// Send work item to crawler with error recovery
    fn send_work_item(&mut self, ready_host: &ReadyHost, queued: QueuedUrl, next_ready: Instant) -> Result<(), ()> {
        let work_item = (
            ready_host.host.clone(),
            queued.url.clone(),
            queued.depth,
            queued.parent_url.clone(),
            queued.permit,
        );

        match self.work_tx.send(work_item) {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Shard {}: CRITICAL - Failed to send work item: {}", self.shard_id, e);

                let (_host_w, url_w, depth_w, parent_w, permit) = e.0;

                // Unwind state
                if let Some(cached) = self.host_state_cache.get(&ready_host.host) {
                    let _ = cached.inflight.fetch_update(
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                        |val| val.checked_sub(1)
                    );
                }

                self.pending_urls.insert(url_w.clone(), ());

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

                self.push_ready_host(ReadyHost {
                    host: ready_host.host.clone(),
                    ready_at: next_ready,
                });

                Err(())
            }
        }
    }

    pub async fn get_next_url(&mut self) -> Option<()> {
        loop {
            // Pop host from ready heap
            if self.ready_heap.is_empty() {
                return None;
            }
            let ready_host = self.pop_ready_host()?;

            // Check if host is ready (passed politeness delay)
            let ready_host = match self.check_host_ready(ready_host).await {
                Ok(host) => host,
                Err(_) => continue,
            };

            // Pop URL from host queue
            let queued = match self.pop_url_from_host_queue(&ready_host.host).await {
                Some(q) => q,
                None => continue,
            };

            // Get or load host state
            let host_state = self.get_or_load_host_state(&ready_host.host);
            self.pending_urls.remove(&queued.url);

            // Check if host is permanently failed
            if self.check_permanently_failed(&host_state, &ready_host, &queued) {
                continue;
            }

            // Check if host is in backoff period
            if !host_state.is_ready() {
                if self.check_backoff(&host_state, &ready_host, queued).is_err() {
                    continue;
                }
                continue;
            }

            // Check per-host concurrency limit
            let current_inflight = host_state.inflight.load(std::sync::atomic::Ordering::Relaxed);
            if current_inflight >= host_state.max_inflight {
                if self.check_concurrency_limit(&host_state, &ready_host, queued).is_err() {
                    continue;
                }
                continue;
            }

            // Handle robots.txt checking
            if self.handle_robots_check(&host_state, &ready_host, &queued).is_err() {
                continue;
            }

            // Prepare host for next crawl
            let crawl_delay = self.prepare_host_for_crawl(&ready_host, host_state);
            let next_ready = Instant::now() + crawl_delay;

            // Check if host has more URLs and schedule next crawl
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

            // Send work item to crawler
            if self.send_work_item(&ready_host, queued, next_ready).is_ok() {
                return Some(());
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

    /// Get snapshot-safe references to frontier state (for future snapshot functionality)
    #[allow(dead_code)]
    pub fn snapshot_refs(
        &self,
    ) -> (
        &DashMap<String, parking_lot::Mutex<VecDeque<QueuedUrl>>>,
        &std::collections::BinaryHeap<ReadyHost>,
        &BloomFilter,
        usize,
    ) {
        (
            &self.host_queues,
            &self.ready_heap,
            &self.url_filter,
            self.url_filter_count.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

/// Shared statistics counters updated by shards in real-time
#[derive(Debug, Clone)]
pub struct SharedFrontierStats {
    pub total_hosts: Arc<AtomicUsize>,
    pub hosts_with_work: Arc<AtomicUsize>,
    pub hosts_in_backoff: Arc<AtomicUsize>,
}

impl SharedFrontierStats {
    pub fn new() -> Self {
        Self {
            total_hosts: Arc::new(AtomicUsize::new(0)),
            hosts_with_work: Arc::new(AtomicUsize::new(0)),
            hosts_in_backoff: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Provides a unified interface to the sharded frontier.
pub struct ShardedFrontier {
    dispatcher: Arc<FrontierDispatcher>,
    /// CRITICAL FIX: Direct access to host state caches for bypassing broken control channels
    host_state_caches: Vec<Arc<DashMap<String, HostState>>>,
    num_shards: usize,
    shared_stats: SharedFrontierStats,
}

/// Receiver for work items from the frontier.
pub type WorkReceiver = tokio::sync::mpsc::UnboundedReceiver<WorkItem>;

impl ShardedFrontier {
    pub fn new(
        dispatcher: FrontierDispatcher,
        host_state_caches: Vec<Arc<DashMap<String, HostState>>>,
        shared_stats: SharedFrontierStats,
    ) -> Self {
        let num_shards = host_state_caches.len();
        Self {
            dispatcher: Arc::new(dispatcher),
            host_state_caches,
            num_shards,
            shared_stats,
        }
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

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id)
            && let Some(mut cached) = host_state_cache.get_mut(host) {
                // Check if host was in backoff before reset
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let was_in_backoff = cached.backoff_until_secs > now_secs;

                let _ = cached.inflight.fetch_update(
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                    |val| val.checked_sub(1)
                );
                cached.reset_failures();

                // If host was in backoff and is now cleared, decrement counter
                let is_in_backoff = cached.backoff_until_secs > now_secs;
                if was_in_backoff && !is_in_backoff {
                    let _ = self.shared_stats.hosts_in_backoff.fetch_update(
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                        |val| val.checked_sub(1)
                    );
                }
            }
    }

    /// Records a failed crawl for a given host.
    pub fn record_failed(&self, host: &str, _latency_ms: u64) {
        // CRITICAL FIX: Directly decrement inflight counter instead of using broken control channels
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id)
            && let Some(mut cached) = host_state_cache.get_mut(host) {
                // Check if host was in backoff before failure
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let was_in_backoff = cached.backoff_until_secs > now_secs;

                let _ = cached.inflight.fetch_update(
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                    |val| val.checked_sub(1)
                );
                cached.record_failure();

                // If host entered backoff, increment counter
                let is_in_backoff = cached.backoff_until_secs > now_secs;
                if !was_in_backoff && is_in_backoff {
                    let _ = self.shared_stats.hosts_in_backoff.fetch_update(
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                        |val| val.checked_add(1)
                    );
                }
            }
    }

    /// CRITICAL FIX: Records task completion (decrements inflight) without incrementing failure count.
    /// Used for transient errors (timeouts, network errors) that shouldn't count towards host blocking.
    pub fn record_completed(&self, host: &str, _latency_ms: u64) {
        // CRITICAL FIX: Directly decrement inflight counter without touching failure count
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.num_shards);

        if let Some(host_state_cache) = self.host_state_caches.get(shard_id)
            && let Some(cached) = host_state_cache.get_mut(host) {
                let _ = cached.inflight.fetch_update(
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                    |val| val.checked_sub(1)
                );
                // Don't touch failure count - that's the whole point of this method
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

        // CRITICAL FIX: Use real-time stats from atomic counters (O(1) instead of O(N))
        let total_hosts = self.shared_stats.total_hosts.load(AtomicOrdering::Relaxed);
        let hosts_with_work = self.shared_stats.hosts_with_work.load(AtomicOrdering::Relaxed);
        let hosts_in_backoff = self.shared_stats.hosts_in_backoff.load(AtomicOrdering::Relaxed);

        FrontierStats {
            total_hosts,
            total_queued,
            hosts_with_work,
            hosts_in_backoff,
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