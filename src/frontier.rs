use dashmap::DashMap;
use fastbloom::BloomFilter;
use parking_lot::Mutex;
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
        latency_ms: u64,
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
    shard_control_senders: Vec<tokio::sync::mpsc::UnboundedSender<ShardMsg>>,
    num_shards: usize,
    global_frontier_size: Arc<AtomicUsize>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
    metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl FrontierDispatcher {
    pub(crate) fn new(num_shards: usize) -> (
        Self,
        Vec<tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>>,
        Vec<tokio::sync::mpsc::UnboundedReceiver<ShardMsg>>,
        Vec<tokio::sync::mpsc::UnboundedSender<ShardMsg>>,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Semaphore>,
    ) {
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
            shard_control_senders: control_senders.clone(),
            num_shards,
            global_frontier_size: global_frontier_size.clone(),
            backpressure_semaphore: backpressure_semaphore.clone(),
            metrics: None, // Set later via set_metrics
        };

        (dispatcher, receivers, control_receivers, control_senders, global_frontier_size, backpressure_semaphore)
    }

    pub fn set_metrics(&mut self, metrics: Arc<crate::metrics::Metrics>) {
        self.metrics = Some(metrics);
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

            // Track enqueue rate
            if let Some(ref metrics) = self.metrics {
                metrics.urls_enqueued_total.lock().inc();
            }
        }

        added_count
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
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

    url_receiver: tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>,
    control_receiver: tokio::sync::mpsc::UnboundedReceiver<ShardMsg>,
    fp_check_semaphore: Arc<tokio::sync::Semaphore>,

    global_frontier_size: Arc<AtomicUsize>,
    backpressure_semaphore: Arc<tokio::sync::Semaphore>,
}

impl FrontierShard {
    pub(crate) fn new(
        shard_id: usize,
        state: Arc<CrawlerState>,
        writer_thread: Arc<WriterThread>,
        user_agent: String,
        ignore_robots: bool,
        url_receiver: tokio::sync::mpsc::UnboundedReceiver<QueuedUrl>,
        control_receiver: tokio::sync::mpsc::UnboundedReceiver<ShardMsg>,
        global_frontier_size: Arc<AtomicUsize>,
        backpressure_semaphore: Arc<tokio::sync::Semaphore>,
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
            backpressure_semaphore,
        }
    }

    /// Process incoming control messages to update host state.
    /// This runs in the shard's LocalSet.
    pub async fn process_control_messages(&mut self) {
        let batch_size = 100;

        for _ in 0..batch_size {
            match self.control_receiver.try_recv() {
                Ok(ShardMsg::Finished { host, ok, latency_ms: _ }) => {
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
    pub async fn get_next_url(&mut self) -> Option<(String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit)> {
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
                    .map_or(false, |q_mutex| !q_mutex.lock().is_empty());

                if host_has_more {
                    self.ready_heap.push(ReadyHost {
                        host: ready_host.host.clone(),
                        ready_at: next_ready,
                    });
                }

                // Extract the permit from queued and return it
                let permit = queued.permit.expect("QueuedUrl must have a permit");
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

    /// Record a successful crawl for a host and decrement inflight counter
    pub fn record_success(&self, host: &str) {
        if let Some(mut cached) = self.host_state_cache.get_mut(host) {
            cached.reset_failures();
            // Decrement inflight counter
            cached.inflight.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: None,
                crawl_delay_secs: None,
                reset_failures: true,
                increment_failures: false,
            }).await;
        });
    }

    /// Record a failure for a host and decrement inflight counter
    pub fn record_failure(&self, host: &str) {
        if let Some(mut cached) = self.host_state_cache.get_mut(host) {
            cached.record_failure();
            // Decrement inflight counter
            cached.inflight.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: None,
                crawl_delay_secs: None,
                reset_failures: false,
                increment_failures: true,
            }).await;
        });
    }

    /// Set robots.txt content for a host
    pub fn set_robots_txt(&mut self, host: &str, robots_txt: String) {
        let crawl_delay_secs = Self::extract_crawl_delay(&robots_txt, &self.user_agent);

        let mut host_state = self.host_state_cache.entry(host.to_string()).or_insert_with(|| {
            HostState::new(host.to_string())
        });
        host_state.robots_txt = Some(robots_txt.clone());
        if let Some(delay) = crawl_delay_secs {
            host_state.crawl_delay_secs = delay;
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: Some(robots_txt),
                crawl_delay_secs,
                reset_failures: false,
                increment_failures: false,
            }).await;
        });
    }

    /// Extract crawl delay from robots.txt
    fn extract_crawl_delay(robots_txt: &str, user_agent: &str) -> Option<u64> {
        let mut in_matching_agent = false;
        let mut crawl_delay = None;

        for line in robots_txt.lines() {
            let line = line.trim();

            if line.to_lowercase().starts_with("user-agent:") {
                let agent = line[11..].trim();
                in_matching_agent = agent == "*" || agent.eq_ignore_ascii_case(user_agent);
            }

            if in_matching_agent && line.to_lowercase().starts_with("crawl-delay:") {
                if let Some(delay_str) = line[12..].trim().split_whitespace().next() {
                    if let Ok(delay) = delay_str.parse::<f64>() {
                        crawl_delay = Some(delay.ceil() as u64);
                    }
                }
            }
        }

        crawl_delay
    }

    /// Check if the frontier is empty
    pub fn is_empty(&self) -> bool {
        for entry in self.host_queues.iter() {
            let queue = entry.value().lock();
            if !queue.is_empty() {
                return false;
            }
        }
        true
    }

    /// Get statistics about this shard's frontier
    pub fn stats(&self) -> FrontierShardStats {
        let total_hosts = self.host_queues.len();
        let mut total_queued = 0;
        let mut hosts_with_work = 0;

        for entry in self.host_queues.iter() {
            let queue = entry.value().lock();
            let queue_len = queue.len();
            total_queued += queue_len;
            if queue_len > 0 {
                hosts_with_work += 1;
            }
        }

        FrontierShardStats {
            shard_id: self.shard_id,
            total_hosts,
            total_queued,
            hosts_with_work,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FrontierShardStats {
    pub shard_id: usize,
    pub total_hosts: usize,
    pub total_queued: usize,
    pub hosts_with_work: usize,
}

impl std::fmt::Display for FrontierShardStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Shard {}: {} hosts, {} queued, {} with work",
            self.shard_id, self.total_hosts, self.total_queued, self.hosts_with_work
        )
    }
}

/// Wrapper around sharded frontier that provides a unified interface compatible with legacy Frontier.
/// Uses work-stealing from shards via thread-local channels.
pub struct ShardedFrontier {
    dispatcher: Arc<FrontierDispatcher>,
    work_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit)>>>,
    work_tx: tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit)>,
    control_senders: Vec<tokio::sync::mpsc::UnboundedSender<ShardMsg>>,
}

impl ShardedFrontier {
    pub fn new(dispatcher: FrontierDispatcher, control_senders: Vec<tokio::sync::mpsc::UnboundedSender<ShardMsg>>) -> Self {
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

    pub async fn get_next_url(&self) -> Option<(String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit)> {
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
    fn send_finish_message(&self, host: &str, ok: bool, latency_ms: u64) {
        let registrable_domain = url_utils::get_registrable_domain(host);
        let shard_id = url_utils::rendezvous_shard_id(&registrable_domain, self.control_senders.len());

        let msg = ShardMsg::Finished {
            host: host.to_string(),
            ok,
            latency_ms,
        };

        if let Err(e) = self.control_senders[shard_id].send(msg) {
            eprintln!("Failed to send finish message to shard {}: {}", shard_id, e);
        }
    }

    pub fn set_robots_txt(&self, _host: &str, _robots_txt: String) {
        // Shards handle this internally
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

    pub fn work_tx(&self) -> tokio::sync::mpsc::UnboundedSender<(String, String, u32, Option<String>, tokio::sync::OwnedSemaphorePermit)> {
        self.work_tx.clone()
    }
}

/// LEGACY: Single-threaded frontier for backward compatibility.
pub struct Frontier {
    state: Arc<CrawlerState>,
    writer_thread: Arc<WriterThread>,

    host_queues: DashMap<String, VecDeque<QueuedUrl>>,
    ready_heap: Arc<Mutex<BinaryHeap<ReadyHost>>>,
    url_filter: Arc<Mutex<BloomFilter>>,
    pending_urls: DashMap<String, ()>,

    host_state_cache: DashMap<String, HostState>,
    user_agent: String,
    ignore_robots: bool,
}

impl Frontier {
    pub fn new(
        state: Arc<CrawlerState>,
        writer_thread: Arc<WriterThread>,
        user_agent: String,
        ignore_robots: bool,
    ) -> Self {
        let url_filter = BloomFilter::with_false_pos(0.01).expected_items(10_000_000);

        Self {
            state,
            writer_thread,
            host_queues: DashMap::new(),
            ready_heap: Arc::new(Mutex::new(BinaryHeap::new())),
            url_filter: Arc::new(Mutex::new(url_filter)),
            pending_urls: DashMap::new(),
            host_state_cache: DashMap::new(),
            user_agent,
            ignore_robots,
        }
    }

    /// Extract the host from a URL so scheduling can group URLs by domain.
    fn extract_host(url: &str) -> Option<String> {
        url_utils::extract_host(url)
    }

    /// Add newly discovered URLs to the frontier so the crawler can explore them later.
    pub async fn add_links(
        &self,
        links: Vec<(String, u32, Option<String>)>,
        start_url_domain: &str,
    ) -> usize {
        let mut added_count = 0;

        for (url, depth, parent_url) in links {
            // Normalize URLs so deduplication treats equivalent variants the same.
            let normalized_url = SitemapNode::normalize_url(&url);

            // Consult the pending set first so we do not queue the same URL multiple times in one add batch.
            if self.pending_urls.contains_key(&normalized_url) {
                continue;
            }

            // Hit the Bloom filter next to keep most duplicate checks O(1).
            let bloom_hit = {
                let mut filter = self.url_filter.lock();
                let hit = filter.contains(&normalized_url);
                if !hit {
                    filter.insert(&normalized_url);
                }
                hit
            };

            // On a Bloom filter positive, do a final disk check to handle false positives.
            // This prevents data loss while maintaining O(1) fast path for true negatives.
            if bloom_hit {
                // Check the actual redb state to confirm this is a true duplicate.
                match self.state.contains_url(&normalized_url) {
                    Ok(true) => {
                        // True duplicate - skip it.
                        continue;
                    }
                    Ok(false) => {
                        // False positive - continue adding the URL.
                    }
                    Err(e) => {
                        eprintln!("Error checking URL in state: {}. Skipping to be safe.", e);
                        continue;
                    }
                }
            }

            // Mark the URL as pending so concurrent additions cannot enqueue it again.
            self.pending_urls.insert(normalized_url.clone(), ());

            // Enforce the domain filter so the crawl never escapes the target site.
            let url_domain = match Self::extract_host(&normalized_url) {
                Some(domain) => domain,
                None => {
                    self.pending_urls.remove(&normalized_url);
                    continue;
                }
            };

            if !Self::is_same_domain(&url_domain, start_url_domain) {
                self.pending_urls.remove(&normalized_url);
                continue;
            }

            // Build a node so persistence has the metadata needed for exports.
            let node = SitemapNode::new(
                url.clone(),
                normalized_url.clone(),
                depth,
                parent_url.clone(),
                None,
            );

            // Send the node to the writer so disk state advances alongside in-memory queues.
            if let Err(e) = self.writer_thread.send_event_async(StateEvent::AddNodeFact(node)).await {
                eprintln!("Failed to send AddNodeFact for {}: {}", normalized_url, e);
                self.pending_urls.remove(&normalized_url);
                continue;
            }

            // Place the normalized URL into its host queue so scheduling respects per-host politeness.
            let host = url_domain;
            let queued = QueuedUrl {
                url: normalized_url.clone(),
                depth,
                parent_url,
                permit: None, // Legacy frontier doesn't use permits
            };

            // Use push_back so FIFO ordering inside the host queue is preserved.
            {
                let mut queue = self.host_queues.entry(host.clone()).or_insert_with(VecDeque::new);
                queue.push_back(queued);
            }

            // Push the host into the ready heap so a worker eventually revisits this host.
            {
                let mut heap = self.ready_heap.lock();
                let now = Instant::now();
                // Allow duplicates in the heap because get_next_url filters outdated entries.
                heap.push(ReadyHost {
                    host: host.clone(),
                    ready_at: now,
                });
            }

            added_count += 1;
        }

        added_count
    }

    /// Get the next URL to crawl.
    pub async fn get_next_url(&self) -> Option<(String, String, u32, Option<String>)> {
        loop {
            // Pop the next ready host so we visit whichever domain's backoff has expired first.
            let ready_host = {
                let mut heap = self.ready_heap.lock();
                heap.pop()?
            };

            let now = Instant::now();
            if now < ready_host.ready_at {
                // Requeue hosts whose ready time has not arrived so we honor politeness delays.
                self.ready_heap.lock().push(ready_host);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            // Pull a URL from that host's queue so we keep per-host ordering intact.
            let url_data = {
                if let Some(mut queue) = self.host_queues.get_mut(&ready_host.host) {
                    queue.pop_front()
                } else {
                    None
                }
            };

            if let Some(queued) = url_data {
                // Load host state from the async cache instead of blocking on disk to keep latency low.
                // This follows a cache-aside pattern so reads stay off the critical path.
                let host_state = if let Some(cached) = self.host_state_cache.get(&ready_host.host) {
                    cached.clone()
                } else {
                    // On a cache miss, load from disk on a helper thread so this worker is not blocked.
                    // Use a default state immediately so we can continue scheduling while the cache warms.
                    let state_arc = Arc::clone(&self.state);
                    let host_clone = ready_host.host.clone();
                    let cache_clone = self.host_state_cache.clone();

                    tokio::task::spawn_blocking(move || {
                        if let Ok(Some(state)) = state_arc.get_host_state(&host_clone) {
                            cache_clone.insert(host_clone, state);
                        }
                    });

                    // Fall back to that default state for the current request and rely on the refresh for future ones.
                    HostState::new(ready_host.host.clone())
                };

                // Remove the URL from pending so future discoveries can enqueue it again if needed.
                self.pending_urls.remove(&queued.url);

                // Skip hosts that remain in backoff to respect crawl-delay directives.
                if !host_state.is_ready() {
                    // Reinsert the URL into pending and the queue so it can run once the host is ready.
                    self.pending_urls.insert(queued.url.clone(), ());

                    // Push the URL to the front so it is the next item processed once backoff ends.
                    if let Some(mut queue) = self.host_queues.get_mut(&ready_host.host) {
                        queue.push_front(queued);
                    }

                    // Compute the remaining delay so we know when the host may be crawled again.
                    let backoff_secs = host_state.backoff_until_secs.saturating_sub(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                    );
                    let backoff_until = Instant::now() + Duration::from_secs(backoff_secs);

                    // Requeue the host with the updated deadline so the scheduler checks back later.
                    self.ready_heap.lock().push(ReadyHost {
                        host: ready_host.host,
                        ready_at: backoff_until,
                    });
                    continue;
                }

                // Respect robots.txt directives when enabled to remain compliant.
                if !self.ignore_robots {
                    if let Some(ref robots_txt) = host_state.robots_txt {
                        let mut matcher = DefaultMatcher::default();
                        if !matcher.one_agent_allowed_by_robots(robots_txt, &self.user_agent, &queued.url) {
                            // Ignore URLs disallowed by robots to prevent policy violations.
                            continue;
                        }
                    }
                }

                // Schedule the next allowed crawl time so the host observes its crawl delay.
                let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);
                let next_ready = Instant::now() + crawl_delay;

                // Check whether the host still has queued URLs so we know if it needs re-enqueuing.
                let host_has_more = self.host_queues
                    .get(&ready_host.host)
                    .map_or(false, |q| !q.is_empty());

                if host_has_more {
                    // Requeue the host so its remaining URLs eventually run after the delay.
                    self.ready_heap.lock().push(ReadyHost {
                        host: ready_host.host.clone(),
                        ready_at: next_ready,
                    });
                }

                return Some((ready_host.host, queued.url, queued.depth, queued.parent_url));
            } else {
                // Ignore hosts whose queues are empty because there is nothing to do.
                continue;
            }
        }
    }

    /// Record a successful crawl for a host so backoff counters reset quickly.
    pub fn record_success(&self, host: &str) {
        // Update the cache immediately so read-heavy callers see the new state without another lookup.
        if let Some(mut cached) = self.host_state_cache.get_mut(host) {
            cached.reset_failures();
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: None,
                crawl_delay_secs: None,
                reset_failures: true,
                increment_failures: false,
            }).await;
        });
    }

    /// Record a failure for a host so backoff penalties accumulate correctly.
    pub fn record_failure(&self, host: &str) {
        // Update the cache immediately so repeated failures increase backoff promptly.
        if let Some(mut cached) = self.host_state_cache.get_mut(host) {
            cached.record_failure();
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: None,
                crawl_delay_secs: None,
                reset_failures: false,
                increment_failures: true,
            }).await;
        });
    }

    /// Set robots.txt content for a host so future scheduling honors the published rules.
    pub fn set_robots_txt(&self, host: &str, robots_txt: String) {
        // Parse the crawl delay so we can throttle requests according to the site's policy.
        let crawl_delay_secs = Self::extract_crawl_delay(&robots_txt, &self.user_agent);

        // Update the cache immediately so future scheduling decisions use the newest directives.
        let mut host_state = self.host_state_cache.entry(host.to_string()).or_insert_with(|| {
            HostState::new(host.to_string())
        });
        host_state.robots_txt = Some(robots_txt.clone());
        if let Some(delay) = crawl_delay_secs {
            host_state.crawl_delay_secs = delay;
        }

        let writer = self.writer_thread.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            let _ = writer.send_event_async(StateEvent::UpdateHostStateFact {
                host,
                robots_txt: Some(robots_txt),
                crawl_delay_secs,
                reset_failures: false,
                increment_failures: false,
            }).await;
        });
    }

    /// Check if we have saved state to resume from so we know whether seeding can be skipped.
    pub fn has_saved_state(&self) -> bool {
        // Examine the Bloom filter to infer whether a previous session queued URLs.
        // A non-empty filter suggests prior work, but we do not yet persist the in-memory queues.
        // Persisting scheduler state is a TODO, so we report false and reseed each run.
        false // Always start fresh because the in-memory queues are not serialized yet.
    }

    /// Check if the frontier is empty so callers know when the crawl is complete.
    pub fn is_empty(&self) -> bool {
        // Scan every host queue to guarantee no crawlable URLs remain.
        for entry in self.host_queues.iter() {
            if !entry.value().is_empty() {
                return false;
            }
        }
        true
    }

    /// Get statistics about the frontier so progress logging can summarize queue health.
    pub fn stats(&self) -> FrontierStats {
        let total_hosts = self.host_queues.len();
        let mut total_queued = 0;
        let mut hosts_with_work = 0;

        for entry in self.host_queues.iter() {
            let queue_len = entry.value().len();
            total_queued += queue_len;
            if queue_len > 0 {
                hosts_with_work += 1;
            }
        }

        FrontierStats {
            total_hosts,
            total_queued,
            hosts_with_work,
            hosts_in_backoff: 0, // TODO: Derive from host state snapshots so stats reflect active backoffs.
        }
    }

    /// Check if two domains are the same so we can enforce same-site restrictions.
    fn is_same_domain(url_domain: &str, base_domain: &str) -> bool {
        url_utils::is_same_domain(url_domain, base_domain)
    }

    /// Extract crawl delay from robots.txt for a specific user agent so we apply the correct throttling.
    fn extract_crawl_delay(robots_txt: &str, user_agent: &str) -> Option<u64> {
        let mut in_matching_agent = false;
        let mut crawl_delay = None;

        for line in robots_txt.lines() {
            let line = line.trim();

        // Look for User-agent directives so we only apply rules meant for our crawler.
            if line.to_lowercase().starts_with("user-agent:") {
                let agent = line[11..].trim();
                in_matching_agent = agent == "*" || agent.eq_ignore_ascii_case(user_agent);
            }

        // Look for Crawl-delay directives so we extract the publisher's throttle preference.
            if in_matching_agent && line.to_lowercase().starts_with("crawl-delay:") {
                if let Some(delay_str) = line[12..].trim().split_whitespace().next() {
                    if let Ok(delay) = delay_str.parse::<f64>() {
                        crawl_delay = Some(delay.ceil() as u64);
                    }
                }
            }
        }

        crawl_delay
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
    use tempfile::TempDir;

    #[test]
    fn test_extract_host() {
        assert_eq!(
            Frontier::extract_host("https://example.com/path"),
            Some("example.com".to_string())
        );
        assert_eq!(Frontier::extract_host("invalid"), None);
    }

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

    #[tokio::test]
    async fn test_frontier_add_and_get() {
        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());
        let wal_writer = Arc::new(tokio::sync::Mutex::new(
            crate::wal::WalWriter::new(dir.path(), 100).unwrap()
        ));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let instance_id = 1;

        let writer_thread = Arc::new(crate::writer_thread::WriterThread::spawn(
            state.clone(),
            wal_writer,
            metrics,
            instance_id,
        ));

        let frontier = Frontier::new(state, writer_thread, "TestBot/1.0".to_string(), false);

        let links = vec![(
            "https://example.com/page1".to_string(),
            1,
            Some("https://example.com".to_string()),
        )];

        let added = frontier.add_links(links, "example.com").await;
        assert_eq!(added, 1);

        // Allow the asynchronous writer a moment to flush the event to disk.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = frontier.stats();
        assert_eq!(stats.total_queued, 1);
        assert_eq!(stats.hosts_with_work, 1);

        // Pull the next URL so we can assert the enqueueing path worked.
        let url = frontier.get_next_url().await;
        assert!(url.is_some());
        let (host, url_str, depth, _parent) = url.unwrap();
        assert_eq!(host, "example.com");
        assert!(url_str.contains("page1"));
        assert_eq!(depth, 1);

        // Confirm the queue drains once the URL is fetched.
        assert!(frontier.is_empty());
    }
}
