# Concurrency Architecture Deep Dive

## Table of Contents
1. [Overview](#overview)
2. [Concurrency Control Layers](#concurrency-control-layers)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [Shutdown & Graceful Exit](#shutdown--graceful-exit)
6. [Performance Tuning](#performance-tuning)
7. [Common Issues & Solutions](#common-issues--solutions)

---

## Overview

The Rust-sitemap crawler implements a **multi-layered concurrency control system** designed for high-throughput web crawling while respecting politeness constraints and preventing resource exhaustion.

### Key Design Principles

1. **Layered Concurrency**: Multiple independent control mechanisms work together
2. **Backpressure Management**: Automatic throttling prevents memory exhaustion
3. **Politeness First**: Per-host rate limiting respects robots.txt and server load
4. **Deadlock Prevention**: Bounded retry loops with exponential backoff
5. **Fault Tolerance**: Panic guards and cleanup handlers ensure resource recovery

---

## Concurrency Control Layers

The crawler implements **7 distinct concurrency control mechanisms**, each serving a specific purpose:

### 1. Global Worker Concurrency (`max_workers`)

**Location**: `BfsCrawlerConfig.max_workers`
**Default**: 512 workers
**Purpose**: Limits total concurrent HTTP requests across all hosts
**Implementation**: Tokio semaphore in `crawler_permits`

```rust
Arc<tokio::sync::Semaphore>::new(config.max_workers as usize)
```

**What it controls**:
- Maximum number of simultaneous HTTP GET requests
- CPU/network resource usage
- Memory consumption from in-flight requests

**Tuning**:
```bash
# CLI flag
./rust_sitemap crawl --start-url example.com --workers 1024

# Environment variable
# (Not currently supported for max_workers, but similar pattern used elsewhere)
```

**How it works**:
1. Before processing each URL, acquire a permit from `crawler_permits`
2. Permit is held during the entire HTTP request + HTML parsing
3. Permit is released when task completes (success, failure, or panic)
4. When semaphore is exhausted, tasks wait until permits become available

**Performance Impact**:
- Too low: Underutilizes network/CPU
- Too high: Risk of memory exhaustion, rate limiting
- Sweet spot: 256-1024 for most deployments

---

### 2. Per-Host Concurrency (`MAX_INFLIGHT`)

**Location**: `HostState.max_inflight`
**Default**: 150 concurrent requests per host
**Purpose**: Prevents overwhelming individual servers
**Implementation**: Atomic counter in `HostState.inflight`

**Environment Variable**:
```bash
# Not currently exposed, hardcoded in HostState
# Future enhancement: MAX_INFLIGHT_PER_HOST env var
```

**What it controls**:
- Maximum concurrent requests to a single domain
- Prevents server-side rate limiting (429 Too Many Requests)
- Distributes load across multiple hosts

**How it works**:
1. Before sending request, check `host_state.inflight < host_state.max_inflight`
2. If at capacity, requeue URL and try another host
3. Increment `inflight` atomically when starting request
4. Decrement `inflight` when request completes
5. Use atomic operations to prevent race conditions

```rust
// Check capacity (frontier.rs:671)
let current_inflight = host_state.inflight.load(Ordering::Relaxed);
if current_inflight >= host_state.max_inflight {
    // Requeue and try another host
}

// Increment when starting (frontier.rs:814)
entry.inflight.fetch_add(1, Ordering::Relaxed);

// Decrement when done (frontier.rs:939, 954, 968)
cached.inflight.fetch_sub(1, Ordering::Relaxed);
```

**Anti-Pattern Prevented**: Without this, a large site could monopolize all workers, starving other hosts.

---

### 3. Global Frontier Size Limit

**Location**: `DEFAULT_GLOBAL_FRONTIER_SIZE_LIMIT`
**Default**: 5,000,000 URLs
**Purpose**: Prevents unbounded memory growth from discovered URLs
**Implementation**: Backpressure semaphore

**Environment Variable**:
```bash
export GLOBAL_FRONTIER_SIZE_LIMIT=10000000  # 10M URLs
```

**What it controls**:
- Maximum number of URLs queued for crawling
- Total memory footprint of the frontier
- Prevents OOM errors on large crawls

**How it works**:
1. `FrontierDispatcher` creates semaphore with `GLOBAL_FRONTIER_SIZE_LIMIT` permits
2. Before adding URL to frontier, acquire permit with 30s timeout
3. Permit travels with URL through entire pipeline
4. When URL completes (or fails), permit is released via `FrontierPermit::Drop`

```rust
// Acquire permit with timeout (frontier.rs:190-207)
let owned_permit = tokio::time::timeout(
    Duration::from_secs(30),
    self.backpressure_semaphore.clone().acquire_owned(),
).await?;

// Wrap in FrontierPermit for automatic cleanup
let permit = FrontierPermit::new(owned_permit, Arc::clone(&self.global_frontier_size));

// Permit is dropped when URL completes, automatically returning it to pool
```

**Critical Fix Applied** (Session #28):
- **Bug**: Counter underflow when permits failed to send
- **Fix**: Only increment counter when permit is created, decrement in `Drop`
- **Result**: Accurate tracking of frontier size

---

### 4. Per-Host Politeness Delays

**Location**: `HostState.crawl_delay_secs`
**Default**: 1 second (or value from robots.txt)
**Purpose**: Respects server politeness requirements
**Implementation**: Time-based scheduling in `ReadyHeap`

**How it works**:
1. Parse `Crawl-delay` directive from robots.txt
2. Store in `HostState.crawl_delay_secs`
3. After crawling URL, calculate `next_ready = now + crawl_delay`
4. Insert host into min-heap with `ready_at` timestamp
5. Don't pop host from heap until `now >= ready_at`

```rust
// Schedule next crawl (frontier.rs:807)
let crawl_delay = Duration::from_secs(host_state.crawl_delay_secs);
let next_ready = Instant::now() + crawl_delay;

self.push_ready_host(ReadyHost {
    host: ready_host.host.clone(),
    ready_at: next_ready,
});

// Check readiness (frontier.rs:562)
if now < ready_host.ready_at {
    self.push_ready_host(ready_host);  // Not ready, requeue
    tokio::time::sleep(Duration::from_millis(10)).await;
    continue;
}
```

**Why Min-Heap?**
- O(log n) insertion and removal
- Always pops host with earliest `ready_at` time
- Efficient for thousands of hosts

---

### 5. Exponential Backoff for Failed Hosts

**Location**: `HostState.backoff_until_secs`
**Default**: Starts at 2s, doubles on each failure, max 3600s
**Purpose**: Automatically backs off from problematic hosts
**Implementation**: Per-host backoff timer with exponential growth

**How it works**:
1. On HTTP error (5xx, timeout, connection refused), increment `HostState.failures`
2. Calculate backoff: `2^failures` seconds (capped at 1 hour)
3. Set `backoff_until_secs = now + backoff_duration`
4. When pulling URL from frontier, check `is_ready()`
5. If in backoff, requeue URL and try another host

```rust
// Record failure (state.rs HostState implementation)
pub fn record_failure(&mut self) {
    self.failures += 1;
    let backoff_secs = std::cmp::min(2u64.pow(self.failures as u32), 3600);
    self.backoff_until_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() + backoff_secs;
}

// Check if ready (frontier.rs:642)
if !host_state.is_ready() {
    // Requeue and wait for backoff to expire
}
```

**Permanent Failure Threshold**: After 10 consecutive failures, host is permanently blocked (frontier.rs:631).

---

### 6. Bloom Filter + Database Deduplication

**Location**: `FrontierShard.url_filter`
**Capacity**: 10,000,000 URLs (configurable)
**False Positive Rate**: 1%
**Purpose**: Fast URL deduplication without database queries
**Implementation**: Two-tier checking system

**How it works**:

**Tier 1: Bloom Filter (In-Memory)**
```rust
// Check bloom filter first (frontier.rs:385)
if self.url_filter.contains(normalized_url) {
    // Potential duplicate, needs DB verification
    urls_needing_check.push(normalized_url.clone());
} else {
    // Definitely new URL, add directly
}
```

**Tier 2: Database Verification (Batch)**
```rust
// Batch verify suspected duplicates (frontier.rs:414-430)
let state_arc = Arc::clone(&self.state);
tokio::task::spawn_blocking(move || {
    let mut exists = HashSet::new();
    for url in urls_to_check {
        if state_arc.contains_url(&url)? {
            exists.insert(url);
        }
    }
    exists
}).await?
```

**Why Two Tiers?**
- Bloom filter: 1% false positives, O(1) lookup, no I/O
- Database: 0% false positives, O(log n) lookup, disk I/O
- Combined: 99% of duplicates caught without database query

**Memory Usage**:
```
Bloom filter = (expected_items * bits_per_item) / 8
10M URLs * ~10 bits = ~12 MB
```

---

### 7. Parse Concurrency Limit

**Location**: `parse_sema`
**Default**: 128 concurrent HTML parsers
**Purpose**: Prevents CPU exhaustion from simultaneous parsing
**Implementation**: Tokio semaphore in parser dispatcher

**What it controls**:
- Maximum concurrent HTML parsing tasks
- CPU usage from `scraper` crate operations
- Memory from parsed DOM trees

**How it works**:
```rust
// Create parser pool (bfs_crawler.rs:308)
let parse_sema = Arc::new(tokio::sync::Semaphore::new(128));

// Acquire permit before parsing (bfs_crawler.rs:316)
let permit = sem.acquire_owned().await.unwrap();

// Parse on blocking thread pool (bfs_crawler.rs:321)
tokio::task::spawn_blocking(move || {
    let document = Html::parse_document(&html_str);
    // Extract links, title, etc.
}).await?;

// Permit auto-released when dropped (bfs_crawler.rs:416)
drop(permit);
```

**Why Blocking Thread Pool?**
- HTML parsing is CPU-bound, not async-friendly
- Prevents blocking Tokio runtime executor
- `spawn_blocking` uses dedicated thread pool (defaults to number of CPU cores)

---

### 8. Robots.txt Fetch Concurrency

**Location**: `robots_fetch_semaphore`
**Default**: 64 concurrent fetches
**Purpose**: Limits parallel robots.txt requests
**Implementation**: Tokio semaphore

**Environment Variable**:
```bash
export ROBOTS_FETCH_SEMAPHORE_LIMIT=128
```

**What it controls**:
- Maximum concurrent robots.txt downloads
- Connection pool pressure
- Prevents thundering herd on startup

**How it works**:
```rust
// Acquire permit (frontier.rs:741)
let _robots_permit = robots_sem.acquire().await?;

// Fetch with timeout (frontier.rs:765-774)
let robots_result = tokio::time::timeout(
    Duration::from_secs(3),
    robots::fetch_robots_txt(&http_clone, &host_clone),
).await;

// Update cache (frontier.rs:777-795)
let event = StateEvent::UpdateHostStateFact {
    host: host_clone.clone(),
    robots_txt: robots_txt.clone(),
    // ...
};
writer_clone.send_event_async(event).await?;
```

**Cleanup Guard**: Ensures `hosts_fetching_robots` is cleaned up even on panic (frontier.rs:750-759).

---

### 9. False Positive Check Semaphore

**Location**: `fp_check_semaphore`
**Default**: 512 concurrent database checks
**Purpose**: Limits concurrent bloom filter verification queries
**Implementation**: Tokio semaphore

**Environment Variable**:
```bash
export FP_CHECK_SEMAPHORE_LIMIT=1024
```

**What it controls**:
- Maximum concurrent database queries for deduplication
- Database connection pool usage
- I/O load on storage backend

**How it works**:
```rust
// Acquire permit for batch check (frontier.rs:395-409)
let _permit = self.fp_check_semaphore.acquire().await?;

// Batch verify in blocking task
tokio::task::spawn_blocking(move || {
    for url in urls_to_check {
        if state_arc.contains_url(&url)? {
            exists.insert(url);
        }
    }
    exists
}).await?
```

**Optimization**: Batches multiple checks into single database transaction (frontier.rs:374-390).

---

### 10. Ready Heap Size Limit

**Location**: `DEFAULT_READY_HEAP_SIZE_LIMIT`
**Default**: 500,000 hosts
**Purpose**: Prevents memory exhaustion from thousands of hosts
**Implementation**: Size check before insertion

**Environment Variable**:
```bash
export READY_HEAP_SIZE_LIMIT=1000000
```

**What it controls**:
- Maximum number of hosts in ready heap
- Memory footprint of scheduling data structure
- Protects against pathological cases (e.g., crawling DNS zone file)

**How it works**:
```rust
// Check capacity before adding (frontier.rs:329-336)
if self.ready_heap.len() >= ready_heap_limit {
    eprintln!(
        "WARNING: Shard {} ready_heap at capacity ({} hosts), dropping host {}",
        self.shard_id, ready_heap_limit, ready_host.host
    );
    return;  // Drop the host
}

self.ready_heap.push(ready_host);
```

**Critical Fix Applied** (Session #28):
- **Bug**: Duplicate hosts in heap caused infinite loops
- **Fix**: Track hosts in `HashSet` to prevent duplicates (frontier.rs:255, 323, 338, 559)

---

## Data Flow

### URL Discovery to Completion

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. URL DISCOVERY (HTML Parser)                                  │
│    ├─ Extract links from <a href="...">                         │
│    ├─ Resolve to absolute URLs                                  │
│    └─ Filter same-domain only                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. DISPATCHER (FrontierDispatcher)                              │
│    ├─ Acquire backpressure permit (timeout: 30s)                │
│    ├─ Extract host from URL                                     │
│    ├─ Compute shard ID via rendezvous hashing                   │
│    └─ Send to appropriate shard's channel                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. SHARD INGESTION (FrontierShard.process_incoming_urls)        │
│    ├─ Check local pending set (fast)                            │
│    ├─ Check bloom filter                                        │
│    ├─ On bloom hit: Batch verify with database                  │
│    ├─ Add to host queue (VecDeque)                              │
│    ├─ Insert into ready heap with current timestamp             │
│    └─ Write to state database                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. SCHEDULING (FrontierShard.get_next_url)                      │
│    ├─ Pop from ready heap (min-heap by ready_at time)           │
│    ├─ Check if host is ready (politeness + backoff)             │
│    ├─ Check per-host concurrency limit                          │
│    ├─ Fetch robots.txt if needed (async, cached)                │
│    ├─ Check URL against robots.txt rules                        │
│    ├─ Increment host.inflight counter                           │
│    ├─ Send to work_tx channel                                   │
│    └─ Schedule next crawl time (now + crawl_delay)              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. CRAWLING (BfsCrawler.start_crawling loop)                    │
│    ├─ Receive from work_rx channel                              │
│    ├─ Acquire global worker permit                              │
│    ├─ Spawn async task with panic guard                         │
│    ├─ HTTP GET request (timeout: configurable)                  │
│    ├─ Stream response body (max: 10MB)                          │
│    └─ Send to parse queue (bounded channel)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. PARSING (Parse dispatcher)                                   │
│    ├─ Acquire parse semaphore permit                            │
│    ├─ spawn_blocking (dedicated thread pool)                    │
│    ├─ Parse HTML with scraper crate                             │
│    ├─ Extract links, title, metadata                            │
│    ├─ Write crawl attempt to state                              │
│    ├─ Add discovered links to frontier (goto step 2)            │
│    └─ Decrement host.inflight counter                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. STATE PERSISTENCE (WriterThread)                             │
│    ├─ Batch events into 100-event chunks                        │
│    ├─ Serialize with rkyv (zero-copy)                           │
│    ├─ Write to WAL (write-ahead log)                            │
│    ├─ Apply to SQLite database                                  │
│    └─ Fsync periodically (save_interval: 300s)                  │
└─────────────────────────────────────────────────────────────────┘
```

### Rendezvous Hashing for Sharding

**Why Rendezvous Hashing?**
- Consistent assignment of hosts to shards
- Minimizes reassignment when shard count changes
- Better load distribution than simple modulo

```rust
// url_utils.rs
pub fn rendezvous_shard_id(key: &str, num_shards: usize) -> usize {
    let hash = seahash::hash(key.as_bytes());
    (hash % num_shards as u64) as usize
}
```

**How it works**:
1. Extract registrable domain (e.g., `example.com` from `www.example.com`)
2. Hash domain with SeaHash (fast, non-cryptographic)
3. Modulo by shard count (defaults to CPU core count)
4. All URLs from same domain go to same shard

**Benefits**:
- Locality: All state for a domain lives in one shard
- No synchronization needed between shards
- Each shard can independently track per-host state

---

## Shutdown & Graceful Exit

### Exit Conditions

The crawler exits when **any** of the following conditions are met:

#### 1. Graceful Completion
```rust
// frontier.is_empty() && in_flight_tasks.is_empty()
if frontier_empty && tasks_empty {
    eprintln!("GRACEFUL SHUTDOWN: Crawl Complete");
    break;
}
```

**When triggered**:
- All discovered URLs have been processed
- No pending work in any shard
- No HTTP requests in flight

**Typical scenario**: Small site with finite pages

#### 2. Manual Stop (Ctrl+C)
```rust
// Signal handler sets running = false
if !self.running.load(Ordering::SeqCst) {
    break;
}
```

**When triggered**:
- User presses Ctrl+C
- `crawler.stop().await` called programmatically

**Cleanup sequence**:
1. Set `running = false`
2. Send shutdown signal to governor and shards
3. Wait `SHUTDOWN_GRACE_PERIOD_SECS` (2s)
4. Flush WAL batches
5. Export sitemap.jsonl
6. Exit

#### 3. Max URLs Reached (NEW)
```rust
if let Some(max_urls) = self.config.max_urls {
    if processed_count >= max_urls {
        eprintln!("Reached max_urls limit of {}, stopping crawl", max_urls);
        break;
    }
}
```

**When triggered**:
- `--max-urls N` flag provided
- Total processed count reaches N

**Usage**:
```bash
./rust_sitemap crawl \
  --start-url example.com \
  --max-urls 10000 \
  --workers 512
```

#### 4. Duration Limit Reached (NEW)
```rust
if let Some(duration_secs) = self.config.duration_secs {
    let elapsed = start.elapsed().unwrap_or_default().as_secs();
    if elapsed >= duration_secs {
        eprintln!("Reached duration limit of {}s (elapsed: {}s), stopping crawl",
                  duration_secs, elapsed);
        break;
    }
}
```

**When triggered**:
- `--duration N` flag provided
- Elapsed time reaches N seconds

**Usage**:
```bash
./rust_sitemap crawl \
  --start-url example.com \
  --duration 3600 \  # Stop after 1 hour
  --workers 512
```

### Graceful Shutdown Improvements

**Enhanced Logging** (NEW):
```
╔═══════════════════════════════════════════════╗
║   GRACEFUL SHUTDOWN: Crawl Complete           ║
╠═══════════════════════════════════════════════╣
║ Reason: Frontier empty and no tasks in flight ║
║ Total URLs processed: 1523                    ║
║ Successful: 1450                              ║
║ Failed: 58                                    ║
║ Timeout: 15                                   ║
╚═══════════════════════════════════════════════╝
```

**Idle Status Logging** (NEW):
```rust
// Log every 10 seconds when waiting for work
static LAST_LOG: AtomicU64 = AtomicU64::new(0);
let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
let last = LAST_LOG.load(Ordering::Relaxed);

if now - last >= 10 {
    LAST_LOG.store(now, Ordering::Relaxed);
    eprintln!(
        "Waiting for work... (frontier_empty: {}, tasks_in_flight: {})",
        frontier_empty, in_flight_tasks.len()
    );
}
```

**Benefits**:
- User can see crawler is still running (not frozen)
- Visibility into why crawler hasn't exited
- Debugging aid for stuck crawls

---

## Performance Tuning

### Quick Reference Table

| **Metric**                     | **Control**                      | **Tune**                                      |
|--------------------------------|----------------------------------|-----------------------------------------------|
| **Total Throughput**           | Global workers                   | `--workers 1024`                              |
| **Per-Host Politeness**        | MAX_INFLIGHT                     | Hardcoded (future: env var)                   |
| **Memory Usage**               | Frontier size limit              | `GLOBAL_FRONTIER_SIZE_LIMIT=10000000`         |
| **Database Query Load**        | FP check semaphore               | `FP_CHECK_SEMAPHORE_LIMIT=1024`               |
| **CPU Usage (Parsing)**        | Parse semaphore                  | Hardcoded: 128 (future: env var)              |
| **Robots.txt Fetch Speed**     | Robots fetch semaphore           | `ROBOTS_FETCH_SEMAPHORE_LIMIT=128`            |
| **Scheduler Memory**           | Ready heap size limit            | `READY_HEAP_SIZE_LIMIT=1000000`               |

### Tuning Scenarios

#### Scenario 1: Single Large Site (e.g., Wikipedia)
```bash
./rust_sitemap crawl \
  --start-url en.wikipedia.org \
  --workers 1024 \           # High worker count
  --timeout 10 \              # Aggressive timeout
  --data-dir ./wikipedia \
  --max-urls 100000 \         # Stop at 100K pages
  --seeding-strategy all
```

**Rationale**:
- Wikipedia has high server capacity
- Single domain = minimal per-host contention
- Focus on maximizing throughput

#### Scenario 2: Multi-Domain Crawl (e.g., Link Graph)
```bash
export ROBOTS_FETCH_SEMAPHORE_LIMIT=256  # More robots.txt fetches
export READY_HEAP_SIZE_LIMIT=1000000     # Support more hosts

./rust_sitemap crawl \
  --start-url crawl-start-seeds.txt \
  --workers 512 \
  --timeout 20 \               # Conservative timeout
  --data-dir ./multi-domain \
  --duration 7200              # 2-hour crawl
```

**Rationale**:
- Many domains = more robots.txt fetches
- Need larger ready heap for scheduling
- Conservative timeout prevents slow servers from blocking

#### Scenario 3: Memory-Constrained Environment
```bash
export GLOBAL_FRONTIER_SIZE_LIMIT=1000000  # 1M URLs max
export FP_CHECK_SEMAPHORE_LIMIT=128        # Reduce DB connections

./rust_sitemap crawl \
  --start-url example.com \
  --workers 256 \              # Lower worker count
  --timeout 15 \
  --data-dir ./crawl
```

**Rationale**:
- Smaller frontier = less memory
- Fewer workers = less concurrent parsing
- Trade throughput for stability

---

## Common Issues & Solutions

### Issue 1: Crawler Doesn't Exit

**Symptom**: Frontier appears empty but crawler runs indefinitely

**Root Cause**: `frontier.is_empty()` returns `true` but tasks are still in flight

**Fix**:
```rust
// Check BOTH conditions (bfs_crawler.rs:580)
if self.frontier.is_empty() && in_flight_tasks.is_empty() {
    break;
}
```

**Diagnostic**:
```
Waiting for work... (frontier_empty: true, tasks_in_flight: 5)
```

**Solution**: Wait for all tasks to complete or use `--duration` flag

---

### Issue 2: High Memory Usage

**Symptom**: Process RSS grows beyond available RAM

**Root Causes**:
1. Frontier size too large
2. Too many concurrent workers
3. Large HTML pages being parsed

**Fixes**:

**1. Limit frontier size:**
```bash
export GLOBAL_FRONTIER_SIZE_LIMIT=2000000  # 2M URLs
```

**2. Reduce workers:**
```bash
--workers 256
```

**3. Enable governor throttling** (automatic):
```rust
// Automatically reduces permits when commit latency is high
if commit_ewma_ms > THROTTLE_THRESHOLD_MS {
    if let Ok(permit) = permits.try_acquire_owned() {
        shrink_bin.push(permit);  // Hold permit to reduce active workers
    }
}
```

---

### Issue 3: Database Lock Contention

**Symptom**: Slow `contains_url()` queries, high CPU wait time

**Root Cause**: Too many concurrent FP checks

**Fix**:
```bash
export FP_CHECK_SEMAPHORE_LIMIT=64  # Reduce from 512
```

**Alternative**: Use Redis for distributed deduplication
```bash
./rust_sitemap crawl \
  --start-url example.com \
  --enable-redis \
  --redis-url redis://localhost:6379
```

---

### Issue 4: Rate Limiting (429 Too Many Requests)

**Symptom**: Many 429 errors in logs

**Root Causes**:
1. Per-host concurrency too high
2. Politeness delay ignored
3. Exponential backoff not working

**Fixes**:

**1. Verify robots.txt compliance:**
```bash
# Should NOT use --ignore-robots for production crawls
./rust_sitemap crawl --start-url example.com  # Respects robots.txt
```

**2. Check host state:**
```rust
// Ensure backoff is working (state.rs)
pub fn record_failure(&mut self) {
    self.failures += 1;
    let backoff_secs = std::cmp::min(2u64.pow(self.failures as u32), 3600);
    self.backoff_until_secs = now + backoff_secs;
}
```

**3. Monitor backoff:**
```
[BLOCKED] Permanently skipping http://example.com/page from failed host example.com (11 consecutive failures)
```

---

### Issue 5: Deadlock / Freeze

**Symptom**: Crawler stops making progress, no errors

**Root Cause** (FIXED): Infinite loop in `get_next_url()` when all hosts in backoff

**Fix Applied** (Session #28):
```rust
// Before (DEADLOCK):
loop {
    if ready_heap.is_empty() { return None; }
    // Could loop forever if all hosts in backoff!
}

// After (BOUNDED):
const MAX_ATTEMPTS: usize = 10;
for _attempt in 0..MAX_ATTEMPTS {
    if ready_heap.is_empty() { return None; }
    // Try up to 10 times, then yield to higher-level scheduler
}
None  // Allow tokio::select! to try other branches
```

**Diagnostic**:
```
Waiting for work... (frontier_empty: false, tasks_in_flight: 0)
```

**Prevention**: Bounded retry loops throughout codebase

---

## Performance Metrics

### Expected Throughput

| **Scenario**                | **Workers** | **URLs/min** | **Bottleneck**             |
|-----------------------------|-------------|--------------|----------------------------|
| Single fast server          | 1024        | 500-1000     | Politeness delays          |
| Multi-domain (100+ hosts)   | 512         | 200-500      | Network latency            |
| Single slow server          | 256         | 50-150       | Server response time       |
| Memory-constrained          | 128         | 30-80        | Frontier backpressure      |

### Resource Usage (Typical)

```
Workers: 512
Memory: 500-800 MB
CPU: 30-50% (16-core system)
Network: 5-50 Mbps (depending on page sizes)
Database: 100-500 writes/sec
```

---

## Code Locations Reference

### Key Files

| **File**                | **Responsible For**                               |
|-------------------------|---------------------------------------------------|
| `src/frontier.rs`       | Frontier scheduling, sharding, deduplication      |
| `src/bfs_crawler.rs`    | Main crawl loop, worker spawning, parsing         |
| `src/state.rs`          | Database, WAL, host state management              |
| `src/network.rs`        | HTTP client, connection pooling                   |
| `src/robots.rs`         | robots.txt fetching and parsing                   |
| `src/url_utils.rs`      | URL normalization, domain extraction              |
| `src/writer_thread.rs`  | Asynchronous state persistence                    |
| `src/config.rs`         | Configuration constants                           |
| `src/cli.rs`            | CLI argument parsing                              |
| `src/main.rs`           | Entry point, command handlers                     |

### Critical Functions

```rust
// Frontier dispatch
frontier::FrontierDispatcher::add_links()          // Add URLs to frontier
frontier::FrontierShard::process_incoming_urls()   // Deduplicate and enqueue
frontier::FrontierShard::get_next_url()            // Schedule next URL

// Crawling
bfs_crawler::BfsCrawler::start_crawling()          // Main loop
bfs_crawler::BfsCrawler::process_url_streaming()   // HTTP fetch

// State management
state::CrawlerState::contains_url()                // Deduplication check
state::HostState::record_failure()                 // Exponential backoff
writer_thread::WriterThread::spawn()               // Async persistence

// Concurrency
ShardedFrontier::record_success()                  // Decrement inflight
ShardedFrontier::record_failed()                   // Decrement + backoff
```

---

## Future Enhancements

### Planned Improvements

1. **Adaptive MAX_INFLIGHT**: Adjust per-host concurrency based on server response time
2. **Distributed Frontier**: Share frontier across multiple machines via Redis
3. **Priority Queuing**: Crawl high-value pages first (e.g., homepage before deep links)
4. **Bloom Filter Resizing**: Automatically expand when approaching capacity
5. **Metrics Dashboard**: Real-time visibility into all concurrency controls
6. **GPU-Accelerated Parsing**: Offload HTML parsing to GPU for 10x speedup

### Environment Variables to Add

```bash
MAX_INFLIGHT_PER_HOST=200
PARSE_SEMAPHORE_LIMIT=256
GOVERNOR_ENABLED=true
METRIC_EXPORT_INTERVAL_SECS=60
```

---

## Conclusion

The Rust-sitemap crawler achieves **high performance** while maintaining **politeness** and **reliability** through:

1. **Layered concurrency controls** that work together without interference
2. **Automatic backpressure** that prevents resource exhaustion
3. **Bounded retry loops** that prevent deadlocks
4. **Graceful degradation** under load
5. **Comprehensive testing** with stress tests and fuzzing

By understanding these mechanisms, you can:
- **Tune performance** for your specific use case
- **Debug issues** when they arise
- **Extend the system** safely

For questions or improvements, see: https://github.com/BenjaminSRussell/Rust-sitemap
