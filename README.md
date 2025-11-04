# Rust Sitemap Crawler

High-performance web crawler built with Rust async runtime (Tokio). Achieves 100-500 URLs/sec with adaptive concurrency and streaming HTML parser.

## Architecture

```
┌──────────────┐ ┌─────────────┐ ┌────────────────┐
│  BfsCrawler  │ │  Frontier   │ │ WriterThread   │
├──────────────┤ ├─────────────┤ ├────────────────┤
│ Workers      │ │ DashMap     │ │ WAL writer     │
│ LocalSet     │ │ BinaryHeap  │ │ Batch commits  │
│ Semaphore    │ │ BloomFilter │ │ Async channel  │
└──────┬───────┘ └──────┬──────┘ └────────┬───────┘
       └────────────────┼─────────────────┘
                        ▼
        ┌───────────────────────────────┐
        │  Governor (Adaptive Throttle) │
        │  Monitor commit latency       │
        │  Adjust permits (32-512)      │
        └───────────────────────────────┘
```

### Core Components

| Component | Implementation | Purpose |
|-----------|---------------|---------|
| **Frontier** | DashMap + BinaryHeap | In-memory URL scheduler, O(1) operations |
| **State** | redb | Persistent storage (nodes, host state) |
| **WriterThread** | OS thread + WAL | Dedicated disk I/O with batching |
| **Parser** | lol_html | Streaming HTML parser, 64KB chunks |
| **Governor** | Tokio Semaphore | Adaptive concurrency (target 10ms commits) |
| **HttpClient** | reqwest | Connection pooling, HTTP/1.1 and HTTP/2 |
| **Lock Manager** | Redis + Lua | Distributed coordination with RAII guards |

### Key Optimizations

1. **Streaming Parser**: 64KB chunks, timeout budgets (30s/page, 10k handlers)
2. **Lock-Free Frontier**: DashMap for host queues, BinaryHeap for politeness timing
3. **Adaptive Concurrency**: AIMD governor adjusts workers (32-512) based on commit latency
4. **No Blocking I/O**: `spawn_blocking` for disk reads, async runtime never freezes
5. **Distributed Locks**: RAII guards with auto-renewal, Lua CAS scripts prevent stealing
6. **Deduplication**: BloomFilter (1% FP @ 10M URLs) + in-flight DashMap

## Quick Start

```bash
# Installation
git clone https://github.com/yourusername/rust-sitemap.git
cd rust-sitemap
cargo build --release

# New crawl
cargo run --release -- crawl --start-url https://example.com

# Resume interrupted crawl
cargo run --release -- resume --data-dir ./data

# Export XML sitemap
cargo run --release -- export-sitemap --output sitemap.xml
```

**Note**: JSONL export auto-generated at `./data/sitemap.jsonl` on completion or Ctrl+C.

## Commands

### `crawl` - Start Crawl

```bash
cargo run --release -- crawl [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--start-url` | *required* | Starting URL |
| `--data-dir` | `./data` | Storage directory |
| `--workers` | `256` | Concurrent workers (adaptive 32-512) |
| `--timeout` | `45` | Request timeout (seconds) |
| `--user-agent` | `RustSitemapCrawler/1.0` | User agent |
| `--ignore-robots` | `false` | Disable robots.txt |
| `--seeding-strategy` | `all` | `sitemap`, `ct`, `commoncrawl`, or `all` |
| `--enable-redis` | `false` | Enable distributed crawling |
| `--redis-url` | `redis://localhost:6379` | Redis connection |
| `--lock-ttl` | `300` | Lock TTL (seconds) |

**Examples**:
```bash
# Basic
cargo run --release -- crawl --start-url https://example.com

# High-performance
cargo run --release -- crawl --start-url https://example.com --workers 512 --timeout 30

# Distributed (multi-instance)
cargo run --release -- crawl --start-url https://example.com --enable-redis
```

### `resume` - Resume Crawl

```bash
cargo run --release -- resume [OPTIONS]
```

Loads state from `./data/crawler_state.redb`, continues from in-memory frontier.

### `export-sitemap` - Generate XML

```bash
cargo run --release -- export-sitemap [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--output` | `./sitemap.xml` | Output path |
| `--include-lastmod` | `false` | Include last modification |
| `--default-priority` | `0.5` | Default priority (0.0-1.0) |

## Output Formats

### JSONL (Auto-Generated)

Location: `./data/sitemap.jsonl`

```json
{
  "url": "https://example.com/page",
  "depth": 1,
  "parent_url": "https://example.com",
  "discovered_at": 1634567890,
  "status_code": 200,
  "content_type": "text/html",
  "title": "Page Title",
  "links": ["https://example.com/link1"]
}
```

### XML Sitemap (Manual Export)

Standard [sitemaps.org](https://www.sitemaps.org/) format with `<loc>`, `<lastmod>`, `<changefreq>`, `<priority>`.

## Performance

### Implementation Details

**Frontier Scheduler** ([src/frontier.rs](src/frontier.rs)):
- `DashMap<String, VecDeque<QueuedUrl>>`: Per-host URL queues
- `BinaryHeap<ReadyHost>`: Min-heap by `next_allowed_at` (politeness)
- `BloomFilter`: 1% false positive rate for 10M URLs

**Streaming Parser** ([src/bfs_crawler.rs](src/bfs_crawler.rs)):
```rust
const CHUNK_SIZE: usize = 64 * 1024;  // 64KB
const MAX_HANDLERS: usize = 10_000;    // Handler budget
const PARSER_TIMEOUT_SECS: u64 = 30;   // Timeout/page

loop {
    let n = buf_reader.read(&mut chunk_buf).await?;
    if n == 0 { break; }
    rewriter.write(&chunk_buf[..n])?;
    tokio::task::yield_now().await;
}
```

**Adaptive Governor** ([src/main.rs](src/main.rs)):
```rust
// Every 5 seconds
if commit_ewma_ms > 15.0 {
    permits.try_acquire().forget();  // Reduce workers
} else if commit_ewma_ms < 7.5 {
    permits.add_permits(1);  // Increase workers
}
```

**Distributed Locks** ([src/url_lock_manager.rs](src/url_lock_manager.rs)):
- Ownership tokens: `crawler-{instance_id}`
- Lua CAS scripts for atomic operations
- RAII guards with 30s auto-renewal

```lua
-- Compare-and-delete
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

### Resource Usage

| Workers | Memory | Throughput | CPU |
|---------|--------|------------|-----|
| 32      | ~200MB | 50-100 URLs/sec | 20-40% |
| 128     | ~500MB | 150-300 URLs/sec | 40-80% |
| 256     | ~1GB   | 200-400 URLs/sec | 60-100% |
| 512     | ~2GB   | 300-500 URLs/sec | 80-100% |

Governor auto-adjusts based on system performance.

## Configuration

### High-Memory Systems (32GB+)

```rust
// src/main.rs - governor_task()
const MIN_PERMITS: usize = 64;
const MAX_PERMITS: usize = 1024;

// src/bfs_crawler.rs
const CHUNK_SIZE: usize = 128 * 1024;

// src/frontier.rs
BloomFilter::with_false_pos(0.01).expected_items(50_000_000)
```

### Low-Memory Systems (8GB)

```rust
const MIN_PERMITS: usize = 16;
const MAX_PERMITS: usize = 128;
const CHUNK_SIZE: usize = 32 * 1024;
BloomFilter::with_false_pos(0.01).expected_items(5_000_000)
```

### NVMe SSD

```rust
const TARGET_COMMIT_MS: f64 = 5.0;
const MAX_PERMITS: usize = 1024;
```

### HDD

```rust
const TARGET_COMMIT_MS: f64 = 50.0;
const MAX_PERMITS: usize = 128;
```

## Library Usage

```rust
use rust_sitemap::{BfsCrawler, BfsCrawlerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BfsCrawlerConfig {
        max_workers: 256,
        user_agent: "MyBot/1.0".to_string(),
        timeout_secs: 45,
        ignore_robots: false,
        max_content_size: 10 * 1024 * 1024,
        enable_redis: false,
        redis_url: None,
        lock_ttl: 300,
    };

    let crawler = build_crawler(
        "https://example.com".to_string(),
        "./data",
        config,
    ).await?;

    crawler.initialize("all").await?;

    // LocalSet required for lol_html parser (!Send)
    let local_set = tokio::task::LocalSet::new();
    let result = local_set.run_until(async move {
        crawler.start_crawling().await
    }).await?;

    println!("Crawled {} URLs in {}s", result.discovered, result.duration_secs);
    Ok(())
}
```

### Distributed Crawling

```rust
let config = BfsCrawlerConfig {
    enable_redis: true,
    redis_url: Some("redis://127.0.0.1:6379".to_string()),
    lock_ttl: 300,
    ..Default::default()
};

// Run multiple instances - coordinate via Redis locks
```

## Features

### Graceful Shutdown

Ctrl+C triggers:
1. Stop accepting URLs
2. Complete in-flight requests
3. Save state to disk
4. Auto-export JSONL
5. Resume with `resume` command

### URL Normalization

- Relative paths: `./page`, `../page`
- Root-relative: `/path/to/page`
- Protocol-relative: `//example.com/page`
- Removes duplicate segments: `/a/b/../c` → `/a/c`

### Robots.txt Compliance

- Fetches and caches per host
- Respects `User-agent: *` and bot-specific rules
- Obeys `Disallow:` and `Crawl-delay:`
- Disable with `--ignore-robots`

### Per-Host Politeness

- Minimum 500ms delay between requests to same host
- Exponential backoff on errors
- Concurrent requests distributed across hosts

## Monitoring

```rust
pub struct Metrics {
    // Throughput
    pub urls_crawled: Counter,
    pub urls_per_second: Gauge,

    // Latency
    pub fetch_latency_ms: Histogram,
    pub commit_latency_ms: Histogram,
    pub commit_ewma_ms: Gauge,  // Governor target

    // Parser safety
    pub parser_abort_timeout: Counter,
    pub parser_abort_mem: Counter,
    pub parser_abort_handler_budget: Counter,
}
```

**Key Metrics**:
- `commit_ewma_ms`: Target ~10ms (governor control loop)
- `parser_abort_timeout`: High count = slow pages
- `urls_per_second`: Expected 100-500 under load

## Legal Warning

### Authorization Required

Obtain explicit permission before crawling:
- Written authorization from site owner
- Terms of service allowing automated access
- Public API for programmatic use
- Your own website for testing

### Legal Risks

| Violation | Consequence |
|-----------|-------------|
| **Terms of Service** | Account termination, lawsuits |
| **CFAA (US)** | Criminal charges, fines |
| **GDPR (EU)** | Fines up to 4% global revenue |
| **Trespass to Chattels** | Civil lawsuits |

### Responsible Usage

```bash
# Recommended for external sites
cargo run --release -- crawl --start-url https://example.com --workers 4 --timeout 60

# Aggressive (not recommended)
cargo run --release -- crawl --start-url https://example.com --workers 512 --ignore-robots
```

**Default settings (256 workers, 100-500 req/sec) are aggressive. Use lower settings for external sites.**

### Disclaimer

Authors not responsible for misuse. Users must verify legality, obtain permissions, use responsible rate limiting, respect robots.txt, and accept all legal responsibility.

## Testing

```bash
cargo test
cargo test test_convert_to_absolute_url
cargo test -- --nocapture
cargo test --release
```

Coverage: URL normalization, deduplication, frontier scheduling, Redis locks, parser streaming, error handling.

## Dependencies

```toml
[dependencies]
tokio = { version = "1.0", features = ["full"] }
reqwest = { version = "0.12", features = ["stream", "json"] }
redb = "2.1"
dashmap = "5.5"
fastbloom = "0.7"
lol_html = "1.2"
parking_lot = "0.12"
redis = { version = "0.24", features = ["tokio-comp", "connection-manager"] }
psl = "2.1"
crossbeam-queue = "0.3"
```

## Contributing

1. Fork repository
2. Create feature branch
3. Run tests (`cargo test`)
4. Commit changes
5. Open PR

## License

[MIT License](LICENSE)
