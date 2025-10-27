# Rust Sitemap Crawler

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge)

**A lock-free web crawler built in Rust for mapping websites**

[Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Legal Notice](#legal--ethical-warning)

---

## Overview

This is a breadth-first web crawler for mapping websites. It uses Rust's async/await with Tokio for concurrent execution, achieving 200+ requests per second with 256 concurrent workers while maintaining zero race conditions through lock-free data structures.

### Key Features

- **High Concurrency**: 256 async workers with non-blocking architecture
- **Memory Efficient**: Disk-backed storage with automatic spillover when memory limits are reached
- **Zero Duplicates**: Triple-layer deduplication using bloom filters, persistent storage, and in-memory cache
- **URL Normalization**: Properly handles relative paths and `..` segments
- **Smart Retry Logic**: Exponential backoff with per-domain failure tracking
- **Robots.txt Compliant**: Respects crawl rules by default
- **Export to JSONL**: Complete sitemap with metadata for each discovered URL

---

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/rust-sitemap.git
cd rust-sitemap
cargo build --release
```

### Basic Usage

```bash
# Crawl a website (respects robots.txt)
cargo run --release -- crawl --start-url https://example.com

# With custom settings
cargo run --release -- crawl \
  --start-url https://example.com \
  --data-dir ./crawl_data \
  --workers 256 \
  --rate-limit 200 \
  --timeout 45 \
  --export-jsonl

# Export an existing crawl
cargo run --release -- orient-map \
  --data-dir ./crawl_data \
  --output ./sitemap.jsonl
```

---

## Features

### Concurrent Architecture

The crawler uses a spawn-and-forget pattern where workers spawn background tasks for each URL and immediately move to the next one. This prevents fast URLs from being blocked by slow ones, maximizing throughput.

**Implementation**: [`bfs_crawler.rs:497-638`](src/bfs_crawler.rs#L497-L638)

### Triple-Layer Deduplication

1. **Bloom Filter**: Fast probabilistic check (O(1), 10M URLs in ~10MB)
2. **Sled DB**: Persistent authoritative storage (prevents duplicates across restarts)
3. **DashMap**: Lock-free in-memory cache for hot nodes

This guarantees zero duplicate crawls while maintaining O(1) lookup performance.

**Implementation**: [`node_map.rs:98-160`](src/node_map.rs#L98-L160)

### URL Normalization

The crawler properly normalizes URLs before deduplication, including:
- Resolving relative paths (`./`, `../`)
- Removing redundant path segments (`/a/b/../c` → `/a/c`)
- Handling all URL types (absolute, protocol-relative, root-relative)

This ensures URLs like `/admission/visit/../default.aspx` and `/admission/default.aspx` are recognized as the same page.

**Implementation**: [`bfs_crawler.rs:773-787`](src/bfs_crawler.rs#L773-L787)

### Domain-Level Failure Tracking

Per-domain exponential backoff prevents wasting resources on slow or failing domains:
- 1st failure: 2s backoff
- 2nd failure: 4s backoff
- 3rd failure: 8s backoff
- 5+ failures: Domain permanently blocked

**Implementation**: [`bfs_crawler.rs:18-124`](src/bfs_crawler.rs#L18-L124)

### Smart Retry Logic

The crawler classifies errors into retryable and permanent:

| Error Type | Retryable | Reason |
|------------|-----------|--------|
| Timeout | Yes | Transient network issue |
| Connection reset | Yes | Temporary server hiccup |
| DNS failure | No | Domain doesn't exist |
| SSL/TLS error | No | Certificate problem |
| Connection refused | No | Server blocking requests |

**Implementation**: [`network.rs:143-227`](src/network.rs#L143-L227)

---

## Command-Line Options

### `crawl` Command

```bash
cargo run --release -- crawl [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--start-url` | *required* | Starting URL to begin crawling |
| `--data-dir` | `./data` | Directory to store crawled data |
| `--workers` | `256` | Number of concurrent workers |
| `--rate-limit` | `200` | Requests per second |
| `--timeout` | `45` | Request timeout in seconds |
| `--user-agent` | `RustSitemapCrawler/1.0` | User agent string |
| `--ignore-robots` | `false` | Disable robots.txt compliance |
| `--export-jsonl` | `false` | Export to JSONL after crawling |

### `orient-map` Command

```bash
cargo run --release -- orient-map [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--data-dir` | `./data` | Directory containing crawled data |
| `--output` | `./sitemap.jsonl` | Output file for the sitemap |

---

## Output Format

The JSONL export contains one JSON object per line:

```json
{
  "url": "https://example.com/page",
  "depth": 1,
  "parent_url": "https://example.com",
  "discovered_at": 1634567890,
  "status_code": 200,
  "content_type": "text/html",
  "title": "Page Title",
  "links": ["https://example.com/link1", "https://example.com/link2"]
}
```

**Fields:**
- `url`: The discovered URL
- `depth`: BFS depth from start URL (0 = start URL)
- `parent_url`: URL that contained this link
- `discovered_at`: Unix timestamp
- `status_code`: HTTP status code (200, 404, etc.)
- `content_type`: MIME type
- `title`: Page title from `<title>` tag
- `links`: All outgoing links discovered on this page

---

## Performance

### Benchmarks

| Metric | Value |
|--------|-------|
| **Throughput** | 200+ requests/second |
| **Concurrency** | 256 concurrent workers |
| **Memory Usage** | ~500MB (with 100K nodes in memory) |
| **Dedup Speed** | O(1) with bloom filter |
| **Channel Capacity** | 500K+ in-flight URLs |

### Optimizations

| Technique | Benefit |
|-----------|---------|
| Lock-free data structures | Zero mutex contention |
| Non-blocking workers | Fast URLs never blocked by slow ones |
| Zero-copy serialization (Rkyv) | No parsing overhead |
| Per-domain rate limiting | Fast domains run at full speed |
| Triple-layer deduplication | O(1) fast path for duplicate detection |

---

## Architecture

### System Overview

```
┌─────────────┐
│  CLI Input  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  BFS Crawler State  │
├─────────────────────┤
│ • NodeMap (dedup)   │
│ • Queue (Rkyv)      │
│ • Workers (256)     │
│ • HTTP Client       │
└──────┬──────────────┘
       │
       ▼
┌────────────────────────────┐
│  Kanal Channel (500K cap)  │
└────────┬───────────────────┘
         │
         ▼
┌────────────────────────────┐
│  Workers (async loop)      │
│  • Receive URL             │
│  • Check domain status     │
│  • Spawn background task   │
│  • Move to next URL        │
└────────┬───────────────────┘
         │
         ▼
┌────────────────────────────┐
│  Background Task           │
│  • HTTP fetch              │
│  • Parse HTML              │
│  • Extract links           │
│  • Normalize URLs          │
│  • Dedup & queue new URLs  │
└────────────────────────────┘
```

### Core Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| **BFS Crawler** | Tokio async | Orchestrates workers and crawl process |
| **NodeMap** | DashMap + Sled | Lock-free URL storage with persistence |
| **Bloom Filter** | [bloomfilter](https://crates.io/crates/bloomfilter) | O(1) duplicate detection |
| **Queue** | [Kanal](https://github.com/fereidani/kanal) + Rkyv | Lock-free MPMC channel with disk spillover |
| **HTTP Client** | Hyper + TLS | Connection pooling, retry logic |
| **Parser** | [Scraper](https://crates.io/crates/scraper) | HTML link extraction |
| **URL Normalization** | [url](https://crates.io/crates/url) | Proper handling of relative paths |

---

## Library Usage

You can use this as a library in your Rust projects:

```rust
use rust_sitemap::{BfsCrawlerState, BfsCrawlerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BfsCrawlerConfig {
        max_workers: 256,
        rate_limit: 200,
        timeout: 45,
        user_agent: "MyBot/1.0".to_string(),
        ignore_robots: false,
        max_memory_bytes: 10 * 1024 * 1024 * 1024, // 10GB
        auto_save_interval: 300, // 5 minutes
        redis_url: None,
        lock_ttl: 60,
        enable_redis_locking: false,
    };

    let mut crawler = BfsCrawlerState::new(
        "https://example.com".to_string(),
        "./data",
        config,
    ).await?;

    crawler.initialize().await?;
    let result = crawler.start_crawling().await?;
    crawler.save_state().await?;
    crawler.export_to_jsonl("./sitemap.jsonl").await?;

    println!("Crawled {} URLs in {} seconds",
             result.total_discovered,
             result.crawl_duration_seconds);

    Ok(())
}
```

### Distributed Crawling with Redis

For multi-machine crawling, enable Redis-based URL locking:

```rust
let config = BfsCrawlerConfig {
    redis_url: Some("redis://127.0.0.1:6379".to_string()),
    enable_redis_locking: true,
    lock_ttl: 60,
    ..Default::default()
};
```

This allows multiple crawler instances to coordinate and avoid duplicate work.

---

## LEGAL & ETHICAL WARNING

### You Must Have Permission

**Before using this tool, ensure you have explicit authorization to crawl the target website.**

This means:
- Written authorization from the website owner, OR
- Clear terms of service that allow automated access, OR
- Public APIs designed for programmatic access, OR
- Your own website for testing purposes

### Legal Consequences

Unauthorized web scraping can result in:

| Violation | Potential Consequence |
|-----------|----------------------|
| **Terms of Service** | Account termination, legal action |
| **CFAA (US)** | Criminal charges, fines, imprisonment |
| **GDPR (EU)** | Fines up to 4% of revenue |
| **Trespass to Chattels** | Civil lawsuits for server resource consumption |

### Respect Robots.txt

This crawler respects robots.txt by default. **Do not use `--ignore-robots` unless you have explicit permission or are crawling your own website.**

### Use Appropriate Rate Limits

The default settings (200 req/s, 256 workers) are aggressive. For most external websites, use much lower settings:

```bash
# Recommended for external sites
cargo run --release -- crawl \
  --start-url https://example.com \
  --workers 4 \
  --rate-limit 2
```

Excessive requests can:
- Overload servers (unintentional DDoS)
- Get your IP banned
- Violate server policies

### You Are Responsible

**The authors of this software are not responsible for how you use it.**

By using this crawler, you agree to:
- Verify legality in your jurisdiction
- Obtain necessary permissions
- Use responsible rate limiting
- Respect robots.txt
- Accept all legal and ethical responsibility

**When in doubt, don't crawl. Ask for permission first.**

---

## Testing

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_convert_to_absolute_url

# Run with output
cargo test -- --nocapture
```

The project includes comprehensive tests for:
- URL normalization and relative path resolution
- Deduplication logic
- Error classification and retry logic
- HTML parsing and link extraction
- Domain failure tracking

---

## Dependencies

### Core Dependencies

```toml
[dependencies]
clap = { version = "4.4", features = ["derive"] }
rkyv = { version = "0.7", features = ["validation"] }
scraper = "0.18"
sled = "0.34"
hyper = { version = "0.14", features = ["client", "http1", "http2", "tcp"] }
hyper-tls = "0.5"
tokio = { version = "1.0", features = ["full"] }
url = "2.4"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
dashmap = "5.5"
kanal = "0.1.0-pre8"
bloomfilter = "1.0"
redis = { version = "0.24", features = ["tokio-comp", "connection-manager"] }
```

### Key Libraries

- **[Tokio](https://tokio.rs/)**: Async runtime for concurrent execution
- **[Rkyv](https://rkyv.org/)**: Zero-copy serialization
- **[Kanal](https://github.com/fereidani/kanal)**: Lock-free MPMC channels
- **[DashMap](https://github.com/xacrimon/dashmap)**: Concurrent HashMap
- **[Sled](https://github.com/spacejam/sled)**: Embedded database
- **[Hyper](https://hyper.rs/)**: HTTP client with TLS
- **[Scraper](https://crates.io/crates/scraper)**: HTML parsing
- **[url](https://crates.io/crates/url)**: URL parsing and normalization

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
git clone https://github.com/yourusername/rust-sitemap.git
cd rust-sitemap
cargo build

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run --release -- crawl --start-url https://example.com
```

---

## Recent Improvements

### URL Normalization (Latest)

The crawler now properly normalizes URLs using the `url` crate's `join()` method, which correctly handles:
- Parent directory references (`../`)
- Redundant path segments (`/a/b/../c` → `/a/c`)
- All URL types (absolute, protocol-relative, root-relative, relative)

This prevents the same page from being crawled multiple times with different URL representations.

**Fix**: [`bfs_crawler.rs:773-787`](src/bfs_crawler.rs#L773-L787)
**Tests**: [`bfs_crawler.rs:965-978`](src/bfs_crawler.rs#L965-L978)

---

## License

[MIT License](LICENSE) - See LICENSE file for details

---

## Acknowledgments

Built with the Rust ecosystem:
- Powered by Tokio for async runtime
- Uses zero-copy serialization with Rkyv
- Lock-free concurrency with Kanal and DashMap
- Persistent storage with Sled

---

**Remember: Always obtain permission before crawling websites. Respect robots.txt and use appropriate rate limits.**
