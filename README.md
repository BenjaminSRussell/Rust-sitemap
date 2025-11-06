# Rust Sitemap Crawler

Multi-threaded web crawler with sharded frontier architecture.

[![CI](https://github.com/BenjaminSRussell/rust-sitemap/workflows/CI/badge.svg)](https://github.com/BenjaminSRussell/rust-sitemap/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Architecture

```
┌──────────────┐   ┌──────────────────┐   ┌────────────────┐
│  BfsCrawler  │───│ Sharded Frontier │───│ WriterThread   │
│  Multi-core  │   │ N shards         │   │ WAL + batching │
│  Send+Sync   │   │ DashMap routing  │   │ Async channel  │
└──────────────┘   └──────────────────┘   └────────────────┘
```

### Core Components

- **Parser**: scraper (thread-safe)
- **Frontier**: Sharded DashMap + BinaryHeap
- **Storage**: redb + WAL
- **Governor**: Adaptive concurrency (32-512 workers)
- **Network**: reqwest with HTTP/2
- **Locks**: Optional Redis for distributed crawling

## Quick Start

```bash
# Build
cargo build --release

# Crawl
./target/release/rust_sitemap crawl --start-url https://example.com

# Resume
./target/release/rust_sitemap resume --data-dir ./data

# Export
./target/release/rust_sitemap export-sitemap --output sitemap.xml
```

## Commands

### crawl

```bash
rust_sitemap crawl --start-url <URL> [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `./data` | Storage directory |
| `--workers` | `256` | Concurrent workers |
| `--timeout` | `45` | Request timeout (seconds) |
| `--seeding-strategy` | `all` | `sitemap`, `ct`, `commoncrawl`, `all`, or `none` |
| `--ignore-robots` | `false` | Skip robots.txt |
| `--enable-redis` | `false` | Distributed mode |

### resume

```bash
rust_sitemap resume --data-dir ./data [OPTIONS]
```

Loads state from `crawler_state.redb` and continues.

### export-sitemap

```bash
rust_sitemap export-sitemap --data-dir ./data --output sitemap.xml [OPTIONS]
```

Generates XML sitemap from crawled data.

## Output

**JSONL**: Auto-generated at `./data/sitemap.jsonl`

```json
{
  "url": "https://example.com/page",
  "depth": 1,
  "status_code": 200,
  "title": "Page Title"
}
```

**XML**: Standard sitemaps.org format via `export-sitemap`

## Performance

| Workers | Memory | Throughput |
|---------|--------|------------|
| 32      | ~200MB | 50-100/s   |
| 128     | ~500MB | 150-300/s  |
| 256     | ~1GB   | 200-400/s  |
| 512     | ~2GB   | 300-500/s  |

Governor auto-adjusts based on commit latency.

## Configuration

Edit constants in source files:

**High memory** (32GB+):
```rust
// src/main.rs
const MAX_PERMITS: usize = 1024;
```

**Low memory** (8GB):
```rust
const MAX_PERMITS: usize = 128;
```

## Library Usage

```rust
use rust_sitemap::{BfsCrawler, BfsCrawlerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BfsCrawlerConfig {
        max_workers: 256,
        ..Default::default()
    };

    let (mut crawler, frontier_shards, _) =
        build_crawler("https://example.com".to_string(), "./data", config).await?;

    crawler.initialize("all").await?;

    // Spawn shards
    for mut shard in frontier_shards {
        tokio::spawn(async move {
            loop {
                shard.process_control_messages().await;
                shard.process_incoming_urls(&domain).await;
                shard.get_next_url().await;
            }
        });
    }

    let result = crawler.start_crawling().await?;
    Ok(())
}
```

## Features

- **Graceful shutdown**: Ctrl+C saves state and exports JSONL
- **URL normalization**: Handles relative/absolute paths
- **Robots.txt**: Fetched per-host with caching
- **Politeness**: Minimum 1s delay per host
- **Deduplication**: BloomFilter + DashMap

## Legal

**Get permission before crawling.** Default settings are aggressive. Use responsibly.

Violations may trigger:
- Account termination
- CFAA charges (US)
- GDPR fines (EU)
- Civil lawsuits

**Recommended for external sites:**
```bash
rust_sitemap crawl --start-url <URL> --workers 4 --timeout 60
```

## Testing

```bash
cargo test
cargo clippy
cargo build --release
```

## Dependencies

- tokio (async runtime)
- reqwest (HTTP client)
- scraper (HTML parser)
- redb (storage)
- dashmap (sharded maps)

## License

MIT
