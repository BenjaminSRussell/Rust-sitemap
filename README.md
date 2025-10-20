# Rust Sitemap Crawler

A high-performance, memory-efficient web crawler built in Rust that generates complete sitemaps using BFS (Breadth-First Search) traversal. Designed to stay under 2GB RAM while crawling unlimited depths.

## Features

- 🚀 **BFS Crawler**: Systematic breadth-first traversal ensuring complete coverage
- 💾 **Memory Efficient**: Uses rkyv for persistent storage, stays under 2GB RAM
- 🔄 **Persistent Queue**: Spills to disk automatically when memory limits reached
- 🗺️ **Node Map**: Tracks all discovered URLs with duplicate detection
- 📊 **JSONL Export**: Export complete sitemap in JSONL format
- 🤖 **Robots.txt**: Respects robots.txt by default (configurable)
- ⚡ **Concurrent**: Multi-worker async architecture for fast crawling
- 📝 **No Depth Limits**: Crawls entire site regardless of depth

## Architecture

### Core Components

1. **BFS Crawler** (`bfs_crawler.rs`): Main crawler using breadth-first search
2. **Node Map** (`node_map.rs`): Persistent storage for discovered URLs with duplicate detection
3. **Rkyv Queue** (`rkyv_queue.rs`): Memory-efficient persistent queue
4. **Network** (`network.rs`): HTTP client for fetching pages
5. **Parser** (`parser.rs`): HTML link extraction
6. **Robots** (`robots.rs`): robots.txt parsing and compliance

### Data Storage

All data is stored in the specified data directory:
- `node_map.rkyv`: Persistent node map (URLs, relationships, metadata)
- `queue.rkyv`: Persistent URL queue
- Automatically flushes to disk when memory limits are reached

### Memory Management

- **Node Map**: ~500MB allocated, stores up to 50K nodes in memory
- **URL Queue**: ~300MB allocated, automatically spills to disk
- **Total Target**: < 2GB RAM usage during operation
- **Scalable**: Can process millions of URLs through disk persistence

## Installation

```bash
cargo build --release
```

## Usage

### Basic Crawl

```bash
cargo run --release -- crawl --start-url https://example.com
```

### Full Options

```bash
cargo run --release -- crawl \
  --start-url https://example.com \
  --data-dir ./crawl_data \
  --workers 4 \
  --rate-limit 10 \
  --timeout 30 \
  --user-agent "MyBot/1.0" \
  --export-jsonl
```

### Export Existing Crawl to JSONL

```bash
cargo run --release -- orient-map \
  --data-dir ./crawl_data \
  --output ./sitemap.jsonl
```

## Command Line Options

### `crawl` Command

- `--start-url` (required): Starting URL to begin crawling
- `--data-dir` (default: `./data`): Directory to store crawled data
- `--workers` (default: `4`): Number of concurrent workers
- `--rate-limit` (default: `2`): Requests per second
- `--timeout` (default: `15`): Request timeout in seconds
- `--user-agent` (default: `RustSitemapCrawler/1.0`): User agent string
- `--ignore-robots`: Disable robots.txt compliance
- `--export-jsonl`: Export to JSONL after crawling

### `orient-map` Command

- `--data-dir` (default: `./data`): Directory containing crawled data
- `--output` (default: `./sitemap.jsonl`): Output file for sitemap

## Output Format

The JSONL export contains one JSON object per line, each representing a discovered URL:

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

## Library Usage

You can also use this as a library in your Rust projects:

```rust
use rust_sitemap::{BfsCrawlerState, BfsCrawlerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BfsCrawlerConfig::default();
    
    let mut crawler = BfsCrawlerState::new(
        "https://example.com".to_string(),
        "./data",
        config,
    )?;
    
    crawler.initialize().await?;
    let result = crawler.start_crawling().await?;
    crawler.export_to_jsonl("./sitemap.jsonl").await?;
    
    println!("Crawled {} URLs", result.total_discovered);
    Ok(())
}
```

## Performance

- **Memory**: Stays under 2GB RAM regardless of site size
- **Scalability**: Can crawl millions of pages through disk persistence
- **Speed**: Concurrent workers with configurable rate limiting
- **Duplicate Detection**: O(1) URL lookups via HashSet

## Technical Details

### Duplicate Detection

The crawler uses a two-tier duplicate detection system:
1. In-memory HashSet for fast lookups
2. Persistent node map for long-term storage

URLs are checked before being added to the queue, ensuring no duplicates are processed.

### BFS Traversal

The crawler uses true breadth-first search:
1. Start URL added to queue at depth 0
2. Workers process URLs in FIFO order
3. Discovered links added at depth + 1
4. No maximum depth limit

### Data Persistence

All data is persisted using rkyv (zero-copy deserialization):
- Fast serialization/deserialization
- Compact binary format
- Length-prefixed records for streaming
- Automatic disk spillover when memory limits reached

## Dependencies

Core dependencies:
- `tokio`: Async runtime
- `rkyv`: Zero-copy serialization
- `reqwest`: HTTP client
- `scraper`: HTML parsing
- `sled`: Not used in current version (removed)
- `clap`: Command-line argument parsing
- `serde`/`serde_json`: JSON serialization

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
# Rust-sitemap
