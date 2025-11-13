# RustMapper Python Package

High-performance web crawler and sitemap generator with Python bindings.

## Installation

### From Source

```bash
# Install maturin if you haven't already
pip install maturin

# Build and install in development mode
maturin develop --release

# Or build a wheel
maturin build --release
```

## Quick Start

```python
import rustmapper

# Create a crawler
crawler = rustmapper.Crawler(
    start_url="https://example.com",
    workers=128,
    timeout=20,
    data_dir="./crawl_data",
    ignore_robots=False
)

# Run the crawl
results = crawler.crawl()

# Process results
for result in results:
    print(f"{result.url} - Status: {result.status_code} - Title: {result.title}")
```

## API Reference

### Crawler

Main crawler class for discovering URLs and generating sitemaps.

#### Constructor

```python
Crawler(
    start_url: str,
    workers: int = 128,
    timeout: int = 10,
    user_agent: str = "Rust-Sitemap-Crawler/1.0",
    ignore_robots: bool = False,
    data_dir: str = "./crawl_data"
)
```

**Parameters:**
- `start_url`: The starting URL to begin crawling from
- `workers`: Number of concurrent requests (default: 128)
- `timeout`: Request timeout in seconds (default: 10)
- `user_agent`: User agent string for requests
- `ignore_robots`: Skip robots.txt compliance (default: False)
- `data_dir`: Directory to store crawled data (default: ./crawl_data)

#### Methods

##### `crawl() -> List[CrawlResult]`

Start the crawl and return all results.

```python
results = crawler.crawl()
```

**Returns:** List of `CrawlResult` objects

**Raises:**
- `RuntimeError`: If the crawler fails

### CrawlResult

Represents a crawled URL with its metadata.

**Attributes:**
- `url`: The URL that was crawled
- `status_code`: HTTP status code (or None if not yet crawled)
- `title`: Page title (if available)
- `content_type`: Content type header (if available)
- `depth`: Depth from the start URL

### CrawlerConfig

Configuration object for the crawler (advanced usage).

```python
config = rustmapper.CrawlerConfig(
    workers=256,
    timeout=20,
    user_agent="MyBot/1.0",
    ignore_robots=False,
    save_interval=300,
    enable_redis=False,
    redis_url="redis://localhost",
    lock_ttl=60
)
```

## Examples

### Basic Crawl

```python
import rustmapper

crawler = rustmapper.Crawler(
    start_url="https://example.com",
    workers=64,
    timeout=15
)

results = crawler.crawl()
print(f"Crawled {len(results)} URLs")
```

### High-Performance Crawl

```python
import rustmapper

# Maximum throughput configuration
crawler = rustmapper.Crawler(
    start_url="https://example.com",
    workers=512,
    timeout=20,
    ignore_robots=True,  # Only if you have permission!
    data_dir="./high_perf_crawl"
)

results = crawler.crawl()
```

### Processing Results

```python
import rustmapper

crawler = rustmapper.Crawler(start_url="https://example.com")
results = crawler.crawl()

# Filter successful results
successful = [r for r in results if r.status_code == 200]

# Group by depth
from collections import defaultdict
by_depth = defaultdict(list)
for result in results:
    by_depth[result.depth].append(result)

print(f"Depth 0: {len(by_depth[0])} URLs")
print(f"Depth 1: {len(by_depth[1])} URLs")
print(f"Depth 2: {len(by_depth[2])} URLs")
```

## Features

- ⚡ **High Performance**: Written in Rust for maximum speed
- 🔄 **Concurrent**: Configurable number of concurrent workers
- 🛡️ **Polite**: Respects robots.txt and crawl-delay directives
- 💾 **Persistent**: Automatic state saving and crash recovery
- 🎯 **BFS**: Breadth-first search ensures efficient URL discovery
- 🔍 **Smart**: Automatic URL normalization and deduplication

## Performance

The Rust backend can handle:
- 512+ concurrent requests
- Thousands of URLs per second
- Millions of URLs with minimal memory usage
- Automatic politeness delays per host

## Requirements

- Python 3.8+
- Rust 1.70+ (for building from source)

## License

MIT License - see LICENSE file for details
