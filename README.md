# Rust Sitemap Crawler

Concurrent web crawler in Rust. Up to 512 concurrent workers with adaptive concurrency control, sharded frontier, persistent state with WAL, distributed crawling with Redis.

Available as both a standalone CLI tool and a Python package.

## Install

### Python Package (Recommended)

```bash
pip install rustmapper
```

### From Source (Rust)

```bash
cargo build --release
```

## Usage

### Python Package

Once installed via pip, use the `rustmapper` command:

```bash
# Basic crawl
rustmapper crawl --start-url example.com

# With options
rustmapper crawl --start-url example.com --workers 128 --timeout 10

# Resume
rustmapper resume --data-dir ./data

# Export sitemap
rustmapper export-sitemap --data-dir ./data --output sitemap.xml

# Classify tech stacks from crawl JSONL
rustmapper classify --data-dir ./data --out tech_report.json
rustmapper classify --input ./fixtures/sample.jsonl --out tech_report.json --shopify-only
```

### Python API

```python
from rustmapper import Crawler

# Create a crawler instance
crawler = Crawler(
    start_url="https://example.com",
    data_dir="./data",
    workers=256,
    timeout=20,
    ignore_robots=False
)

# Start crawling
results = crawler.crawl()
print(f"Discovered: {results['discovered']}, Processed: {results['processed']}")

# Export to sitemap
crawler.export_sitemap(
    output="sitemap.xml",
    include_lastmod=True,
    include_changefreq=True,
    default_priority=0.5
)
```

### Rust CLI (from source)

```bash
# Basic crawl
cargo run --release -- crawl --start-url example.com

# With options
cargo run --release -- crawl --start-url example.com --workers 128 --timeout 10

# Resume
cargo run --release -- resume --data-dir ./data

# Export sitemap
cargo run --release -- export-sitemap --data-dir ./data --output sitemap.xml
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--start-url` | required | Starting URL |
| `--workers` | 512 | Concurrent requests (adaptive) |
| `--timeout` | 20 | Request timeout (seconds) |
| `--data-dir` | ./data | Storage location |
| `--seeding-strategy` | all | none/sitemap/ct/commoncrawl/all |
| `--seeder-timeout` | 120 | Seconds each seeder may run; on expiry the crawl starts with what was seeded |
| `--wal-checkpoint-every` | 64 | Truncate the WAL after N committed batches (0 = off) |
| `--wal-max-bytes` | 67108864 | Truncate the WAL once it reaches this size (0 = off) |
| `--ignore-robots` | false | Skip robots.txt |
| `--enable-redis` | false | Distributed mode |
| `--redis-url` | - | Redis connection |
| `--html-report` | off | Write a static HTML report at end of crawl |
| `--metrics-addr` | off | Serve Prometheus `/metrics` and a live `/report` (crawl and resume) |

## Seeding Strategies

- `none` - Only start URL
- `sitemap` - Discover from sitemap.xml
- `ct` - Certificate Transparency logs (finds subdomains)
- `commoncrawl` - Query Common Crawl index
- `all` - Use all methods

## Performance

**Timing breakdown per URL:**
- Body download: 700-900ms (70-90%)
- Network fetch: 50-550ms (10-20%)
- Everything else: <50ms (<5%)

**Throughput:** 50-200 URLs/minute depending on page size. Network I/O bound.

**Recommended settings:**
```bash
# Focused crawl (skip subdomains)
--timeout 10 --seeding-strategy sitemap

# University sites (avoid internal hosts)
--timeout 5 --seeding-strategy sitemap --start-url www.university.edu

# Maximum discovery (all seeders)
--workers 256 --timeout 10 --seeding-strategy all
```

## Output

Sitemap export (`export-sitemap`) writes a single `sitemap.xml` urlset when the crawl has ≤50,000 URLs. Larger crawls split into `sitemap-1.xml`, `sitemap-2.xml`, … and write a `sitemapindex` at the `--output` path (override the cap with `--max-urls-per-sitemap`).


**JSONL** (automatic): `./data/sitemap.jsonl`
```json
{"url":"https://example.com/","depth":0,"status_code":200,"content_length":1024,"title":"Example","link_count":5}
```

**XML sitemap:**
```bash
cargo run --release -- export-sitemap --data-dir ./data --output sitemap.xml
```

## Distributed Crawling

```bash
# Instance 1
cargo run --release -- crawl --start-url example.com --enable-redis --redis-url redis://localhost:6379

# Instance 2
cargo run --release -- crawl --start-url example.com --enable-redis --redis-url redis://localhost:6379
```

Automatic URL deduplication, work stealing, distributed locks.

## Architecture

- **Frontier**: Sharded queues (CPU-core based sharding), bloom filter dedup, per-host politeness
- **State**: Embedded redb database + WAL for crash recovery
- **Governor**: Adaptive concurrency control (32-512 workers) based on commit latency
- **Workers**: Async task pool with semaphore-based backpressure
- **Privacy**: Collects metadata (cookies, tracking pixels, third-party scripts) for privacy analysis. JSONL rows include a `privacy_signals` object (`cookie_count`, `high_risk_cookie_count`, `third_party_api_count`, `external_resource_count`, `tracking_suspected`, `has_etag`). Disable with `--no-emit-privacy`.

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Slow crawling | Normal - large pages take ~1s to download | Network I/O bound, expected |
| Crawl start stalls on seeding | crt.sh / Common Crawl slow or rate-limiting | Lower `--seeder-timeout 30`; seeders log `accepted/rejected/errors/timed_out` |
| Many timeouts | Internal/unreachable hosts (CT log discovery) | Reduce timeout: `--timeout 5` or use `--seeding-strategy sitemap` |
| Out of memory | Too many concurrent large pages | Reduce workers: `--workers 64` |
| Stops unexpectedly | Check if naturally completed (frontier empty) | Use `resume` to continue |

## Testing

```bash
cargo test
```
## License

MIT


### Platform parsers (#39)

Optional crawl flags:

- `--enable-nextjs-parser` — extract URLs from `__NEXT_DATA__` on Next.js pages
- `--enable-shopify-parser` — enqueue Shopify product `.json` discovery URLs

Both default **off** so baseline crawls stay lean.

### Operator HTML report (#35)

Pass `--html-report ./report.html` on `crawl` to write a static Metrics summary at end of run (no Prometheus required).
The report includes the per-host budget table described below.

### Prometheus endpoint and live report (#34)

`--metrics-addr 127.0.0.1:9100` (on `crawl` or `resume`) starts a small HTTP server. It is off by default, so CLI and Python embeds stay quiet.

| Path | Content |
|------|---------|
| `/metrics` | Prometheus text exposition |
| `/report` (or `/`) | The HTML report, regenerated per request and auto-refreshing every 5 s |
| `/healthz` | `ok` |

Exported series:

| Metric | Type | Meaning |
|--------|------|---------|
| `rustmapper_urls_discovered_total` / `_fetched_total` / `_processed_total` | counter | Throughput |
| `rustmapper_urls_failed_total`, `rustmapper_urls_timeout_total` | counter | Errors and timeouts |
| `rustmapper_commit_ewma_ms` | gauge | Writer commit EWMA that drives the governor |
| `rustmapper_throttle_adjustments_total`, `rustmapper_throttle_permits_available` | counter / gauge | Governor activity |
| `rustmapper_writer_batches_total`, `rustmapper_writer_batch_bytes_total`, `rustmapper_wal_appends_total` | counter | Persistence |
| `rustmapper_seconds_since_last_discovery` | gauge | Plateau signal |
| `rustmapper_http_responses_total{version}` | counter | HTTP/1.1, 2 and 3 mix |
| `rustmapper_hosts{status}` | gauge | Hosts that are ready, delayed, saturated, in backoff, or blocked |

All values are read from counters the crawler already maintains, so scraping adds no work to the fetch path.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: rustmapper
    scrape_interval: 5s
    static_configs:
      - targets: ["127.0.0.1:9100"]
```

Example Grafana panels:
- Throughput: `rate(rustmapper_urls_processed_total[1m])`
- Timeout ratio: `rate(rustmapper_urls_timeout_total[5m]) / rate(rustmapper_urls_fetched_total[5m])`
- Governor pressure: `rustmapper_commit_ewma_ms`
- Starved hosts: `sum(rustmapper_hosts{status=~"backoff|blocked"})`

### Per-host rate-limit budget (#36)

Both the static and the live report list the 25 most constrained hosts, built from each frontier shard's politeness state. Columns:

- status (`blocked` > `backoff` > `saturated` > `delayed` > `ready`)
- inflight / max concurrent requests, and the slots remaining
- robots.txt crawl-delay
- seconds until the host is next eligible
- seconds of error backoff left
- consecutive failures

When any host is in backoff or blocked, the report shows a red starvation alert, because those hosts' queued URLs are not being fetched. For a live view during a crawl, keep `http://127.0.0.1:9100/report` open in a browser.
