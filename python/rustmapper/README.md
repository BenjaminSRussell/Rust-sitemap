# RustMapper Python package

Python access to the Rust sitemap crawler, in two forms.

| | Subprocess wrapper (`rustmapper.Crawler`) | In-process native module (`rustmapper.native`) |
|---|---|---|
| Needs | the `rust_sitemap` CLI binary | a maturin-built wheel (`_rustmapper` extension) |
| Results | `sitemap.jsonl`, read or **streamed** | a list returned from `crawl()` |
| Options | the full CLI: seeding, Redis, resume, export | start URL, workers, timeout, UA, robots, data dir |

## Install

```bash
# 1) the binary the wrapper drives (installs `rust_sitemap`)
cargo install --git https://github.com/BenjaminSRussell/Rust-sitemap
#    ...or use a local build:  export RUSTMAPPER_BIN=$PWD/target/release/rust_sitemap

# 2) the Python package (wrapper + native module), from a checkout
pip install maturin
maturin develop --release          # or: maturin build --release && pip install target/wheels/*.whl
```

The binary is found in this order: `Crawler(binary=...)`, then `$RUSTMAPPER_BIN`, then `rust_sitemap` or `rustmapper` on `PATH`. If none is found you get a `FileNotFoundError` that lists exactly these options.

## Streaming results

```python
from rustmapper import Crawler, iter_results

crawler = Crawler("https://example.com", data_dir="./data", workers=128)

for r in crawler.crawl_stream():          # yields while sitemap.jsonl is being written
    print(r.url, r.status_code, r.title)
    if r.depth > 3:
        break                             # closing the generator terminates the crawler

for r in iter_results("./data/sitemap.jsonl"):   # stream an existing export
    ...
```

How `crawl_stream()` behaves:
- It starts the binary (`crawl`, or `resume` with `resume=True`) and tails `<data_dir>/sitemap.jsonl`. Each line becomes a `CrawlResult` once it is complete. A record split across writes is held until its newline arrives, and a final line without a newline is still delivered.
- Memory stays flat, because results are never collected into a list.
- The binary writes `sitemap.jsonl` in its export phase, at the end of the crawl or on graceful shutdown. Results therefore start arriving once the crawl itself is done. What streams is the export, not the fetches.
- If the crawler exits non-zero, every record it wrote is yielded first, and then a `RuntimeError` is raised with the exit code and the tail of stderr.

`crawl()`, `resume()` and `read_results()` return lists. `export_sitemap()` writes XML.

## CrawlResult

`CrawlResult` wraps one `sitemap.jsonl` record (a serialized `SitemapNode`).

| Attribute | Type | Notes |
|---|---|---|
| `url`, `title` | `str` | `""` when null |
| `depth`, `status_code`, `content_length`, `link_count` | `int` | `0` when null (not yet crawled or failed) |
| `parent_url`, `content_type` | `str` or `None` | |
| `crawled_at`, `response_time_ms`, `schema_version` | `int` or `None` | |

Other members:
- `crawled`: whether the URL has been fetched.
- `to_dict()`: the raw record, including fields like `privacy_signals`.
- `CrawlResult.from_json_line(line, lineno)`: raises `ValueError` naming the line if the record is invalid.

## In-process crawl

```python
import rustmapper
if rustmapper.native is not None:
    results = rustmapper.native.Crawler("https://example.com", workers=64, timeout=10).crawl()
```

`rustmapper.native` is `None` when the package is imported from source without building the extension.

## Tests

```bash
pytest python/tests     # no network, no Rust build: a fake binary stands in via RUSTMAPPER_BIN
```

## Privacy signals (#38)

Crawl JSONL may include `privacy_signals` with these fields:
- `cookie_count`
- `high_risk_cookie_count`
- `third_party_api_count`
- `external_resource_count`
- `tracking_suspected`
- `has_etag`

Pass `--no-emit-privacy` on the CLI to omit them.
