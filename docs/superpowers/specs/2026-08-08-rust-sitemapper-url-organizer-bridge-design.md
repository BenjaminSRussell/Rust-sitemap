# Bridge: rust-sitemapper → url-organizer

## Purpose

`rust-sitemapper` (this repo) crawls sites and produces `sitemap.jsonl` per crawl.
`url-organizer` (a separate repo at `~/Desktop/url organizer`,
github.com/BenjaminSRussell/ideal-url-organizer) analyzes URL/content data with 25+
organization methods (by domain, depth, TLD, path structure, dedup, network graph,
schema.org type, semantic clustering, etc.) and produces insight reports.

Today there's no connection between them — any rust-sitemapper crawl's output just
sits as a JSONL file. This adds a small, repeatable command that hands a crawl's
output to url-organizer and runs its analysis pipeline, so every future crawl can be
turned into an insight report with one command.

The two repos stay fully separate (independent git histories, independent
dependencies — Rust vs. Python). The bridge is a single new script living in
url-organizer, since url-organizer is the "ingestion" side and already has a
pluggable `DataLoader` abstraction built for this.

## Scope: v1 covers methods 1–21 only

url-organizer has two independent input pathways, not one:

- **Methods 1–21** (pure URL-structure: by domain, depth, TLD, path, query params,
  dedup, network graph, etc.) read `data/raw/urls.jsonl` into a strict Python
  dataclass, `URLRecord` (15 fields: `schema_version, url, url_normalized, depth,
  parent_url, fragments, discovered_at, queued_at, crawled_at, response_time_ms,
  status_code, content_type, content_length, title, link_count`), loaded via
  `URLRecord(**data)` — **any unexpected key in the JSON raises `TypeError` and
  aborts the whole load.**
- **Methods 22–25** (schema.org type, page authority, semantic similarity — the
  content-based "goldmine" methods) read a *differently shaped* dataclass,
  `PageContent` (nested `json_ld`, `schema_org_types`, `h1_tags`/`h2_tags`,
  `redirect_chain`, `text_content`, …) from `data/crawled/pages.jsonl`, normally
  produced by url-organizer's own built-in web crawler.

rust-sitemapper's `sitemap.jsonl` already contains every field the 15-field
`URLRecord` schema needs, verified directly against a live sample
(`https://www.ebay.com/` crawl output) — it's a strict superset, not a mismatch.
It also carries ~13 additional rich fields (`metadata_json`, `structured_data_json`,
`privacy_metadata_json`, `tech_profile`, etc.) that the strict `URLRecord(**data)`
loader would choke on unfiltered.

v1 bridges methods 1–21 only: a straight field-filter, no restructuring, immediate
value. Mapping into `PageContent`'s different shape for methods 22–25 is real,
separate work (nested structured-data extraction, nothing to reuse from v1) — a
natural fast-follow once v1 is proven, not bundled in now.

## Design

**One script, two steps**, run as `python3 scripts/import_rust_sitemapper.py
<path-to-sitemap.jsonl>` from url-organizer's repo root:

1. **Convert**: stream the input JSONL line by line (memory-safe for large crawls).
   For each record, keep only the 15 `URLRecord` field names (silently drop the rest
   — they're not lost, the original rust-sitemapper file is untouched) and write the
   result to `data/raw/urls.jsonl`, replacing whatever was there before. That file is
   git-tracked in url-organizer with a clean working tree today, so the existing
   sample dataset (UMass Amherst crawl) stays recoverable via git even after being
   overwritten.
2. **Run**: invoke url-organizer's existing `./run.sh --all` (methods 1–21 + data
   quality analysis + visualization + unified HTML report) against the freshly
   written data. No new pipeline code — this reuses url-organizer's entry point
   exactly as it exists today.

**Error handling**: skip and count (not abort on) individual malformed input lines
(bad JSON, missing a required field) — print a one-line summary of skipped-vs-loaded
at the end. A single bad line in a 10,000-URL crawl shouldn't block the whole import.
Abort with a clear message if the input file doesn't exist or is empty.

**Testing**: a small unit test (module-only, no full pipeline run) that feeds a
handful of synthetic rust-sitemapper-shaped JSON lines — including one with extra
rich fields, one with a missing optional field, one malformed line — through the
filter function and asserts the output matches `URLRecord`'s expected shape and the
malformed line is skipped, not fatal. Full end-to-end validation is a manual run
against a real rust-sitemapper crawl output (e.g. today's `ebay-test`/`walmart` test
data), confirming `./run.sh --all` completes and produces the expected
`data/results/` method outputs.

## Out of scope (fast-follow, not now)

- Methods 22–25 (`PageContent`/`data/crawled/pages.jsonl` pathway) — different
  schema shape, needs real mapping work, not a filter.
- Any change to rust-sitemapper itself — it already produces a compatible superset
  of the required fields; no Rust-side work needed for v1.
- Automatic/triggered invocation (e.g. rust-sitemapper calling the bridge itself
  post-crawl) — keeps the repos loosely coupled; this stays a manual, explicit step
  for now.
