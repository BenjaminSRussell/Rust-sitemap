"""Export crawl JSONL to Parquet / Delta for lakehouse handoff (#33).

    python -m rustmapper.export_parquet --data-dir ./data --output ./data/parquet/discovery/
    python -m rustmapper.export_parquet --data-dir ./data --delta ./data/delta/stage1_discovery

The export lives in the Python package, so the Rust binary doesn't grow
(no arrow/parquet crates). It needs ``pyarrow`` (``pip install
"rustmapper[parquet]"``), and ``--delta`` also needs ``deltalake``
(``pip install "rustmapper[delta]"``).

The table schema is ``SCHEMA`` (versioned by ``EXPORT_SCHEMA_VERSION``,
which is also stored in every row and in the Parquet metadata). Field
order and types only change with a version bump; new fields are appended
as nullable columns.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union
from urllib.parse import urlsplit

EXPORT_SCHEMA_VERSION = 1

# (name, arrow type name, nullable, description), the documented contract.
# Timestamps are microseconds UTC: Parquet has no second unit and Delta Lake
# stores timestamps as microseconds, so this round-trips unchanged through both.
COLUMNS = [
    ("schema_version", "int16", False, "export schema version (this table's contract)"),
    ("node_schema_version", "int16", True, "SitemapNode schema version that produced the record"),
    ("url", "string", False, "URL as discovered"),
    ("url_normalized", "string", True, "normalized URL used for de-duplication"),
    ("host", "string", True, "lower-cased host of url"),
    ("depth", "int32", False, "BFS depth from the start URL"),
    ("parent_url", "string", True, "page the URL was discovered on"),
    ("status_code", "int32", True, "HTTP status; null when not fetched"),
    ("content_type", "string", True, "Content-Type header"),
    ("content_length", "int64", True, "response size in bytes"),
    ("title", "string", True, "<title>"),
    ("link_count", "int32", True, "links found on the page"),
    ("response_time_ms", "int64", True, "fetch latency"),
    ("discovered_at", "timestamp[us, UTC]", True, "when the URL entered the frontier"),
    ("fetched_at", "timestamp[us, UTC]", True, "when it was crawled (SitemapNode.crawled_at); null if never"),
    ("description", "string", True, "meta description"),
    ("canonical_url", "string", True, "rel=canonical"),
    ("language", "string", True, "page language"),
    ("tech_profile", "string", True, "technology classification (e.g. Shopify, Next.js)"),
    ("privacy_signals_json", "string", True, "privacy_signals object as JSON (absent with --no-emit-privacy)"),
    ("exported_at", "timestamp[us, UTC]", False, "when this export ran"),
]


def _require_pyarrow():
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised via message only
        raise ImportError('Parquet export needs pyarrow: pip install "rustmapper[parquet]"') from exc
    import pyarrow as pa
    return pa


def arrow_schema():
    pa = _require_pyarrow()
    types = {
        "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(), "string": pa.string(),
        "timestamp[us, UTC]": pa.timestamp("us", tz="UTC"),
    }
    fields = [pa.field(n, types[t], nullable=nullable, metadata={"description": d})
              for n, t, nullable, d in COLUMNS]
    return pa.schema(fields, metadata={"rustmapper.export_schema_version": str(EXPORT_SCHEMA_VERSION),
                                       "rustmapper.table": "discovery"})


def _ts(value: Any) -> Optional[datetime]:
    if value in (None, 0):
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def _host(url: str) -> Optional[str]:
    try:
        return (urlsplit(url).hostname or None)
    except ValueError:
        return None


def record_to_row(rec: Dict[str, Any], exported_at: datetime) -> Dict[str, Any]:
    """Map one sitemap.jsonl record (a serialized SitemapNode) to a SCHEMA row."""
    url = rec.get("url") or ""
    privacy = rec.get("privacy_signals")
    return {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "node_schema_version": rec.get("schema_version"),
        "url": url,
        "url_normalized": rec.get("url_normalized"),
        "host": _host(url),
        "depth": int(rec.get("depth") or 0),
        "parent_url": rec.get("parent_url"),
        "status_code": rec.get("status_code"),
        "content_type": rec.get("content_type"),
        "content_length": rec.get("content_length"),
        "title": rec.get("title"),
        "link_count": rec.get("link_count"),
        "response_time_ms": rec.get("response_time_ms"),
        "discovered_at": _ts(rec.get("discovered_at")),
        "fetched_at": _ts(rec.get("crawled_at")),
        "description": rec.get("description"),
        "canonical_url": rec.get("canonical_url"),
        "language": rec.get("language"),
        "tech_profile": rec.get("tech_profile"),
        "privacy_signals_json": json.dumps(privacy, sort_keys=True) if privacy is not None else None,
        "exported_at": exported_at,
    }


def _records(path: Path) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: invalid JSON ({exc})") from exc
            if not isinstance(rec, dict) or not rec.get("url"):
                raise ValueError(f"{path}:{lineno}: expected a SitemapNode object with a url")
            yield rec


def _batches(records: Iterable[Dict[str, Any]], exported_at: datetime, batch_rows: int):
    pa = _require_pyarrow()
    schema = arrow_schema()
    rows: List[Dict[str, Any]] = []
    for rec in records:
        rows.append(record_to_row(rec, exported_at))
        if len(rows) >= batch_rows:
            yield pa.RecordBatch.from_pylist(rows, schema=schema)
            rows = []
    if rows:
        yield pa.RecordBatch.from_pylist(rows, schema=schema)


def export_parquet(jsonl: Union[str, Path], output: Union[str, Path], batch_rows: int = 50_000,
                   exported_at: Optional[datetime] = None) -> Dict[str, Any]:
    """Write ``jsonl`` as one Parquet file in directory ``output``. Streams in batches (flat memory).

    Returns ``{"path", "rows", "schema_version"}``.
    """
    pa = _require_pyarrow()
    import pyarrow.parquet as pq
    jsonl, out_dir = Path(jsonl), Path(output)
    if not jsonl.is_file():
        raise FileNotFoundError(f"no crawl output at {jsonl} (run a crawl, or pass --input)")
    out_dir.mkdir(parents=True, exist_ok=True)
    exported_at = (exported_at or datetime.now(timezone.utc)).replace(microsecond=0)
    path = out_dir / f"discovery-{exported_at.strftime('%Y%m%dT%H%M%SZ')}.parquet"
    rows = 0
    schema = arrow_schema()
    with pq.ParquetWriter(str(path), schema, compression="zstd") as writer:
        for batch in _batches(_records(jsonl), exported_at, batch_rows):
            writer.write_batch(batch)
            rows += batch.num_rows
    if rows == 0:
        writer_table = pa.Table.from_pylist([], schema=schema)
        pq.write_table(writer_table, str(path), compression="zstd")
    return {"path": str(path), "rows": rows, "schema_version": EXPORT_SCHEMA_VERSION}


def export_delta(jsonl: Union[str, Path], table_uri: Union[str, Path], mode: str = "append",
                 batch_rows: int = 50_000, exported_at: Optional[datetime] = None) -> Dict[str, Any]:
    """Append (or overwrite) a Delta Lake table at ``table_uri`` with the same schema (needs ``deltalake``)."""
    pa = _require_pyarrow()
    try:
        from deltalake import write_deltalake
    except ImportError as exc:
        raise ImportError('Delta export needs deltalake: pip install "rustmapper[delta]"') from exc
    jsonl = Path(jsonl)
    if not jsonl.is_file():
        raise FileNotFoundError(f"no crawl output at {jsonl} (run a crawl, or pass --input)")
    exported_at = (exported_at or datetime.now(timezone.utc)).replace(microsecond=0)
    schema = arrow_schema()
    reader = pa.RecordBatchReader.from_batches(schema, _batches(_records(jsonl), exported_at, batch_rows))
    table = reader.read_all()
    write_deltalake(str(table_uri), table, mode=mode)
    return {"path": str(table_uri), "rows": table.num_rows, "schema_version": EXPORT_SCHEMA_VERSION}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m rustmapper.export_parquet",
                                 description="Export rustmapper sitemap.jsonl to Parquet or Delta (#33)")
    ap.add_argument("--data-dir", default="./data", help="directory containing sitemap.jsonl")
    ap.add_argument("--input", help="explicit JSONL path (overrides --data-dir)")
    dest = ap.add_mutually_exclusive_group()
    dest.add_argument("--output", help="Parquet output directory (default: <data-dir>/parquet/discovery)")
    dest.add_argument("--delta", help="Delta table path/URI to write instead of plain Parquet")
    ap.add_argument("--mode", choices=["append", "overwrite"], default="append", help="Delta write mode")
    ap.add_argument("--batch-rows", type=int, default=50_000)
    ap.add_argument("--print-schema", action="store_true", help="print the column contract and exit")
    args = ap.parse_args(argv)
    if args.print_schema:
        print(f"export schema v{EXPORT_SCHEMA_VERSION}")
        for name, typ, nullable, desc in COLUMNS:
            print(f"  {name:<22} {typ:<18} {'null' if nullable else 'not null':<9} {desc}")
        return 0
    jsonl = Path(args.input) if args.input else Path(args.data_dir) / "sitemap.jsonl"
    try:
        if args.delta:
            res = export_delta(jsonl, args.delta, mode=args.mode, batch_rows=args.batch_rows)
        else:
            out = args.output or str(Path(args.data_dir) / "parquet" / "discovery")
            res = export_parquet(jsonl, out, batch_rows=args.batch_rows)
    except (FileNotFoundError, ImportError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
