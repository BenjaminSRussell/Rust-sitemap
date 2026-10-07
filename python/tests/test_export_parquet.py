"""Parquet / Delta export of crawl JSONL (#33)."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow", reason='needs pyarrow (pip install "rustmapper[parquet]")')
import pyarrow.parquet as pq  # noqa: E402

PYDIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYDIR))
from rustmapper.export_parquet import (COLUMNS, EXPORT_SCHEMA_VERSION, arrow_schema, export_parquet,  # noqa: E402
                                       main, record_to_row)

FIXTURE = PYDIR / "tests" / "fixtures" / "sitemap.jsonl"
NOW = datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)


def test_schema_contract_is_versioned_and_documented():
    schema = arrow_schema()
    assert schema.names == [c[0] for c in COLUMNS]
    assert schema.metadata[b"rustmapper.export_schema_version"] == str(EXPORT_SCHEMA_VERSION).encode()
    assert not schema.field("url").nullable and schema.field("status_code").nullable
    assert str(schema.field("fetched_at").type) == "timestamp[us, tz=UTC]"
    readme = (PYDIR.parent / "README.md").read_text()
    for name, *_ in COLUMNS:  # every column is in the README contract table
        assert f"`{name}`" in readme, name


def test_record_mapping_handles_uncrawled_nodes():
    row = record_to_row({"url": "https://Shop.Example.com/x?y=1", "depth": 2, "status_code": None,
                         "crawled_at": None, "discovered_at": 1700000000,
                         "privacy_signals": {"cookie_count": 2}}, NOW)
    assert row["host"] == "shop.example.com" and row["fetched_at"] is None
    assert row["discovered_at"] == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
    assert json.loads(row["privacy_signals_json"]) == {"cookie_count": 2}
    assert row["schema_version"] == EXPORT_SCHEMA_VERSION


def test_export_parquet_readable_by_pyarrow(tmp_path):
    res = export_parquet(FIXTURE, tmp_path / "discovery", batch_rows=2, exported_at=NOW)
    assert res["rows"] == 4 and res["path"].endswith("discovery-20261007T120000Z.parquet")
    table = pq.read_table(res["path"])
    sig = lambda sc: [(f.name, str(f.type), f.nullable) for f in sc]
    assert sig(table.schema) == sig(arrow_schema())
    assert table.schema.metadata[b"rustmapper.export_schema_version"] == b"1"
    rows = {r["url"]: r for r in table.to_pylist()}
    home = rows["https://example.com/"]
    assert (home["status_code"], home["title"], home["depth"], home["host"]) == (200, "Example", 0, "example.com")
    assert rows["https://example.com/never-fetched"]["fetched_at"] is None
    assert set(table.column("schema_version").to_pylist()) == {EXPORT_SCHEMA_VERSION}
    # the whole directory loads as one dataset without custom parsing
    import pyarrow.dataset as ds
    assert ds.dataset(str(tmp_path / "discovery"), format="parquet").count_rows() == 4


def test_cli_and_errors(tmp_path, capsys):
    data = tmp_path / "data"
    data.mkdir()
    (data / "sitemap.jsonl").write_text(FIXTURE.read_text())
    assert main(["--data-dir", str(data)]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["rows"] == 4 and Path(out["path"]).parent == data / "parquet" / "discovery"
    assert main(["--data-dir", str(tmp_path / "missing")]) == 2
    assert "no crawl output" in capsys.readouterr().err
    (data / "bad.jsonl").write_text('{"url": "https://a"}\n{oops\n')
    assert main(["--input", str(data / "bad.jsonl"), "--output", str(tmp_path / "o")]) == 2
    assert "bad.jsonl:2" in capsys.readouterr().err
    assert main(["--print-schema"]) == 0 and "fetched_at" in capsys.readouterr().out
    proc = subprocess.run([sys.executable, "-m", "rustmapper.export_parquet", "--print-schema"],
                          cwd=PYDIR, capture_output=True, text=True)
    assert proc.returncode == 0 and f"v{EXPORT_SCHEMA_VERSION}" in proc.stdout


def test_delta_append_readable_by_delta_rs(tmp_path):
    deltalake = pytest.importorskip("deltalake", reason='needs deltalake (pip install "rustmapper[delta]")')
    uri = tmp_path / "delta" / "stage1_discovery"
    assert main(["--input", str(FIXTURE), "--delta", str(uri)]) == 0
    assert main(["--input", str(FIXTURE), "--delta", str(uri)]) == 0  # append
    dt = deltalake.DeltaTable(str(uri))
    assert dt.version() == 1
    table = dt.to_pyarrow_table()
    assert table.num_rows == 8 and "fetched_at" in table.schema.names
    assert main(["--input", str(FIXTURE), "--delta", str(uri), "--mode", "overwrite"]) == 0
    assert deltalake.DeltaTable(str(uri)).to_pyarrow_table().num_rows == 4
