"""Python wrapper: CrawlResult parsing, binary discovery, streaming (#41).

No network and no Rust build: a fake crawler script (FAKE) stands in for
the binary through ``RUSTMAPPER_BIN``. It writes ``sitemap.jsonl``
incrementally, the way the real export phase does.
"""
import json
import os
import stat
import sys
import textwrap
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import rustmapper  # noqa: E402
from rustmapper import CrawlResult, Crawler, find_binary, iter_results  # noqa: E402

NODE = {
    "schema_version": 4, "url": "https://example.com/a", "url_normalized": "https://example.com/a",
    "depth": 1, "parent_url": "https://example.com/", "fragments": [], "discovered_at": 1, "queued_at": 2,
    "crawled_at": 1700000000, "response_time_ms": 42, "status_code": 200, "content_type": "text/html",
    "content_length": 1234, "title": "A", "link_count": 7, "keywords": [],
}


def test_crawl_result_parses_full_node():
    r = CrawlResult(NODE)
    assert (r.url, r.depth, r.status_code, r.content_length, r.title, r.link_count) == \
        ("https://example.com/a", 1, 200, 1234, "A", 7)
    assert (r.parent_url, r.content_type, r.crawled_at, r.response_time_ms, r.schema_version) == \
        ("https://example.com/", "text/html", 1700000000, 42, 4)
    assert r.crawled and r.to_dict() is NODE and "status=200" in repr(r)


def test_crawl_result_handles_uncrawled_nulls():
    r = CrawlResult({**NODE, "status_code": None, "content_length": None, "title": None,
                     "link_count": None, "crawled_at": None, "response_time_ms": None})
    assert (r.status_code, r.content_length, r.title, r.link_count) == (0, 0, "", 0)
    assert r.crawled_at is None and not r.crawled


def test_from_json_line_reports_line_number():
    with pytest.raises(ValueError, match=r"line 3"):
        CrawlResult.from_json_line("{not json", 3)
    with pytest.raises(ValueError, match="JSON object"):
        CrawlResult.from_json_line("[1, 2]", 1)


def test_iter_results_streams_and_skips_blanks(tmp_path):
    p = tmp_path / "sitemap.jsonl"
    p.write_text(json.dumps(NODE) + "\n\n" + json.dumps({**NODE, "url": "https://example.com/b"}) + "\n")
    it = iter_results(p)
    assert next(it).url.endswith("/a") and next(it).url.endswith("/b")
    assert list(iter_results(tmp_path / "missing.jsonl")) == []


def test_find_binary_guidance(monkeypatch, tmp_path):
    monkeypatch.delenv("RUSTMAPPER_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(FileNotFoundError) as exc:
        find_binary()
    msg = str(exc.value)
    assert "cargo install" in msg and "RUSTMAPPER_BIN" in msg and "rustmapper.native" in msg
    monkeypatch.setenv("RUSTMAPPER_BIN", str(tmp_path / "nope"))
    with pytest.raises(FileNotFoundError, match="not an executable"):
        find_binary()
    exe = tmp_path / "rust_sitemap"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    monkeypatch.delenv("RUSTMAPPER_BIN")
    assert find_binary() == str(exe)  # found on PATH under the real binary name
    with pytest.raises(FileNotFoundError, match="cargo install"):
        Crawler("https://example.com", binary=str(tmp_path / "missing")).crawl()


FAKE = textwrap.dedent('''\
    #!{python}
    """Fake rust_sitemap: writes sitemap.jsonl slowly, with a split line, then exits."""
    import json, os, sys, time
    args = sys.argv[1:]
    data_dir = args[args.index("--data-dir") + 1]
    mode = os.environ.get("FAKE_MODE", "ok")
    path = os.path.join(data_dir, "sitemap.jsonl")
    with open(path, "w") as f:
        for i in range(3):
            line = json.dumps({{"url": f"https://example.com/{{i}}", "depth": i, "status_code": 200}})
            if i == 1:  # split a record across two writes
                f.write(line[:10]); f.flush(); time.sleep(0.3); f.write(line[10:] + "\\n")
            else:
                f.write(line + "\\n")
            f.flush()
            time.sleep(0.3 if mode != "hang" else 30)
        if mode == "no-newline":
            f.write(json.dumps({{"url": "https://example.com/last"}}))
    if mode == "fail":
        sys.stderr.write("boom: redis unreachable\\n")
        sys.exit(3)
''')


@pytest.fixture
def fake_bin(tmp_path, monkeypatch):
    exe = tmp_path / "fake_rust_sitemap"
    exe.write_text(FAKE.format(python=sys.executable))
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("RUSTMAPPER_BIN", str(exe))
    return exe


def test_crawl_stream_yields_before_the_process_exits(tmp_path, fake_bin):
    crawler = Crawler("https://example.com", data_dir=str(tmp_path / "data"))
    stream = crawler.crawl_stream(poll_interval=0.05)
    t0 = time.monotonic()
    first = next(stream)
    assert first.url == "https://example.com/0" and time.monotonic() - t0 < 0.8  # fake runs ~0.9s+
    rest = list(stream)
    assert [r.url for r in rest] == ["https://example.com/1", "https://example.com/2"]  # split line reassembled
    assert crawler.read_results() == [first] + rest


def test_crawl_stream_trailing_line_and_failure(tmp_path, fake_bin, monkeypatch):
    monkeypatch.setenv("FAKE_MODE", "no-newline")
    urls = [r.url for r in Crawler("https://e.com", data_dir=str(tmp_path / "d1")).crawl_stream(0.05)]
    assert urls[-1] == "https://example.com/last" and len(urls) == 4
    monkeypatch.setenv("FAKE_MODE", "fail")
    got = []
    with pytest.raises(RuntimeError, match=r"exit 3.*redis unreachable"):
        for r in Crawler("https://e.com", data_dir=str(tmp_path / "d2")).crawl_stream(0.05):
            got.append(r)
    assert len(got) == 3  # what was written is still delivered before the error


def test_closing_the_stream_terminates_the_crawler(tmp_path, fake_bin, monkeypatch):
    monkeypatch.setenv("FAKE_MODE", "hang")
    stream = Crawler("https://e.com", data_dir=str(tmp_path / "d")).crawl_stream(0.05)
    assert next(stream).url == "https://example.com/0"
    t0 = time.monotonic()
    stream.close()
    assert time.monotonic() - t0 < 5


def test_build_command_uses_resolved_binary(fake_bin):
    cmd = Crawler("https://example.com", workers=8, ignore_robots=True, enable_redis=True)._build_command()
    assert cmd[0] == str(fake_bin) and cmd[1] == "crawl"
    assert cmd[cmd.index("--workers") + 1] == "8" and "--ignore-robots" in cmd and "--enable-redis" in cmd


def test_native_module_is_optional():
    # Pure-Python checkout: no compiled module, wrapper still importable.
    assert rustmapper.native is None or hasattr(rustmapper.native, "Crawler")
    assert isinstance(rustmapper.__version__, str)
