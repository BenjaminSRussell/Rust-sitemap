"""
RustMapper - Concurrent web crawler and sitemap generator

Two ways to drive the Rust crawler from Python (#41):

* **Subprocess (default):** ``Crawler`` runs the ``rust_sitemap`` binary,
  reads ``<data_dir>/sitemap.jsonl`` and exposes the results as
  ``CrawlResult`` objects. ``Crawler.crawl_stream()`` yields each result as
  soon as its JSONL line is written, without collecting a list first.
  ``iter_results(path)`` streams any existing JSONL file.
* **In-process (optional):** wheels built with maturin include the native
  module ``rustmapper._rustmapper``, exposed here as ``rustmapper.native``
  (``None`` when it isn't built). ``native.Crawler(start_url, ...).crawl()``
  runs the crawl inside the Python process, with no binary needed.

The binary writes ``sitemap.jsonl`` in its export phase, at the end of a
crawl or on graceful shutdown. That is when ``crawl_stream`` starts
yielding. Streaming saves memory and lets you start processing while the
export is still being written; the results still arrive after the crawl
itself has finished.
"""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

try:  # native PyO3 module (present in maturin-built wheels)
    from rustmapper import _rustmapper as native  # type: ignore[attr-defined]
    __version__ = native.__version__
except ImportError:
    native = None
    __version__ = "0.1.3"

__all__ = ["Crawler", "CrawlResult", "iter_results", "find_binary", "native", "main"]

BINARY_NAMES = ("rust_sitemap", "rustmapper")
INSTALL_HINT = (
    "rust_sitemap binary not found. The Python wrapper drives the Rust CLI as a subprocess.\n"
    "  * install it:   cargo install --git https://github.com/BenjaminSRussell/Rust-sitemap\n"
    "  * or point to a build:   export RUSTMAPPER_BIN=/path/to/target/release/rust_sitemap\n"
    "  * or run in-process without a binary:   rustmapper.native.Crawler(...) "
    "(needs a maturin-built wheel: maturin develop --release)"
)


def find_binary(explicit: Optional[str] = None) -> str:
    """Locate the crawler CLI: explicit path, ``$RUSTMAPPER_BIN``, then ``rust_sitemap``/``rustmapper`` on PATH.

    Raises:
        FileNotFoundError: with install guidance when nothing is found.
    """
    for candidate in (explicit, os.environ.get("RUSTMAPPER_BIN")):
        if candidate:
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
            found = shutil.which(candidate)
            if found:
                return found
            raise FileNotFoundError(f"rustmapper binary {candidate!r} is not an executable file.\n{INSTALL_HINT}")
    for name in BINARY_NAMES:
        found = shutil.which(name)
        if found:
            return found
    raise FileNotFoundError(INSTALL_HINT)


def _opt_int(value: Any) -> Optional[int]:
    return None if value is None else int(value)


class CrawlResult:
    """One crawled URL (a line of ``sitemap.jsonl``, i.e. a serialized ``SitemapNode``)."""

    def __init__(self, data: Dict[str, Any]):
        if not isinstance(data, dict):
            raise TypeError(f"CrawlResult expects a JSON object, got {type(data).__name__}")
        self.url: str = data.get("url") or ""
        self.depth: int = int(data.get("depth") or 0)
        # Uncrawled / failed nodes serialize these as null; keep the old int/str defaults.
        self.status_code: int = int(data.get("status_code") or 0)
        self.content_length: int = int(data.get("content_length") or 0)
        self.title: str = data.get("title") or ""
        self.link_count: int = int(data.get("link_count") or 0)
        self.parent_url: Optional[str] = data.get("parent_url")
        self.content_type: Optional[str] = data.get("content_type")
        self.crawled_at: Optional[int] = _opt_int(data.get("crawled_at"))
        self.response_time_ms: Optional[int] = _opt_int(data.get("response_time_ms"))
        self.schema_version: Optional[int] = _opt_int(data.get("schema_version"))
        self._raw = data

    @classmethod
    def from_json_line(cls, line: str, lineno: Optional[int] = None) -> "CrawlResult":
        try:
            return cls(json.loads(line))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            where = f" (line {lineno})" if lineno is not None else ""
            raise ValueError(f"invalid sitemap.jsonl record{where}: {exc}") from exc

    @property
    def crawled(self) -> bool:
        return self.crawled_at is not None or self.status_code != 0

    def __repr__(self) -> str:
        return f"<CrawlResult url={self.url} status={self.status_code}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CrawlResult) and other._raw == self._raw

    def to_dict(self) -> Dict[str, Any]:
        """Return the raw dictionary representation."""
        return self._raw


def iter_results(path: Union[str, Path]) -> Iterator[CrawlResult]:
    """Stream ``CrawlResult`` objects from a JSONL file, one line at a time (blank lines skipped)."""
    path = Path(path)
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if line.strip():
                yield CrawlResult.from_json_line(line, lineno)


class Crawler:
    """
    Web crawler for discovering URLs and generating sitemaps (subprocess driver).

    Example:
        >>> from rustmapper import Crawler
        >>> crawler = Crawler(start_url="https://example.com", workers=128)
        >>> for result in crawler.crawl_stream():
        ...     print(f"{result.url}: {result.status_code}")
    """

    def __init__(
        self,
        start_url: str,
        data_dir: str = "./data",
        workers: int = 256,
        user_agent: str = "RustSitemapCrawler/1.0",
        timeout: int = 20,
        ignore_robots: bool = False,
        seeding_strategy: str = "sitemap",
        enable_redis: bool = False,
        redis_url: str = "redis://localhost:6379",
        lock_ttl: int = 300,
        save_interval: int = 300,
        binary: Optional[str] = None,
    ):
        """
        Initialize the crawler.

        Args:
            start_url: The starting URL to begin crawling from
            data_dir: Directory to store crawled data (default: ./data)
            workers: Number of concurrent requests (default: 256)
            user_agent: User agent string for requests
            timeout: Request timeout in seconds (default: 20)
            ignore_robots: Skip robots.txt compliance (default: False)
            seeding_strategy: Comma-separated strategies: none, sitemap, ct, commoncrawl, all (default: sitemap)
            enable_redis: Enable distributed crawling with Redis
            redis_url: Redis connection URL
            lock_ttl: Redis lock TTL in seconds
            save_interval: Save interval in seconds
            binary: Path to the crawler CLI (default: $RUSTMAPPER_BIN, then rust_sitemap on PATH)
        """
        self.start_url = start_url
        self.data_dir = data_dir
        self.workers = workers
        self.user_agent = user_agent
        self.timeout = timeout
        self.ignore_robots = ignore_robots
        self.seeding_strategy = seeding_strategy
        self.enable_redis = enable_redis
        self.redis_url = redis_url
        self.lock_ttl = lock_ttl
        self.save_interval = save_interval
        self.binary = binary

    @property
    def results_path(self) -> Path:
        return Path(self.data_dir) / "sitemap.jsonl"

    def _build_command(self) -> List[str]:
        """Build the command line arguments for the crawler."""
        cmd = [
            find_binary(self.binary),
            "crawl",
            "--start-url", self.start_url,
            "--data-dir", self.data_dir,
            "--workers", str(self.workers),
            "--user-agent", self.user_agent,
            "--timeout", str(self.timeout),
            "--seeding-strategy", self.seeding_strategy,
            "--lock-ttl", str(self.lock_ttl),
            "--save-interval", str(self.save_interval),
        ]

        if self.ignore_robots:
            cmd.append("--ignore-robots")

        if self.enable_redis:
            cmd.append("--enable-redis")
            cmd.extend(["--redis-url", self.redis_url])

        return cmd

    def _resume_command(self) -> List[str]:
        cmd = [
            find_binary(self.binary),
            "resume",
            "--data-dir", self.data_dir,
            "--workers", str(self.workers),
            "--user-agent", self.user_agent,
            "--timeout", str(self.timeout),
        ]
        if self.ignore_robots:
            cmd.append("--ignore-robots")
        if self.enable_redis:
            cmd.append("--enable-redis")
            cmd.extend(["--redis-url", self.redis_url])
            cmd.extend(["--lock-ttl", str(self.lock_ttl)])
        return cmd

    @staticmethod
    def _run(cmd: List[str], what: str) -> None:
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except FileNotFoundError:
            raise FileNotFoundError(INSTALL_HINT)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"{what} failed (exit {e.returncode}): {e.stderr}")

    def crawl(self) -> List[CrawlResult]:
        """
        Run the crawl to completion and return all results.

        Raises:
            RuntimeError: If the crawler exits non-zero
            FileNotFoundError: If the crawler binary is not found (message says how to install it)
        """
        self._run(self._build_command(), "Crawler")
        return self.read_results()

    def crawl_stream(self, poll_interval: float = 0.1, resume: bool = False) -> Iterator[CrawlResult]:
        """
        Run the crawler and yield each result as soon as its JSONL line is written.

        Lines are parsed as the binary writes ``sitemap.jsonl``. Results are
        never collected into a list, so memory use stays flat on large
        crawls. If you close the generator early (``break``), the crawler
        process is terminated.

        Args:
            poll_interval: Seconds between checks for new output
            resume: Run ``resume`` instead of ``crawl``

        Raises:
            RuntimeError: If the crawler exits non-zero (after yielding what it wrote)
            FileNotFoundError: If the crawler binary is not found
        """
        cmd = self._resume_command() if resume else self._build_command()
        path = self.results_path
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        except FileNotFoundError:
            raise FileNotFoundError(INSTALL_HINT)

        stderr_chunks: List[str] = []
        buf = ""
        pos = 0
        inode = None
        lineno = 0
        try:
            while True:
                exited = proc.poll() is not None
                if path.exists():
                    st = path.stat()
                    if inode is not None and (st.st_ino != inode or st.st_size < pos):
                        pos, buf = 0, ""  # export truncates/recreates the file: start over
                    inode = st.st_ino
                    with open(path, "r", encoding="utf-8") as f:
                        f.seek(pos)
                        chunk = f.read()
                        pos = f.tell()
                    buf += chunk
                    *lines, buf = buf.split("\n")
                    for line in lines:
                        lineno += 1
                        if line.strip():
                            yield CrawlResult.from_json_line(line, lineno)
                if exited:
                    if buf.strip():  # last line without a trailing newline
                        lineno += 1
                        yield CrawlResult.from_json_line(buf, lineno)
                    break
                time.sleep(poll_interval)
            if proc.stderr is not None:
                stderr_chunks.append(proc.stderr.read())
            if proc.returncode != 0:
                raise RuntimeError(f"Crawler failed (exit {proc.returncode}): {''.join(stderr_chunks)[-2000:]}")
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            if proc.stderr is not None:
                proc.stderr.close()

    def read_results(self) -> List[CrawlResult]:
        """Read all crawl results from the data directory."""
        return list(iter_results(self.results_path))

    def read_results_stream(self) -> Iterator[CrawlResult]:
        """Stream crawl results from the data directory, one at a time."""
        return iter_results(self.results_path)

    def resume(self) -> List[CrawlResult]:
        """
        Resume an interrupted crawl.

        Raises:
            RuntimeError: If the crawler exits non-zero
            FileNotFoundError: If the crawler binary is not found
        """
        self._run(self._resume_command(), "Crawler")
        return self.read_results()

    def export_sitemap(
        self,
        output: str = "./sitemap.xml",
        include_lastmod: bool = False,
        include_changefreq: bool = False,
        default_priority: float = 0.5,
    ) -> None:
        """
        Export crawl results as XML sitemap.

        Raises:
            RuntimeError: If export fails
            FileNotFoundError: If the crawler binary is not found
        """
        cmd = [
            find_binary(self.binary),
            "export-sitemap",
            "--data-dir", self.data_dir,
            "--output", output,
            "--default-priority", str(default_priority),
        ]

        if include_lastmod:
            cmd.append("--include-lastmod")

        if include_changefreq:
            cmd.append("--include-changefreq")

        self._run(cmd, "Export")


def main() -> None:
    """Command-line entry point: forward arguments to the crawler binary."""
    import sys

    try:
        binary = find_binary()
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(subprocess.run([binary] + sys.argv[1:]).returncode)


if __name__ == "__main__":
    main()
