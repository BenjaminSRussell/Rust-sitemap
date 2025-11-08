"""
RustMapper - Concurrent web crawler and sitemap generator

A Python wrapper around the Rust-based web crawler for easy integration
into Python projects.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterator

# Get version from the Rust extension module
try:
    from rustmapper._rustmapper import __version__
except ImportError:
    # Fallback if the extension module isn't built yet
    __version__ = "0.0.0.dev"

__all__ = ["Crawler", "CrawlResult", "main"]


class CrawlResult:
    """Represents a crawled URL with its metadata."""

    def __init__(self, data: Dict[str, Any]):
        self.url: str = data.get("url", "")
        self.depth: int = data.get("depth", 0)
        self.status_code: int = data.get("status_code", 0)
        self.content_length: int = data.get("content_length", 0)
        self.title: str = data.get("title", "")
        self.link_count: int = data.get("link_count", 0)
        self._raw = data

    def __repr__(self) -> str:
        return f"<CrawlResult url={self.url} status={self.status_code}>"

    def to_dict(self) -> Dict[str, Any]:
        """Return the raw dictionary representation."""
        return self._raw


class Crawler:
    """
    Web crawler for discovering URLs and generating sitemaps.

    Example:
        >>> from rustmapper import Crawler
        >>> crawler = Crawler(start_url="https://example.com", workers=128)
        >>> results = crawler.crawl()
        >>> for result in results:
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

    def _build_command(self) -> List[str]:
        """Build the command line arguments for the crawler."""
        cmd = [
            "rustmapper",
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

    def crawl(self) -> List[CrawlResult]:
        """
        Start the crawl and return results.

        Returns:
            List of CrawlResult objects

        Raises:
            subprocess.CalledProcessError: If the crawler fails
            FileNotFoundError: If rustmapper binary is not found
        """
        cmd = self._build_command()

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except FileNotFoundError:
            raise FileNotFoundError(
                "rustmapper binary not found. Install with: pip install rustmapper"
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Crawler failed: {e.stderr}")

        return self.read_results()

    def read_results(self) -> List[CrawlResult]:
        """
        Read crawl results from the data directory.

        Returns:
            List of CrawlResult objects
        """
        results_file = Path(self.data_dir) / "sitemap.jsonl"

        if not results_file.exists():
            return []

        results = []
        with open(results_file, "r") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    results.append(CrawlResult(data))

        return results

    def read_results_stream(self) -> Iterator[CrawlResult]:
        """
        Stream crawl results from the data directory.

        Yields:
            CrawlResult objects one at a time
        """
        results_file = Path(self.data_dir) / "sitemap.jsonl"

        if not results_file.exists():
            return

        with open(results_file, "r") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    yield CrawlResult(data)

    def resume(self) -> List[CrawlResult]:
        """
        Resume an interrupted crawl.

        Returns:
            List of CrawlResult objects

        Raises:
            subprocess.CalledProcessError: If the crawler fails
        """
        cmd = [
            "rustmapper",
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

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Crawler failed: {e.stderr}")

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

        Args:
            output: Output sitemap XML file path
            include_lastmod: Include last modification times
            include_changefreq: Include change frequencies
            default_priority: Default priority for pages (0.0-1.0)

        Raises:
            subprocess.CalledProcessError: If export fails
        """
        cmd = [
            "rustmapper",
            "export-sitemap",
            "--data-dir", self.data_dir,
            "--output", output,
            "--default-priority", str(default_priority),
        ]

        if include_lastmod:
            cmd.append("--include-lastmod")

        if include_changefreq:
            cmd.append("--include-changefreq")

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Export failed: {e.stderr}")


def main():
    """Command-line interface entry point."""
    import sys
    import os

    # Find and execute the rustmapper binary
    try:
        subprocess.run(["rustmapper"] + sys.argv[1:])
    except FileNotFoundError:
        print("Error: rustmapper binary not found", file=sys.stderr)
        print("Install with: pip install rustmapper", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
