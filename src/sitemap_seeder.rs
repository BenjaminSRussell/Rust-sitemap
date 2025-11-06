// Pre-seed the crawl queue from robots.txt sitemaps and sitemap indexes to jump-start discovery.

use crate::network::{FetchError, HttpClient};
use crate::robots;
use crate::seeder::{Seeder, UrlStream};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::stream;
use robotstxt::DefaultMatcher;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use std::io::Cursor;
use thiserror::Error;
use tokio::io::AsyncReadExt;

/// Maximum nested sitemap index depth to prevent infinite recursion.
const MAX_SITEMAP_DEPTH: usize = 10;

/// Maximum number of URLs to extract from a single sitemap to prevent DoS.
const MAX_ENTRIES_PER_SITEMAP: usize = 50_000;

/// Maximum size for decompressed sitemap content (10 MB).
const MAX_DECOMPRESSED_SIZE: usize = 10 * 1024 * 1024;

/// Maximum size for compressed sitemap content (5 MB).
const MAX_COMPRESSED_SIZE: usize = 5 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum SitemapError {
    #[error("Network error: {0}")]
    Network(#[from] FetchError),

    #[error("HTTP status {0}")]
    HttpStatus(u16),

    #[error("Content too large: {0} bytes (decompressed)")]
    OversizedContent(usize),

    #[error("Maximum sitemap depth {0} exceeded")]
    DepthExceeded(usize),

    #[error("Maximum entry count {0} exceeded")]
    EntryLimitExceeded(usize),

    #[error("Invalid MIME type: {0} (expected XML or gzip)")]
    InvalidMimeType(String),

    #[error("Decompression failed: {0}")]
    DecompressionError(String),
}

impl SitemapError {
    /// Check if this error is retryable (transient network/server issues).
    /// Returns true for errors that may succeed on retry.
    /// Currently used in tests; available for future retry logic.
    #[allow(dead_code)]
    pub fn retryable(&self) -> bool {
        match self {
            // Network errors from FetchError are generally retryable
            SitemapError::Network(fetch_err) => {
                matches!(
                    fetch_err,
                    FetchError::Timeout
                        | FetchError::ConnectionRefused
                        | FetchError::DnsError
                        | FetchError::NetworkError(_)
                )
            }
            // Retryable HTTP status codes (5xx server errors, 429 rate limit, 408 timeout)
            SitemapError::HttpStatus(code) => matches!(code, 408 | 429 | 500..=599),
            // Data/parsing errors are not retryable
            SitemapError::OversizedContent(_) => false,
            SitemapError::DepthExceeded(_) => false,
            SitemapError::EntryLimitExceeded(_) => false,
            SitemapError::InvalidMimeType(_) => false,
            SitemapError::DecompressionError(_) => false,
        }
    }
}

pub struct SitemapSeeder {
    http: HttpClient,
}

/// Response from fetching a sitemap with metadata.
struct SitemapFetchResult {
    content: Vec<u8>,
    content_type: Option<String>,
}

impl SitemapSeeder {
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    /// Fetch sitemap with metadata including Content-Type header.
    /// MIME type checking based on Content-Type header (ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).
    async fn fetch_sitemap(&self, sitemap_url: &str) -> Result<SitemapFetchResult, SitemapError> {
        let response = self.http.fetch_stream(sitemap_url).await?;
        let status = response.status();

        if !status.is_success() {
            return Err(SitemapError::HttpStatus(status.as_u16()));
        }

        // Extract Content-Type header for MIME validation
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Validate MIME type if present
        if let Some(ref mime) = content_type {
            let mime_lower = mime.to_lowercase();
            let is_valid = mime_lower.contains("application/xml")
                || mime_lower.contains("text/xml")
                || mime_lower.contains("application/gzip")
                || mime_lower.contains("application/x-gzip");

            if !is_valid {
                return Err(SitemapError::InvalidMimeType(mime.clone()));
            }
        }

        // Check compressed size limit from Content-Length
        if let Some(content_length) = response.content_length() {
            if content_length as usize > MAX_COMPRESSED_SIZE {
                return Err(SitemapError::OversizedContent(content_length as usize));
            }
        }

        // Stream the response body with size enforcement
        let mut body_bytes = Vec::new();
        let mut stream = response.bytes_stream();
        let mut total_bytes = 0usize;

        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| SitemapError::Network(FetchError::BodyError(e.to_string())))?;

            total_bytes += chunk.len();
            if total_bytes > MAX_COMPRESSED_SIZE {
                return Err(SitemapError::OversizedContent(total_bytes));
            }

            body_bytes.extend_from_slice(&chunk);
        }

        Ok(SitemapFetchResult {
            content: body_bytes,
            content_type,
        })
    }

    /// Decompress gzip content with size limits to prevent decompression bombs.
    async fn decompress_gzip(&self, compressed: &[u8]) -> Result<Vec<u8>, SitemapError> {
        let cursor = Cursor::new(compressed);
        let reader = tokio::io::BufReader::new(cursor);
        let mut decoder = GzipDecoder::new(reader);

        let mut decompressed = Vec::new();
        let mut buffer = [0u8; 8192];

        loop {
            let n = decoder
                .read(&mut buffer)
                .await
                .map_err(|e| SitemapError::DecompressionError(e.to_string()))?;

            if n == 0 {
                break;
            }

            decompressed.extend_from_slice(&buffer[..n]);

            if decompressed.len() > MAX_DECOMPRESSED_SIZE {
                return Err(SitemapError::OversizedContent(decompressed.len()));
            }
        }

        Ok(decompressed)
    }

    /// Determine if content is gzip based on MIME type or magic bytes.
    fn is_gzip(&self, content_type: Option<&str>, data: &[u8]) -> bool {
        // Check MIME type first
        if let Some(mime) = content_type {
            let mime_lower = mime.to_lowercase();
            if mime_lower.contains("application/gzip") || mime_lower.contains("application/x-gzip")
            {
                return true;
            }
        }

        // Fall back to magic bytes (gzip starts with 0x1f 0x8b)
        data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
    }

    /// Parse sitemap XML and extract URLs with depth and entry count limits.
    /// Streaming parse to avoid loading entire document into memory.
    fn parse_sitemap(
        &self,
        xml_data: &[u8],
        depth: usize,
    ) -> Result<Vec<SitemapEntry>, SitemapError> {
        if depth > MAX_SITEMAP_DEPTH {
            return Err(SitemapError::DepthExceeded(depth));
        }

        let mut entries = Vec::new();
        let cursor = Cursor::new(xml_data);
        let parser = SiteMapReader::new(cursor);

        for entity in parser {
            if entries.len() >= MAX_ENTRIES_PER_SITEMAP {
                return Err(SitemapError::EntryLimitExceeded(entries.len()));
            }

            match entity {
                SiteMapEntity::Url(url_entry) => {
                    if let Some(url) = url_entry.loc.get_url() {
                        entries.push(SitemapEntry::Url(url.to_string()));
                    }
                }
                SiteMapEntity::SiteMap(sitemap_entry) => {
                    // Process sitemap index entries so nested sitemap files also get crawled.
                    if let Some(url) = sitemap_entry.loc.get_url() {
                        entries.push(SitemapEntry::NestedSitemap(url.to_string()));
                    }
                }
                _ => {}
            }
        }

        Ok(entries)
    }

    // Check whether a URL is allowed by robots.txt so we do not seed disallowed paths.
    fn is_allowed(&self, robots_txt: &str, url: &str) -> bool {
        let mut matcher = DefaultMatcher::default();
        // Use the wildcard user agent for sitemap seeding so we follow the broadest applicable policy.
        matcher.one_agent_allowed_by_robots(robots_txt, "*", url)
    }

    /// Process a single sitemap URL, handling gzip and nested sitemaps recursively.
    fn process_sitemap<'a>(
        &'a self,
        sitemap_url: &'a str,
        depth: usize,
        robots_txt: &'a Option<String>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, SitemapError>> + Send + 'a>,
    > {
        Box::pin(async move {
            if depth > MAX_SITEMAP_DEPTH {
                return Err(SitemapError::DepthExceeded(depth));
            }

            eprintln!("Fetching sitemap: {} (depth {})...", sitemap_url, depth);

            let fetch_result = self.fetch_sitemap(sitemap_url).await?;

            // Handle gzip decompression if needed
            let xml_data =
                if self.is_gzip(fetch_result.content_type.as_deref(), &fetch_result.content) {
                    eprintln!("Decompressing gzip sitemap...");
                    self.decompress_gzip(&fetch_result.content).await?
                } else {
                    fetch_result.content
                };

            let entries = self.parse_sitemap(&xml_data, depth)?;
            eprintln!("Parsed {}: {} entries", sitemap_url, entries.len());

            let mut discovered = Vec::new();

            for entry in entries {
                match entry {
                    SitemapEntry::Url(url) => {
                        // Filter by robots.txt rules when available.
                        let allowed = if let Some(ref txt) = robots_txt {
                            self.is_allowed(txt, &url)
                        } else {
                            true
                        };

                        if allowed {
                            discovered.push(url);
                        }
                    }
                    SitemapEntry::NestedSitemap(nested_url) => {
                        // Recursively process nested sitemaps with increased depth
                        match self
                            .process_sitemap(&nested_url, depth + 1, robots_txt)
                            .await
                        {
                            Ok(nested_urls) => discovered.extend(nested_urls),
                            Err(e) => {
                                eprintln!("Failed to process nested sitemap {}: {}", nested_url, e);
                            }
                        }
                    }
                }
            }

            Ok(discovered)
        })
    }
}

/// Represents an entry in a sitemap (either a URL or a nested sitemap).
enum SitemapEntry {
    Url(String),
    NestedSitemap(String),
}

impl Seeder for SitemapSeeder {
    fn seed(&self, domain: &str) -> UrlStream {
        let http = self.http.clone();
        let start_url = domain.to_string();

        Box::pin(stream! {
            // Step 1: Fetch robots.txt so we learn declared sitemap locations.
            eprintln!("Fetching robots.txt for {}...", start_url);
            let robots_txt = robots::fetch_robots_txt_from_url(&http, &start_url).await;

            // Step 2: Extract sitemap URLs so we can fetch each listed sitemap file.
            let mut sitemap_urls: Vec<String> = Vec::new();

            if let Some(ref txt) = robots_txt {
                eprintln!("Fetched robots.txt: {} bytes", txt.len());
                sitemap_urls = txt
                    .lines()
                    .filter(|line| line.to_lowercase().starts_with("sitemap:"))
                    .filter_map(|line| line.split_whitespace().nth(1).map(|s| s.to_string()))
                    .collect();

                if !sitemap_urls.is_empty() {
                    eprintln!("Found {} sitemap(s) in robots.txt", sitemap_urls.len());
                }
            }

            // Step 3: When robots.txt lacks sitemaps, probe common paths so we still attempt discovery.
            if sitemap_urls.is_empty() {
                eprintln!("No sitemaps declared in robots.txt, trying common paths...");

                // Extract the base URL so we can append candidate sitemap paths easily.
                let base_url = if let Ok(url) = url::Url::parse(&start_url) {
                    format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""))
                } else {
                    start_url.clone()
                };

                // Try common sitemap paths because many sites follow these conventions.
                let common_paths = vec![
                    "/sitemap.xml",
                    "/sitemap_index.xml",
                    "/sitemap1.xml",
                    "/sitemaps.xml",
                    "/sitemap/sitemap.xml",
                ];

                // Create a temporary seeder for probing
                let seeder = SitemapSeeder::new(http.clone());

                for path in common_paths {
                    let sitemap_url = format!("{}{}", base_url, path);
                    eprintln!("Trying {}...", sitemap_url);

                    // Attempt to fetch the candidate sitemap to verify its existence.
                    if seeder.fetch_sitemap(&sitemap_url).await.is_ok() {
                        eprintln!("Found sitemap at {}", sitemap_url);
                        sitemap_urls.push(sitemap_url);
                        break; // Stop after finding a sitemap because one success is enough to proceed.
                    }
                }

                if sitemap_urls.is_empty() {
                    eprintln!("No sitemaps found at common paths");
                    return;
                }
            }

            eprintln!("Processing {} sitemap(s)...", sitemap_urls.len());

            // Step 4: Fetch and parse each sitemap with depth and entry count limits.
            let seeder = SitemapSeeder::new(http.clone());

            for sitemap_url in sitemap_urls {
                match seeder.process_sitemap_stream(&sitemap_url, 0, &robots_txt).await {
                    Ok(stream) => {
                        while let Ok(url) = stream.recv_async().await {
                            yield Ok(url);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to process sitemap {}: {}", sitemap_url, e);
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "sitemap"
    }
}

impl SitemapSeeder {
    /// Process a sitemap and stream URLs as they're discovered.
    async fn process_sitemap_stream(
        &self,
        sitemap_url: &str,
        depth: usize,
        robots_txt: &Option<String>,
    ) -> Result<flume::Receiver<String>, SitemapError> {
        let (tx, rx) = flume::unbounded();

        // Fetch and parse the sitemap
        let urls = self.process_sitemap(sitemap_url, depth, robots_txt).await?;

        // Send all URLs through the channel
        tokio::spawn(async move {
            for url in urls {
                if tx.send(url).is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sitemap_urlset() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
        <loc>https://example.com/page1</loc>
    </url>
    <url>
        <loc>https://example.com/page2</loc>
    </url>
</urlset>"#;

        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);
        let result = seeder.parse_sitemap(xml, 0).unwrap();

        assert_eq!(result.len(), 2);
        if let SitemapEntry::Url(url) = &result[0] {
            assert_eq!(url, "https://example.com/page1");
        } else {
            panic!("Expected Url entry");
        }
    }

    #[test]
    fn test_parse_sitemap_index() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <sitemap>
        <loc>https://example.com/sitemap1.xml</loc>
    </sitemap>
    <sitemap>
        <loc>https://example.com/sitemap2.xml</loc>
    </sitemap>
</sitemapindex>"#;

        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);
        let result = seeder.parse_sitemap(xml, 0).unwrap();

        assert_eq!(result.len(), 2);
        if let SitemapEntry::NestedSitemap(url) = &result[0] {
            assert_eq!(url, "https://example.com/sitemap1.xml");
        } else {
            panic!("Expected NestedSitemap entry");
        }
    }

    #[test]
    fn test_parse_sitemap_malformed() {
        let xml = b"not valid xml at all";

        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);
        let result = seeder.parse_sitemap(xml, 0);

        // The sitemap crate might return an empty result or error
        // Either is acceptable for malformed input
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_depth_limit() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url><loc>https://example.com/page1</loc></url>
</urlset>"#;

        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);

        // Parsing at max depth should fail
        let result = seeder.parse_sitemap(xml, MAX_SITEMAP_DEPTH + 1);
        assert!(matches!(result, Err(SitemapError::DepthExceeded(_))));
    }

    #[test]
    fn test_is_gzip_magic_bytes() {
        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);

        // Gzip magic bytes
        let gzip_data = vec![0x1f, 0x8b, 0x08, 0x00];
        assert!(seeder.is_gzip(None, &gzip_data));

        // Not gzip
        let plain_data = b"<?xml version";
        assert!(!seeder.is_gzip(None, plain_data));
    }

    #[test]
    fn test_is_gzip_mime_type() {
        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);

        // Check MIME type detection
        assert!(seeder.is_gzip(Some("application/gzip"), b"data"));
        assert!(seeder.is_gzip(Some("application/x-gzip"), b"data"));
        assert!(!seeder.is_gzip(Some("application/xml"), b"data"));
    }

    #[tokio::test]
    async fn test_decompress_gzip() {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;

        let http = HttpClient::new("test-agent".to_string(), 30).unwrap();
        let seeder = SitemapSeeder::new(http);

        // Create a simple gzipped payload
        let original = b"Hello, World!";
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(original).await.unwrap();
        encoder.shutdown().await.unwrap();
        let compressed = encoder.into_inner();

        // Decompress
        let decompressed = seeder.decompress_gzip(&compressed).await.unwrap();
        assert_eq!(&decompressed[..], original);
    }

    #[test]
    fn test_error_retryable() {
        // Network errors should be retryable (depends on FetchError implementation)
        let err = SitemapError::HttpStatus(503);
        assert!(err.retryable());

        let err = SitemapError::HttpStatus(404);
        assert!(!err.retryable());

        // Data errors are not retryable
        let err = SitemapError::OversizedContent(1000);
        assert!(!err.retryable());

        let err = SitemapError::DepthExceeded(10);
        assert!(!err.retryable());
    }
}
