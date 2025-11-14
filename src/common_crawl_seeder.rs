use crate::network::{FetchError, HttpClient};
use crate::seeder::{Seeder, UrlStream};
use async_stream::stream;
use serde::Deserialize;
use std::fmt;
use tokio::io::BufReader;

// Keep the result set bounded so we do not exhaust memory after large CDX queries.
const MAX_COMMON_CRAWL_RESULTS: usize = 100_000;

// Cap line size to prevent OOM on malformed or malicious input (e.g., missing newlines).
const MAX_LINE_SIZE: usize = 16 * 1024; // 16 KB per line

// Cap collinfo.json response to prevent excessive buffering.
const MAX_COLLINFO_SIZE: usize = 1024 * 1024; // 1 MB

/// Errors specific to Common Crawl seeding operations.
#[derive(Debug)]
pub enum SeederError {
    /// HTTP-level errors (4xx, 5xx).
    Http(u16, String),
    /// Network errors (timeouts, connection failures).
    Network(String),
    /// Data parsing or validation errors.
    Data(String),
    /// I/O errors during streaming.
    Io(std::io::Error),
}

impl fmt::Display for SeederError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeederError::Http(code, msg) => write!(f, "HTTP {}: {}", code, msg),
            SeederError::Network(msg) => write!(f, "Network error: {}", msg),
            SeederError::Data(msg) => write!(f, "Data error: {}", msg),
            SeederError::Io(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl std::error::Error for SeederError {}

impl SeederError {
    /// Check if this error is retryable (transient network/server issues).
    /// Returns true for errors that may succeed on retry.
    /// Currently used in tests; available for future retry logic.
    #[allow(dead_code)]
    pub fn retryable(&self) -> bool {
        match self {
            // Retryable HTTP errors (server errors, rate limits, timeouts)
            SeederError::Http(code, _) => matches!(code, 408 | 429 | 500..=599),
            // Network errors are generally retryable
            SeederError::Network(_) => true,
            // I/O timeouts are retryable
            SeederError::Io(e) => e.kind() == std::io::ErrorKind::TimedOut,
            // Data/parsing errors are not retryable
            SeederError::Data(_) => false,
        }
    }
}

impl From<FetchError> for SeederError {
    fn from(e: FetchError) -> Self {
        match e {
            FetchError::Timeout | FetchError::ConnectionRefused | FetchError::DnsError => {
                SeederError::Network(e.to_string())
            }
            FetchError::BodyError(msg) => SeederError::Data(msg),
            _ => SeederError::Network(e.to_string()),
        }
    }
}

impl From<serde_json::Error> for SeederError {
    fn from(e: serde_json::Error) -> Self {
        SeederError::Data(format!("JSON parse error: {}", e))
    }
}

impl From<std::io::Error> for SeederError {
    fn from(e: std::io::Error) -> Self {
        SeederError::Io(e)
    }
}

/// CDX index entry from Common Crawl so we can deserialize each JSON line.
#[derive(Debug, Deserialize)]
struct CdxEntry {
    url: String,
}

/// Information about a Common Crawl collection to locate the most recent dataset.
#[derive(Debug, Deserialize)]
struct CollectionInfo {
    id: String,
}

/// Seed URLs from the Common Crawl CDX index so we can prime the crawler with archived pages.
pub struct CommonCrawlSeeder {
    http: HttpClient,
}

impl CommonCrawlSeeder {
    /// Create a seeder backed by the shared HTTP client so we reuse the crawler's connection pool.
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    /// Fetch and parse the collection info to retrieve the latest index ID.
    async fn fetch_latest_index_id(http: &HttpClient) -> Result<String, SeederError> {
        const URL: &str = "https://index.commoncrawl.org/collinfo.json";

        let result = http.fetch_bytes(URL).await?;

        // Validate HTTP status
        Self::validate_http_status(result.status_code, "Collection info")?;

        // Validate response size
        if result.content.len() > MAX_COLLINFO_SIZE {
            return Err(SeederError::Data(format!(
                "Collection info too large: {} bytes (max: {})",
                result.content.len(),
                MAX_COLLINFO_SIZE
            )));
        }

        // Parse and extract the latest collection ID
        let collections: Vec<CollectionInfo> = serde_json::from_slice(&result.content)?;

        collections
            .first()
            .map(|c| c.id.clone())
            .ok_or_else(|| SeederError::Data("No collections found in collinfo.json".to_string()))
    }

    /// Parse a single CDX line and extract the URL if valid.
    fn parse_cdx_line(line: &[u8]) -> Result<String, SeederError> {
        if line.is_empty() {
            return Err(SeederError::Data("Empty line".to_string()));
        }

        let entry: CdxEntry = serde_json::from_slice(line)?;

        if entry.url.is_empty() {
            return Err(SeederError::Data("Missing url field".to_string()));
        }

        Ok(entry.url)
    }

    /// Validate HTTP status code and return appropriate error if not 200.
    fn validate_http_status(status: u16, context: &str) -> Result<(), SeederError> {
        if status == 200 {
            Ok(())
        } else if status >= 500 {
            Err(SeederError::Http(status, format!("{} server error", context)))
        } else if status >= 400 {
            Err(SeederError::Http(status, format!("{} client error", context)))
        } else {
            Err(SeederError::Http(status, format!("{} unexpected status", context)))
        }
    }

    /// Construct the CDX query URL for a given domain and index ID.
    fn build_cdx_query_url(index_id: &str, domain: &str) -> String {
        format!(
            "https://index.commoncrawl.org/{}-index?url=*.{}&output=json&fl=url",
            index_id, domain
        )
    }

    /// Fetch the CDX response stream and validate the status code.
    async fn fetch_cdx_stream(
        http: &HttpClient,
        url: &str,
    ) -> Result<reqwest::Response, SeederError> {
        let response = http
            .fetch_stream(url)
            .await
            .map_err(SeederError::from)?;

        let status = response.status().as_u16();
        Self::validate_http_status(status, "CDX")?;
        Ok(response)
    }

    /// Process a single line from the CDX stream, handling errors gracefully.
    fn handle_cdx_line(
        line_buffer: &[u8],
        line_count: usize,
        url_count: &mut usize,
    ) -> Option<String> {
        match Self::parse_cdx_line(line_buffer) {
            Ok(url) => {
                *url_count += 1;
                Some(url)
            }
            Err(_) => {
                // Skip malformed lines so bad records do not abort the whole seeding pass.
                if line_count <= 10 {
                    if let Ok(line_str) = std::str::from_utf8(line_buffer) {
                        eprintln!("Warning: Failed to parse CDX line {}: {}", line_count, line_str);
                    } else {
                        eprintln!("Warning: Failed to parse CDX line {} (non-UTF8)", line_count);
                    }
                }
                None
            }
        }
    }

    /// Strip trailing newline and carriage return characters from a line buffer.
    fn strip_line_endings(line_buffer: &mut Vec<u8>) {
        while line_buffer.last() == Some(&b'\n') || line_buffer.last() == Some(&b'\r') {
            line_buffer.pop();
        }
    }

    /// Check if a line should be skipped due to size constraints.
    fn should_skip_line(line_buffer: &[u8], line_count: usize) -> bool {
        if line_buffer.len() > MAX_LINE_SIZE {
            eprintln!(
                "Warning: Line {} exceeds max size ({} bytes), skipping",
                line_count + 1,
                line_buffer.len()
            );
            true
        } else {
            false
        }
    }

    /// Log progress at regular intervals.
    fn log_progress_if_needed(line_count: usize, url_count: usize) {
        if line_count.is_multiple_of(10_000) {
            eprintln!(
                "Processed {} lines from Common Crawl ({} URLs streamed)...",
                line_count, url_count
            );
        }
    }
}

impl Seeder for CommonCrawlSeeder {
    fn seed(&self, domain: &str) -> UrlStream {
        let http = self.http.clone();
        let domain = domain.to_string();

        Box::pin(stream! {
            // Retrieve the latest index ID so the query targets the freshest crawl data.
            let index_id = match Self::fetch_latest_index_id(&http).await {
                Ok(id) => id,
                Err(e) => {
                    yield Err(e.into());
                    return;
                }
            };

            eprintln!("Using Common Crawl index: {}", index_id);

            // Construct the query URL so the CDX API scopes results to the requested domain.
            let url = Self::build_cdx_query_url(&index_id, &domain);

            eprintln!(
                "Querying Common Crawl CDX index for domain: {} (streaming results...)",
                domain
            );

            // Fetch the response as a stream so we can process huge result sets incrementally.
            let response = match Self::fetch_cdx_stream(&http, &url).await {
                Ok(resp) => resp,
                Err(e) => {
                    yield Err(e.into());
                    return;
                }
            };

            // Stream the body to avoid buffering millions of entries into memory.
            // Reqwest automatically handles gzip/brotli/deflate decompression, so we can
            // directly stream the decompressed bytes.
            use futures_util::TryStreamExt;

            let body_stream = response
                .bytes_stream()
                .map_err(std::io::Error::other);

            let stream_reader = tokio_util::io::StreamReader::new(body_stream);
            let mut reader = Box::pin(BufReader::new(stream_reader));

            let mut line_buffer = Vec::new();
            let mut line_count = 0_usize;
            let mut url_count = 0_usize;

            // Read lines and yield each URL as we go, enforcing the cap to respect memory limits.
            loop {
                // Stop once we reach the cap to respect the memory-safety guardrail.
                if url_count >= MAX_COMMON_CRAWL_RESULTS {
                    eprintln!(
                        "Reached Common Crawl result limit of {} URLs, stopping early",
                        MAX_COMMON_CRAWL_RESULTS
                    );
                    break;
                }

                line_buffer.clear();
                use tokio::io::AsyncBufReadExt;

                let bytes_read = match reader.read_until(b'\n', &mut line_buffer).await {
                    Ok(n) => n,
                    Err(e) => {
                        yield Err(SeederError::Io(e).into());
                        break;
                    }
                };

                if bytes_read == 0 {
                    break; // Stop here because EOF means the stream is exhausted.
                }

                line_count += 1;

                // Enforce max line size to prevent OOM on malformed input.
                if Self::should_skip_line(&line_buffer, line_count) {
                    continue;
                }

                // Strip trailing newline/carriage return
                Self::strip_line_endings(&mut line_buffer);

                // Parse each JSON line and yield the URL if valid
                if let Some(url) = Self::handle_cdx_line(&line_buffer, line_count, &mut url_count) {
                    yield Ok(url);
                }

                // Log progress every 10,000 lines to keep operators informed without spamming.
                Self::log_progress_if_needed(line_count, url_count);
            }

            eprintln!(
                "Streamed {} URLs from Common Crawl (processed {} lines)",
                url_count,
                line_count
            );
        })
    }

    fn name(&self) -> &'static str {
        "common-crawl"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_common_crawl_seeder_creation() {
        let http = HttpClient::new("TestBot/1.0".to_string(), 120)
            .expect("Failed to create HTTP client in test");
        let _seeder = CommonCrawlSeeder::new(http);
        assert!(true);
    }

    #[test]
    fn test_parse_cdx_line_valid() {
        let line = br#"{"url":"https://example.com/page"}"#;
        let result = CommonCrawlSeeder::parse_cdx_line(line);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "https://example.com/page");
    }

    #[test]
    fn test_parse_cdx_line_empty_url() {
        let line = br#"{"url":""}"#;
        let result = CommonCrawlSeeder::parse_cdx_line(line);
        assert!(result.is_err());
        match result {
            Err(SeederError::Data(msg)) => assert!(msg.contains("Missing url")),
            _ => panic!("Expected Data error"),
        }
    }

    #[test]
    fn test_parse_cdx_line_malformed_json() {
        let line = b"not json";
        let result = CommonCrawlSeeder::parse_cdx_line(line);
        assert!(result.is_err());
        match result {
            Err(SeederError::Data(_)) => {}
            _ => panic!("Expected Data error"),
        }
    }

    #[test]
    fn test_parse_cdx_line_empty() {
        let line = b"";
        let result = CommonCrawlSeeder::parse_cdx_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cdx_line_missing_url_field() {
        let line = br#"{"other":"value"}"#;
        let result = CommonCrawlSeeder::parse_cdx_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_seeder_error_retryable() {
        assert!(SeederError::Http(500, "".to_string()).retryable());
        assert!(SeederError::Http(503, "".to_string()).retryable());
        assert!(SeederError::Http(408, "".to_string()).retryable());
        assert!(SeederError::Http(429, "".to_string()).retryable());
        assert!(SeederError::Network("timeout".to_string()).retryable());
        assert!(SeederError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)).retryable());

        assert!(!SeederError::Http(400, "".to_string()).retryable());
        assert!(!SeederError::Http(404, "".to_string()).retryable());
        assert!(!SeederError::Data("bad data".to_string()).retryable());
    }
}
