use crate::network::{FetchError, HttpClient};
use crate::seeder::{Seeder, UrlStream};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::stream;
use serde::Deserialize;
use tokio::io::BufReader;

// Keep the result set bounded so we do not exhaust memory after large CDX queries.
const MAX_COMMON_CRAWL_RESULTS: usize = 100_000;

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

    /// Fetch the newest Common Crawl index ID so we always pull from the latest crawl.
    async fn get_latest_index_id(&self) -> Result<String, FetchError> {
        let url = "https://index.commoncrawl.org/collinfo.json";

        eprintln!("Fetching latest Common Crawl index ID...");

        let result = self.http.fetch(url).await?;

        if result.status_code != 200 {
            return Err(FetchError::NetworkError(format!(
                "Failed to fetch collection info with status code: {}",
                result.status_code
            )));
        }

        let collections: Vec<CollectionInfo> =
            serde_json::from_str(&result.content).map_err(|e| {
                FetchError::BodyError(format!("Failed to parse collection info JSON: {}", e))
            })?;

        collections
            .first()
            .map(|c| c.id.clone())
            .ok_or_else(|| FetchError::BodyError("No collections found".to_string()))
    }

    /// Stream URLs for the domain from the Common Crawl CDX index so the crawler starts with historical coverage.
    pub fn seed_stream(&self, domain: String) -> UrlStream {
        let http = self.http.clone();

        Box::pin(stream! {
            // Retrieve the latest index ID so the query targets the freshest crawl data.
            let index_id = match http.fetch("https://index.commoncrawl.org/collinfo.json").await {
                Ok(result) if result.status_code == 200 => {
                    match serde_json::from_str::<Vec<CollectionInfo>>(&result.content) {
                        Ok(collections) => {
                            if let Some(first) = collections.first() {
                                first.id.clone()
                            } else {
                                yield Err("No Common Crawl collections found".into());
                                return;
                            }
                        }
                        Err(e) => {
                            yield Err(format!("Failed to parse collection info: {}", e).into());
                            return;
                        }
                    }
                }
                Ok(result) => {
                    yield Err(format!("Failed to fetch collection info: status {}", result.status_code).into());
                    return;
                }
                Err(e) => {
                    yield Err(format!("Network error fetching collection info: {}", e).into());
                    return;
                }
            };

            eprintln!("Using Common Crawl index: {}", index_id);

            // Construct the query URL so the CDX API scopes results to the requested domain.
            let url = format!(
                "https://index.commoncrawl.org/{}?url=*.{}&output=json&fl=url",
                index_id, domain
            );

            eprintln!(
                "Querying Common Crawl CDX index for domain: {} (streaming results...)",
                domain
            );

            // Fetch the response as a stream so we can process huge result sets incrementally.
            let response = match http.fetch_stream(&url).await {
                Ok(resp) if resp.status().as_u16() == 200 => resp,
                Ok(resp) => {
                    yield Err(format!("CDX query failed with status: {}", resp.status()).into());
                    return;
                }
                Err(e) => {
                    yield Err(format!("Network error: {}", e).into());
                    return;
                }
            };

            // Stream the body to avoid buffering millions of entries into memory.
            use futures_util::TryStreamExt;

            // Convert reqwest's byte stream to AsyncRead so tokio utilities can consume it line by line.
            let body_stream = response
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            let stream_reader = tokio_util::io::StreamReader::new(body_stream);
            let gzip_decoder = GzipDecoder::new(stream_reader);
            let mut reader = Box::pin(BufReader::new(gzip_decoder));

            let mut line_buffer = Vec::new();
            let mut line_count = 0;
            let mut url_count = 0;

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
                        yield Err(format!("Failed to read line from gzip stream: {}", e).into());
                        break;
                    }
                };

                if bytes_read == 0 {
                    break; // Stop here because EOF means the stream is exhausted.
                }

                line_count += 1;

                // Strip trailing newline/carriage return
                while line_buffer.last() == Some(&b'\n') || line_buffer.last() == Some(&b'\r') {
                    line_buffer.pop();
                }

                // Parse each JSON line using from_slice to handle non-UTF8 gracefully
                if let Ok(entry) = serde_json::from_slice::<CdxEntry>(&line_buffer) {
                    url_count += 1;
                    yield Ok(entry.url);
                } else {
                    // Skip malformed lines so bad records do not abort the whole seeding pass.
                    if line_count < 10 {
                        if let Ok(line_str) = std::str::from_utf8(&line_buffer) {
                            eprintln!("Warning: Failed to parse CDX line: {}", line_str);
                        } else {
                            eprintln!("Warning: Failed to parse CDX line (non-UTF8)");
                        }
                    }
                }

                // Log progress every 10,000 lines to keep operators informed without spamming.
                if line_count % 10000 == 0 {
                    eprintln!(
                        "Processed {} lines from Common Crawl ({} URLs streamed)...",
                        line_count,
                        url_count
                    );
                }
            }

            eprintln!(
                "Streamed {} URLs from Common Crawl (processed {} lines)",
                url_count,
                line_count
            );
        })
    }
}

impl Seeder for CommonCrawlSeeder {
    fn seed(&self, domain: &str) -> UrlStream {
        let http = self.http.clone();
        let domain = domain.to_string();

        Box::pin(stream! {
            // Retrieve the latest index ID so the query targets the freshest crawl data.
            let index_id = match http.fetch("https://index.commoncrawl.org/collinfo.json").await {
                Ok(result) if result.status_code == 200 => {
                    match serde_json::from_str::<Vec<CollectionInfo>>(&result.content) {
                        Ok(collections) => {
                            if let Some(first) = collections.first() {
                                first.id.clone()
                            } else {
                                yield Err("No Common Crawl collections found".into());
                                return;
                            }
                        }
                        Err(e) => {
                            yield Err(format!("Failed to parse collection info: {}", e).into());
                            return;
                        }
                    }
                }
                Ok(result) => {
                    yield Err(format!("Failed to fetch collection info: status {}", result.status_code).into());
                    return;
                }
                Err(e) => {
                    yield Err(format!("Network error fetching collection info: {}", e).into());
                    return;
                }
            };

            eprintln!("Using Common Crawl index: {}", index_id);

            // Construct the query URL so the CDX API scopes results to the requested domain.
            let url = format!(
                "https://index.commoncrawl.org/{}?url=*.{}&output=json&fl=url",
                index_id, domain
            );

            eprintln!(
                "Querying Common Crawl CDX index for domain: {} (streaming results...)",
                domain
            );

            // Fetch the response as a stream so we can process huge result sets incrementally.
            let response = match http.fetch_stream(&url).await {
                Ok(resp) if resp.status().as_u16() == 200 => resp,
                Ok(resp) => {
                    yield Err(format!("CDX query failed with status: {}", resp.status()).into());
                    return;
                }
                Err(e) => {
                    yield Err(format!("Network error: {}", e).into());
                    return;
                }
            };

            // Stream the body to avoid buffering millions of entries into memory.
            use futures_util::TryStreamExt;

            // Convert reqwest's byte stream to AsyncRead so tokio utilities can consume it line by line.
            let body_stream = response
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            let stream_reader = tokio_util::io::StreamReader::new(body_stream);
            let gzip_decoder = GzipDecoder::new(stream_reader);
            let mut reader = Box::pin(BufReader::new(gzip_decoder));

            let mut line_buffer = Vec::new();
            let mut line_count = 0;
            let mut url_count = 0;

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
                        yield Err(format!("Failed to read line from gzip stream: {}", e).into());
                        break;
                    }
                };

                if bytes_read == 0 {
                    break; // Stop here because EOF means the stream is exhausted.
                }

                line_count += 1;

                // Strip trailing newline/carriage return
                while line_buffer.last() == Some(&b'\n') || line_buffer.last() == Some(&b'\r') {
                    line_buffer.pop();
                }

                // Parse each JSON line using from_slice to handle non-UTF8 gracefully
                if let Ok(entry) = serde_json::from_slice::<CdxEntry>(&line_buffer) {
                    url_count += 1;
                    yield Ok(entry.url);
                } else {
                    // Skip malformed lines so bad records do not abort the whole seeding pass.
                    if line_count < 10 {
                        if let Ok(line_str) = std::str::from_utf8(&line_buffer) {
                            eprintln!("Warning: Failed to parse CDX line: {}", line_str);
                        } else {
                            eprintln!("Warning: Failed to parse CDX line (non-UTF8)");
                        }
                    }
                }

                // Log progress every 10,000 lines to keep operators informed without spamming.
                if line_count % 10000 == 0 {
                    eprintln!(
                        "Processed {} lines from Common Crawl ({} URLs streamed)...",
                        line_count,
                        url_count
                    );
                }
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
        // Keep this smoke test so a panic in the constructor surfaces immediately.
        assert!(true);
    }
}
