use crate::network::{FetchError, HttpClient};
use crate::seeder::Seeder;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};

// Cap results to avoid exhausting memory
const MAX_COMMON_CRAWL_RESULTS: usize = 100_000;

/// CDX index entry from Common Crawl
#[derive(Debug, Deserialize)]
struct CdxEntry {
    url: String,
}

/// Information about a Common Crawl collection
#[derive(Debug, Deserialize)]
struct CollectionInfo {
    id: String,
}

/// Seed URLs from the Common Crawl CDX index
pub struct CommonCrawlSeeder {
    http: HttpClient,
}

impl CommonCrawlSeeder {
    /// Create a seeder backed by the shared HTTP client
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    /// Fetch the newest Common Crawl index ID
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

    /// Fetch URLs for the domain from the Common Crawl CDX index
    pub async fn seed(&self, domain: &str) -> Result<Vec<String>, FetchError> {
        // Fetch latest index ID
        let index_id = self.get_latest_index_id().await?;

        eprintln!("Using Common Crawl index: {}", index_id);

        // Build query URL
        let url = format!(
            "https://index.commoncrawl.org/{}?url=*.{}&output=json&fl=url",
            index_id, domain
        );

        eprintln!(
            "Querying Common Crawl CDX index for domain: {} (this may take a while...)",
            domain
        );

        // Fetch response stream
        let response = self.http.fetch_stream(&url).await?;

        if response.status().as_u16() != 200 {
            return Err(FetchError::NetworkError(format!(
                "CDX query failed with status code: {}",
                response.status().as_u16()
            )));
        }

        // Stream body to avoid buffering everything
        use futures_util::TryStreamExt;

        // Convert reqwest's byte stream to a tokio::io::AsyncRead
        let body_stream = response
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        let stream_reader = tokio_util::io::StreamReader::new(body_stream);
        let gzip_decoder = GzipDecoder::new(stream_reader);
        let mut reader = Box::pin(BufReader::new(gzip_decoder));

        let mut urls = Vec::new();
        let mut line = String::new();
        let mut line_count = 0;

        // Read lines while enforcing the limit
        loop {
            // Stop once we hit the limit
            if urls.len() >= MAX_COMMON_CRAWL_RESULTS {
                eprintln!(
                    "Reached Common Crawl result limit of {} URLs, stopping early",
                    MAX_COMMON_CRAWL_RESULTS
                );
                break;
            }

            line.clear();
            let bytes_read = reader.read_line(&mut line).await.map_err(|e| {
                FetchError::BodyError(format!("Failed to read line from gzip stream: {}", e))
            })?;

            if bytes_read == 0 {
                break; // EOF
            }

            line_count += 1;

            // Parse each JSON line
            if let Ok(entry) = serde_json::from_str::<CdxEntry>(line.trim()) {
                urls.push(entry.url);
            } else {
                // Skip malformed lines
                if line_count < 10 {
                    eprintln!("Warning: Failed to parse CDX line: {}", line.trim());
                }
            }

            // Log progress every 10,000 lines
            if line_count % 10000 == 0 {
                eprintln!(
                    "Processed {} lines from Common Crawl ({} URLs collected)...",
                    line_count,
                    urls.len()
                );
            }
        }

        eprintln!(
            "Found {} URLs from Common Crawl (processed {} lines)",
            urls.len(),
            line_count
        );

        Ok(urls)
    }
}

#[async_trait]
impl Seeder for CommonCrawlSeeder {
    async fn seed(&self, domain: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.seed(domain).await?)
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
        let http = HttpClient::new("TestBot/1.0".to_string(), 120);
        let _seeder = CommonCrawlSeeder::new(http);
        // Ensure constructor succeeds
        assert!(true);
    }
}
