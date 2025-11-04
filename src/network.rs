use crate::config::Config;
use reqwest::{Client, Response};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    pub max_content_size: usize,
}

impl HttpClient {
    /// Create an HTTP client with the default content size limit.
    pub fn new(user_agent: String, timeout_secs: u64) -> Result<Self, FetchError> {
        Self::with_content_limit(user_agent, timeout_secs, Config::MAX_CONTENT_SIZE)
    }

    /// Create an HTTP client with a custom content size limit.
    pub fn with_content_limit(
        user_agent: String,
        timeout_secs: u64,
        max_content: usize,
    ) -> Result<Self, FetchError> {
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(Config::POOL_IDLE_PER_HOST)
            .pool_idle_timeout(Duration::from_secs(Config::POOL_IDLE_TIMEOUT_SECS))
            // Disable automatic decompression because we handle it manually.
            .no_gzip()
            .no_brotli()
            .no_deflate()
            // Enable the HTTP/2 adaptive window for better performance.
            .http2_adaptive_window(true)
            // Disable automatic redirect following so the crawler can decide how to handle redirects.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| FetchError::ClientBuildError(e.to_string()))?;

        Ok(Self {
            client,
            max_content_size: max_content,
        })
    }

    /// Fetch a URL with a streaming response (used by bfs_crawler.rs).
    pub async fn fetch_stream(&self, url: &str) -> Result<Response, FetchError> {
        let response = self
            .client
            .get(url)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("Accept-Language", "en-US,en;q=0.5")
            // Advertise manual compression support because auto-decode is disabled.
            .header("Accept-Encoding", "gzip, br, deflate")
            // Avoid custom Connection or Upgrade headers; let the client manage connections.
            .send()
            .await
            .map_err(FetchError::from_reqwest_error)?;

        // Enforce the size limit using the content-length header.
        if let Some(content_length) = response.content_length() {
            if content_length as usize > self.max_content_size {
                return Err(FetchError::ContentTooLarge(
                    content_length as usize,
                    self.max_content_size,
                ));
            }
        }

        Ok(response)
    }

    /// Fetch a URL and buffer the entire response (used by seeders and robots.rs).
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        let response = self.fetch_stream(url).await?;
        let status_code = response.status();

        let body_bytes = response
            .bytes()
            .await
            .map_err(|e| FetchError::BodyError(e.to_string()))?;

        // Enforce the size limit after buffering.
        if body_bytes.len() > self.max_content_size {
            return Err(FetchError::ContentTooLarge(
                body_bytes.len(),
                self.max_content_size,
            ));
        }

        let content = String::from_utf8(body_bytes.into())
            .map_err(|e| FetchError::BodyError(format!("Invalid UTF-8: {}", e)))?;

        Ok(FetchResult {
            content,
            status_code: status_code.as_u16(),
        })
    }
}

/// Legacy result for backward compatibility (used by robots.txt fetching).
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub content: String,
    pub status_code: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Connection refused - server not accepting connections")]
    ConnectionRefused,

    #[error("DNS resolution failed")]
    DnsError,

    #[error("SSL/TLS error - certificate or encryption issue")]
    SslError,

    #[error("Request timeout")]
    Timeout,

    #[error("Failed to read response body: {0}")]
    BodyError(String),

    #[error("Content too large: {0} bytes (max: {1} bytes)")]
    ContentTooLarge(usize, usize),

    #[error("Failed to build HTTP client: {0}")]
    ClientBuildError(String),
}

impl FetchError {
    /// Convert reqwest::Error into FetchError.
    fn from_reqwest_error(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            return FetchError::Timeout;
        }

        let error_msg_lower = error.to_string().to_lowercase();

        if error.is_connect() {
            if error_msg_lower.contains("connection refused") {
                return FetchError::ConnectionRefused;
            }
            if error_msg_lower.contains("dns")
                || error_msg_lower.contains("name resolution")
                || error_msg_lower.contains("no such host")
            {
                return FetchError::DnsError;
            }
        }

        if error_msg_lower.contains("certificate")
            || error_msg_lower.contains("ssl")
            || error_msg_lower.contains("tls")
        {
            return FetchError::SslError;
        }

        FetchError::NetworkError(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_invalid_url() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30)
            .expect("Failed to create client in test");

        let result = client.fetch("not-a-url").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30)
            .expect("Failed to create client in test");
        // Confirm the constructor honors MAX_CONTENT_SIZE so regressions surface in tests.
        assert_eq!(client.max_content_size, Config::MAX_CONTENT_SIZE);
    }
}
