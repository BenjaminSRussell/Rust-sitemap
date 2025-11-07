//! HTTP client with content-length enforcement and automatic decompression.
//!
//! Provides a reqwest-based HTTP client configured with:
//! - Content size limits to prevent memory exhaustion
//! - Connection pooling for reduced latency
//! - Automatic redirect following (max 10)
//! - HTTP/2 adaptive window for better performance
//! - Transparent gzip/brotli/deflate decompression

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
            // Automatic decompression is enabled by default for transparent gzip/brotli/deflate handling
            // Enable the HTTP/2 adaptive window for better performance.
            .http2_adaptive_window(true)
            // Automatic redirect following is enabled by default (max 10 redirects)
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
            // Accept-Encoding is automatically added by reqwest when decompression is enabled
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

        let content = String::from_utf8(body_bytes.into()).map_err(|e| {
            FetchError::BodyError(format!("Invalid UTF-8 in response from {}: {}", url, e))
        })?;

        Ok(FetchResult {
            content,
            status_code: status_code.as_u16(),
        })
    }

    /// Fetch a URL and return raw bytes (no UTF-8 validation).
    /// Use this when you need to handle binary data or parse non-UTF-8 content.
    pub async fn fetch_bytes(&self, url: &str) -> Result<FetchBytesResult, FetchError> {
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

        Ok(FetchBytesResult {
            content: body_bytes.to_vec(),
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

/// Result for fetch_bytes containing raw bytes without UTF-8 validation.
#[derive(Debug, Clone)]
pub struct FetchBytesResult {
    pub content: Vec<u8>,
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

    #[error("Already locked")]
    AlreadyLocked,

    #[error("Failed to acquire network permit: semaphore closed")]
    PermitAcquisition,

    #[error("Timeout acquiring network permit after 30s")]
    PermitTimeout,

    #[error("Invalid UTF-8")]
    InvalidUtf8,

    #[error("HTML too large for parsing")]
    HtmlTooLarge,
}

impl FetchError {
    /// Convert reqwest::Error into FetchError.
    pub(crate) fn from_reqwest_error(error: reqwest::Error) -> Self {
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
