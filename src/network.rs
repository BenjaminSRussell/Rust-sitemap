use crate::config::Config;
use reqwest::{Client, Response};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    timeout: Duration,
    user_agent: String,
    pub max_content_size: usize,
}

impl HttpClient {
    /// Create HTTP client with default content size limit
    pub fn new(user_agent: String, timeout_secs: u64) -> Self {
        Self::with_content_limit(user_agent, timeout_secs, Config::MAX_CONTENT_SIZE)
    }

    /// Create HTTP client with custom content size limit
    pub fn with_content_limit(
        user_agent: String,
        timeout_secs: u64,
        max_content: usize,
    ) -> Self {
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(Config::POOL_IDLE_PER_HOST)
            .pool_idle_timeout(Duration::from_secs(Config::POOL_IDLE_TIMEOUT_SECS))
            .gzip(true)
            .build()
            .expect("Failed to build reqwest client");

        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
            user_agent,
            max_content_size: max_content,
        }
    }

    /// Fetch URL with streaming response (used by bfs_crawler.rs)
    pub async fn fetch_stream(&self, url: &str) -> Result<Response, FetchError> {
        let response = self
            .client
            .get(url)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Connection", "keep-alive")
            .header("Upgrade-Insecure-Requests", "1")
            .send()
            .await
            .map_err(|e| FetchError::from_reqwest_error(e))?;

        // Check content-length for size limit
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

    /// Fetch URL and buffer the entire response (used by seeders and robots.rs)
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        let response = self.fetch_stream(url).await?;
        let status_code = response.status();

        let body_bytes = response
            .bytes()
            .await
            .map_err(|e| FetchError::BodyError(e.to_string()))?;

        // Check size after buffering
        if body_bytes.len() > self.max_content_size {
            return Err(FetchError::ContentTooLarge(
                body_bytes.len(),
                self.max_content_size,
            ));
        }

        let content = String::from_utf8(body_bytes.to_vec())
            .map_err(|e| FetchError::BodyError(format!("Invalid UTF-8: {}", e)))?;

        Ok(FetchResult {
            content,
            status_code: status_code.as_u16(),
        })
    }
}

/// Legacy result for backward compatibility (used by robots.txt fetching)
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
}

impl FetchError {
    /// Convert reqwest::Error into FetchError
    fn from_reqwest_error(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            return FetchError::Timeout;
        }

        if error.is_connect() {
            let error_msg = error.to_string().to_lowercase();
            if error_msg.contains("connection refused") {
                return FetchError::ConnectionRefused;
            }
            if error_msg.contains("dns")
                || error_msg.contains("name resolution")
                || error_msg.contains("no such host")
            {
                return FetchError::DnsError;
            }
        }

        if error.to_string().to_lowercase().contains("certificate")
            || error.to_string().to_lowercase().contains("ssl")
            || error.to_string().to_lowercase().contains("tls")
        {
            return FetchError::SslError;
        }

        FetchError::NetworkError(error.to_string())
    }

    /// Identify which errors warrant a retry
    pub fn is_retryable(&self) -> bool {
        match self {
            FetchError::Timeout => true,
            FetchError::NetworkError(msg) => {
                let msg_lower = msg.to_lowercase();
                msg_lower.contains("timeout")
                    || msg_lower.contains("broken pipe")
                    || msg_lower.contains("connection reset")
                    || msg_lower.contains("temporary")
            }
            FetchError::ConnectionRefused => false,
            FetchError::DnsError => false,
            FetchError::SslError => false,
            FetchError::BodyError(_) => false,
            FetchError::ContentTooLarge(_, _) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_invalid_url() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30);

        let result = client.fetch("not-a-url").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30);
        assert_eq!(client.user_agent, "TestBot/1.0");
    }
}
