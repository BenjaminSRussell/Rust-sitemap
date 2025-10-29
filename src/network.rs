use crate::config::Config;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Uri};
use hyper_tls::HttpsConnector;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client<HttpsConnector<HttpConnector>>,
    timeout: Duration,
    user_agent: String,
    max_content_size: usize,
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
        let https = HttpsConnector::new();
        let client = Client::builder()
            .pool_max_idle_per_host(Config::POOL_IDLE_PER_HOST)
            .pool_idle_timeout(Duration::from_secs(Config::POOL_IDLE_TIMEOUT_SECS))
            .build::<_, Body>(https);

        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
            user_agent,
            max_content_size: max_content,
        }
    }

    /// Fetch URL with automatic retries
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        let mut last_error = None;

        for attempt in 0..=Config::MAX_RETRIES {
            if attempt > 0 {
                let backoff_ms = Config::RETRY_BACKOFF_MS * attempt as u64;
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            match self.fetch_once(url).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if e.is_retryable() && attempt < Config::MAX_RETRIES {
                        last_error = Some(e);
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(last_error.unwrap_or(FetchError::NetworkError("Max retries exceeded".to_string())))
    }

    /// Fetch URL once without retries
    async fn fetch_once(&self, url: &str) -> Result<FetchResult, FetchError> {
        let start_time = std::time::Instant::now();
        let uri: Uri = url
            .parse()
            .map_err(|e| FetchError::NetworkError(format!("Invalid URL: {}", e)))?;

        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .header("User-Agent", &self.user_agent)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Connection", "keep-alive")
            .header("Upgrade-Insecure-Requests", "1")
            .body(Body::empty())
            .map_err(|e| FetchError::NetworkError(format!("Failed to build request: {}", e)))?;

        let response = timeout(self.timeout, self.client.request(req))
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(Self::classify_hyper_error)?;

        let status_code = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        if let Some(content_length) = response.headers().get("content-length") {
            if let Ok(length_str) = content_length.to_str() {
                if let Ok(length) = length_str.parse::<usize>() {
                    if length > self.max_content_size {
                        return Err(FetchError::ContentTooLarge(length, self.max_content_size));
                    }
                }
            }
        }

        let body_bytes = timeout(self.timeout, hyper::body::to_bytes(response.into_body()))
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(|e| FetchError::BodyError(e.to_string()))?;

        let response_time_ms = start_time.elapsed().as_millis() as u64;
        let content_length = body_bytes.len();
        let content = String::from_utf8(body_bytes.to_vec())
            .map_err(|e| FetchError::BodyError(format!("Invalid UTF-8: {}", e)))?;

        Ok(FetchResult {
            content,
            status_code,
            content_type,
            content_length,
            response_time_ms,
        })
    }

    /// Classify Hyper errors into FetchError types
    fn classify_hyper_error(error: hyper::Error) -> FetchError {
        if error.is_timeout() {
            return FetchError::Timeout;
        }

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

        if error_msg.contains("ssl")
            || error_msg.contains("tls")
            || error_msg.contains("certificate")
        {
            return FetchError::SslError;
        }

        FetchError::NetworkError(error.to_string())
    }
}
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub content: String,
    pub status_code: u16,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub response_time_ms: u64,
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
    /// Check if error is retryable
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
