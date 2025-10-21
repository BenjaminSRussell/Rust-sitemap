use reqwest;
use std::time::Duration;
use tokio::time::timeout;

/// HTTP client for making web requests
#[derive(Debug)]
pub struct HttpClient {
    client: reqwest::Client,
    timeout_duration: Duration,
    #[allow(dead_code)]
    user_agent: String,
    max_content_size: usize,
}

impl HttpClient {
    /// Create a new HTTP client with default settings optimized for crawling
    pub fn new(user_agent: String, timeout_secs: u64) -> Self {
        Self::with_content_limit(user_agent, timeout_secs, 10 * 1024 * 1024) // 10MB default (was failing at 5MB)
    }

    /// Create a new HTTP client with custom content size limit
    pub fn with_content_limit(
        user_agent: String,
        timeout_secs: u64,
        max_content_size: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(timeout_secs))
            .connect_timeout(Duration::from_secs(10)) // Separate connect timeout
            // Reduced pool size to prevent overwhelming servers
            .pool_max_idle_per_host(16) // Reduced from 64 to 16
            .pool_idle_timeout(Duration::from_secs(30)) // Shorter keepalive
            // Force HTTP/1.1 - more reliable than HTTP/2 for broad compatibility
            .http1_only() // Use HTTP/1.1 instead of HTTP/2 to avoid compatibility issues
            .tcp_keepalive(Duration::from_secs(60)) // TCP keepalive
            .tcp_nodelay(true) // Disable Nagle's algorithm for lower latency
            .redirect(reqwest::redirect::Policy::limited(5)) // Limit redirects
            .danger_accept_invalid_certs(false) // Ensure we validate certificates
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            timeout_duration: Duration::from_secs(timeout_secs),
            user_agent,
            max_content_size,
        }
    }

    /// Get the user agent string used by this client
    #[allow(dead_code)]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Fetch a URL and return the response body as a string
    /// Implements retry logic with exponential backoff for transient errors
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        const MAX_RETRIES: u32 = 2; // Total of 3 attempts (1 initial + 2 retries)
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            // Exponential backoff: 0ms, 500ms, 1000ms
            if attempt > 0 {
                let backoff_ms = 500 * attempt as u64;
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            match self.fetch_once(url).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Check if error is retryable
                    if e.is_retryable() && attempt < MAX_RETRIES {
                        last_error = Some(e);
                        continue; // Retry
                    } else {
                        return Err(e); // Don't retry permanent errors
                    }
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or(FetchError::NetworkError("Max retries exceeded".to_string())))
    }

    /// Fetch a URL once (internal helper for retry logic)
    async fn fetch_once(&self, url: &str) -> Result<FetchResult, FetchError> {
        let response = timeout(
            self.timeout_duration,
            self.client
                .get(url)
                .header(
                    "Accept",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                )
                .header("Accept-Language", "en-US,en;q=0.5")
                .header("Connection", "keep-alive")
                .header("Upgrade-Insecure-Requests", "1")
                .send(),
        )
        .await
        .map_err(|_| FetchError::Timeout)?
        .map_err(|e| Self::classify_error(e))?;

        let status_code = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        // Check content length header first
        if let Some(content_length) = response.headers().get("content-length") {
            if let Ok(length_str) = content_length.to_str() {
                if let Ok(length) = length_str.parse::<usize>() {
                    if length > self.max_content_size {
                        return Err(FetchError::ContentTooLarge(length, self.max_content_size));
                    }
                }
            }
        }

        let content = timeout(self.timeout_duration, response.text())
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(|e| FetchError::BodyError(e.to_string()))?;

        // Check actual content size
        if content.len() > self.max_content_size {
            return Err(FetchError::ContentTooLarge(
                content.len(),
                self.max_content_size,
            ));
        }

        Ok(FetchResult {
            content,
            status_code,
            content_type,
        })
    }

    /// Classify reqwest errors into our FetchError types with better categorization
    fn classify_error(error: reqwest::Error) -> FetchError {
        let error_msg = error.to_string().to_lowercase();
        
        // Connection refused - server not accepting connections
        if error_msg.contains("connection refused") {
            return FetchError::ConnectionRefused;
        }
        
        // DNS resolution failures
        if error_msg.contains("dns") || error_msg.contains("name resolution") {
            return FetchError::DnsError;
        }
        
        // SSL/TLS errors
        if error_msg.contains("ssl") || error_msg.contains("tls") || error_msg.contains("certificate") {
            return FetchError::SslError;
        }
        
        // Timeout (should be caught earlier, but just in case)
        if error.is_timeout() {
            return FetchError::Timeout;
        }
        
        // Generic network error
        FetchError::NetworkError(error.to_string())
    }
}

/// Result of a successful HTTP fetch
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub content: String,
    pub status_code: u16,
    pub content_type: Option<String>,
}

/// Errors that can occur during HTTP fetching
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
    /// Check if this error is retryable (transient) or permanent
    pub fn is_retryable(&self) -> bool {
        match self {
            // Retryable: transient network issues
            FetchError::Timeout => true,
            FetchError::NetworkError(msg) => {
                // Some network errors are retryable
                let msg_lower = msg.to_lowercase();
                msg_lower.contains("timeout")
                    || msg_lower.contains("broken pipe")
                    || msg_lower.contains("connection reset")
                    || msg_lower.contains("temporary")
            }
            // Not retryable: permanent failures
            FetchError::ConnectionRefused => false, // Server is down or blocking us
            FetchError::DnsError => false,          // DNS won't suddenly work
            FetchError::SslError => false,          // Certificate issues won't fix themselves
            FetchError::BodyError(_) => false,      // Body parsing issues are permanent
            FetchError::ContentTooLarge(_, _) => false, // Content size won't change
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

        assert!(result.is_err()); // Any error is acceptable for invalid URL
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30);
        // Just test that the client can be created without panicking
        assert_eq!(client.user_agent, "TestBot/1.0");
    }
}
