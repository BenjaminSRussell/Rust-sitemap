use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Uri};
use hyper_tls::HttpsConnector;
use std::time::Duration;
use tokio::time::timeout;

/// http client for hyper requests
#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client<HttpsConnector<HttpConnector>>,
    timeout: Duration,
    user_agent: String,
    max_content_size: usize,
}

impl HttpClient {
    /// create http client with crawl tuned defaults
    pub fn new(user_agent: String, timeout_secs: u64) -> Self {
        Self::with_content_limit(user_agent, timeout_secs, 10 * 1024 * 1024) // 10mb default limit
    }

    /// create http client with custom content cap
    pub fn with_content_limit(
        user_agent: String,
        timeout_secs: u64,
        max_content_size: usize,
    ) -> Self {
        // create https connector
        let https = HttpsConnector::new();

        // create hyper client with connection pooling
        let client = Client::builder()
            .pool_max_idle_per_host(16) // lower cap to avoid overwhelming servers
            .pool_idle_timeout(Duration::from_secs(30)) // shorter keepalive
            .build::<_, Body>(https);

        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
            user_agent,
            max_content_size,
        }
    }

    /// fetch a url and return the response body as a string
    /// includes retry logic with exponential backoff for transient errors
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        const MAX_RETRIES: u32 = 2; // total of three attempts
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            // exponential backoff: 0ms, 500ms, 1000ms
            if attempt > 0 {
                let backoff_ms = 500 * attempt as u64;
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            match self.fetch_once(url).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // check if error is retryable
                    if e.is_retryable() && attempt < MAX_RETRIES {
                        last_error = Some(e);
                        continue; // retry
                    } else {
                        return Err(e); // skip retry for permanent errors
                    }
                }
            }
        }

        // all retries exhausted
        Err(last_error.unwrap_or(FetchError::NetworkError("Max retries exceeded".to_string())))
    }

    /// fetch a url once as a helper
    async fn fetch_once(&self, url: &str) -> Result<FetchResult, FetchError> {
        // start timing
        let start_time = std::time::Instant::now();

        // parse the url
        let uri: Uri = url
            .parse()
            .map_err(|e| FetchError::NetworkError(format!("Invalid URL: {}", e)))?;

        // build request with headers
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

        // run request within timeout
        let response = timeout(self.timeout, self.client.request(req))
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(Self::classify_hyper_error)?;

        let status_code = response.status().as_u16();

        // extract content type
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        // check content length before download
        if let Some(content_length) = response.headers().get("content-length") {
            if let Ok(length_str) = content_length.to_str() {
                if let Ok(length) = length_str.parse::<usize>() {
                    if length > self.max_content_size {
                        return Err(FetchError::ContentTooLarge(length, self.max_content_size));
                    }
                }
            }
        }

        // read response body with timeout
        let body_bytes = timeout(self.timeout, hyper::body::to_bytes(response.into_body()))
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(|e| FetchError::BodyError(e.to_string()))?;

        // calculate response time
        let response_time_ms = start_time.elapsed().as_millis() as u64;

        // record content length
        let content_length = body_bytes.len();

        // convert bytes to string
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

    /// classify hyper errors into fetcherror variants
    fn classify_hyper_error(error: hyper::Error) -> FetchError {
        // catch timeout even if handled earlier
        if error.is_timeout() {
            return FetchError::Timeout;
        }

        let error_msg = error.to_string().to_lowercase();

        // connection refused indicates server is blocking
        if error_msg.contains("connection refused") {
            return FetchError::ConnectionRefused;
        }

        // dns resolution failures
        if error_msg.contains("dns")
            || error_msg.contains("name resolution")
            || error_msg.contains("no such host")
        {
            return FetchError::DnsError;
        }

        // tls or certificate errors
        if error_msg.contains("ssl")
            || error_msg.contains("tls")
            || error_msg.contains("certificate")
        {
            return FetchError::SslError;
        }

        // fallback network error
        FetchError::NetworkError(error.to_string())
    }
}

/// result of a successful http fetch
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub content: String,
    pub status_code: u16,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub response_time_ms: u64,
}

/// errors returned during http fetching
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
    /// tell if error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            // retryable transient network issues
            FetchError::Timeout => true,
            FetchError::NetworkError(msg) => {
                // some network errors are retryable
                let msg_lower = msg.to_lowercase();
                msg_lower.contains("timeout")
                    || msg_lower.contains("broken pipe")
                    || msg_lower.contains("connection reset")
                    || msg_lower.contains("temporary")
            }
            // not retryable permanent failures
            FetchError::ConnectionRefused => false, // server is down or blocking us
            FetchError::DnsError => false,          // dns will not recover automatically
            FetchError::SslError => false,          // certificate issues persist
            FetchError::BodyError(_) => false,      // body parsing issues are permanent
            FetchError::ContentTooLarge(_, _) => false, // content size won't change
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

        assert!(result.is_err()); // any error is acceptable for an invalid url
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new("TestBot/1.0".to_string(), 30);
        // verify the client builds without panic
        assert_eq!(client.user_agent, "TestBot/1.0");
    }
}
