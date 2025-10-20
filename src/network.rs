use std::time::Duration;
use tokio::time::timeout;
use reqwest;

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
        Self::with_content_limit(user_agent, timeout_secs, 5 * 1024 * 1024) // 5MB default for better performance
    }
    
    /// Create a new HTTP client with custom content size limit
    pub fn with_content_limit(user_agent: String, timeout_secs: u64, max_content_size: usize) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(4) // Optimize for 4 workers
            .pool_idle_timeout(Duration::from_secs(30))
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
    pub async fn fetch(&self, url: &str) -> Result<FetchResult, FetchError> {
        let response = timeout(
            self.timeout_duration,
            self.client.get(url)
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.5")
                .header("Connection", "keep-alive")
                .header("Upgrade-Insecure-Requests", "1")
                .send()
        )
        .await
        .map_err(|_| FetchError::Timeout)?
        .map_err(|e| FetchError::NetworkError(e.to_string()))?;

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

        let content = timeout(
            self.timeout_duration,
            response.text()
        )
        .await
        .map_err(|_| FetchError::Timeout)?
        .map_err(|e| FetchError::BodyError(e.to_string()))?;
        
        // Check actual content size
        if content.len() > self.max_content_size {
            return Err(FetchError::ContentTooLarge(content.len(), self.max_content_size));
        }

        Ok(FetchResult {
            content,
            status_code,
            content_type,
        })
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
    
    #[error("Timeout error")]
    Timeout,
    
    #[error("Body error: {0}")]
    BodyError(String),
    
    
    #[error("Content too large: {0} bytes (max: {1} bytes)")]
    ContentTooLarge(usize, usize),
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