use dashmap::DashMap;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use hyper_tls::HttpsConnector;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Result of fetching a URL with timing information
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub url: String,
    pub status_code: u16,
    pub content: String,
    pub content_type: Option<String>,
    pub response_time: Duration,
}

/// Smart connection pool with per-domain concurrency limits
pub struct SmartConnectionPool {
    client: Client<HttpsConnector<HttpConnector>, Empty<Bytes>>,
    per_domain_limits: DashMap<String, Arc<Semaphore>>,
    user_agent: String,
    timeout: Duration,
    max_content_size: usize,
}

impl SmartConnectionPool {
    /// Create a new smart connection pool
    /// 
    /// # Arguments
    /// * `user_agent` - User agent string
    /// * `timeout_secs` - Request timeout in seconds
    /// * `max_content_size` - Maximum content size in bytes (default: 10MB)
    pub fn new(user_agent: String, timeout_secs: u64, max_content_size: Option<usize>) -> Self {
        // Configure HTTP connector
        let mut http_connector = HttpConnector::new();
        http_connector.set_connect_timeout(Some(Duration::from_secs(10)));
        http_connector.set_keepalive(Some(Duration::from_secs(30)));
        http_connector.enforce_http(false);
        
        // Configure HTTPS connector
        let https_connector = HttpsConnector::new_with_connector(http_connector);
        
        // Build hyper client with connection pooling
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(8)
            .build(https_connector);
        
        Self {
            client,
            per_domain_limits: DashMap::new(),
            user_agent,
            timeout: Duration::from_secs(timeout_secs),
            max_content_size: max_content_size.unwrap_or(10 * 1024 * 1024),
        }
    }

    /// Get or create a semaphore for a domain
    fn get_domain_semaphore(&self, domain: &str, optimal_concurrency: usize) -> Arc<Semaphore> {
        // Check if we already have a semaphore for this domain
        if let Some(sem) = self.per_domain_limits.get(domain) {
            return sem.clone();
        }
        
        // Create new semaphore with optimal concurrency
        let sem = Arc::new(Semaphore::new(optimal_concurrency));
        self.per_domain_limits.insert(domain.to_string(), sem.clone());
        sem
    }

    /// Acquire a connection permit for a domain
    /// This waits until a permit is available, with timeout
    /// Returns the Arc<Semaphore> that must be held for the duration of the request
    pub async fn acquire_connection(
        &self,
        domain: &str,
        optimal_concurrency: usize,
    ) -> Result<Arc<Semaphore>, String> {
        let sem = self.get_domain_semaphore(domain, optimal_concurrency);
        
        // Try to acquire with timeout to prevent deadlock
        // We need to keep the Arc alive, so we'll just check availability
        match tokio::time::timeout(Duration::from_secs(30), sem.clone().acquire_owned()).await {
            Ok(Ok(_permit)) => {
                // Permit acquired successfully, drop it and return the semaphore
                // The caller will acquire their own permit
                Ok(sem)
            }
            Ok(Err(e)) => Err(format!("Semaphore acquire error: {}", e)),
            Err(_) => Err("Timeout waiting for connection permit".to_string()),
        }
    }

    /// Fetch a URL with timing information
    /// 
    /// # Arguments
    /// * `url` - The URL to fetch
    /// * `domain` - The domain (for semaphore lookup)
    /// * `optimal_concurrency` - Optimal concurrency level for this domain
    pub async fn fetch_with_timing(
        &self,
        url: &str,
        domain: &str,
        optimal_concurrency: usize,
    ) -> Result<FetchResult, String> {
        // Acquire domain permit (per-domain rate limiting)
        let sem = self.acquire_connection(domain, optimal_concurrency).await?;
        let _permit = sem.acquire_owned().await
            .map_err(|e| format!("Failed to acquire permit: {}", e))?;
        
        // Start timing
        let start = Instant::now();
        
        // Build request
        let uri = url.parse::<hyper::Uri>()
            .map_err(|e| format!("Invalid URL: {}", e))?;
        
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .header("User-Agent", &self.user_agent)
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Connection", "keep-alive")
            .body(Empty::<Bytes>::new())
            .map_err(|e| format!("Failed to build request: {}", e))?;
        
        // Execute request with timeout
        let response = match tokio::time::timeout(self.timeout, self.client.request(request)).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(format!("Network error: {}", e)),
            Err(_) => return Err("Request timeout".to_string()),
        };
        
        // Extract status code
        let status_code = response.status().as_u16();
        
        // Extract content type
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        
        // Read response body
        let body = response.into_body();
        
        // Collect body with size limit
        let body_bytes = match tokio::time::timeout(
            Duration::from_secs(30),
            body.collect()
        ).await {
            Ok(Ok(collected)) => collected.to_bytes(),
            Ok(Err(e)) => return Err(format!("Failed to read body: {}", e)),
            Err(_) => return Err("Timeout reading response body".to_string()),
        };
        
        // Check content size
        if body_bytes.len() > self.max_content_size {
            return Err(format!(
                "Content too large: {} bytes (max: {})",
                body_bytes.len(),
                self.max_content_size
            ));
        }
        
        // Convert to string
        let content = String::from_utf8_lossy(&body_bytes).to_string();
        
        // Calculate response time
        let response_time = start.elapsed();
        
        Ok(FetchResult {
            url: url.to_string(),
            status_code,
            content,
            content_type,
            response_time,
        })
    }

    /// Update concurrency limit for a domain
    pub fn update_domain_concurrency(&self, domain: &str, new_concurrency: usize) {
        // Remove old semaphore
        self.per_domain_limits.remove(domain);
        
        // Create new semaphore with updated concurrency
        let sem = Arc::new(Semaphore::new(new_concurrency));
        self.per_domain_limits.insert(domain.to_string(), sem);
    }

    /// Get current concurrency limit for a domain
    pub fn get_domain_concurrency(&self, domain: &str) -> Option<usize> {
        self.per_domain_limits
            .get(domain)
            .map(|sem| sem.available_permits())
    }

    /// Get statistics about the connection pool
    pub fn get_stats(&self) -> PoolStats {
        PoolStats {
            tracked_domains: self.per_domain_limits.len(),
        }
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub tracked_domains: usize,
}

impl std::fmt::Display for PoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pool: {} domains tracked", self.tracked_domains)
    }
}

/// Extract domain from URL
pub fn extract_domain(url: &str) -> Result<String, String> {
    let parsed = url::Url::parse(url)
        .map_err(|e| format!("Failed to parse URL: {}", e))?;
    
    parsed
        .host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| "No host in URL".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_domain() {
        assert_eq!(
            extract_domain("https://example.com/path").unwrap(),
            "example.com"
        );
        
        assert_eq!(
            extract_domain("http://sub.example.com/path?query=1").unwrap(),
            "sub.example.com"
        );
        
        assert!(extract_domain("not-a-url").is_err());
    }

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let pool = SmartConnectionPool::new(
            "TestBot/1.0".to_string(),
            30,
            Some(5 * 1024 * 1024),
        );
        
        let stats = pool.get_stats();
        assert_eq!(stats.tracked_domains, 0);
    }

    #[tokio::test]
    async fn test_domain_semaphore() {
        let pool = SmartConnectionPool::new(
            "TestBot/1.0".to_string(),
            30,
            Some(5 * 1024 * 1024),
        );
        
        // Get semaphore for domain
        let sem1 = pool.get_domain_semaphore("example.com", 5);
        assert_eq!(sem1.available_permits(), 5);
        
        // Should return same semaphore
        let sem2 = pool.get_domain_semaphore("example.com", 10);
        assert_eq!(sem2.available_permits(), 5); // Still 5, not updated
    }

    #[tokio::test]
    async fn test_update_domain_concurrency() {
        let pool = SmartConnectionPool::new(
            "TestBot/1.0".to_string(),
            30,
            Some(5 * 1024 * 1024),
        );
        
        // Create initial semaphore
        let _sem1 = pool.get_domain_semaphore("example.com", 5);
        
        // Update concurrency
        pool.update_domain_concurrency("example.com", 10);
        
        // Get updated semaphore
        let sem2 = pool.get_domain_semaphore("example.com", 10);
        assert_eq!(sem2.available_permits(), 10);
    }
}

