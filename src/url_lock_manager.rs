use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisError};

/// Global URL Lock Manager using Redis for distributed coordination
/// Prevents multiple workers from fetching the same URL simultaneously
#[derive(Clone)]
pub struct UrlLockManager {
    client: ConnectionManager,
    lock_ttl: u64, // Time-to-live in seconds
}

impl UrlLockManager {
    /// Create a new UrlLockManager
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., "redis://127.0.0.1:6379")
    /// * `lock_ttl` - Lock time-to-live in seconds (default: 60)
    pub async fn new(redis_url: &str, lock_ttl: Option<u64>) -> Result<Self, RedisError> {
        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        Ok(Self {
            client: connection_manager,
            lock_ttl: lock_ttl.unwrap_or(60),
        })
    }

    /// Try to acquire a lock for a URL
    ///
    /// Returns:
    /// - `Ok(true)` if lock was acquired
    /// - `Ok(false)` if lock already exists (another worker is processing)
    /// - `Err(e)` if Redis error occurred
    pub async fn try_acquire_url(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        // Use SET NX (set if not exists) with expiry
        // This is atomic and returns true only if the key was set
        let result: bool = redis::cmd("SET")
            .arg(&key)
            .arg("locked")
            .arg("NX") // Only set if not exists
            .arg("EX") // Set expiry time
            .arg(self.lock_ttl)
            .query_async(&mut self.client)
            .await?;

        if result {
            // Log lock acquisition for debugging
            // println!("Acquired lock for: {}", url);
        }

        Ok(result)
    }

    /// Release a lock for a URL
    ///
    /// This should be called after processing the URL (success or failure)
    pub async fn release_url(&mut self, url: &str) -> Result<(), RedisError> {
        let key = format!("crawl:lock:{}", url);

        // Delete the key
        let _: () = self.client.del(&key).await?;

        // println!("Released lock for: {}", url);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_lock_acquire_and_release() {
        // Skip test if Redis is not available
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(5)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test";

        // First acquisition should succeed
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        // Second acquisition should fail (already locked)
        let acquired_again = manager.try_acquire_url(test_url).await.unwrap();
        assert!(!acquired_again);

        // Release lock
        manager.release_url(test_url).await.unwrap();

        // Should be able to acquire again
        let acquired_after_release = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_release);

        // Cleanup
        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        // Skip test if Redis is not available
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(2)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test-expiry";

        // Acquire lock
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        // Wait for expiry
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Should be able to acquire again after expiry
        let acquired_after_expiry = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_expiry);

        // Cleanup
        manager.release_url(test_url).await.unwrap();
    }
}
