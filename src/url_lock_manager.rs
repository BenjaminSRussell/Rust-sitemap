use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisError};

/// redis backed url lock manager for distributed coordination
/// avoids multiple workers fetching the same url at once
#[derive(Clone)]
pub struct UrlLockManager {
    client: ConnectionManager,
    lock_ttl: u64, // time-to-live in seconds
}

impl UrlLockManager {
    /// create a new url lock manager
    ///
    /// # arguments
    /// * `redis_url` - redis connection url (e.g., "redis://127.0.0.1:6379")
    /// * `lock_ttl` - lock time-to-live in seconds (default: 60)
    pub async fn new(redis_url: &str, lock_ttl: Option<u64>) -> Result<Self, RedisError> {
        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        Ok(Self {
            client: connection_manager,
            lock_ttl: lock_ttl.unwrap_or(60),
        })
    }

    /// try to acquire a lock for a url
    ///
    /// returns:
    /// - true if the lock was acquired
    /// - false if the url was already locked
    /// - redis errors bubble out unchanged
    pub async fn try_acquire_url(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        // use set nx (set if not exists) with expiry
        // this is atomic and returns true only if the key was set
        let result: bool = redis::cmd("SET")
            .arg(&key)
            .arg("locked")
            .arg("NX") // only set if not exists
            .arg("EX") // set expiry time
            .arg(self.lock_ttl)
            .query_async(&mut self.client)
            .await?;

        if result {
            // log lock acquisition for debugging
            // println!("acquired lock for: {}", url);
        }

        Ok(result)
    }

    /// release a lock for a url
    ///
    /// this should be called after processing the url
    pub async fn release_url(&mut self, url: &str) -> Result<(), RedisError> {
        let key = format!("crawl:lock:{}", url);

        // delete the key
        let _: () = self.client.del(&key).await?;

        // println!("released lock for: {}", url);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_lock_acquire_and_release() {
        // skip test if redis is not available
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(5)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test";

        // first acquisition should succeed
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        // second acquisition should fail because it is already locked
        let acquired_again = manager.try_acquire_url(test_url).await.unwrap();
        assert!(!acquired_again);

        // release lock
        manager.release_url(test_url).await.unwrap();

        // should be able to acquire again
        let acquired_after_release = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_release);

        // cleanup
        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        // skip test if redis is not available
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(2)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test-expiry";

        // acquire lock
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        // wait for expiry
        tokio::time::sleep(Duration::from_secs(3)).await;

        // should be able to acquire again after expiry
        let acquired_after_expiry = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_expiry);

        // cleanup
        manager.release_url(test_url).await.unwrap();
    }
}
