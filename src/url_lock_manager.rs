use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisError};

#[derive(Clone)]
pub struct UrlLockManager {
    client: ConnectionManager,
    lock_ttl: u64,
}

impl UrlLockManager {
    pub async fn new(redis_url: &str, lock_ttl: Option<u64>) -> Result<Self, RedisError> {
        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        Ok(Self {
            client: connection_manager,
            lock_ttl: lock_ttl.unwrap_or(60),
        })
    }

    pub async fn try_acquire_url(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        let result: bool = redis::cmd("SET")
            .arg(&key)
            .arg("locked")
            .arg("NX")
            .arg("EX")
            .arg(self.lock_ttl)
            .query_async(&mut self.client)
            .await?;

        Ok(result)
    }

    pub async fn release_url(&mut self, url: &str) -> Result<(), RedisError> {
        let key = format!("crawl:lock:{}", url);
        let _: () = self.client.del(&key).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_lock_acquire_and_release() {
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(5)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test";

        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        let acquired_again = manager.try_acquire_url(test_url).await.unwrap();
        assert!(!acquired_again);

        manager.release_url(test_url).await.unwrap();

        let acquired_after_release = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_release);

        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        let manager = match UrlLockManager::new("redis://127.0.0.1:6379", Some(2)).await {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test-expiry";

        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let acquired_after_expiry = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_expiry);

        manager.release_url(test_url).await.unwrap();
    }
}
