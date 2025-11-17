use redis::aio::ConnectionManager;
use redis::{Client, RedisError, Script};
use std::sync::Arc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

/// RAII guard for a crawl lock with automatic renewal so distributed crawlers do not double-process URLs.
pub struct CrawlLock {
    url: String,
    manager: Arc<tokio::sync::Mutex<UrlLockManager>>,
    renewal_task: Option<tokio::task::JoinHandle<()>>,
}

impl CrawlLock {
    // [Zencoder Task Doc]
    // WHAT: Acquires an exclusive distributed lock on a URL with automatic renewal, or returns None if already locked.
    // USED_BY: src/bfs_crawler.rs (crawl_single_url), src/url_lock_manager.rs (test functions)

    /// Attempt to acquire a lock on a URL so a single worker owns it at a time.
    /// If renewal fails, trigger the provided cancellation token to signal lock loss.
    ///
    /// # Cancellation Pattern
    /// Callers should wait on `cancellation_token.cancelled().await` to detect lock loss
    /// in their work loops and abort gracefully.
    pub async fn acquire(
        manager: Arc<tokio::sync::Mutex<UrlLockManager>>,
        url: String,
        cancellation_token: CancellationToken,
    ) -> Result<Option<Self>, RedisError> {
        let acquired = {
            let mut mgr = manager.lock().await;
            mgr.try_acquire_url(&url).await?
        };

        if !acquired {
            return Ok(None);
        }

        // Spawn a renewal task that runs every thirty seconds (half the TTL) so the lock never expires mid-crawl.
        let renewal_manager = Arc::clone(&manager);
        let renewal_url = url.clone();
        let cancel_token = cancellation_token.clone();
        let renewal_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Acquire the lock guard, perform the renewal, then drop the guard
                // BEFORE awaiting cancellation or sleeping again to avoid holding
                // the sync mutex across await points.
                let renewal_result = {
                    let mut mgr = renewal_manager.lock().await;
                    mgr.renew_lock(&renewal_url).await
                };

                match renewal_result {
                    Ok(true) => {
                        // Renewal succeeded; keep the loop alive.
                    }
                    Ok(false) => {
                        eprintln!(
                            "[lock-lost] Lock renewal failed for {} (lost ownership)",
                            renewal_url
                        );
                        cancel_token.cancel();
                        break;
                    }
                    Err(e) => {
                        eprintln!("[lock-lost] Lock renewal error for {}: {}", renewal_url, e);
                        cancel_token.cancel();
                        break;
                    }
                }
            }
        });

        Ok(Some(Self {
            url,
            manager,
            renewal_task: Some(renewal_task),
        }))
    }
}

impl Drop for CrawlLock {
    fn drop(&mut self) {
        // Abort the renewal task so no background future keeps renewing a dropped lock.
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }

        // Release the lock in a spawned task so Drop remains non-blocking in synchronous contexts.
        let manager = Arc::clone(&self.manager);
        let url = self.url.clone();
        tokio::spawn(async move {
            let mut mgr = manager.lock().await;
            if let Err(e) = mgr.release_url(&url).await {
                eprintln!("Failed to release lock for {}: {}", url, e);
            }
        });
    }
}

#[derive(Clone)]
pub struct UrlLockManager {
    client: ConnectionManager,
    lock_ttl: u64,
    instance_id: String, // Owner token for safe locks so renew/release can verify ownership.
}

impl UrlLockManager {
    pub async fn new(
        redis_url: &str,
        lock_ttl: Option<u64>,
        instance_id: String,
    ) -> Result<Self, RedisError> {
        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        Ok(Self {
            client: connection_manager,
            lock_ttl: lock_ttl.unwrap_or(60),
            instance_id,
        })
    }

    /// Try to acquire a lock on a URL.
    /// Returns true if the lock was acquired, false if another instance already owns it.
    pub async fn try_acquire_url(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        // SET key owner_token NX EX ttl.
        // NX sets only when the key does not exist.
        // EX applies the expiry in seconds.
        let result: bool = redis::cmd("SET")
            .arg(&key)
            .arg(&self.instance_id) // Store the owner token instead of a placeholder.
            .arg("NX")
            .arg("EX")
            .arg(self.lock_ttl)
            .query_async(&mut self.client)
            .await?;

        Ok(result)
    }

    /// Release a lock on a URL when we own it.
    /// Returns true if the lock was released, false if we did not own it.
    pub async fn release_url(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        // Compare-and-delete Lua script.
        // Delete only when the stored value matches our instance ID.
        let script = Script::new(
            r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            ",
        );

        let result: i32 = script
            .key(&key)
            .arg(&self.instance_id)
            .invoke_async(&mut self.client)
            .await?;

        Ok(result == 1)
    }

    /// Renew a lock on a URL by extending the TTL when we own it.
    /// Returns true if the lock was renewed, false if we did not own it.
    pub async fn renew_lock(&mut self, url: &str) -> Result<bool, RedisError> {
        let key = format!("crawl:lock:{}", url);

        // Compare-and-extend Lua script.
        // Extend the TTL only when the stored value matches our instance ID.
        let script = Script::new(
            r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
            ",
        );

        let result: i32 = script
            .key(&key)
            .arg(&self.instance_id)
            .arg(self.lock_ttl)
            .invoke_async(&mut self.client)
            .await?;

        Ok(result == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[tokio::test]
    async fn test_lock_acquire_and_release() {
        let instance1 = "test-instance-1";
        let manager =
            match UrlLockManager::new("redis://127.0.0.1:6379", Some(5), instance1.to_string())
                .await
            {
                Ok(m) => m,
                Err(_) => {
                    println!("Redis not available, skipping test");
                    return;
                }
            };

        let mut manager = manager;
        let test_url = "https://example.com/test-safe-locks";

        // Acquire the lock to prove the happy path succeeds.
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired, "Should acquire lock on first attempt");

        // Attempt a second acquisition to ensure exclusivity is enforced.
        let acquired_again = manager.try_acquire_url(test_url).await.unwrap();
        assert!(!acquired_again, "Should not acquire lock twice");

        // Release the lock to verify the owner can free it.
        let released = manager.release_url(test_url).await.unwrap();
        assert!(released, "Should successfully release lock");

        // Acquire the lock again after release to confirm it becomes available.
        let acquired_after_release = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_release, "Should acquire lock after release");

        // Clean up so the test leaves Redis in a known state.
        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_ownership() {
        let instance1 = "test-instance-1";
        let instance2 = "test-instance-2";

        let manager1 =
            match UrlLockManager::new("redis://127.0.0.1:6379", Some(5), instance1.to_string())
                .await
            {
                Ok(m) => m,
                Err(_) => {
                    println!("Redis not available, skipping test");
                    return;
                }
            };

        let manager2 =
            match UrlLockManager::new("redis://127.0.0.1:6379", Some(5), instance2.to_string())
                .await
            {
                Ok(m) => m,
                Err(_) => {
                    println!("Redis not available, skipping test");
                    return;
                }
            };

        let mut manager1 = manager1;
        let mut manager2 = manager2;
        let test_url = "https://example.com/test-ownership";

        // Instance 1 acquires the lock to establish ownership.
        let acquired1 = manager1.try_acquire_url(test_url).await.unwrap();
        assert!(acquired1, "Instance 1 should acquire lock");

        // Instance 2 attempts acquisition to assert instance-level exclusivity.
        let acquired2 = manager2.try_acquire_url(test_url).await.unwrap();
        assert!(
            !acquired2,
            "Instance 2 should not acquire lock held by instance 1"
        );

        // Instance 2 tries to release instance 1's lock to confirm ownership guarding works.
        let released2 = manager2.release_url(test_url).await.unwrap();
        assert!(
            !released2,
            "Instance 2 should not release instance 1's lock"
        );

        // Instance 1 releases its own lock to show the rightful owner can free it.
        let released1 = manager1.release_url(test_url).await.unwrap();
        assert!(released1, "Instance 1 should release its own lock");

        // Now instance 2 acquires the lock to demonstrate the baton passes correctly.
        let acquired2_after = manager2.try_acquire_url(test_url).await.unwrap();
        assert!(
            acquired2_after,
            "Instance 2 should acquire after instance 1 released"
        );

        // Clean up so the test ends with no lingering locks.
        manager2.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        let instance = "test-instance-expiry";
        let manager = match UrlLockManager::new(
            "redis://127.0.0.1:6379",
            Some(2),
            instance.to_string(),
        )
        .await
        {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test-expiry";

        // Acquire the lock with a two-second TTL to test expiry behavior.
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired, "Should acquire lock");

        // Wait for expiry so the TTL lapses naturally.
        tokio::time::sleep(StdDuration::from_secs(3)).await;

        // Acquire again after expiry to confirm the lock releases automatically.
        let acquired_after_expiry = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired_after_expiry, "Should acquire after expiry");

        // Clean up to remove any leftover keys.
        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_renewal() {
        let instance = "test-instance-renewal";
        let manager = match UrlLockManager::new(
            "redis://127.0.0.1:6379",
            Some(3),
            instance.to_string(),
        )
        .await
        {
            Ok(m) => m,
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let mut manager = manager;
        let test_url = "https://example.com/test-renewal";

        // Acquire the lock with a three-second TTL to test renewal.
        let acquired = manager.try_acquire_url(test_url).await.unwrap();
        assert!(acquired, "Should acquire lock");

        // Wait two seconds so we renew before the TTL lapses.
        tokio::time::sleep(StdDuration::from_secs(2)).await;

        // Renew the lock to extend ownership.
        let renewed = manager.renew_lock(test_url).await.unwrap();
        assert!(renewed, "Should renew lock");

        // Wait another two seconds to confirm the renewal took effect.
        tokio::time::sleep(StdDuration::from_secs(2)).await;

        // Confirm the lock remains held so renewal logic is validated.
        let acquired_again = manager.try_acquire_url(test_url).await.unwrap();
        assert!(!acquired_again, "Lock should still be held after renewal");

        // Clean up to release the test key.
        manager.release_url(test_url).await.unwrap();
    }

    #[tokio::test]
    async fn test_crawl_lock_guard() {
        let instance = "test-instance-guard";
        let manager = match UrlLockManager::new(
            "redis://127.0.0.1:6379",
            Some(5),
            instance.to_string(),
        )
        .await
        {
            Ok(m) => Arc::new(tokio::sync::Mutex::new(m)),
            Err(_) => {
                println!("Redis not available, skipping test");
                return;
            }
        };

        let test_url = "https://example.com/test-guard";

        // Acquire the lock via the guard to exercise the RAII helper.
        let cancel_token1 = CancellationToken::new();
        let lock_guard = CrawlLock::acquire(
            Arc::clone(&manager),
            test_url.to_string(),
            cancel_token1,
        )
        .await
        .unwrap();
        assert!(lock_guard.is_some(), "Should acquire lock");

        // Try to acquire the same URL to ensure the guard prevents reentrancy.
        let cancel_token2 = CancellationToken::new();
        let second_guard = CrawlLock::acquire(
            Arc::clone(&manager),
            test_url.to_string(),
            cancel_token2,
        )
        .await
        .unwrap();
        assert!(second_guard.is_none(), "Should not acquire locked URL");

        // Drop the first guard so Drop releases the Redis lock.
        drop(lock_guard);

        // Sleep briefly so the async release has time to run.
        tokio::time::sleep(StdDuration::from_millis(100)).await;

        // Attempt to acquire again to prove the lock becomes available post-drop.
        let cancel_token3 = CancellationToken::new();
        let third_guard = CrawlLock::acquire(
            Arc::clone(&manager),
            test_url.to_string(),
            cancel_token3,
        )
        .await
        .unwrap();
        assert!(third_guard.is_some(), "Should acquire after guard dropped");

        // Clean up so the guard test leaves Redis untouched.
        drop(third_guard);
    }
}
