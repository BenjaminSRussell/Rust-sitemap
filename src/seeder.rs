//! Trait implemented by URL seeders so different seed strategies plug into the crawler.

use futures_util::StreamExt;
use futures_util::stream::Stream;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

/// Typed error for seeder operations (replaces Box<dyn Error>).
#[derive(Error, Debug)]
pub enum SeederError {
    #[error("network: {0}")]
    Network(String),

    #[error("http {0}")]
    Http(u16),

    #[error("data: {0}")]
    Data(String),

    #[error("io: {0}")]
    Io(String),
}

// Bridge from external error types
impl From<reqwest::Error> for SeederError {
    fn from(err: reqwest::Error) -> Self {
        if let Some(status) = err.status() {
            SeederError::Http(status.as_u16())
        } else if err.is_timeout() || err.is_connect() {
            SeederError::Network(err.to_string())
        } else {
            SeederError::Data(err.to_string())
        }
    }
}

impl From<std::io::Error> for SeederError {
    fn from(err: std::io::Error) -> Self {
        SeederError::Io(err.to_string())
    }
}

impl From<String> for SeederError {
    fn from(msg: String) -> Self {
        SeederError::Data(msg)
    }
}

impl From<&str> for SeederError {
    fn from(msg: &str) -> Self {
        SeederError::Data(msg.to_string())
    }
}

// Bridge from module-specific error types
impl From<crate::common_crawl_seeder::SeederError> for SeederError {
    fn from(err: crate::common_crawl_seeder::SeederError) -> Self {
        match err {
            crate::common_crawl_seeder::SeederError::Http(code, _msg) => {
                // For HTTP errors, we discard the detailed message in the generic error
                SeederError::Http(code)
            }
            crate::common_crawl_seeder::SeederError::Network(msg) => SeederError::Network(msg),
            crate::common_crawl_seeder::SeederError::Data(msg) => SeederError::Data(msg),
            crate::common_crawl_seeder::SeederError::Io(err) => SeederError::Io(err.to_string()),
        }
    }
}

impl From<crate::ct_log_seeder::SeederError> for SeederError {
    fn from(err: crate::ct_log_seeder::SeederError) -> Self {
        match err {
            crate::ct_log_seeder::SeederError::Http(code) => SeederError::Http(code),
            crate::ct_log_seeder::SeederError::Network(msg) => SeederError::Network(msg),
            crate::ct_log_seeder::SeederError::Data(msg) => SeederError::Data(msg),
        }
    }
}

/// Box-aliased stream of individual URLs so seeders can yield results one at a time without buffering.
pub type UrlStream = Pin<Box<dyn Stream<Item = Result<String, SeederError>> + Send>>;

pub trait Seeder: Send + Sync {
    /// Discover seed URLs for the domain so the crawler starts with relevant entry points.
    /// Returns a stream to enable unbounded result sets without OOM risk.
    fn seed(&self, domain: &str) -> UrlStream;

    /// Human-readable seeder name so logs identify which seeder produced new URLs.
    fn name(&self) -> &'static str;
}

/// Default wall-clock budget for a single seeder run (`--seeder-timeout`).
pub const DEFAULT_SEEDER_TIMEOUT_SECS: u64 = 120;

/// Per-seeder accounting so operators can see accepted vs rejected seed URLs (#42).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SeedOutcome {
    /// Syntactically valid http(s) URLs handed to the frontier.
    pub accepted: usize,
    /// URLs the seeder yielded that failed validation (bad scheme, no host, unparsable).
    pub rejected: usize,
    /// Errors the seeder yielded (HTTP 429/5xx after retries, parse failures, ...).
    pub errors: usize,
    /// True if the seeder was cut off by its time budget.
    pub timed_out: bool,
}

impl SeedOutcome {
    /// Fold another seeder's outcome into a running total.
    pub fn merge(&mut self, other: &SeedOutcome) {
        self.accepted += other.accepted;
        self.rejected += other.rejected;
        self.errors += other.errors;
        self.timed_out |= other.timed_out;
    }
}

/// One step of draining a seeder stream under a deadline.
#[derive(Debug)]
pub enum SeedPoll {
    Url(String),
    Rejected(String),
    Error(SeederError),
    Done,
    TimedOut,
}

/// Accept only absolute http(s) URLs with a host.
pub fn is_valid_seed_url(raw: &str) -> bool {
    url::Url::parse(raw)
        .map(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
        .unwrap_or(false)
}

/// Pull the next item from `stream`, giving up at `deadline` so a hung upstream
/// (crt.sh / Common Crawl stalls are common) cannot block crawl start forever.
pub async fn poll_seed(stream: &mut UrlStream, deadline: tokio::time::Instant) -> SeedPoll {
    match tokio::time::timeout_at(deadline, stream.next()).await {
        Err(_) => SeedPoll::TimedOut,
        Ok(None) => SeedPoll::Done,
        Ok(Some(Err(e))) => SeedPoll::Error(e),
        Ok(Some(Ok(url))) => {
            if is_valid_seed_url(&url) {
                SeedPoll::Url(url)
            } else {
                SeedPoll::Rejected(url)
            }
        }
    }
}

/// Drain a seeder stream within `budget`, returning accepted URLs and stats.
/// Used by tests and by callers that do not need incremental flushing.
pub async fn collect_with_budget(
    mut stream: UrlStream,
    budget: Duration,
) -> (Vec<String>, SeedOutcome) {
    let deadline = tokio::time::Instant::now() + budget;
    let mut urls = Vec::new();
    let mut outcome = SeedOutcome::default();
    loop {
        match poll_seed(&mut stream, deadline).await {
            SeedPoll::Url(u) => {
                outcome.accepted += 1;
                urls.push(u);
            }
            SeedPoll::Rejected(_) => outcome.rejected += 1,
            SeedPoll::Error(_) => outcome.errors += 1,
            SeedPoll::Done => break,
            SeedPoll::TimedOut => {
                outcome.timed_out = true;
                break;
            }
        }
    }
    (urls, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_url_validation() {
        assert!(is_valid_seed_url("https://example.com/"));
        assert!(is_valid_seed_url("http://a.example.com/x?y=1"));
        assert!(!is_valid_seed_url("ftp://example.com/"));
        assert!(!is_valid_seed_url("not a url"));
        assert!(!is_valid_seed_url("mailto:me@example.com"));
    }

    #[tokio::test]
    async fn budget_cuts_off_a_hung_stream() {
        let stream: UrlStream = Box::pin(async_stream::stream! {
            yield Ok("https://example.com/a".to_string());
            yield Ok("javascript:alert(1)".to_string());
            yield Err(SeederError::Http(503));
            tokio::time::sleep(Duration::from_secs(30)).await;
            yield Ok("https://example.com/never".to_string());
        });
        let started = std::time::Instant::now();
        let (urls, outcome) = collect_with_budget(stream, Duration::from_millis(150)).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(urls, vec!["https://example.com/a".to_string()]);
        assert_eq!(
            outcome,
            SeedOutcome {
                accepted: 1,
                rejected: 1,
                errors: 1,
                timed_out: true
            }
        );
    }
}
