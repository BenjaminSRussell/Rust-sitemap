//! Trait implemented by URL seeders so different seed strategies plug into the crawler.

use futures_util::stream::Stream;
use std::pin::Pin;
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

impl SeederError {
    /// Returns true if the error is retryable per HTTP semantics (5xx or network).
    pub fn retryable(&self) -> bool {
        match self {
            SeederError::Network(_) => true,
            SeederError::Http(code) => (500..=599).contains(code),
            SeederError::Data(_) | SeederError::Io(_) => false,
        }
    }
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

/// Box-aliased stream of individual URLs so seeders can yield results one at a time without buffering.
pub type UrlStream = Pin<Box<dyn Stream<Item = Result<String, SeederError>> + Send>>;

pub trait Seeder: Send + Sync {
    /// Discover seed URLs for the domain so the crawler starts with relevant entry points.
    /// Returns a stream to enable unbounded result sets without OOM risk.
    fn seed(&self, domain: &str) -> UrlStream;

    /// Human-readable seeder name so logs identify which seeder produced new URLs.
    fn name(&self) -> &'static str;
}
