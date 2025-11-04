//! Trait implemented by URL seeders so different seed strategies plug into the crawler.

use futures_util::stream::Stream;
use std::error::Error;
use std::pin::Pin;

/// Box-aliased stream of individual URLs so seeders can yield results one at a time without buffering.
pub type UrlStream = Pin<Box<dyn Stream<Item = Result<String, Box<dyn Error + Send + Sync>>> + Send>>;

pub trait Seeder: Send + Sync {
    /// Discover seed URLs for the domain so the crawler starts with relevant entry points.
    /// Returns a stream to enable unbounded result sets without OOM risk.
    fn seed(&self, domain: &str) -> UrlStream;

    /// Human-readable seeder name so logs identify which seeder produced new URLs.
    fn name(&self) -> &'static str;
}
