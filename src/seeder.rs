//! Trait implemented by URL seeders so different seed strategies plug into the crawler.

use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait Seeder: Send + Sync {
    /// Discover seed URLs for the domain so the crawler starts with relevant entry points.
    async fn seed(&self, domain: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>>;

    /// Human-readable seeder name so logs identify which seeder produced new URLs.
    fn name(&self) -> &'static str;
}
