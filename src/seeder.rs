//! Trait implemented by URL seeders

use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait Seeder: Send + Sync {
    /// Discover seed URLs for the domain
    async fn seed(&self, domain: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>>;

    /// Human-readable seeder name
    fn name(&self) -> &'static str;
}
