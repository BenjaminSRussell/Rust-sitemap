use crate::network::{FetchError, HttpClient};
use crate::seeder::Seeder;
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashSet;

/// Certificate Transparency log entry from crt.sh
#[derive(Debug, Deserialize)]
struct CtLogEntry {
    name_value: String,
}

/// Seeds URLs by querying Certificate Transparency logs for subdomains
pub struct CtLogSeeder {
    http: HttpClient,
}

impl CtLogSeeder {
    /// Create a CT log seeder backed by the shared client
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    /// Fetch subdomains for the domain from crt.sh
    pub async fn seed(&self, domain: &str) -> Result<Vec<String>, FetchError> {
        let url = format!("https://crt.sh/?q=%.{}&output=json", domain);

        eprintln!("Querying CT logs for domain: {}", domain);

        let result = self.http.fetch(&url).await?;

        if result.status_code != 200 {
            return Err(FetchError::NetworkError(format!(
                "CT log query failed with status code: {}",
                result.status_code
            )));
        }

        // Parse response JSON
        let entries: Vec<CtLogEntry> = serde_json::from_str(&result.content).map_err(|e| {
            FetchError::BodyError(format!("Failed to parse CT log JSON: {}", e))
        })?;

        // Use HashSet for deduped subdomains
        let mut subdomains = HashSet::new();

        for entry in entries {
            // name_value can contain newline-separated names
            for line in entry.name_value.lines() {
                let subdomain = line.trim();

                // Skip wildcard entries
                if subdomain.starts_with('*') {
                    continue;
                }

                // Skip empty entries
                if subdomain.is_empty() {
                    continue;
                }

                // Normalize case
                let subdomain_lower = subdomain.to_lowercase();

                // Keep only entries for the target domain
                if subdomain_lower.ends_with(domain) || subdomain_lower == domain {
                    subdomains.insert(subdomain_lower);
                }
            }
        }

        let subdomain_vec: Vec<String> = subdomains.into_iter().collect();
        eprintln!("Found {} unique subdomains from CT logs", subdomain_vec.len());

        Ok(subdomain_vec)
    }
}

#[async_trait]
impl Seeder for CtLogSeeder {
    async fn seed(&self, domain: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        // Convert subdomains to full URLs
        let subdomains = self.seed(domain).await?;
        let urls = subdomains
            .into_iter()
            .map(|subdomain| format!("https://{}/", subdomain))
            .collect();
        Ok(urls)
    }

    fn name(&self) -> &'static str {
        "ct-logs"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ct_seeder_creation() {
        let http = HttpClient::new("TestBot/1.0".to_string(), 30);
        let _seeder = CtLogSeeder::new(http);
        // Ensure constructor works
        assert!(true);
    }
}
