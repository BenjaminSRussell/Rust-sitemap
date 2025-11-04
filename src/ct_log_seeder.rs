use crate::network::{FetchError, HttpClient};
use crate::seeder::{Seeder, UrlStream};
use async_stream::stream;
use serde::Deserialize;
use std::collections::HashSet;

/// Certificate Transparency log entry from crt.sh so we can deserialize individual results.
#[derive(Debug, Deserialize)]
struct CtLogEntry {
    name_value: String,
}

/// Seed URLs by querying Certificate Transparency logs for subdomains, which surfaces hosts public certificates already reference.
pub struct CtLogSeeder {
    http: HttpClient,
}

impl CtLogSeeder {
    /// Create a CT log seeder backed by the shared client so we reuse the crawler's HTTP pool.
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    /// Fetch subdomains for the domain from crt.sh so the crawler starts with certificate-observed hosts.
    pub async fn seed(&self, domain: &str) -> Result<Vec<String>, FetchError> {
        let url = format!("https://crt.sh/?q=%.{}&output=json", domain);

        eprintln!("Querying CT logs for domain: {}", domain);

        // Retry with exponential backoff for 503 errors
        let max_retries = 3;
        let mut retry_count = 0;
        let result = loop {
            match self.http.fetch(&url).await {
                Ok(r) if r.status_code == 200 => break r,
                Ok(r) if r.status_code == 503 && retry_count < max_retries => {
                    retry_count += 1;
                    let backoff_ms = 1000 * (2_u64.pow(retry_count - 1));
                    eprintln!(
                        "CT log query returned 503, retrying in {}ms (attempt {}/{})",
                        backoff_ms, retry_count, max_retries
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Ok(r) => {
                    return Err(FetchError::NetworkError(format!(
                        "CT log query failed with status code: {}",
                        r.status_code
                    )));
                }
                Err(e) => return Err(e),
            }
        };

        // Parse the response JSON so we can iterate over each name_value block.
        let entries: Vec<CtLogEntry> = serde_json::from_str(&result.content).map_err(|e| {
            FetchError::BodyError(format!("Failed to parse CT log JSON: {}", e))
        })?;

        // Track subdomains in a HashSet so duplicate entries collapse before returning.
        let mut subdomains = HashSet::new();

        for entry in entries {
            // Handle newline-separated names because crt.sh may return multiple hostnames per record.
            for line in entry.name_value.lines() {
                let subdomain = line.trim();

                // Skip wildcard entries because they do not map to concrete hosts.
                if subdomain.starts_with('*') {
                    continue;
                }

                // Skip empty entries to avoid returning blank URLs.
                if subdomain.is_empty() {
                    continue;
                }

                // Normalize case so duplicate entries differing only in case deduplicate.
                let subdomain_lower = subdomain.to_lowercase();

                // Only keep hostnames within the requested domain so we do not crawl strangers.
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

impl Seeder for CtLogSeeder {
    fn seed(&self, domain: &str) -> UrlStream {
        let http = self.http.clone();
        let domain = domain.to_string();

        Box::pin(stream! {
            let url = format!("https://crt.sh/?q=%.{}&output=json", domain);

            eprintln!("Querying CT logs for domain: {}", domain);

            // Retry with exponential backoff for 503 errors
            let max_retries = 3;
            let mut retry_count = 0;
            let result = loop {
                match http.fetch(&url).await {
                    Ok(r) if r.status_code == 200 => break r,
                    Ok(r) if r.status_code == 503 && retry_count < max_retries => {
                        retry_count += 1;
                        let backoff_ms = 1000 * (2_u64.pow(retry_count - 1));
                        eprintln!(
                            "CT log query returned 503, retrying in {}ms (attempt {}/{})",
                            backoff_ms, retry_count, max_retries
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    Ok(r) => {
                        yield Err(format!("CT log query failed with status: {}", r.status_code).into());
                        return;
                    }
                    Err(e) => {
                        yield Err(format!("Network error: {}", e).into());
                        return;
                    }
                }
            };

            // Parse the response JSON so we can iterate over each name_value block.
            let entries: Vec<CtLogEntry> = match serde_json::from_str(&result.content) {
                Ok(e) => e,
                Err(e) => {
                    yield Err(format!("Failed to parse CT log JSON: {}", e).into());
                    return;
                }
            };

            // Track subdomains in a HashSet so duplicate entries collapse before returning.
            let mut subdomains = HashSet::new();

            for entry in entries {
                // Handle newline-separated names because crt.sh may return multiple hostnames per record.
                for line in entry.name_value.lines() {
                    let subdomain = line.trim();

                    // Skip wildcard entries because they do not map to concrete hosts.
                    if subdomain.starts_with('*') {
                        continue;
                    }

                    // Skip empty entries to avoid returning blank URLs.
                    if subdomain.is_empty() {
                        continue;
                    }

                    // Normalize case so duplicate entries differing only in case deduplicate.
                    let subdomain_lower = subdomain.to_lowercase();

                    // Only keep hostnames within the requested domain so we do not crawl strangers.
                    if subdomain_lower.ends_with(&domain) || subdomain_lower == domain {
                        subdomains.insert(subdomain_lower);
                    }
                }
            }

            eprintln!("Found {} unique subdomains from CT logs", subdomains.len());

            // Stream each subdomain as a full HTTPS URL
            for subdomain in subdomains {
                yield Ok(format!("https://{}/", subdomain));
            }
        })
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
        // Smoke-test the constructor so regressions surface quickly.
        assert!(true);
    }
}
