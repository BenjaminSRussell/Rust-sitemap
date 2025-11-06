use crate::network::{FetchError, HttpClient};
use crate::seeder::{Seeder, UrlStream};
use async_stream::stream;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::net::ToSocketAddrs;
use std::time::Duration;

// Pagination & rate-limit sanity bounds to prevent unbounded resource consumption.
const MAX_CT_ENTRIES: usize = 50_000;
const MAX_RETRIES: u32 = 3;

// DNS validation configuration for parallel subdomain verification.
const DNS_CHUNK_SIZE: usize = 100;
const DNS_TIMEOUT_SECS: u64 = 5;

/// Typed error for CT log seeding operations with retryability classification.
#[derive(Debug)]
pub enum SeederError {
    /// HTTP error with status code; 429 and 5xx are retryable.
    Http(u16),
    /// Network-level error (DNS, timeout, connection failure); always retryable.
    Network(String),
    /// Data validation or parse error; never retryable.
    Data(String),
}

impl SeederError {
    /// Returns true if this error is transient and the operation should be retried.
    pub fn retryable(&self) -> bool {
        match self {
            // RFC 9110 §15.5.30: 429 Too Many Requests is retryable with backoff.
            SeederError::Http(429) => true,
            // 5xx server errors are transient; retry.
            SeederError::Http(status) => (500..600).contains(status),
            // Network failures are transient; retry.
            SeederError::Network(_) => true,
            // Data errors are permanent; do not retry.
            SeederError::Data(_) => false,
        }
    }
}

impl fmt::Display for SeederError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeederError::Http(code) => write!(f, "HTTP error: {}", code),
            SeederError::Network(msg) => write!(f, "Network error: {}", msg),
            SeederError::Data(msg) => write!(f, "Data error: {}", msg),
        }
    }
}

impl std::error::Error for SeederError {}

impl From<FetchError> for SeederError {
    fn from(e: FetchError) -> Self {
        match e {
            FetchError::NetworkError(msg) => SeederError::Network(msg),
            FetchError::BodyError(msg) => SeederError::Data(msg),
            _ => SeederError::Network(e.to_string()),
        }
    }
}

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

    /// Check if a hostname looks like an internal/infrastructure host that likely won't have a public web server.
    /// This helps filter out CT log noise (VPN hosts, network equipment, internal services).
    fn is_likely_internal_host(hostname: &str) -> bool {
        let lower = hostname.to_lowercase();

        // Skip entries with port numbers (e.g., "host:8080")
        if lower.contains(':') {
            return true;
        }

        // Skip entries that look like IPv4 addresses
        if lower.split('.').all(|part| part.parse::<u8>().is_ok()) && lower.split('.').count() == 4 {
            return true;
        }

        // Expanded infrastructure and development patterns
        let infrastructure_patterns = [
            "vpn", "rtr-", "router", "switch", "firewall", "gateway",
            "dc-", "dns-", "dhcp-", "proxy-", "internal", "localhost",
            "banweb", "myphone", "printer", "backup", "monitoring",
            "mail", "smtp", "imap", "pop", "mx", "email",
            "jenkins", "ci-", "build", "gitlab-runner",
            "-dev", "-staging", "-test", "-uat", "-qa",
            "dev.", "staging.", "test.", "uat.", "qa.",
            "dev-", "staging-", "test-", "uat-", "qa-",
        ];

        for pattern in &infrastructure_patterns {
            if lower.contains(pattern) {
                return true;
            }
        }

        false
    }

    /// Validate a subdomain by attempting DNS resolution with timeout.
    async fn dns_validate(subdomain: &str) -> bool {
        let addr = format!("{}:443", subdomain);

        // Use tokio::task::spawn_blocking since ToSocketAddrs is synchronous
        let result = tokio::time::timeout(
            Duration::from_secs(DNS_TIMEOUT_SECS),
            tokio::task::spawn_blocking({
                let addr = addr.clone();
                move || addr.to_socket_addrs()
            })
        ).await;

        match result {
            Ok(Ok(Ok(mut addrs))) => addrs.next().is_some(),
            _ => false,
        }
    }
}

impl Seeder for CtLogSeeder {
    fn seed(&self, domain: &str) -> UrlStream {
        let http = self.http.clone();
        let domain = domain.to_string();

        Box::pin(stream! {
            // Calculate date 90 days ago for filtering recent certificates
            let now = std::time::SystemTime::now();
            let ninety_days_ago = now - std::time::Duration::from_secs(90 * 24 * 60 * 60);
            let since_epoch = ninety_days_ago
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let days_since_epoch = since_epoch.as_secs() / (24 * 60 * 60);

            // Convert to YYYY-MM-DD format (approximate)
            let year = 1970 + (days_since_epoch / 365);
            let remaining_days = days_since_epoch % 365;
            let month = (remaining_days / 30).min(11) + 1;
            let day = (remaining_days % 30).max(1);
            let after_date = format!("{:04}-{:02}-{:02}", year, month, day);

            let url = format!(
                "https://crt.sh/?q=%.{}&output=json&exclude=expired&after={}",
                domain, after_date
            );

            eprintln!("Querying CT logs for domain: {} (after: {}, excluding expired)", domain, after_date);

            // Retry with exponential backoff for retryable errors (5xx, 429).
            let mut retry_count = 0;
            let result = loop {
                match http.fetch(&url).await {
                    Ok(r) if r.status_code == 200 => break r,
                    Ok(r) => {
                        let err = SeederError::Http(r.status_code);
                        if err.retryable() && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = 1000 * (2_u64.pow(retry_count - 1));
                            eprintln!(
                                "CT log query returned {}, retrying in {}ms (attempt {}/{})",
                                r.status_code, backoff_ms, retry_count, MAX_RETRIES
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                            continue;
                        } else {
                            yield Err(err.into());
                            return;
                        }
                    }
                    Err(e) => {
                        let err = SeederError::from(e);
                        if err.retryable() && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = 1000 * (2_u64.pow(retry_count - 1));
                            eprintln!(
                                "CT log query network error, retrying in {}ms (attempt {}/{})",
                                backoff_ms, retry_count, MAX_RETRIES
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                            continue;
                        } else {
                            yield Err(err.into());
                            return;
                        }
                    }
                }
            };

            // Parse the response JSON so we can iterate over each name_value block.
            let entries: Vec<CtLogEntry> = match serde_json::from_str(&result.content) {
                Ok(e) => e,
                Err(e) => {
                    yield Err(SeederError::Data(format!("Failed to parse CT log JSON: {}", e)).into());
                    return;
                }
            };

            // Enforce pagination sanity bounds to prevent unbounded memory consumption.
            if entries.len() > MAX_CT_ENTRIES {
                yield Err(SeederError::Data(format!(
                    "CT log returned {} entries, exceeding limit of {}",
                    entries.len(),
                    MAX_CT_ENTRIES
                )).into());
                return;
            }

            // Track subdomains in a HashSet so duplicate entries collapse before returning.
            let mut subdomains = HashSet::new();
            let mut filtered_count = 0;

            for entry in entries {
                // Validate that name_value exists and is non-empty to catch malformed JSON.
                if entry.name_value.is_empty() {
                    continue;
                }

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
                        // Filter out likely internal/infrastructure hosts
                        if Self::is_likely_internal_host(&subdomain_lower) {
                            filtered_count += 1;
                            continue;
                        }
                        subdomains.insert(subdomain_lower);
                    }
                }
            }

            eprintln!(
                "Found {} unique subdomains from CT logs (filtered {} likely internal hosts)",
                subdomains.len(), filtered_count
            );

            // DNS validation: verify subdomains resolve in parallel chunks
            let subdomains_vec: Vec<String> = subdomains.into_iter().collect();
            let mut validated_subdomains = Vec::new();
            let mut dns_failed_count = 0;

            eprintln!("Starting DNS validation for {} subdomains...", subdomains_vec.len());

            // Process subdomains in chunks for parallel DNS validation
            for chunk in subdomains_vec.chunks(DNS_CHUNK_SIZE) {
                let mut handles = Vec::new();

                for subdomain in chunk {
                    let subdomain_clone = subdomain.clone();
                    let handle = tokio::spawn(async move {
                        let valid = Self::dns_validate(&subdomain_clone).await;
                        (subdomain_clone, valid)
                    });
                    handles.push(handle);
                }

                // Collect results from this chunk
                for handle in handles {
                    if let Ok((subdomain, valid)) = handle.await {
                        if valid {
                            validated_subdomains.push(subdomain);
                        } else {
                            dns_failed_count += 1;
                        }
                    }
                }
            }

            eprintln!(
                "DNS validation complete: {} valid, {} failed to resolve",
                validated_subdomains.len(), dns_failed_count
            );

            // Stream each validated subdomain as a full HTTPS URL
            for subdomain in validated_subdomains {
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

    #[test]
    fn test_seeder_error_retryability() {
        // 429 is retryable per RFC 9110 §15.5.30
        assert!(SeederError::Http(429).retryable());

        // 5xx errors are retryable
        assert!(SeederError::Http(500).retryable());
        assert!(SeederError::Http(503).retryable());
        assert!(SeederError::Http(599).retryable());

        // 4xx errors (except 429) are not retryable
        assert!(!SeederError::Http(400).retryable());
        assert!(!SeederError::Http(404).retryable());
        assert!(!SeederError::Http(499).retryable());

        // Network errors are retryable
        assert!(SeederError::Network("timeout".to_string()).retryable());

        // Data errors are not retryable
        assert!(!SeederError::Data("parse failure".to_string()).retryable());
    }

    #[test]
    fn test_ct_log_entry_valid_json() {
        // Valid JSON with name_value field
        let json = r#"{"name_value":"example.com"}"#;
        let entry: Result<CtLogEntry, _> = serde_json::from_str(json);
        assert!(entry.is_ok());
        assert_eq!(entry.unwrap().name_value, "example.com");
    }

    #[test]
    fn test_ct_log_entry_missing_field() {
        // Missing name_value field should fail to deserialize
        let json = r#"{"other_field":"value"}"#;
        let entry: Result<CtLogEntry, _> = serde_json::from_str(json);
        assert!(entry.is_err());
    }

    #[test]
    fn test_ct_log_entry_array() {
        // Array of entries should parse correctly
        let json = r#"[
            {"name_value":"example.com"},
            {"name_value":"test.example.com"}
        ]"#;
        let entries: Result<Vec<CtLogEntry>, _> = serde_json::from_str(json);
        assert!(entries.is_ok());
        let entries = entries.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name_value, "example.com");
        assert_eq!(entries[1].name_value, "test.example.com");
    }

    #[test]
    fn test_ct_log_entry_empty_name_value() {
        // Empty name_value should parse but be skipped in processing
        let json = r#"{"name_value":""}"#;
        let entry: Result<CtLogEntry, _> = serde_json::from_str(json);
        assert!(entry.is_ok());
        assert_eq!(entry.unwrap().name_value, "");
    }

    #[test]
    fn test_pagination_bounds() {
        // Ensure our constant is reasonable
        const _: () = assert!(MAX_CT_ENTRIES > 0);
        const _: () = assert!(MAX_CT_ENTRIES <= 100_000); // Sanity check
    }

    #[tokio::test]
    async fn test_ct_seeder_creation() {
        let http = HttpClient::new("TestBot/1.0".to_string(), 30)
            .expect("Failed to create HTTP client in test");
        let _seeder = CtLogSeeder::new(http);
        // Smoke-test the constructor so regressions surface quickly.
    }
}
