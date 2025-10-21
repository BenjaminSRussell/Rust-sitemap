use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use atomic_float::AtomicF32;

/// Performance profile for a single domain
/// Tracks response times, success rates, and optimal concurrency
#[derive(Debug)]
pub struct DomainProfile {
    avg_response_time: AtomicU64,    // Average response time in milliseconds
    success_rate: AtomicF32,          // Success rate (0.0 to 1.0)
    optimal_concurrency: AtomicUsize, // Optimal number of concurrent requests
    total_requests: AtomicUsize,      // Total number of requests made
}

impl DomainProfile {
    /// Create a new domain profile with default values
    pub fn new() -> Self {
        Self {
            avg_response_time: AtomicU64::new(1000), // Start with 1000ms average
            success_rate: AtomicF32::new(1.0),        // Start optimistic
            optimal_concurrency: AtomicUsize::new(5), // Start with moderate concurrency
            total_requests: AtomicUsize::new(0),
        }
    }

    /// Record a request and update statistics
    /// 
    /// # Arguments
    /// * `response_time_ms` - Response time in milliseconds
    /// * `success` - Whether the request was successful
    pub fn record_request(&self, response_time_ms: u64, success: bool) {
        // Update total requests
        let n = self.total_requests.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Update rolling average response time: new_avg = (old_avg * (n-1) + new_time) / n
        let old_avg = self.avg_response_time.load(Ordering::Relaxed);
        let new_avg = if n == 1 {
            response_time_ms
        } else {
            (old_avg * (n as u64 - 1) + response_time_ms) / n as u64
        };
        self.avg_response_time.store(new_avg, Ordering::Relaxed);
        
        // Update success rate using exponential moving average (EMA)
        // new_rate = old_rate * 0.9 + success * 0.1
        let old_rate = self.success_rate.load(Ordering::Relaxed);
        let success_value = if success { 1.0 } else { 0.0 };
        let new_rate = old_rate * 0.9 + success_value * 0.1;
        self.success_rate.store(new_rate, Ordering::Relaxed);
        
        // Adjust optimal concurrency based on performance
        let current_concurrency = self.optimal_concurrency.load(Ordering::Relaxed);
        
        // Fast and reliable → increase concurrency
        if new_avg < 500 && new_rate > 0.95 {
            let new_concurrency = current_concurrency.saturating_add(1).min(10);
            if new_concurrency != current_concurrency {
                self.optimal_concurrency.store(new_concurrency, Ordering::Relaxed);
            }
        }
        // Slow or unreliable → decrease concurrency
        else if new_avg > 3000 || new_rate < 0.70 {
            let new_concurrency = current_concurrency.saturating_sub(1).max(2);
            if new_concurrency != current_concurrency {
                self.optimal_concurrency.store(new_concurrency, Ordering::Relaxed);
            }
        }
    }

    /// Get average response time in milliseconds
    pub fn get_avg_response_time(&self) -> u64 {
        self.avg_response_time.load(Ordering::Relaxed)
    }

    /// Get success rate (0.0 to 1.0)
    pub fn get_success_rate(&self) -> f32 {
        self.success_rate.load(Ordering::Relaxed)
    }

    /// Get optimal concurrency level
    pub fn get_optimal_concurrency(&self) -> usize {
        self.optimal_concurrency.load(Ordering::Relaxed)
    }

    /// Get total number of requests
    pub fn get_total_requests(&self) -> usize {
        self.total_requests.load(Ordering::Relaxed)
    }
}

impl Default for DomainProfile {
    fn default() -> Self {
        Self::new()
    }
}

/// Domain performance profiler
/// Tracks performance metrics across multiple domains
pub struct DomainProfiler {
    profiles: DashMap<String, Arc<DomainProfile>>,
    default_concurrency: usize,
}

impl DomainProfiler {
    /// Create a new domain profiler
    /// 
    /// # Arguments
    /// * `default_concurrency` - Default concurrency level for unknown domains (default: 5)
    pub fn new(default_concurrency: Option<usize>) -> Self {
        Self {
            profiles: DashMap::new(),
            default_concurrency: default_concurrency.unwrap_or(5),
        }
    }

    /// Get or create a profile for a domain
    fn get_profile(&self, domain: &str) -> Arc<DomainProfile> {
        self.profiles
            .entry(domain.to_string())
            .or_insert_with(|| Arc::new(DomainProfile::new()))
            .clone()
    }

    /// Record a request for a domain
    /// 
    /// # Arguments
    /// * `domain` - The domain name
    /// * `response_time_ms` - Response time in milliseconds
    /// * `success` - Whether the request was successful
    pub fn record_request(&self, domain: &str, response_time_ms: u64, success: bool) {
        let profile = self.get_profile(domain);
        profile.record_request(response_time_ms, success);
    }

    /// Get optimal concurrency for a domain
    pub fn get_optimal_concurrency(&self, domain: &str) -> usize {
        if let Some(profile) = self.profiles.get(domain) {
            profile.get_optimal_concurrency()
        } else {
            self.default_concurrency
        }
    }

    /// Get average response time for a domain
    pub fn get_avg_response_time(&self, domain: &str) -> Option<u64> {
        self.profiles.get(domain).map(|p| p.get_avg_response_time())
    }

    /// Get success rate for a domain
    pub fn get_success_rate(&self, domain: &str) -> Option<f32> {
        self.profiles.get(domain).map(|p| p.get_success_rate())
    }

    /// Get statistics for a domain
    pub fn get_stats(&self, domain: &str) -> Option<DomainStats> {
        self.profiles.get(domain).map(|p| DomainStats {
            domain: domain.to_string(),
            avg_response_time_ms: p.get_avg_response_time(),
            success_rate: p.get_success_rate(),
            optimal_concurrency: p.get_optimal_concurrency(),
            total_requests: p.get_total_requests(),
        })
    }

    /// Get statistics for all domains
    pub fn get_all_stats(&self) -> Vec<DomainStats> {
        self.profiles
            .iter()
            .map(|entry| {
                let domain = entry.key().clone();
                let profile = entry.value();
                DomainStats {
                    domain,
                    avg_response_time_ms: profile.get_avg_response_time(),
                    success_rate: profile.get_success_rate(),
                    optimal_concurrency: profile.get_optimal_concurrency(),
                    total_requests: profile.get_total_requests(),
                }
            })
            .collect()
    }

    /// Print statistics for all domains
    pub fn print_stats(&self) {
        let mut stats = self.get_all_stats();
        stats.sort_by(|a, b| b.total_requests.cmp(&a.total_requests));
        
        println!("\n📊 Domain Performance Statistics:");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("{:<40} {:>8} {:>10} {:>12} {:>10}", 
                 "Domain", "Requests", "Avg Time", "Success", "Concurr");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        for stat in stats.iter().take(20) { // Show top 20
            println!("{:<40} {:>8} {:>9}ms {:>11.1}% {:>10}",
                     truncate_domain(&stat.domain, 40),
                     stat.total_requests,
                     stat.avg_response_time_ms,
                     stat.success_rate * 100.0,
                     stat.optimal_concurrency);
        }
        
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    /// Clear all profiles
    pub fn clear(&self) {
        self.profiles.clear();
    }

    /// Get number of tracked domains
    pub fn domain_count(&self) -> usize {
        self.profiles.len()
    }
}

/// Domain statistics snapshot
#[derive(Debug, Clone)]
pub struct DomainStats {
    pub domain: String,
    pub avg_response_time_ms: u64,
    pub success_rate: f32,
    pub optimal_concurrency: usize,
    pub total_requests: usize,
}

impl std::fmt::Display for DomainStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Domain {}: avg {}ms, success {:.1}%, optimal concurrency {}",
            self.domain,
            self.avg_response_time_ms,
            self.success_rate * 100.0,
            self.optimal_concurrency
        )
    }
}

/// Truncate a domain name to fit within a specified length
fn truncate_domain(domain: &str, max_len: usize) -> String {
    if domain.len() <= max_len {
        domain.to_string()
    } else {
        format!("...{}", &domain[domain.len() - (max_len - 3)..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_profile_initialization() {
        let profile = DomainProfile::new();
        
        assert_eq!(profile.get_avg_response_time(), 1000);
        assert_eq!(profile.get_success_rate(), 1.0);
        assert_eq!(profile.get_optimal_concurrency(), 5);
        assert_eq!(profile.get_total_requests(), 0);
    }

    #[test]
    fn test_domain_profile_fast_success() {
        let profile = DomainProfile::new();
        
        // Record several fast successful requests
        for _ in 0..20 {
            profile.record_request(100, true);
        }
        
        // Response time should be low
        assert!(profile.get_avg_response_time() < 500);
        
        // Success rate should be high
        assert!(profile.get_success_rate() > 0.95);
        
        // Concurrency should increase for fast, reliable domains
        assert!(profile.get_optimal_concurrency() >= 5);
    }

    #[test]
    fn test_domain_profile_slow_failures() {
        let profile = DomainProfile::new();
        
        // Record several slow failed requests
        for _ in 0..20 {
            profile.record_request(4000, false);
        }
        
        // Response time should be high
        assert!(profile.get_avg_response_time() > 3000);
        
        // Success rate should be low
        assert!(profile.get_success_rate() < 0.5);
        
        // Concurrency should decrease for slow, unreliable domains
        assert!(profile.get_optimal_concurrency() <= 2);
    }

    #[test]
    fn test_domain_profiler() {
        let profiler = DomainProfiler::new(Some(5));
        
        // Record requests for multiple domains
        profiler.record_request("fast.com", 100, true);
        profiler.record_request("fast.com", 150, true);
        profiler.record_request("slow.com", 3500, false);
        profiler.record_request("slow.com", 4000, false);
        
        // Fast domain should have higher concurrency
        let fast_concurrency = profiler.get_optimal_concurrency("fast.com");
        let slow_concurrency = profiler.get_optimal_concurrency("slow.com");
        
        assert!(fast_concurrency >= slow_concurrency);
        
        // Unknown domain should use default
        assert_eq!(profiler.get_optimal_concurrency("unknown.com"), 5);
    }

    #[test]
    fn test_exponential_moving_average() {
        let profile = DomainProfile::new();
        
        // First failure
        profile.record_request(1000, false);
        let rate1 = profile.get_success_rate();
        
        // EMA: 1.0 * 0.9 + 0.0 * 0.1 = 0.9
        assert!((rate1 - 0.9).abs() < 0.01);
        
        // Second failure
        profile.record_request(1000, false);
        let rate2 = profile.get_success_rate();
        
        // EMA: 0.9 * 0.9 + 0.0 * 0.1 = 0.81
        assert!((rate2 - 0.81).abs() < 0.01);
    }

    #[test]
    fn test_rolling_average_response_time() {
        let profile = DomainProfile::new();
        
        profile.record_request(100, true);
        assert_eq!(profile.get_avg_response_time(), 100);
        
        profile.record_request(200, true);
        assert_eq!(profile.get_avg_response_time(), 150); // (100 + 200) / 2
        
        profile.record_request(300, true);
        assert_eq!(profile.get_avg_response_time(), 200); // (100 + 200 + 300) / 3
    }
}

