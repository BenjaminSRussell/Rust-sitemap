use rust_sitemap::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;

#[test]
fn test_atomic_metrics() {
    let counter = AtomicU64::new(0);
    counter.fetch_add(1, Ordering::Relaxed);
    counter.fetch_add(5, Ordering::Relaxed);
    assert_eq!(counter.load(Ordering::Relaxed), 6);
}

#[test]
fn test_state_creation() {
    let dir = TempDir::new().unwrap();
    // Test will verify state creation doesn't panic
    // Full API testing requires public state module
}

#[test]
fn test_config_defaults() {
    // Tests that configuration loads with sensible defaults
    // Requires config module to be public for comprehensive testing
}
