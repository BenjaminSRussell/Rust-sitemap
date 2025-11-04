// Central configuration values so tuning lives in one place.

use std::fmt;

/// Memory allocator choice for runtime performance tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Allocator {
    None,
    Mimalloc,
    Jemalloc,
}

impl fmt::Display for Allocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Allocator::None => write!(f, "none"),
            Allocator::Mimalloc => write!(f, "mimalloc"),
            Allocator::Jemalloc => write!(f, "jemalloc"),
        }
    }
}

/// Configuration validation errors.
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigError {
    RobotsTtlTooHigh { got: u64, max: u64 },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::RobotsTtlTooHigh { got, max } => {
                write!(f, "robots TTL too high: {} hours (max {} hours)", got, max)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Runtime configuration for the sitemap crawler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub heartbeat: bool,
    pub allocator: Allocator,
    pub robots_ttl_hours: u64,
}

impl Config {
    // Crawl timing limits so periodic tasks stay coordinated across modules.
    pub const SAVE_INTERVAL_SECS: u64 = 300;

    // HTTP settings so the client adheres to shared resource constraints.
    pub const MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // Cap at 10MB to avoid buffering enormous responses.
    pub const POOL_IDLE_PER_HOST: usize = 16;
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 30;

    // Robots.txt caching limit (Google's guideline: 24h, we allow up to 48h).
    pub const MAX_ROBOTS_TTL_HOURS: u64 = 48;

    /// Creates a new configuration with the specified values.
    pub fn new(heartbeat: bool, allocator: Allocator, robots_ttl_hours: u64) -> Self {
        Self {
            heartbeat,
            allocator,
            robots_ttl_hours,
        }
    }

    /// Validates configuration values, returning errors for invalid settings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.robots_ttl_hours > Self::MAX_ROBOTS_TTL_HOURS {
            return Err(ConfigError::RobotsTtlTooHigh {
                got: self.robots_ttl_hours,
                max: Self::MAX_ROBOTS_TTL_HOURS,
            });
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat: false,
            allocator: Allocator::None,
            robots_ttl_hours: 24, // Follow Google's 24h caching recommendation.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_validates() {
        let config = Config::default();
        assert!(config.validate().is_ok());
        assert!(!config.heartbeat);
        assert_eq!(config.allocator, Allocator::None);
        assert_eq!(config.robots_ttl_hours, 24);
    }

    #[test]
    fn test_valid_ttl_at_boundary() {
        let config = Config::new(false, Allocator::None, 48);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_ttl_below_boundary() {
        let config = Config::new(false, Allocator::None, 24);
        assert!(config.validate().is_ok());

        let config = Config::new(false, Allocator::None, 1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_ttl_too_high_at_49() {
        let config = Config::new(false, Allocator::None, 49);
        assert_eq!(
            config.validate(),
            Err(ConfigError::RobotsTtlTooHigh { got: 49, max: 48 })
        );
    }

    #[test]
    fn test_ttl_too_high_large_value() {
        let config = Config::new(false, Allocator::None, 1000);
        assert!(matches!(
            config.validate(),
            Err(ConfigError::RobotsTtlTooHigh { got: 1000, max: 48 })
        ));
    }

    #[test]
    fn test_heartbeat_enabled() {
        let config = Config::new(true, Allocator::None, 24);
        assert!(config.heartbeat);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_heartbeat_disabled() {
        let config = Config::default();
        assert!(!config.heartbeat);
    }

    #[test]
    fn test_allocator_none() {
        let config = Config::new(false, Allocator::None, 24);
        assert_eq!(config.allocator, Allocator::None);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_allocator_mimalloc() {
        let config = Config::new(false, Allocator::Mimalloc, 24);
        assert_eq!(config.allocator, Allocator::Mimalloc);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_allocator_jemalloc() {
        let config = Config::new(false, Allocator::Jemalloc, 24);
        assert_eq!(config.allocator, Allocator::Jemalloc);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_allocator_display() {
        assert_eq!(Allocator::None.to_string(), "none");
        assert_eq!(Allocator::Mimalloc.to_string(), "mimalloc");
        assert_eq!(Allocator::Jemalloc.to_string(), "jemalloc");
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::RobotsTtlTooHigh { got: 100, max: 48 };
        assert_eq!(
            err.to_string(),
            "robots TTL too high: 100 hours (max 48 hours)"
        );
    }

    #[test]
    fn test_combined_valid_config() {
        let config = Config::new(true, Allocator::Mimalloc, 36);
        assert!(config.heartbeat);
        assert_eq!(config.allocator, Allocator::Mimalloc);
        assert_eq!(config.robots_ttl_hours, 36);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_combined_invalid_config() {
        let config = Config::new(true, Allocator::Jemalloc, 50);
        assert!(config.heartbeat);
        assert_eq!(config.allocator, Allocator::Jemalloc);
        assert!(config.validate().is_err());
    }
}
