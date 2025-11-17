use rand::Rng;
use std::time::Duration;

pub struct ExponentialBackoff {
    base_ms: u64,
    max_ms: u64,
    jitter_percent: u64,
}

impl ExponentialBackoff {
    pub const fn new(base_ms: u64, max_ms: u64) -> Self {
        Self {
            base_ms,
            max_ms,
            jitter_percent: 10,
        }
    }

    pub fn with_jitter(mut self, jitter_percent: u64) -> Self {
        self.jitter_percent = jitter_percent;
        self
    }

    pub fn delay(&self, attempt: u32) -> Duration {
        let exponential_delay = self
            .base_ms
            .saturating_mul(2u64.saturating_pow(attempt.min(20)));
        let capped_delay = exponential_delay.min(self.max_ms);
        let jitter = if self.jitter_percent > 0 {
            rand::thread_rng().gen_range(0..capped_delay / self.jitter_percent + 1)
        } else {
            0
        };
        Duration::from_millis(capped_delay + jitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_growth() {
        let backoff = ExponentialBackoff::new(100, 10000).with_jitter(0);
        assert_eq!(backoff.delay(0).as_millis(), 100);
        assert_eq!(backoff.delay(1).as_millis(), 200);
        assert_eq!(backoff.delay(2).as_millis(), 400);
    }

    #[test]
    fn test_max_cap() {
        let backoff = ExponentialBackoff::new(100, 1000).with_jitter(0);
        assert!(backoff.delay(10).as_millis() <= 1000);
    }
}
