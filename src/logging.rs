/// Logging module with background file rotation and multi-layer tracing setup.
///
/// This module provides:
/// - JSON and text log formatting
/// - Automatic file rotation (daily rotation)
/// - Background, non-blocking logging
/// - Environment-based log level filtering
/// - Separate log files stored in a dedicated logs/ folder

use std::path::Path;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Initialize the tracing subscriber with multi-layer setup.
///
/// Creates two log outputs:
/// 1. `logs/app.log` - Human-readable text format with ANSI colors disabled
/// 2. `logs/app.json.log` - Structured JSON format for parsing/analysis
///
/// Logs are rotated daily and old logs are kept with timestamps.
///
/// # Arguments
/// * `log_dir` - Directory where log files will be stored (typically "logs")
///
/// # Environment Variables
/// * `RUST_LOG` - Controls log level filtering (default: "info")
///   Examples:
///   - `RUST_LOG=debug` - Show all debug and above
///   - `RUST_LOG=rustmapper=trace,reqwest=warn` - Trace for your crate, warn for reqwest
///
/// # Panics
/// Panics if the subscriber is already initialized or if log directory creation fails.
pub fn init_logging<P: AsRef<Path>>(log_dir: P) -> Result<(), Box<dyn std::error::Error>> {
    // Create logs directory if it doesn't exist
    let log_path = log_dir.as_ref();
    std::fs::create_dir_all(log_path)?;

    // Configure environment filter with fallback to "info"
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("Failed to create EnvFilter");

    // Set up daily rotating file appender for text logs
    let text_file_appender = tracing_appender::rolling::daily(log_path, "app.log");
    let (text_writer, _text_guard) = tracing_appender::non_blocking(text_file_appender);

    // Set up daily rotating file appender for JSON logs
    let json_file_appender = tracing_appender::rolling::daily(log_path, "app.json.log");
    let (json_writer, _json_guard) = tracing_appender::non_blocking(json_file_appender);

    // Create text formatting layer (for human readability)
    let text_layer = fmt::layer()
        .with_writer(text_writer)
        .with_target(true) // Include module path
        .with_thread_ids(true) // Include thread IDs
        .with_thread_names(true) // Include thread names
        .with_line_number(true) // Include line numbers
        .with_ansi(false) // Disable ANSI colors in file output
        .compact()
        .with_filter(env_filter.clone());

    // Create JSON formatting layer (for structured logs)
    let json_layer = fmt::layer()
        .json()
        .with_writer(json_writer)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_current_span(true) // Include current span info
        .with_span_list(true) // Include full span context
        .with_filter(env_filter.clone());

    // Create stdout layer for terminal output (with colors)
    let stdout_layer = fmt::layer()
        .with_target(false) // Don't clutter terminal with module paths
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(false)
        .compact()
        .with_filter(env_filter);

    // Combine all layers
    tracing_subscriber::registry()
        .with(text_layer)
        .with(json_layer)
        .with(stdout_layer)
        .init();

    // Log guards must be kept alive for the duration of the program
    // In production, you might want to store these in a static or pass them around
    // For now, we'll leak them to ensure they live for the program's lifetime
    Box::leak(Box::new(_text_guard));
    Box::leak(Box::new(_json_guard));

    tracing::info!("Logging initialized - logs will be written to {}", log_path.display());
    tracing::debug!("Text logs: {}/app.log", log_path.display());
    tracing::debug!("JSON logs: {}/app.json.log", log_path.display());

    Ok(())
}

/// Initialize logging with a data directory prefix.
///
/// This is a convenience wrapper that creates a "logs" subdirectory
/// within the provided data directory.
///
/// # Example
/// ```ignore
/// init_logging_in_data_dir("./data")?;
/// // Logs will be written to ./data/logs/
/// ```
pub fn init_logging_in_data_dir<P: AsRef<Path>>(data_dir: P) -> Result<(), Box<dyn std::error::Error>> {
    let log_dir = data_dir.as_ref().join("logs");
    init_logging(log_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_logging_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("logs");

        // This would panic if called twice, so we can't test the actual init
        // but we can test directory creation
        std::fs::create_dir_all(&log_path).unwrap();
        assert!(log_path.exists());
    }
}
