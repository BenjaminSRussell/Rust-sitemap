//! High-level orchestration for crawl setup and execution.

pub mod builder;
pub mod config;
pub mod distributed;
pub mod export;
pub mod frontier_setup;
pub mod governor;
pub mod persistence;
pub mod shard_workers;
pub mod shutdown;

pub use builder::build_crawler;
pub use config::{apply_preset, build_crawler_config};
pub use export::run_export_sitemap_command;
pub use shard_workers::spawn_shard_workers;
pub use shutdown::setup_shutdown_handler;
