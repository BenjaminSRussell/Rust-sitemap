pub mod cli;
pub mod models;
pub mod parser;
pub mod export;
pub mod network;
pub mod rkyv_queue;
pub mod node_map;
pub mod bfs_crawler;
pub mod robots;

// Re-export main types for library usage
pub use bfs_crawler::{BfsCrawlerState, BfsCrawlerConfig, BfsCrawlerResult};
pub use node_map::{NodeMap, SitemapNode, NodeMapStats};
pub use rkyv_queue::{RkyvQueue, QueuedUrl, QueueStats};
pub use models::PageNode;
pub use network::{HttpClient, FetchError, FetchResult};
pub use parser::extract_links;
pub use robots::RobotsTxt;
