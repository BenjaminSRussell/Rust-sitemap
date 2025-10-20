use clap::{Parser, Subcommand};

/// A web crawler and sitemap reorientation tool
#[derive(Parser)]
#[command(name = "rust_sitemap")]
#[command(about = "A web crawler and sitemap reorientation tool")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Crawl websites and extract sitemap data
    Crawl {
        /// Starting URL to begin crawling from
        #[arg(short, long, help = "The starting URL to begin crawling from")]
        start_url: String,
        
        /// Directory to store crawled data
        #[arg(short, long, default_value = "./data", help = "Directory to store crawled data")]
        data_dir: String,
        
        /// Number of concurrent workers for crawling
        #[arg(short, long, default_value = "4", help = "Number of concurrent workers for crawling")]
        workers: usize,
        
        /// Rate limit in requests per second
        #[arg(short, long, default_value = "2", help = "Rate limit in requests per second")]
        rate_limit: u64,
        
        /// Export results in JSONL format
        #[arg(long, help = "Export results in JSONL format")]
        export_jsonl: bool,
        
        /// Maximum depth to crawl (0 = unlimited)
        #[arg(short, long, default_value = "0", help = "Maximum depth to crawl (0 = unlimited)")]
        max_depth: u32,
        
        /// User agent string for requests
        #[arg(short, long, default_value = "RustSitemapCrawler/1.0", help = "User agent string for requests")]
        user_agent: String,
        
        /// Timeout in seconds for each request
        #[arg(short, long, default_value = "15", help = "Timeout in seconds for each request")]
        timeout: u64,
        
        /// Disable robots.txt compliance (default: respect robots.txt)
        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,
    },
    
    /// Reorient and map crawled data
    OrientMap {
        /// Directory containing crawled data
        #[arg(short, long, default_value = "./data", help = "Directory containing crawled data")]
        data_dir: String,
        
        /// Starting URL used for the original crawl
        #[arg(short, long, help = "Starting URL used for the original crawl")]
        start_url: String,
        
        /// Output file for the reoriented sitemap
        #[arg(short, long, default_value = "./sitemap.jsonl", help = "Output file for the reoriented sitemap")]
        output: String,
        
        /// Include last modification times in sitemap
        #[arg(long, help = "Include last modification times in sitemap")]
        include_lastmod: bool,
        
        /// Include change frequencies in sitemap
        #[arg(long, help = "Include change frequencies in sitemap")]
        include_changefreq: bool,
        
        /// Default priority for pages without explicit priority
        #[arg(long, default_value = "0.5", help = "Default priority for pages without explicit priority")]
        default_priority: f32,
    },
}

impl Cli {
    /// Parse command line arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
