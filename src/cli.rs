use clap::{Parser, Subcommand};

/// web crawler and sitemap reorientation tool
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
    /// crawl target site and collect sitemap data
    Crawl {
        /// starting url for the first request
        #[arg(short, long, help = "The starting URL to begin crawling from")]
        start_url: String,

        /// directory where crawl data is stored
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory to store crawled data"
        )]
        data_dir: String,

        /// worker count for concurrent crawl
        #[arg(
            short,
            long,
            default_value = "256",
            help = "HIGH CONCURRENCY: 256 workers process URLs non-blocking (adjust based on CPU cores)"
        )]
        workers: usize,

        /// request rate per second cap
        #[arg(
            short,
            long,
            default_value = "200",
            help = "Rate limit in requests per second (200+ for high concurrency with async tasks)"
        )]
        rate_limit: u64,

        /// export crawl data as jsonl
        #[arg(long, help = "Export results in JSONL format")]
        export_jsonl: bool,

        /// max crawl depth; 0 means unlimited
        #[arg(
            short,
            long,
            default_value = "0",
            help = "Maximum depth to crawl (0 = unlimited)"
        )]
        max_depth: u32,

        /// user agent applied to every request
        #[arg(
            short,
            long,
            default_value = "RustSitemapCrawler/1.0",
            help = "User agent string for requests"
        )]
        user_agent: String,

        /// per request timeout in seconds
        #[arg(
            short,
            long,
            default_value = "45",
            help = "Timeout in seconds for each request (45s allows slow pages to load in background)"
        )]
        timeout: u64,

        /// ignore robots.txt when set
        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,
    },

    /// build sitemap from stored crawl data
    OrientMap {
        /// directory containing crawled data
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory containing crawled data"
        )]
        data_dir: String,

        /// starting url used for the original crawl
        #[arg(short, long, help = "Starting URL used for the original crawl")]
        start_url: String,

        /// output file for the reoriented sitemap
        #[arg(
            short,
            long,
            default_value = "./sitemap.jsonl",
            help = "Output file for the reoriented sitemap"
        )]
        output: String,

        /// include last modification timestamps
        #[arg(long, help = "Include last modification times in sitemap")]
        include_lastmod: bool,

        /// include change frequency values
        #[arg(long, help = "Include change frequencies in sitemap")]
        include_changefreq: bool,

        /// default priority when none is provided
        #[arg(
            long,
            default_value = "0.5",
            help = "Default priority for pages without explicit priority"
        )]
        default_priority: f32,
    },
}

impl Cli {
    /// parse command line arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
