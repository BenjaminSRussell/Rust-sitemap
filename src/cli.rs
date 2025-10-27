use clap::{Parser, Subcommand};

/// sitemap cli
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
    /// crawl command
    Crawl {
        /// crawl start url
        #[arg(short, long, help = "The starting URL to begin crawling from")]
        start_url: String,

        /// crawl data dir
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory to store crawled data"
        )]
        data_dir: String,

        /// worker count
        #[arg(
            short,
            long,
            default_value = "256",
            help = "HIGH CONCURRENCY: 256 workers process URLs non-blocking (adjust based on CPU cores)"
        )]
        workers: usize,

        /// rate limit
        #[arg(
            short,
            long,
            default_value = "200",
            help = "Rate limit in requests per second (200+ for high concurrency with async tasks)"
        )]
        rate_limit: u64,

        /// export as jsonl
        #[arg(long, help = "Export results in JSONL format")]
        export_jsonl: bool,

        /// max depth (0 unlimited)
        #[arg(
            short,
            long,
            default_value = "0",
            help = "Maximum depth to crawl (0 = unlimited)"
        )]
        max_depth: u32,

        /// request user agent
        #[arg(
            short,
            long,
            default_value = "RustSitemapCrawler/1.0",
            help = "User agent string for requests"
        )]
        user_agent: String,

        /// request timeout
        #[arg(
            short,
            long,
            default_value = "45",
            help = "Timeout in seconds for each request (45s allows slow pages to load in background)"
        )]
        timeout: u64,

        /// ignore robots.txt
        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,
    },

    /// orient command
    OrientMap {
        /// crawl data dir
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory containing crawled data"
        )]
        data_dir: String,

        /// crawl origin url
        #[arg(short, long, help = "Starting URL used for the original crawl")]
        start_url: String,

        /// sitemap output
        #[arg(
            short,
            long,
            default_value = "./sitemap.jsonl",
            help = "Output file for the reoriented sitemap"
        )]
        output: String,

        /// include lastmod
        #[arg(long, help = "Include last modification times in sitemap")]
        include_lastmod: bool,

        /// include changefreq
        #[arg(long, help = "Include change frequencies in sitemap")]
        include_changefreq: bool,

        /// default priority
        #[arg(
            long,
            default_value = "0.5",
            help = "Default priority for pages without explicit priority"
        )]
        default_priority: f32,
    },
}

impl Cli {
    /// parse args
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
