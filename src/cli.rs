use clap::{Parser, Subcommand};

/// CLI entry point so users can control the crawler from the command line.
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
    /// Crawl starting from seed URLs so a fresh run can discover new pages.
    Crawl {
        #[arg(short, long, help = "The starting URL to begin crawling from")]
        start_url: String,

        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory to store crawled data"
        )]
        data_dir: String,

        #[arg(
            short,
            long,
            default_value = "256",
            help = "Concurrent requests (balances CPU and network throughput)"
        )]
        workers: usize,

        #[arg(long, help = "Export results in JSONL format")]
        export_jsonl: bool,

        #[arg(
            short,
            long,
            default_value = "RustSitemapCrawler/1.0",
            help = "User agent string for requests"
        )]
        user_agent: String,

        #[arg(
            short,
            long,
            default_value = "45",
            help = "Request timeout in seconds (45s prevents blocking on slow servers)"
        )]
        timeout: u64,

        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,

        #[arg(
            long,
            default_value = "all",
            help = "Seeding strategy: sitemap, ct, commoncrawl, or all"
        )]
        seeding_strategy: String,

        #[arg(long, help = "Enable distributed crawling with Redis")]
        enable_redis: bool,

        #[arg(
            long,
            default_value = "redis://localhost:6379",
            help = "Redis connection URL for distributed crawling"
        )]
        redis_url: String,

        #[arg(
            long,
            default_value_t = 300,
            help = "Redis lock TTL in seconds (time before lock expires)"
        )]
        lock_ttl: u64,
    },

    /// Resume a previous crawl from saved state so interrupted jobs can continue.
    Resume {
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory containing crawl state"
        )]
        data_dir: String,

        #[arg(
            short,
            long,
            default_value = "256",
            help = "Concurrent requests"
        )]
        workers: usize,

        #[arg(
            short,
            long,
            default_value = "RustSitemapCrawler/1.0",
            help = "User agent string for requests"
        )]
        user_agent: String,

        #[arg(
            short,
            long,
            default_value = "45",
            help = "Request timeout in seconds"
        )]
        timeout: u64,

        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,

        #[arg(long, help = "Enable distributed crawling with Redis")]
        enable_redis: bool,

        #[arg(
            long,
            default_value = "redis://localhost:6379",
            help = "Redis connection URL"
        )]
        redis_url: String,

        #[arg(
            long,
            default_value_t = 300,
            help = "Redis lock TTL in seconds"
        )]
        lock_ttl: u64,
    },

    /// Export crawled data to sitemap.xml format so downstream systems can ingest it.
    ExportSitemap {
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory containing crawled data"
        )]
        data_dir: String,
        #[arg(
            short,
            long,
            default_value = "./sitemap.xml",
            help = "Output sitemap XML file"
        )]
        output: String,

        #[arg(long, help = "Include last modification times in sitemap")]
        include_lastmod: bool,

        #[arg(long, help = "Include change frequencies in sitemap")]
        include_changefreq: bool,

        #[arg(
            long,
            default_value = "0.5",
            help = "Default priority for pages (0.0-1.0)"
        )]
        default_priority: f32,
    },
}

impl Cli {
    /// Parse CLI arguments so the rest of the program can rely on structured options.
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
