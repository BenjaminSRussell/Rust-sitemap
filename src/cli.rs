use clap::{Parser, Subcommand};

/// CLI entry point
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
    /// Crawl starting from seed URLs
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
            default_value = "0",
            help = "Maximum depth to crawl (0 = unlimited)"
        )]
        max_depth: u32,

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
    },

    /// Export crawled data to sitemap.xml format
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
    /// Wrapper for clap parsing
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
