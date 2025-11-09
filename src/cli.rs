use clap::{Parser, Subcommand};

/// CLI interface that funnels user intent into structured crawler configs.
/// Exit codes: 0=success, 2=usage error, 3=config/IO failure, 4=network failure.
#[derive(Parser, Debug)]
#[command(name = "rust_sitemap")]
#[command(about = "A web crawler and sitemap generation tool")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Crawl from scratch so brand-new runs can discover pages.
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
            default_value = "512",
            help = "Concurrent requests (balances CPU and network throughput)"
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
            default_value = "20",
            help = "Request timeout in seconds (20s balances discovery speed vs slow servers)"
        )]
        timeout: u64,

        #[arg(long, help = "Disable robots.txt compliance")]
        ignore_robots: bool,

        #[arg(
            long,
            default_value = "none",
            help = "Seeding strategy: comma-separated list of sitemap, ct, commoncrawl, all, or none (e.g., 'sitemap,commoncrawl')"
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

        #[arg(
            long,
            default_value_t = 300,
            help = "Save interval in seconds (how often to persist state)"
        )]
        save_interval: u64,
    },

    /// Resume from persisted state so interrupted jobs continue.
    Resume {
        #[arg(
            short,
            long,
            default_value = "./data",
            help = "Directory containing crawl state"
        )]
        data_dir: String,

        #[arg(short, long, default_value = "512", help = "Concurrent requests")]
        workers: usize,

        #[arg(
            short,
            long,
            default_value = "RustSitemapCrawler/1.0",
            help = "User agent string for requests"
        )]
        user_agent: String,

        #[arg(short, long, default_value = "20", help = "Request timeout in seconds")]
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

        #[arg(long, default_value_t = 300, help = "Redis lock TTL in seconds")]
        lock_ttl: u64,
    },

    /// Export crawled data as sitemap.xml for downstream consumers.
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
    /// Parse CLI arguments so downstream code always receives validated options.
    /// clap exits with code 2 on usage errors, which we surface to users.
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crawl_command_minimal() {
        let cli = Cli::try_parse_from([
            "rust_sitemap",
            "crawl",
            "--start-url",
            "https://example.com",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        match cli.command {
            Commands::Crawl {
                start_url,
                workers,
                timeout,
                ..
            } => {
                assert_eq!(start_url, "https://example.com");
                assert_eq!(workers, 256);
                assert_eq!(timeout, 20);
            }
            _ => panic!("Expected Crawl command"),
        }
    }

    #[test]
    fn test_crawl_command_with_options() {
        let cli = Cli::try_parse_from([
            "rust_sitemap",
            "crawl",
            "--start-url",
            "https://example.com",
            "--workers",
            "128",
            "--timeout",
            "30",
            "--data-dir",
            "/tmp/data",
            "--ignore-robots",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        match cli.command {
            Commands::Crawl {
                start_url,
                workers,
                timeout,
                data_dir,
                ignore_robots,
                ..
            } => {
                assert_eq!(start_url, "https://example.com");
                assert_eq!(workers, 128);
                assert_eq!(timeout, 30);
                assert_eq!(data_dir, "/tmp/data");
                assert!(ignore_robots);
            }
            _ => panic!("Expected Crawl command"),
        }
    }

    #[test]
    fn test_crawl_command_redis_options() {
        let cli = Cli::try_parse_from([
            "rust_sitemap",
            "crawl",
            "--start-url",
            "https://example.com",
            "--enable-redis",
            "--redis-url",
            "redis://127.0.0.1:6379",
            "--lock-ttl",
            "600",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        match cli.command {
            Commands::Crawl {
                enable_redis,
                redis_url,
                lock_ttl,
                ..
            } => {
                assert!(enable_redis);
                assert_eq!(redis_url, "redis://127.0.0.1:6379");
                assert_eq!(lock_ttl, 600);
            }
            _ => panic!("Expected Crawl command"),
        }
    }

    #[test]
    fn test_resume_command() {
        let cli = Cli::try_parse_from([
            "rust_sitemap",
            "resume",
            "--data-dir",
            "./resume_data",
            "--workers",
            "64",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        match cli.command {
            Commands::Resume {
                data_dir, workers, ..
            } => {
                assert_eq!(data_dir, "./resume_data");
                assert_eq!(workers, 64);
            }
            _ => panic!("Expected Resume command"),
        }
    }

    #[test]
    fn test_export_sitemap_command() {
        let cli = Cli::try_parse_from([
            "rust_sitemap",
            "export-sitemap",
            "--data-dir",
            "./data",
            "--output",
            "./output.xml",
            "--include-lastmod",
            "--include-changefreq",
            "--default-priority",
            "0.8",
        ]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        match cli.command {
            Commands::ExportSitemap {
                data_dir,
                output,
                include_lastmod,
                include_changefreq,
                default_priority,
            } => {
                assert_eq!(data_dir, "./data");
                assert_eq!(output, "./output.xml");
                assert!(include_lastmod);
                assert!(include_changefreq);
                assert_eq!(default_priority, 0.8);
            }
            _ => panic!("Expected ExportSitemap command"),
        }
    }

    #[test]
    fn test_missing_required_arg() {
        let cli = Cli::try_parse_from(["rust_sitemap", "crawl"]);
        assert!(cli.is_err());
        let err = cli.unwrap_err();
        // Missing start_url should trigger the clap usage error path.
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn test_invalid_command() {
        let cli = Cli::try_parse_from(["rust_sitemap", "invalid-command"]);
        assert!(cli.is_err());
    }

    #[test]
    fn test_help_does_not_panic() {
        // Help output must error with DisplayHelp instead of panicking.
        let cli = Cli::try_parse_from(["rust_sitemap", "--help"]);
        assert!(cli.is_err());
        let err = cli.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn test_version_does_not_panic() {
        let cli = Cli::try_parse_from(["rust_sitemap", "--version"]);
        assert!(cli.is_err());
        let err = cli.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
    }
}
