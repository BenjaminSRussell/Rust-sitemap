// Pre-seed crawl queue from robots.txt sitemaps and sitemap indexes

use crate::network::HttpClient;
use crate::robots;
use crate::seeder::Seeder;
use async_trait::async_trait;
use robotstxt::DefaultMatcher;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use std::io::Cursor;

pub struct SitemapSeeder {
    http: HttpClient,
}

impl SitemapSeeder {
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    // Fetch robots.txt using the shared HTTP client
    async fn fetch_robots(&self, start_url: &str) -> Option<String> {
        robots::fetch_robots_txt_from_url(&self.http, start_url).await
    }

    // Fetch sitemap XML using the shared HTTP client
    async fn fetch_sitemap(&self, sitemap_url: &str) -> Option<Vec<u8>> {
        match self.http.fetch(sitemap_url).await {
            Ok(result) if result.status_code == 200 => Some(result.content.into_bytes()),
            _ => None,
        }
    }

    // Parse sitemap XML and extract URLs (stream-based, no full load)
    fn parse_sitemap(&self, xml_data: &[u8]) -> Vec<String> {
        let mut urls = Vec::new();
        let cursor = Cursor::new(xml_data);
        let parser = SiteMapReader::new(cursor);

        for entity in parser {
            match entity {
                SiteMapEntity::Url(url_entry) => {
                    if let Some(url) = url_entry.loc.get_url() {
                        urls.push(url.to_string());
                    }
                }
                SiteMapEntity::SiteMap(sitemap_entry) => {
                    // Sitemap index entry
                    if let Some(url) = sitemap_entry.loc.get_url() {
                        urls.push(url.to_string());
                    }
                }
                _ => {}
            }
        }

        urls
    }

    // Check if URL is allowed by robots.txt
    fn is_allowed(&self, robots_txt: &str, url: &str) -> bool {
        let mut matcher = DefaultMatcher::default();
        // Use wildcard user agent for sitemap seeding
        matcher.one_agent_allowed_by_robots(robots_txt, "*", url)
    }

    // Main entry point: seed URLs from robots.txt + sitemaps
    pub async fn seed(&self, start_url: &str) -> Vec<String> {
        let mut discovered = Vec::new();

        // Step 1: Fetch robots.txt
        eprintln!("Fetching robots.txt for {}...", start_url);
        let robots_txt = self.fetch_robots(start_url).await;

        // Step 2: Extract sitemap URLs from robots.txt
        let mut sitemap_urls: Vec<String> = Vec::new();

        if let Some(ref txt) = robots_txt {
            eprintln!("Fetched robots.txt: {} bytes", txt.len());
            sitemap_urls = txt
                .lines()
                .filter(|line| line.to_lowercase().starts_with("sitemap:"))
                .filter_map(|line| line.split_whitespace().nth(1).map(|s| s.to_string()))
                .collect();

            if !sitemap_urls.is_empty() {
                eprintln!("Found {} sitemap(s) in robots.txt", sitemap_urls.len());
            }
        }

        // Step 3: If no sitemaps found in robots.txt, try common paths
        if sitemap_urls.is_empty() {
            eprintln!("No sitemaps declared in robots.txt, trying common paths...");

            // Extract base URL (scheme + host)
            let base_url = if let Some(url) = url::Url::parse(start_url).ok() {
                format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""))
            } else {
                start_url.to_string()
            };

            // Common sitemap paths to try
            let common_paths = vec![
                "/sitemap.xml",
                "/sitemap_index.xml",
                "/sitemap1.xml",
                "/sitemaps.xml",
                "/sitemap/sitemap.xml",
            ];

            for path in common_paths {
                let sitemap_url = format!("{}{}", base_url, path);
                eprintln!("Trying {}...", sitemap_url);

                // Try to fetch it
                if let Some(_) = self.fetch_sitemap(&sitemap_url).await {
                    eprintln!("Found sitemap at {}", sitemap_url);
                    sitemap_urls.push(sitemap_url);
                    break; // Found one, stop trying
                }
            }

            if sitemap_urls.is_empty() {
                eprintln!("No sitemaps found at common paths");
                return discovered;
            }
        }

        eprintln!("Processing {} sitemap(s)...", sitemap_urls.len());

        // Step 3: Fetch and parse each sitemap
        for sitemap_url in sitemap_urls {
            eprintln!("Fetching sitemap: {}...", sitemap_url);
            let xml_data = match self.fetch_sitemap(&sitemap_url).await {
                Some(data) => data,
                None => {
                    eprintln!("Failed to fetch: {}", sitemap_url);
                    continue;
                }
            };

            let urls = self.parse_sitemap(&xml_data);
            eprintln!("Parsed {}: {} URLs", sitemap_url, urls.len());

            // Step 4: Filter by robots.txt rules (if we have robots.txt)
            for url in urls {
                let allowed = if let Some(ref txt) = robots_txt {
                    self.is_allowed(txt, &url)
                } else {
                    // No robots.txt, allow all URLs
                    true
                };

                if allowed {
                    discovered.push(url);
                }
            }
        }

        eprintln!("Seeded {} URLs (after robots.txt filtering)", discovered.len());
        discovered
    }
}

#[async_trait]
impl Seeder for SitemapSeeder {
    async fn seed(&self, domain: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.seed(domain).await)
    }

    fn name(&self) -> &'static str {
        "sitemap"
    }
}
