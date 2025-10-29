// Pre-seed crawl queue from robots.txt sitemaps and sitemap indexes

use crate::config::Config;
use hyper::{Body, Client, Method, Request, StatusCode};
use hyper_tls::HttpsConnector;
use robotstxt::DefaultMatcher;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use std::io::Cursor;
use std::time::Duration;
use url::Url;

#[derive(Debug)]
pub struct SitemapSeeder {
    client: Client<HttpsConnector<hyper::client::HttpConnector>>,
    user_agent: String,
}

impl SitemapSeeder {
    pub fn new(user_agent: String) -> Self {
        let https = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .build(https);

        Self { client, user_agent }
    }

    // Extract domain and construct robots.txt URL
    fn robots_url(start_url: &str) -> Option<String> {
        let parsed = Url::parse(start_url).ok()?;
        let scheme = parsed.scheme();
        let host = parsed.host_str()?;
        Some(format!("{}://{}/robots.txt", scheme, host))
    }

    // Fetch robots.txt with timeout
    async fn fetch_robots(&self, robots_url: &str) -> Option<String> {
        let req = Request::builder()
            .method(Method::GET)
            .uri(robots_url)
            .header("User-Agent", &self.user_agent)
            .body(Body::empty())
            .ok()?;

        let timeout = Duration::from_secs(Config::ROBOTS_TIMEOUT_SECS);
        let response = tokio::time::timeout(timeout, self.client.request(req))
            .await
            .ok()?
            .ok()?;

        if response.status() != StatusCode::OK {
            return None;
        }

        let bytes = hyper::body::to_bytes(response.into_body()).await.ok()?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    // Fetch sitemap XML with extended timeout
    async fn fetch_sitemap(&self, sitemap_url: &str) -> Option<Vec<u8>> {
        let req = Request::builder()
            .method(Method::GET)
            .uri(sitemap_url)
            .header("User-Agent", &self.user_agent)
            .body(Body::empty())
            .ok()?;

        let timeout = Duration::from_secs(Config::SITEMAP_TIMEOUT_SECS);
        let response = tokio::time::timeout(timeout, self.client.request(req))
            .await
            .ok()?
            .ok()?;

        if response.status() != StatusCode::OK {
            return None;
        }

        let bytes = hyper::body::to_bytes(response.into_body()).await.ok()?;
        Some(bytes.to_vec())
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
    fn is_allowed(&self, robots_txt: &str, url: &str, user_agent: &str) -> bool {
        let mut matcher = DefaultMatcher::default();
        matcher.one_agent_allowed_by_robots(robots_txt, user_agent, url)
    }

    // Main entry point: seed URLs from robots.txt + sitemaps
    pub async fn seed(&self, start_url: &str) -> Vec<String> {
        let mut discovered = Vec::new();

        // Step 1: Fetch robots.txt
        let robots_url = match Self::robots_url(start_url) {
            Some(url) => url,
            None => {
                eprintln!("Failed to construct robots.txt URL");
                return discovered;
            }
        };

        eprintln!("Fetching robots.txt from {}...", robots_url);
        let robots_txt = match self.fetch_robots(&robots_url).await {
            Some(txt) => {
                eprintln!("Fetched robots.txt: {} bytes", txt.len());
                txt
            }
            None => {
                eprintln!("No robots.txt found, skipping sitemap pre-seed");
                return discovered;
            }
        };

        // Step 2: Extract sitemap URLs from robots.txt
        let sitemap_urls: Vec<String> = robots_txt
            .lines()
            .filter(|line| line.to_lowercase().starts_with("sitemap:"))
            .filter_map(|line| line.split_whitespace().nth(1).map(|s| s.to_string()))
            .collect();

        if sitemap_urls.is_empty() {
            eprintln!("No sitemaps declared in robots.txt");
            return discovered;
        }

        eprintln!("Found {} sitemap(s) in robots.txt", sitemap_urls.len());

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

            // Step 4: Filter by robots.txt rules
            for url in urls {
                if self.is_allowed(&robots_txt, &url, &self.user_agent) {
                    discovered.push(url);
                }
            }
        }

        eprintln!("Seeded {} URLs (after robots.txt filtering)", discovered.len());
        discovered
    }
}
