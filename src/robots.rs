//! Robots.txt fetching and crawl-delay helpers

use crate::network::HttpClient;
use crate::url_utils;

/// Fetch robots.txt content for the domain
pub async fn fetch_robots_txt(http: &HttpClient, domain: &str) -> Option<String> {
    let robots_url = format!("https://{}/robots.txt", domain);

    match http.fetch(&robots_url).await {
        Ok(result) if result.status_code == 200 => Some(result.content),
        _ => None,
    }
}

/// Fetch robots.txt for the host derived from a URL
pub async fn fetch_robots_txt_from_url(http: &HttpClient, start_url: &str) -> Option<String> {
    let robots_url = url_utils::robots_url(start_url)?;

    match http.fetch(&robots_url).await {
        Ok(result) if result.status_code == 200 => Some(result.content),
        _ => None,
    }
}

