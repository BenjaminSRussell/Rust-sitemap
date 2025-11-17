//! Sitemap export command.

use crate::sitemap_writer::{SitemapUrl, SitemapWriter};
use crate::state::CrawlerState;

/// Exports crawled URLs to sitemap.xml.
#[tracing::instrument]
pub async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Exporting sitemap to {}...", output);

    let state = CrawlerState::new(&data_dir)?;
    let mut writer = SitemapWriter::new(&output)?;
    let node_iter = state.iter_nodes()?;

    node_iter
        .for_each(|node| {
            if node.status_code == Some(200) {
                let lastmod = if include_lastmod {
                    node.crawled_at.map(|ts| {
                        let dt = chrono::DateTime::from_timestamp(ts as i64, 0).unwrap_or_default();
                        dt.format("%Y-%m-%d").to_string()
                    })
                } else {
                    None
                };

                let changefreq = if include_changefreq {
                    Some("weekly".to_string())
                } else {
                    None
                };

                let priority = match node.depth {
                    0 => Some(1.0),
                    1 => Some(0.8),
                    2 => Some(0.6),
                    _ => Some(default_priority),
                };

                writer
                    .add_url(SitemapUrl {
                        loc: node.url.clone(),
                        lastmod,
                        changefreq,
                        priority,
                    })?;
            }
            Ok(())
        })?;

    let count = writer.finish()?;
    println!("Exported {} URLs to {}", count, output);

    Ok(())
}
