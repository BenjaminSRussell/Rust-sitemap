//! Sitemap export command.

use crate::sitemap_writer::{DEFAULT_MAX_URLS_PER_SITEMAP, SitemapIndexWriter, SitemapUrl};
use crate::state::CrawlerState;
use crate::url_utils;
use std::path::Path;

/// Exports crawled URLs to sitemap.xml (or a sitemap index + parts when over the URL cap).
///
/// Filters (#46):
/// - HTTP 200 only
/// - HTML content types when `content_type` is known
/// - Skip nodes whose `canonical_url` points elsewhere (alternate pages)
/// - Single-host scope (first accepted host unless overridden later)
/// - Do not invent `lastmod` from `crawled_at` (Google ignores fabricated dates)
///
/// Splitting (#32): when URL count exceeds `max_urls_per_sitemap` (default 50_000),
/// writes `stem-1.xml`, `stem-2.xml`, … and a sitemap index at `output`.
#[tracing::instrument]
pub async fn run_export_sitemap_command(
    data_dir: String,
    output: String,
    include_lastmod: bool,
    include_changefreq: bool,
    default_priority: f32,
    max_urls_per_sitemap: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_per = max_urls_per_sitemap.unwrap_or(DEFAULT_MAX_URLS_PER_SITEMAP);
    println!(
        "Exporting sitemap to {} (max {} URLs per file)...",
        output, max_per
    );

    let state = CrawlerState::new(&data_dir)?;
    let output_path = Path::new(&output);
    let mut writer = SitemapIndexWriter::new(output_path, max_per)?;
    let node_iter = state.iter_nodes()?;

    let mut sitemap_host: Option<String> = None;
    let mut skipped = 0usize;

    node_iter.for_each(|node| {
        if node.status_code != Some(200) {
            skipped += 1;
            return Ok(());
        }

        if let Some(ref ct) = node.content_type {
            if !url_utils::is_html_content_type(ct) {
                skipped += 1;
                return Ok(());
            }
        }

        if let Some(ref canon) = node.canonical_url {
            if !canon.is_empty() && canon != &node.url {
                skipped += 1;
                return Ok(());
            }
        }

        let loc = node.url.clone();
        let host = match url_utils::extract_host(&loc) {
            Some(h) => h,
            None => {
                skipped += 1;
                return Ok(());
            }
        };
        if sitemap_host.is_none() {
            sitemap_host = Some(host.clone());
        }
        if sitemap_host.as_ref() != Some(&host) {
            skipped += 1;
            return Ok(());
        }

        let lastmod = None;
        let _ = include_lastmod;

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

        writer.add_url(SitemapUrl {
            loc,
            lastmod,
            changefreq,
            priority,
        })?;
        Ok(())
    })?;

    let (count, index_path, parts) = writer.finish()?;
    if parts.len() <= 1 {
        println!(
            "Exported {} URLs to {} (skipped {})",
            count,
            index_path.display(),
            skipped
        );
        println!("sitemap: {}", index_path.display());
    } else {
        println!(
            "Exported {} URLs across {} parts (skipped {})",
            count,
            parts.len(),
            skipped
        );
        println!("sitemap index: {}", index_path.display());
        for p in &parts {
            println!("  part: {}", p.display());
        }
    }

    Ok(())
}
