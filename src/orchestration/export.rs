//! Sitemap export command.

use crate::sitemap_writer::{SitemapUrl, SitemapWriter};
use crate::state::CrawlerState;
use crate::url_utils;
use std::fs;
use std::path::Path;

/// Exports crawled URLs to sitemap.xml.
///
/// Filters (#46):
/// - HTTP 200 only
/// - HTML content types when `content_type` is known
/// - Skip nodes whose `canonical_url` points elsewhere (alternate pages)
/// - Single-host scope (first accepted host unless overridden later)
/// - Atomic write via `*.xml.partial` + rename
/// - Do not invent `lastmod` from `crawled_at` (Google ignores fabricated dates)
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
    let output_path = Path::new(&output);
    let tmp_path = {
        let mut p = output_path.as_os_str().to_owned();
        p.push(".partial");
        Path::new(&p).to_path_buf()
    };
    let mut writer = SitemapWriter::new(&tmp_path)?;
    let node_iter = state.iter_nodes()?;

    let mut sitemap_host: Option<String> = None;
    let mut skipped = 0usize;

    node_iter.for_each(|node| {
        if node.status_code != Some(200) {
            skipped += 1;
            return Ok(());
        }

        // Skip non-HTML when content-type is known
        if let Some(ref ct) = node.content_type {
            if !url_utils::is_html_content_type(ct) {
                skipped += 1;
                return Ok(());
            }
        }

        // Skip alternate pages that declare a different canonical (#46)
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

        // Never fabricate lastmod from crawled_at (#46). Omit until Last-Modified is stored.
        let lastmod = None;
        let _ = include_lastmod;
        let _ = include_lastmod; // reserved for future Last-Modified field

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

    let count = writer.finish()?;
    fs::rename(&tmp_path, output_path)?;
    println!(
        "Exported {} URLs to {} (skipped {})",
        count, output, skipped
    );

    Ok(())
}
