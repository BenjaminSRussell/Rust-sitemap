use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Sitemap URL entry so we hold optional metadata for each location.
#[allow(dead_code)]
pub struct SitemapUrl {
    pub loc: String,
    pub lastmod: Option<String>,
    pub changefreq: Option<String>,
    pub priority: Option<f32>,
}

/// Writes sitemap XML so callers can stream sitemap documents to disk.
#[allow(dead_code)]
pub struct SitemapWriter {
    writer: BufWriter<File>,
    pub(crate) url_count: usize,
}

/// Helper to format error with its source chain for logging.
#[allow(dead_code)]
fn format_error_chain(e: &dyn Error) -> String {
    let mut chain = vec![e.to_string()];
    let mut source = e.source();
    while let Some(src) = source {
        chain.push(src.to_string());
        source = src.source();
    }
    chain.join(" -> ")
}

impl SitemapWriter {
    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Self::new_impl(path).inspect_err(|e| {
            tracing::error!(
                "sitemap create failed: {:?}: {}",
                e.kind(),
                format_error_chain(e)
            );
        })
    }

    #[allow(dead_code)]
    fn new_impl<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Emit the XML header and urlset opening tag so the file conforms to the sitemap schema.
        writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(
            writer,
            r#"<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"#
        )?;

        Ok(Self {
            writer,
            url_count: 0,
        })
    }

    #[allow(dead_code)]
    pub fn add_url(&mut self, url: SitemapUrl) -> std::io::Result<()> {
        self.add_url_impl(url).inspect_err(|e| {
            tracing::error!(
                "sitemap url write failed: {:?}: {}",
                e.kind(),
                format_error_chain(e)
            );
        })
    }

    #[allow(dead_code)]
    fn add_url_impl(&mut self, url: SitemapUrl) -> std::io::Result<()> {
        writeln!(self.writer, "  <url>")?;
        writeln!(self.writer, "    <loc>{}</loc>", escape_xml(&url.loc))?;

        if let Some(lastmod) = url.lastmod {
            writeln!(
                self.writer,
                "    <lastmod>{}</lastmod>",
                escape_xml(&lastmod)
            )?;
        }

        if let Some(changefreq) = url.changefreq {
            writeln!(
                self.writer,
                "    <changefreq>{}</changefreq>",
                escape_xml(&changefreq)
            )?;
        }

        if let Some(priority) = url.priority {
            writeln!(self.writer, "    <priority>{:.1}</priority>", priority)?;
        }

        writeln!(self.writer, "  </url>")?;
        self.url_count += 1;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn finish(mut self) -> std::io::Result<usize> {
        self.finish_impl().inspect_err(|e| {
            tracing::error!(
                "sitemap finalize failed: {:?}: {}",
                e.kind(),
                format_error_chain(e)
            );
        })
    }

    #[allow(dead_code)]
    fn finish_impl(&mut self) -> std::io::Result<usize> {
        writeln!(self.writer, "</urlset>")?;
        self.writer.flush()?;
        Ok(self.url_count)
    }
}

#[allow(dead_code)]
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}



/// Default sitemaps.org URL cap per urlset file.
pub const DEFAULT_MAX_URLS_PER_SITEMAP: usize = 50_000;

/// Streams URLs into one or more urlset files and writes a sitemap index when needed.
///
/// When the total stays under `max_urls_per_file`, a single urlset is written to `output`.
/// Otherwise parts `stem-1.xml`, `stem-2.xml`, … land beside `output` and `output` becomes
/// the sitemap index listing those parts.
pub struct SitemapIndexWriter {
    output: PathBuf,
    max_urls_per_file: usize,
    part_paths: Vec<PathBuf>,
    current: Option<SitemapWriter>,
    current_part: usize,
    total_urls: usize,
    /// Absolute or CLI-facing base URL/path prefix for index <loc> entries (file paths).
    loc_base: PathBuf,
}

impl SitemapIndexWriter {
    pub fn new<P: AsRef<Path>>(output: P, max_urls_per_file: usize) -> std::io::Result<Self> {
        let output = output.as_ref().to_path_buf();
        let max_urls_per_file = max_urls_per_file.max(1);
        Ok(Self {
            loc_base: output
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            output,
            max_urls_per_file,
            part_paths: Vec::new(),
            current: None,
            current_part: 0,
            total_urls: 0,
        })
    }

    fn part_path(&self, part: usize) -> PathBuf {
        let stem = self
            .output
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("sitemap");
        let parent = self.output.parent().unwrap_or_else(|| Path::new("."));
        parent.join(format!("{}-{}.xml", stem, part))
    }

    fn ensure_writer(&mut self) -> std::io::Result<()> {
        if self.current.is_some() {
            return Ok(());
        }
        self.current_part += 1;
        let path = if self.current_part == 1 {
            // First part uses a temp sibling; may be promoted to `output` if sole part.
            let mut p = self.output.as_os_str().to_owned();
            p.push(".part1");
            PathBuf::from(p)
        } else {
            self.part_path(self.current_part)
        };
        let writer = SitemapWriter::new(&path)?;
        self.part_paths.push(path);
        self.current = Some(writer);
        Ok(())
    }

    pub fn add_url(&mut self, url: SitemapUrl) -> std::io::Result<()> {
        self.ensure_writer()?;
        let needs_rotate = self
            .current
            .as_ref()
            .map(|w| w.url_count >= self.max_urls_per_file)
            .unwrap_or(false);
        if needs_rotate {
            if let Some(finished) = self.current.take() {
                finished.finish()?;
            }
            self.ensure_writer()?;
        }
        if let Some(ref mut w) = self.current {
            w.add_url(url)?;
            self.total_urls += 1;
        }
        Ok(())
    }

    /// Finishes writers and returns (total_urls, index_or_sitemap_path, part_paths).
    pub fn finish(mut self) -> std::io::Result<(usize, PathBuf, Vec<PathBuf>)> {
        if let Some(w) = self.current.take() {
            w.finish()?;
        }
        if self.part_paths.is_empty() {
            // Empty sitemap
            let mut w = SitemapWriter::new(&self.output)?;
            w.finish()?;
            return Ok((0, self.output.clone(), vec![self.output.clone()]));
        }

        if self.part_paths.len() == 1 {
            // Single urlset → move/rename to output
            let src = &self.part_paths[0];
            if src != &self.output {
                if self.output.exists() {
                    std::fs::remove_file(&self.output)?;
                }
                std::fs::rename(src, &self.output)?;
            }
            return Ok((self.total_urls, self.output.clone(), vec![self.output.clone()]));
        }

        // Multi-part: rename part1 temp to stem-1.xml, write index at output
        let final_parts: Vec<PathBuf> = (1..=self.part_paths.len())
            .map(|i| self.part_path(i))
            .collect();
        for (src, dst) in self.part_paths.iter().zip(final_parts.iter()) {
            if src != dst {
                if dst.exists() {
                    std::fs::remove_file(dst)?;
                }
                std::fs::rename(src, dst)?;
            }
        }

        let mut index = BufWriter::new(File::create(&self.output)?);
        writeln!(index, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(
            index,
            r#"<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"#
        )?;
        for part in &final_parts {
            let loc = part
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("sitemap.xml");
            writeln!(index, "  <sitemap>")?;
            writeln!(index, "    <loc>{}</loc>", escape_xml(loc))?;
            writeln!(index, "  </sitemap>")?;
        }
        writeln!(index, "</sitemapindex>")?;
        index.flush()?;

        Ok((self.total_urls, self.output.clone(), final_parts))
    }

    pub fn total_urls(&self) -> usize {
        self.total_urls
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("hello"), "hello");
        assert_eq!(escape_xml("a&b"), "a&amp;b");
        assert_eq!(escape_xml("<tag>"), "&lt;tag&gt;");
        assert_eq!(escape_xml("\"quoted\""), "&quot;quoted&quot;");
        assert_eq!(escape_xml("'apostrophe'"), "&apos;apostrophe&apos;");
        assert_eq!(
            escape_xml("<a>&\"'</a>"),
            "&lt;a&gt;&amp;&quot;&apos;&lt;/a&gt;"
        );
    }

    #[test]
    fn test_sitemap_writer() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let mut writer = SitemapWriter::new(path).unwrap();
        writer
            .add_url(SitemapUrl {
                loc: "https://example.com/".to_string(),
                lastmod: Some("2024-01-01".to_string()),
                changefreq: Some("daily".to_string()),
                priority: Some(1.0),
            })
            .unwrap();

        writer
            .add_url(SitemapUrl {
                loc: "https://example.com/about".to_string(),
                lastmod: None,
                changefreq: None,
                priority: Some(0.8),
            })
            .unwrap();

        let count = writer.finish().unwrap();
        assert_eq!(count, 2);

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains(r#"<?xml version="1.0" encoding="UTF-8"?>"#));
        assert!(content.contains("<urlset"));
        assert!(content.contains("<loc>https://example.com/</loc>"));
        assert!(content.contains("</urlset>"));
    }

    #[test]
    fn test_sitemap_index_splits_at_cap() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("sitemap.xml");
        let mut w = SitemapIndexWriter::new(&out, 3).unwrap();
        for i in 0..7 {
            w.add_url(SitemapUrl {
                loc: format!("https://example.com/p{}", i),
                lastmod: None,
                changefreq: None,
                priority: None,
            })
            .unwrap();
        }
        let (total, index_path, parts) = w.finish().unwrap();
        assert_eq!(total, 7);
        assert_eq!(parts.len(), 3);
        assert_eq!(index_path, out);
        let index = std::fs::read_to_string(&out).unwrap();
        assert!(index.contains("<sitemapindex"));
        assert!(index.contains("sitemap-1.xml"));
        assert!(index.contains("sitemap-2.xml"));
        assert!(index.contains("sitemap-3.xml"));
        for (i, part) in parts.iter().enumerate() {
            let body = std::fs::read_to_string(part).unwrap();
            assert!(body.contains("<urlset"));
            let n = body.matches("<url>").count();
            if i < 2 {
                assert_eq!(n, 3);
            } else {
                assert_eq!(n, 1);
            }
        }
    }

    #[test]
    fn test_sitemap_index_single_file_under_cap() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("sitemap.xml");
        let mut w = SitemapIndexWriter::new(&out, 50).unwrap();
        w.add_url(SitemapUrl {
            loc: "https://example.com/".into(),
            lastmod: None,
            changefreq: None,
            priority: Some(1.0),
        })
        .unwrap();
        let (total, path, parts) = w.finish().unwrap();
        assert_eq!(total, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(path, out);
        let body = std::fs::read_to_string(&out).unwrap();
        assert!(body.contains("<urlset"));
        assert!(!body.contains("<sitemapindex"));
    }

}
