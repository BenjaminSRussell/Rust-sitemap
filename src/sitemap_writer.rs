use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

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
    url_count: usize,
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
}
