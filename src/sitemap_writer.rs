use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Sitemap URL entry
pub struct SitemapUrl {
    pub loc: String,
    pub lastmod: Option<String>,
    pub changefreq: Option<String>,
    pub priority: Option<f32>,
}

/// Writes sitemap XML
pub struct SitemapWriter {
    writer: BufWriter<File>,
    url_count: usize,
}

impl SitemapWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write XML header and urlset opening tag
        writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(writer, r#"<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"#)?;

        Ok(Self {
            writer,
            url_count: 0,
        })
    }

    pub fn add_url(&mut self, url: SitemapUrl) -> std::io::Result<()> {
        writeln!(self.writer, "  <url>")?;
        writeln!(self.writer, "    <loc>{}</loc>", escape_xml(&url.loc))?;

        if let Some(lastmod) = url.lastmod {
            writeln!(self.writer, "    <lastmod>{}</lastmod>", escape_xml(&lastmod))?;
        }

        if let Some(changefreq) = url.changefreq {
            writeln!(self.writer, "    <changefreq>{}</changefreq>", escape_xml(&changefreq))?;
        }

        if let Some(priority) = url.priority {
            writeln!(self.writer, "    <priority>{:.1}</priority>", priority)?;
        }

        writeln!(self.writer, "  </url>")?;
        self.url_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<usize> {
        writeln!(self.writer, "</urlset>")?;
        self.writer.flush()?;
        Ok(self.url_count)
    }
}

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
    fn test_sitemap_writer() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let mut writer = SitemapWriter::new(path).unwrap();
        writer.add_url(SitemapUrl {
            loc: "https://example.com/".to_string(),
            lastmod: Some("2024-01-01".to_string()),
            changefreq: Some("daily".to_string()),
            priority: Some(1.0),
        }).unwrap();

        writer.add_url(SitemapUrl {
            loc: "https://example.com/about".to_string(),
            lastmod: None,
            changefreq: None,
            priority: Some(0.8),
        }).unwrap();

        let count = writer.finish().unwrap();
        assert_eq!(count, 2);

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains(r#"<?xml version="1.0" encoding="UTF-8"?>"#));
        assert!(content.contains("<urlset"));
        assert!(content.contains("<loc>https://example.com/</loc>"));
        assert!(content.contains("</urlset>"));
    }
}