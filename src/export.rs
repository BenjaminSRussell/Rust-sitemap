use serde::Serialize;
use std::io::Write;

/// Represents a node for export to JSONL format
/// Contains the essential information needed for sitemap generation
#[derive(Debug, Clone, Serialize)]
pub struct ExportNode {
    /// The URL of the page
    pub url: String,
    
    /// Parent URL that led to this page (None for root)
    pub parent_url: Option<String>,
    
    /// Depth level from the starting URL
    pub depth: u32,
    
    /// HTTP status code returned when accessing the page
    pub status_code: u16,
    
    /// Title of the page (from <title> tag)
    pub title: Option<String>,
    
    /// Timestamp when the page was crawled (Unix timestamp)
    pub crawled_at: u64,
    
    /// Size of the page content in bytes
    pub content_size: u64,
    
    /// Content type of the page (e.g., "text/html")
    pub content_type: Option<String>,
}

impl ExportNode {
    /// Create a new ExportNode with the given URL and depth
    pub fn new(url: String, depth: u32) -> Self {
        Self {
            url,
            parent_url: None,
            depth,
            status_code: 0,
            title: None,
            crawled_at: 0,
            content_size: 0,
            content_type: None,
        }
    }
    
    /// Set the parent URL for this node
    pub fn set_parent_url(&mut self, parent_url: Option<String>) {
        self.parent_url = parent_url;
    }
    
    /// Set the status code for this node
    pub fn set_status_code(&mut self, status_code: u16) {
        self.status_code = status_code;
    }
    
    /// Set the title for this node
    pub fn set_title(&mut self, title: Option<String>) {
        self.title = title;
    }
    
    /// Set the crawled timestamp for this node
    pub fn set_crawled_at(&mut self, crawled_at: u64) {
        self.crawled_at = crawled_at;
    }
    
    /// Set the content size for this node
    pub fn set_content_size(&mut self, content_size: u64) {
        self.content_size = content_size;
    }
    
    /// Set the content type for this node
    pub fn set_content_type(&mut self, content_type: Option<String>) {
        self.content_type = content_type;
    }
}

/// Export a collection of ExportNodes to JSONL format
/// Each node is written as a single line of JSON followed by a newline
/// 
/// # Arguments
/// * `nodes` - An iterator of ExportNode structs to export
/// * `writer` - A writer implementing std::io::Write to write the JSONL data to
/// 
/// # Errors
/// Returns an error if JSON serialization or writing fails
pub fn export_to_jsonl<W: Write, I: IntoIterator<Item = ExportNode>>(
    nodes: I,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    for node in nodes {
        serde_json::to_writer(&mut *writer, &node)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_to_jsonl() {
        let mut node = ExportNode::new("https://example.com/".to_string(), 0);
        node.set_status_code(200);
        node.set_title(Some("Example Homepage".to_string()));
        node.set_crawled_at(1640995200); // 2022-01-01 00:00:00 UTC
        node.set_content_size(1024);
        node.set_content_type(Some("text/html".to_string()));

        let mut buffer = Vec::new();
        export_to_jsonl(std::iter::once(node), &mut buffer).unwrap();

        let output = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        
        // Should have exactly one line
        assert_eq!(lines.len(), 1);
        
        // Should end with a newline character
        assert!(output.ends_with('\n'));
        
        // Should be valid JSON
        let json_line = lines[0];
        let parsed: serde_json::Value = serde_json::from_str(json_line).unwrap();
        
        // Verify the JSON contains expected fields
        assert_eq!(parsed["url"], "https://example.com/");
        assert_eq!(parsed["depth"], 0);
        assert_eq!(parsed["status_code"], 200);
        assert_eq!(parsed["title"], "Example Homepage");
        assert_eq!(parsed["crawled_at"], 1640995200);
        assert_eq!(parsed["content_size"], 1024);
        assert_eq!(parsed["content_type"], "text/html");
        assert_eq!(parsed["parent_url"], serde_json::Value::Null);
    }

    #[test]
    fn test_multiple_nodes_to_jsonl() {
        let mut node1 = ExportNode::new("https://example.com/".to_string(), 0);
        node1.set_status_code(200);
        node1.set_title(Some("Homepage".to_string()));
        node1.set_crawled_at(1640995200);

        let mut node2 = ExportNode::new("https://example.com/about".to_string(), 1);
        node2.set_parent_url(Some("https://example.com/".to_string()));
        node2.set_status_code(200);
        node2.set_title(Some("About Page".to_string()));
        node2.set_crawled_at(1640995260);

        let mut node3 = ExportNode::new("https://example.com/docs".to_string(), 1);
        node3.set_parent_url(Some("https://example.com/".to_string()));
        node3.set_status_code(200);
        node3.set_title(Some("Documentation".to_string()));
        node3.set_crawled_at(1640995320);

        let nodes = vec![node1, node2, node3];
        let mut buffer = Vec::new();
        export_to_jsonl(nodes.iter().cloned(), &mut buffer).unwrap();

        let output = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        
        // Should have exactly three lines
        assert_eq!(lines.len(), 3);
        
        // Should end with a newline character
        assert!(output.ends_with('\n'));
        
        // Each line should be valid JSON
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
        
        // Verify the content of each line
        let parsed1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed1["url"], "https://example.com/");
        assert_eq!(parsed1["depth"], 0);
        assert_eq!(parsed1["parent_url"], serde_json::Value::Null);
        
        let parsed2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2["url"], "https://example.com/about");
        assert_eq!(parsed2["depth"], 1);
        assert_eq!(parsed2["parent_url"], "https://example.com/");
        
        let parsed3: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(parsed3["url"], "https://example.com/docs");
        assert_eq!(parsed3["depth"], 1);
        assert_eq!(parsed3["parent_url"], "https://example.com/");
    }

    #[test]
    fn test_export_to_jsonl_with_empty_iterator() {
        let mut buffer = Vec::new();
        let result = export_to_jsonl(std::iter::empty::<ExportNode>(), &mut buffer);
        assert!(result.is_ok());
        
        let output = String::from_utf8(buffer).unwrap();
        assert_eq!(output, "");
    }

    #[test]
    fn test_export_node_builder_methods() {
        let mut node = ExportNode::new("https://test.com/".to_string(), 2);
        
        node.set_parent_url(Some("https://test.com/".to_string()));
        node.set_status_code(404);
        node.set_title(Some("Not Found".to_string()));
        node.set_crawled_at(1234567890);
        node.set_content_size(512);
        node.set_content_type(Some("text/html".to_string()));
        
        assert_eq!(node.url, "https://test.com/");
        assert_eq!(node.depth, 2);
        assert_eq!(node.parent_url, Some("https://test.com/".to_string()));
        assert_eq!(node.status_code, 404);
        assert_eq!(node.title, Some("Not Found".to_string()));
        assert_eq!(node.crawled_at, 1234567890);
        assert_eq!(node.content_size, 512);
        assert_eq!(node.content_type, Some("text/html".to_string()));
    }
}
