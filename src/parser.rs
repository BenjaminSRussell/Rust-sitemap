use scraper::{Html, Selector};

/// Extract all hyperlink URLs from HTML content
/// 
/// # Arguments
/// * `html_body` - The HTML content as a string
/// 
/// # Returns
/// A vector of strings containing all href attributes from <a> tags
/// 
/// # Examples
/// ```
/// use rust_sitemap::parser::extract_links;
/// 
/// let html = r#"<html><body><a href="https://example.com">Link</a></body></html>"#;
/// let links = extract_links(html);
/// assert_eq!(links, vec!["https://example.com"]);
/// ```
pub fn extract_links(html_body: &str) -> Vec<String> {
    let document = Html::parse_document(html_body);
    let selector = Selector::parse("a[href]").expect("Invalid CSS selector");
    
    let mut links = Vec::new();
    
    for element in document.select(&selector) {
        if let Some(href) = element.value().attr("href") {
            // Clean up the URL
            let cleaned_href = href.trim();
            
            // Skip empty links, javascript links, mailto, tel, etc.
            if !cleaned_href.is_empty() 
                && !cleaned_href.starts_with("javascript:")
                && !cleaned_href.starts_with("mailto:")
                && !cleaned_href.starts_with("tel:")
                && !cleaned_href.starts_with("data:")
                && !cleaned_href.starts_with("file:") {
                links.push(cleaned_href.to_string());
            }
        }
    }
    
    links
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_absolute_links() {
        let html = "<html><body><a href=\"https://example.com/page1\">Link 1</a><a href=\"https://example.com/page2\">Link 2</a><a href=\"https://other-site.com/about\">External Link</a></body></html>";
        
        let links = extract_links(html);
        let expected = vec![
            "https://example.com/page1".to_string(),
            "https://example.com/page2".to_string(),
            "https://other-site.com/about".to_string(),
        ];
        
        assert_eq!(links, expected);
    }

    #[test]
    fn test_extract_relative_links() {
        let html = "<html><body><a href=\"/about\">About</a><a href=\"../parent\">Parent</a><a href=\"relative/path\">Relative Path</a><a href=\"#section\">Anchor</a></body></html>";
        
        let links = extract_links(html);
        let expected = vec![
            "/about".to_string(),
            "../parent".to_string(),
            "relative/path".to_string(),
            "#section".to_string(),
        ];
        
        assert_eq!(links, expected);
    }

    #[test]
    fn test_no_links_present() {
        let html = "<html><body><h1>No Links Here</h1><p>Just some text content.</p><div>No anchor tags at all.</div></body></html>";
        
        let links = extract_links(html);
        assert!(links.is_empty());
    }

    #[test]
    fn test_malformed_html() {
        let html = "<html><body><a href=\"https://example.com\">Valid Link</a><a href=\"https://broken.com\">Broken Link<div>Unclosed div<p>Some text without closing tag</body></html>";
        
        // The scraper library should handle malformed HTML gracefully
        let links = extract_links(html);
        let expected = vec![
            "https://example.com".to_string(),
            "https://broken.com".to_string(),
        ];
        
        assert_eq!(links, expected);
    }

    #[test]
    fn test_mixed_absolute_and_relative_links() {
        let html = "<html><body><a href=\"https://example.com\">Absolute</a><a href=\"/relative\">Relative</a><a href=\"https://external.com\">External</a><a href=\"../parent\">Parent</a></body></html>";
        
        let links = extract_links(html);
        let expected = vec![
            "https://example.com".to_string(),
            "/relative".to_string(),
            "https://external.com".to_string(),
            "../parent".to_string(),
        ];
        
        assert_eq!(links, expected);
    }

    #[test]
    fn test_empty_html() {
        let html = "";
        let links = extract_links(html);
        assert!(links.is_empty());
    }

    #[test]
    fn test_links_with_attributes() {
        let html = "<html><body><a href=\"https://example.com\" target=\"_blank\" class=\"external\">External Link</a><a href=\"/internal\" id=\"internal-link\" data-test=\"value\">Internal Link</a></body></html>";
        
        let links = extract_links(html);
        let expected = vec![
            "https://example.com".to_string(),
            "/internal".to_string(),
        ];
        
        assert_eq!(links, expected);
    }

    #[test]
    fn test_duplicate_links() {
        let html = "<html><body><a href=\"https://example.com\">Link 1</a><a href=\"https://example.com\">Link 2</a><a href=\"/about\">About 1</a><a href=\"/about\">About 2</a></body></html>";
        
        let links = extract_links(html);
        let expected = vec![
            "https://example.com".to_string(),
            "https://example.com".to_string(),
            "/about".to_string(),
            "/about".to_string(),
        ];
        
        assert_eq!(links, expected);
    }
}