use std::collections::HashMap;

/// Reorients the graph by determining parent-child relationships based on URL path hierarchy
/// using a trie data structure for efficient longest prefix matching.
/// 
/// # Arguments
/// * `urls` - A vector of all crawled URLs
/// * `start_url` - The starting URL to use as the root parent
/// 
/// # Returns
/// A HashMap mapping child URLs to their parent URLs
pub fn reorient_graph(urls: Vec<String>, start_url: &str) -> HashMap<String, String> {
    let mut parent_map = HashMap::new();
    
    // Build a list of paths and their corresponding URLs
    let mut path_urls: Vec<(String, String)> = Vec::new();
    for url in &urls {
        if let Some(path) = extract_path(url) {
            path_urls.push((path, url.clone()));
        }
    }
    
    // Sort paths by length to ensure we process shorter paths first
    path_urls.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
    
    // For each URL, find its parent by looking for the longest prefix match
    for (path, url) in &path_urls {
        let mut best_parent = None;
        let mut best_prefix_len = 0;
        
        // Look for the longest prefix match among all other paths
        for (other_path, other_url) in &path_urls {
            if other_path != path && other_url != url && path.starts_with(other_path) {
                // Skip the root path "/" as a parent for other paths
                if other_path == "/" {
                    continue;
                }
                // Ensure it's a proper path boundary (ends with / or is at the end)
                if other_path.ends_with('/') || 
                   path.chars().nth(other_path.len()).map_or(true, |c| c == '/') {
                    if other_path.len() > best_prefix_len {
                        best_prefix_len = other_path.len();
                        best_parent = Some(other_url.clone());
                    }
                }
            }
        }
        
        // Special case: if this is the root path "/" or the URL is the same as start_url, 
        // always use start_url as parent
        if path == "/" || url == start_url {
            parent_map.insert(url.clone(), start_url.to_string());
        } else if let Some(parent) = best_parent {
            parent_map.insert(url.clone(), parent);
        } else {
            parent_map.insert(url.clone(), start_url.to_string());
        }
    }
    
    parent_map
}

/// Extracts the path component from a URL
/// 
/// # Arguments
/// * `url` - The URL to extract the path from
/// 
/// # Returns
/// Some(path) if successful, None if the URL is invalid
fn extract_path(url: &str) -> Option<String> {
    if let Ok(parsed_url) = url::Url::parse(url) {
        Some(parsed_url.path().to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_direct_parent() {
        let urls = vec![
            "https://example.com/articles".to_string(),
            "https://example.com/articles/tech".to_string(),
            "https://example.com/articles/tech/rust".to_string(),
        ];
        
        let parent_map = reorient_graph(urls, "https://example.com");
        
        // The longest prefix match for /articles/tech/rust should be /articles/tech
        assert_eq!(
            parent_map.get("https://example.com/articles/tech/rust"),
            Some(&"https://example.com/articles/tech".to_string())
        );
        
        // The longest prefix match for /articles/tech should be /articles
        assert_eq!(
            parent_map.get("https://example.com/articles/tech"),
            Some(&"https://example.com/articles".to_string())
        );
    }

    #[test]
    fn test_no_direct_parent() {
        let urls = vec![
            "https://example.com/articles".to_string(),
            "https://example.com/articles/tech/rust".to_string(),
        ];
        
        let parent_map = reorient_graph(urls, "https://example.com");
        
        // The longest prefix match for /articles/tech/rust should be /articles
        assert_eq!(
            parent_map.get("https://example.com/articles/tech/rust"),
            Some(&"https://example.com/articles".to_string())
        );
    }

    #[test]
    fn test_root_url_parent() {
        let urls = vec![
            "https://example.com/".to_string(),
            "https://example.com/about".to_string(),
        ];
        
        let parent_map = reorient_graph(urls, "https://example.com");
        
        // Root URLs should have the start URL as their parent
        assert_eq!(
            parent_map.get("https://example.com/"),
            Some(&"https://example.com".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/about"),
            Some(&"https://example.com".to_string())
        );
    }

    #[test]
    fn test_empty_trie() {
        let urls = vec![
            "https://example.com/some/path".to_string(),
        ];
        
        let parent_map = reorient_graph(urls, "https://example.com");
        
        // When no prefix is found, should use start URL as parent
        assert_eq!(
            parent_map.get("https://example.com/some/path"),
            Some(&"https://example.com".to_string())
        );
    }

    #[test]
    fn test_complex_hierarchy() {
        let urls = vec![
            "https://example.com/".to_string(),
            "https://example.com/docs".to_string(),
            "https://example.com/docs/api".to_string(),
            "https://example.com/docs/api/v1".to_string(),
            "https://example.com/docs/api/v1/users".to_string(),
            "https://example.com/docs/api/v1/users/123".to_string(),
            "https://example.com/blog".to_string(),
            "https://example.com/blog/2023".to_string(),
            "https://example.com/blog/2023/12".to_string(),
        ];
        
        let parent_map = reorient_graph(urls, "https://example.com");
        
        // Test the hierarchy
        assert_eq!(
            parent_map.get("https://example.com/docs/api/v1/users/123"),
            Some(&"https://example.com/docs/api/v1/users".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/docs/api/v1/users"),
            Some(&"https://example.com/docs/api/v1".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/docs/api/v1"),
            Some(&"https://example.com/docs/api".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/docs/api"),
            Some(&"https://example.com/docs".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/docs"),
            Some(&"https://example.com".to_string())
        );
        
        assert_eq!(
            parent_map.get("https://example.com/blog/2023/12"),
            Some(&"https://example.com/blog/2023".to_string())
        );
    }
}
