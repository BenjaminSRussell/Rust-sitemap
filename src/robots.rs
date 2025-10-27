use regex::Regex;
use std::collections::HashMap;
use url::Url;

#[derive(Debug, Clone)]
pub struct RobotsTxt {
    rules: HashMap<String, Vec<Rule>>,
    default_user_agent: String,
}

#[derive(Debug, Clone)]
struct Rule {
    is_allow: bool,
    path: String,
    regex: Option<Regex>,
}

impl RobotsTxt {
    pub fn new(content: &str, user_agent: &str) -> Self {
        let mut robots = Self {
            rules: HashMap::new(),
            default_user_agent: user_agent.to_string(),
        };
        robots.parse(content);
        robots
    }

    fn parse(&mut self, content: &str) {
        let mut current_user_agents: Vec<String> = Vec::new();
        let mut current_rules: Vec<Rule> = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim().to_lowercase();
                let value = value.trim();

                match key.as_str() {
                    "user-agent" => {
                        if !current_rules.is_empty() {
                            for user_agent in &current_user_agents {
                                self.rules.insert(user_agent.clone(), current_rules.clone());
                            }
                        }

                        current_user_agents.clear();
                        current_rules.clear();
                        current_user_agents.push(value.to_string());
                    }
                    "disallow" => {
                        if !value.is_empty() {
                            current_rules.push(Rule {
                                is_allow: false,
                                path: value.to_string(),
                                regex: self.create_regex(value),
                            });
                        }
                    }
                    "allow" => {
                        current_rules.push(Rule {
                            is_allow: true,
                            path: value.to_string(),
                            regex: self.create_regex(value),
                        });
                    }
                    _ => {}
                }
            }
        }

        if !current_rules.is_empty() {
            for user_agent in &current_user_agents {
                self.rules.insert(user_agent.clone(), current_rules.clone());
            }
        }
    }

    fn create_regex(&self, pattern: &str) -> Option<Regex> {
        if pattern.is_empty() {
            return None;
        }

        let mut regex_pattern = regex::escape(pattern);
        regex_pattern = regex_pattern.replace("\\*", ".*");
        regex_pattern = regex_pattern.replace("\\$", "$");

        if !regex_pattern.starts_with('^') {
            regex_pattern = format!("^{}", regex_pattern);
        }

        Regex::new(&regex_pattern).ok()
    }

    pub fn is_allowed(&self, url: &str, user_agent: &str) -> bool {
        if let Ok(parsed_url) = Url::parse(url) {
            self.is_path_allowed(parsed_url.path(), user_agent)
        } else {
            true
        }
    }

    pub fn is_path_allowed(&self, path: &str, user_agent: &str) -> bool {
        let rules = self
            .rules
            .get(user_agent)
            .or_else(|| self.rules.get("*"))
            .or_else(|| self.rules.get(&self.default_user_agent));

        if let Some(rules) = rules {
            for rule in rules {
                if let Some(ref regex) = rule.regex {
                    if regex.is_match(path) {
                        return rule.is_allow;
                    }
                } else if path.starts_with(&rule.path) {
                    return rule.is_allow;
                }
            }
        }

        true
    }
}

impl Default for RobotsTxt {
    fn default() -> Self {
        Self {
            rules: HashMap::new(),
            default_user_agent: "RustSitemapCrawler/1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_robots_txt_parsing() {
        let content = r#"
User-agent: *
Disallow: /private/
Disallow: /admin/
Allow: /public/

User-agent: Googlebot
Disallow: /secret/
"#;

        let robots = RobotsTxt::new(content, "TestBot/1.0");

        assert!(!robots.is_path_allowed("/private/secret", "*"));
        assert!(!robots.is_path_allowed("/admin/dashboard", "*"));
        assert!(robots.is_path_allowed("/public/info", "*"));
        assert!(robots.is_path_allowed("/other/page", "*"));

        assert!(!robots.is_path_allowed("/secret/data", "Googlebot"));
        assert!(robots.is_path_allowed("/private/secret", "Googlebot"));
    }

    #[test]
    fn test_robots_txt_wildcards() {
        let content = r#"
User-agent: *
Disallow: /temp*
Disallow: /backup/
Allow: /temp/public/
"#;

        let robots = RobotsTxt::new(content, "TestBot/1.0");

        assert!(!robots.is_path_allowed("/temp123", "*"));
        assert!(!robots.is_path_allowed("/temp/old", "*"));
        assert!(!robots.is_path_allowed("/temp/public/", "*"));
        assert!(!robots.is_path_allowed("/backup/data", "*"));
    }

    #[test]
    fn test_robots_txt_empty_disallow() {
        let content = r#"
User-agent: *
Disallow:
Allow: /everything/
"#;

        let robots = RobotsTxt::new(content, "TestBot/1.0");

        assert!(robots.is_path_allowed("/anything", "*"));
    }

    #[test]
    fn test_robots_txt_default_behavior() {
        let robots = RobotsTxt::default();

        assert!(robots.is_path_allowed("/anything", "TestBot"));
    }

    #[test]
    fn test_robots_txt_url_parsing() {
        let content = r#"
User-agent: *
Disallow: /private/
"#;

        let robots = RobotsTxt::new(content, "TestBot/1.0");

        assert!(!robots.is_allowed("https://example.com/private/secret", "TestBot"));
        assert!(robots.is_allowed("https://example.com/public/info", "TestBot"));
    }
}
