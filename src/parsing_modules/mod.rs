//! Platform-specific parsing modules for extracting structured data.
//!
//! This module contains parsers that extract data from platform-specific
//! "hidden APIs" - embedded JSON data that websites use internally for
//! client-side rendering. These parsers are much faster and more reliable
//! than CSS selectors because they access the source of truth directly.
//!
//! # Architecture
//!
//! Each parser is tech-stack specific and knows how to extract the JSON
//! payload that the platform uses for hydration/rendering:
//!
//! - **Next.js**: Extracts `__NEXT_DATA__` script block
//! - **Shopify**: Extracts product JSON from `.json` endpoints
//! - **Nuxt.js**: Extracts `__NUXT__` window variable
//! - etc.
//!
//! These parsers are called in the tiered extraction pipeline (Tier 2),
//! after schema-based extraction (Tier 1) but before CSS selectors (Tier 3).

pub mod nextjs_parser;
pub mod shopify_parser;
