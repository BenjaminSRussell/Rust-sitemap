use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rust_sitemap::state::{CrawlerState, SitemapNode, StateEvent, StateEventWithSeqno};
use rust_sitemap::wal::SeqNo;
use tempfile::TempDir;
use std::sync::Arc;

// Benchmark O(N) node count vs O(1) metadata lookup
fn bench_node_count_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_count");

    for num_nodes in [100, 1000, 10000] {
        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        // Populate with test nodes
        let mut events = Vec::new();
        for i in 0..num_nodes {
            let url = format!("https://example.com/page{}", i);
            let node = SitemapNode::new(
                url.clone(),
                url.to_lowercase(),
                0,
                None,
                None,
            );
            events.push(StateEventWithSeqno {
                seqno: SeqNo::new(1, i as u64),
                event: StateEvent::AddNodeFact(node),
            });
        }

        // Apply events in batches
        for chunk in events.chunks(1000) {
            state.apply_event_batch(chunk).unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("current_O(N)_scan", num_nodes),
            &num_nodes,
            |b, _| {
                b.iter(|| {
                    black_box(state.get_node_count().unwrap())
                });
            },
        );
    }

    group.finish();
}

// Benchmark crawled node count (O(N) with deserialization)
fn bench_crawled_node_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("crawled_node_count");

    for num_nodes in [100, 1000, 5000] {
        let dir = TempDir::new().unwrap();
        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

        // Populate with test nodes (half crawled, half not)
        let mut events = Vec::new();
        for i in 0..num_nodes {
            let url = format!("https://example.com/page{}", i);
            let node = SitemapNode::new(
                url.clone(),
                url.to_lowercase(),
                0,
                None,
                None,
            );
            events.push(StateEventWithSeqno {
                seqno: SeqNo::new(1, i as u64 * 2),
                event: StateEvent::AddNodeFact(node),
            });

            // Mark half as crawled
            if i % 2 == 0 {
                events.push(StateEventWithSeqno {
                    seqno: SeqNo::new(1, i as u64 * 2 + 1),
                    event: StateEvent::CrawlAttemptFact {
                        url_normalized: url.to_lowercase(),
                        status_code: 200,
                        content_type: Some("text/html".to_string()),
                        content_length: Some(1024),
                        title: Some("Test Page".to_string()),
                        link_count: 10,
                        response_time_ms: Some(100),
                    },
                });
            }
        }

        // Apply events in batches
        for chunk in events.chunks(1000) {
            state.apply_event_batch(chunk).unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("current_O(N)_deserialize", num_nodes),
            &num_nodes,
            |b, _| {
                b.iter(|| {
                    black_box(state.get_crawled_node_count().unwrap())
                });
            },
        );
    }

    group.finish();
}

// Benchmark batch event application
fn bench_event_batch_application(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_batching");

    for batch_size in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("apply_batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        let state = Arc::new(CrawlerState::new(dir.path()).unwrap());

                        let mut events = Vec::new();
                        for i in 0..batch_size {
                            let url = format!("https://example.com/page{}", i);
                            let node = SitemapNode::new(
                                url.clone(),
                                url.to_lowercase(),
                                0,
                                None,
                                None,
                            );
                            events.push(StateEventWithSeqno {
                                seqno: SeqNo::new(1, i as u64),
                                event: StateEvent::AddNodeFact(node),
                            });
                        }
                        (state, events)
                    },
                    |(state, events)| {
                        black_box(state.apply_event_batch(&events).unwrap())
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// Benchmark URL normalization
fn bench_url_normalization(c: &mut Criterion) {
    let mut group = c.benchmark_group("url_normalization");

    let test_urls = vec![
        "https://example.com/page",
        "HTTPS://EXAMPLE.COM/PAGE",
        "https://example.com/page#fragment",
        "https://Example.Com/PaGe#Fragment",
    ];

    group.bench_function("normalize_repeated", |b| {
        b.iter(|| {
            for url in &test_urls {
                for _ in 0..100 {
                    black_box(SitemapNode::normalize_url(url));
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_node_count_operations,
    bench_crawled_node_count,
    bench_event_batch_application,
    bench_url_normalization
);
criterion_main!(benches);
