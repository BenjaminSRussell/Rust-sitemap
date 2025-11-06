use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

// Benchmark mutex-based counter (current implementation)
fn bench_mutex_counter_single_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("counter_single_threaded");

    group.bench_function("mutex", |b| {
        let counter = Mutex::new(0u64);
        b.iter(|| {
            for _ in 0..1000 {
                *counter.lock() += 1;
            }
        });
    });

    group.bench_function("atomic", |b| {
        let counter = AtomicU64::new(0);
        b.iter(|| {
            for _ in 0..1000 {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    group.finish();
}

// Benchmark multi-threaded contention (realistic scenario)
fn bench_mutex_counter_multi_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("counter_multi_threaded");

    for num_threads in [2, 4, 8, 16] {
        group.throughput(Throughput::Elements(num_threads as u64 * 1000));

        group.bench_with_input(BenchmarkId::new("mutex", num_threads), &num_threads, |b, &num_threads| {
            let counter = Arc::new(Mutex::new(0u64));
            b.iter(|| {
                let mut handles = vec![];
                for _ in 0..num_threads {
                    let counter_clone = Arc::clone(&counter);
                    let handle = thread::spawn(move || {
                        for _ in 0..1000 {
                            *counter_clone.lock() += 1;
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("atomic", num_threads), &num_threads, |b, &num_threads| {
            let counter = Arc::new(AtomicU64::new(0));
            b.iter(|| {
                let mut handles = vec![];
                for _ in 0..num_threads {
                    let counter_clone = Arc::clone(&counter);
                    let handle = thread::spawn(move || {
                        for _ in 0..1000 {
                            counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

// Benchmark histogram operations (mixed read/write)
fn bench_histogram(c: &mut Criterion) {
    use std::time::Duration;

    #[derive(Debug, Clone)]
    struct Histogram {
        buckets: Vec<(u64, u64)>,
        sum_ms: u64,
        count: u64,
    }

    impl Histogram {
        fn new() -> Self {
            Self {
                buckets: vec![
                    (1, 0), (5, 0), (10, 0), (50, 0),
                    (100, 0), (500, 0), (1000, 0), (5000, 0),
                ],
                sum_ms: 0,
                count: 0,
            }
        }

        fn observe(&mut self, value_ms: u64) {
            self.sum_ms += value_ms;
            self.count += 1;
            for (threshold, count) in &mut self.buckets {
                if value_ms <= *threshold {
                    *count += 1;
                    break;
                }
            }
        }
    }

    let mut group = c.benchmark_group("histogram");

    group.bench_function("mutex_observe", |b| {
        let hist = Mutex::new(Histogram::new());
        b.iter(|| {
            for i in 0..100 {
                hist.lock().observe(black_box(i % 100));
            }
        });
    });

    group.finish();
}

// Benchmark realistic metrics usage pattern
fn bench_realistic_metrics_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_pattern");

    // Simulate: fetch URL, update 3 metrics (fetched, processed, latency)
    group.bench_function("mutex_metrics", |b| {
        let urls_fetched = Arc::new(Mutex::new(0u64));
        let urls_processed = Arc::new(Mutex::new(0u64));
        let latency_sum = Arc::new(Mutex::new(0u64));

        b.iter(|| {
            // Simulate processing 100 URLs
            for _ in 0..100 {
                *urls_fetched.lock() += 1;
                *latency_sum.lock() += black_box(50); // 50ms latency
                *urls_processed.lock() += 1;
            }
        });
    });

    group.bench_function("atomic_metrics", |b| {
        let urls_fetched = Arc::new(AtomicU64::new(0));
        let urls_processed = Arc::new(AtomicU64::new(0));
        let latency_sum = Arc::new(AtomicU64::new(0));

        b.iter(|| {
            // Simulate processing 100 URLs
            for _ in 0..100 {
                urls_fetched.fetch_add(1, Ordering::Relaxed);
                latency_sum.fetch_add(black_box(50), Ordering::Relaxed);
                urls_processed.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mutex_counter_single_threaded,
    bench_mutex_counter_multi_threaded,
    bench_histogram,
    bench_realistic_metrics_pattern
);
criterion_main!(benches);
