# Rate Limiting & Deduplication Improvements

## Problem Summary

The crawler was experiencing severe issues when encountering slow or timing-out domains:

1. **Repeated URL Processing**: Workers would process the same URLs multiple times when rate-limited
2. **Domain Hammering**: All 256 workers could hit the same slow domain simultaneously  
3. **Channel Bloat**: 512K channel capacity filled with URLs from failing domains
4. **No Backoff Strategy**: Failed domains were retried immediately with no cooldown
5. **Task Explosion**: Unlimited background tasks could be spawned per domain

## Solutions Implemented

### 1. Per-Domain Rate Limiting ✅

**Before**: Global rate limit only (200 req/s across all domains)
**After**: Max 5 concurrent requests per domain + global limit

```rust
struct DomainState {
    concurrent_requests: usize,  // Track active requests per domain
    // ...
}

// Check before processing
if !failure_tracker.can_request(&url_domain) {
    // Put URL back in queue for later
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = url_sender.send(queued_url).await;
    continue;
}
```

**Impact**: Prevents hammering slow domains with hundreds of simultaneous requests

### 2. Exponential Backoff with Cooldown ✅

**Before**: Domains skipped only after 10 failures, no backoff
**After**: Exponential backoff (2^failures seconds) + skip after 5 failures

```rust
struct DomainState {
    failures: usize,
    backoff_until: Instant,  // Cooldown period
    // ...
}

fn record_failure(&self, domain: &str) {
    // Exponential backoff: 2, 4, 8, 16, 32, 64, 128, 256, 300 seconds (max)
    let backoff_secs = (2_u32.pow(failures.min(8) as u32)).min(300);
    state.backoff_until = now + Duration::from_secs(backoff_secs);
}
```

**Impact**: 
- After 1st failure: 2s cooldown
- After 2nd failure: 4s cooldown
- After 3rd failure: 8s cooldown
- After 5th failure: Permanently blocked

### 3. Reduced Channel Capacity ✅

**Before**: 512,000 URLs (256 workers × 2000)
**After**: 50,000 URLs (256 workers × 100, min 50K)

```rust
let channel_capacity = (config.max_workers as usize * 100).max(50_000);
```

**Impact**: 
- Faster feedback when domains are slow
- Prevents queue bloat with URLs from failing domains
- Workers notice failures sooner and stop queueing

### 4. Lower Failure Threshold ✅

**Before**: Skip domain after 10 failures
**After**: Skip domain after 5 failures

```rust
let failure_tracker = DomainFailureTracker::new(
    5,  // failures before permanent skip
    5   // max concurrent per domain
);
```

**Impact**: Failing domains are blocked faster, freeing resources

### 5. Permit Management with Cleanup ✅

**Before**: No per-domain permit tracking
**After**: Acquire/release domain permits with guaranteed cleanup

```rust
// Acquire permit before processing
if !failure_tracker.acquire_request(&url_domain) {
    // Re-queue if can't acquire
    let _ = url_sender.send(queued_url).await;
    continue;
}

// In background task
tokio::spawn(async move {
    let result = process_url(...).await;
    
    // Process result...
    
    // Always release permit (success or failure)
    failure_tracker.release_request(&url_domain);
});
```

**Impact**: Prevents permit leaks, ensures domain limits are respected

### 6. Better Error Messages ✅

**Before**: Generic error messages
**After**: Categorized errors with icons

```rust
let error_msg = if e.contains("timeout") {
    format!("⏱️  Timeout")
} else if e.contains("DNS") {
    format!("🌐 DNS error")
} else if e.contains("Content too large") {
    format!("📦 Content too large")
} else if e.contains("record overflow") {
    format!("❌ Network error")
}
```

**Impact**: Easier to identify patterns in failures

### 7. URL Deduplication (Already Existed, Clarified) ✅

The code already had proper deduplication before queueing:

```rust
// Check bloom filter + sled DB before queueing
if !node_map_bg.contains(&absolute_url) {
    if let Ok(added) = node_map_bg.add_url(...) {
        if added {
            // Only queue if truly new
            urls_to_queue.push(absolute_url);
        }
    }
}
```

**Impact**: No duplicate URLs enter the channel

## Performance Comparison

### Before
```
256 workers × slow domain = 256 simultaneous requests
→ Rate limiting kicks in
→ 512K channel fills with slow domain URLs
→ Workers keep processing same URLs
→ Timeouts cascade
```

### After
```
Max 5 workers per domain (even if it's slow)
→ After 1st timeout: 2s backoff
→ After 2nd timeout: 4s backoff
→ After 5th timeout: permanently blocked
→ 50K channel means faster feedback
→ Workers move on to healthy domains
```

## Configuration

Current settings (tunable in `bfs_crawler.rs`):

```rust
// Channel capacity
let channel_capacity = (max_workers * 100).max(50_000);  // 50K default

// Domain limits
DomainFailureTracker::new(
    5,  // Max failures before permanent block
    5   // Max concurrent requests per domain
);
```

### Tuning Recommendations

- **For very reliable domains**: Increase `max_concurrent_per_domain` to 10-20
- **For unreliable networks**: Decrease `failure_threshold` to 3
- **For memory-constrained systems**: Reduce `channel_capacity` to 25K
- **For high-memory systems**: Can increase to 100K (but probably not needed)

## Testing Recommendations

1. **Test with mixed domains**: Some fast, some slow, some failing
2. **Monitor domain states**: Add logging to see concurrent request counts
3. **Watch channel size**: Should stay well below capacity
4. **Verify backoff**: Confirm failed domains get exponentially increasing cooldowns
5. **Check permit cleanup**: Ensure concurrent_requests doesn't grow unbounded

## Future Improvements (Optional)

1. **Per-domain timeout**: Allow faster timeouts for known-slow domains
2. **Domain health metrics**: Track average response time per domain
3. **Adaptive rate limiting**: Increase limits for fast domains, decrease for slow
4. **Persistent domain state**: Remember failed domains across restarts
5. **Domain priority queue**: Process healthy domains before recovering ones

## Metrics to Monitor

When running the crawler, watch for:

```
✅ Processed [url] - Success rate
⏱️  Timeout - Timeout rate per domain
⛔ Skipping domain [domain] (failed 5 times) - Blocked domains
Channel queue: [size] - Should stay < 50K
concurrent_requests - Should stay ≤ 5 per domain
```

## Summary

These changes transform the crawler from a "hammer everything" approach to an intelligent, adaptive system that:

- **Respects slow domains** by limiting concurrent requests
- **Backs off gracefully** with exponential delays
- **Fails fast** by blocking problematic domains quickly
- **Prevents resource waste** with smaller channel buffers
- **Guarantees cleanup** with proper permit management

The crawler will now handle rate limiting and slow domains gracefully, avoiding the cascade of repeated URLs and timeouts you experienced.

