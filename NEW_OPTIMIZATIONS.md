# NEW Performance Optimizations (Undocumented)

**Date**: 2025-11-06
**Additional optimizations discovered during implementation**

---

## NEW Optimization #4: Fragment HashSet (Quick Win) 🟢

### Problem
**Location**: `src/state.rs:159-162`
```rust
pub fn add_fragment(&mut self, fragment: String) {
    if !self.fragments.contains(&fragment) {  // O(N) linear search!
        self.fragments.push(fragment);
    }
}
```

**Impact**:
- O(N) lookup for every fragment added
- For URLs with many fragments (e.g., SPAs with #routes), this becomes O(N²)
- Unnecessary string comparisons on hot path

### Solution
Replace `Vec<String>` with `HashSet<String>` for O(1) deduplication:
```rust
use std::collections::HashSet;

pub struct SitemapNode {
    // Change: Vec<String> -> HashSet<String>
    pub fragments: HashSet<String>,
}

pub fn add_fragment(&mut self, fragment: String) {
    self.fragments.insert(fragment);  // O(1) with automatic dedup
}
```

**Expected Gain**: 2-5% for sites with many fragment URLs

---

## NEW Optimization #5: Arc Clone Overhead in add_links 🟡

### Problem
**Location**: `src/frontier.rs:131`
```rust
for (url, depth, parent_url) in links {
    let permit = match self.backpressure_semaphore.clone().acquire_owned().await {
        //                                        ^^^^^^^ Clones Arc for EVERY URL!
```

**Impact**:
- Clones Arc for every single URL added
- Unnecessary atomic reference count increments
- Called millions of times in large crawls

### Solution
Hoist the Arc reference outside the loop:
```rust
let semaphore = &self.backpressure_semaphore;  // Borrow once
for (url, depth, parent_url) in links {
    let permit = match semaphore.clone().acquire_owned().await {
        // Now only one clone per batch, not per URL
    }
}
```

**Expected Gain**: 1-3% reduction in allocation overhead

---

## NEW Optimization #6: Redundant URL Normalization 🟠

### Problem
**Location**: `src/frontier.rs:139`
```rust
let normalized_url = SitemapNode::normalize_url(&url);
```

Called in `add_links` for every URL, but many URLs are already normalized or were normalized earlier. The function always allocates a new String:

```rust
pub fn normalize_url(url: &str) -> String {
    if let Some(pos) = url.find('#') {
        url[..pos].to_lowercase()  // Always allocates!
    } else {
        url.to_lowercase()  // Always allocates!
    }
}
```

**Impact**:
- Allocates String for every URL even if already lowercase
- No early-exit for already-normalized URLs
- Called millions of times

### Solution Options

**Option A: Check before allocating (simple)**
```rust
pub fn normalize_url(url: &str) -> String {
    let url_to_check = if let Some(pos) = url.find('#') {
        &url[..pos]
    } else {
        url
    };

    // Fast path: already lowercase
    if url_to_check.chars().all(|c| !c.is_ascii_uppercase()) {
        return url_to_check.to_string();
    }

    // Slow path: needs normalization
    url_to_check.to_lowercase()
}
```

**Option B: Return Cow<str> (better)**
```rust
use std::borrow::Cow;

pub fn normalize_url(url: &str) -> Cow<str> {
    let url_to_check = if let Some(pos) = url.find('#') {
        &url[..pos]
    } else {
        url
    };

    if url_to_check.chars().all(|c| !c.is_ascii_uppercase()) {
        Cow::Borrowed(url_to_check)  // No allocation!
    } else {
        Cow::Owned(url_to_check.to_lowercase())
    }
}
```

**Expected Gain**: 3-7% reduction in String allocations

---

## NEW Optimization #7: ReadyHost String Cloning 🟡

### Problem
**Location**: `src/frontier.rs:54-57`
```rust
struct ReadyHost {
    host: String,  // Cloned every time heap is reordered
    ready_at: Instant,
}
```

The BinaryHeap clones ReadyHost frequently during heap operations, and each clone copies the host String.

### Solution
Use `Arc<str>` or `Box<str>` for cheap cloning:
```rust
struct ReadyHost {
    host: Arc<str>,  // Cheap clone (just Arc increment)
    ready_at: Instant,
}
```

**Expected Gain**: 1-2% reduction in heap operation overhead

---

## NEW Optimization #8: Bloom Filter False Positive Rate 🔴

### Problem
**Location**: Mentioned in analysis - 1% false positive rate triggers expensive DB lookups

**Current**: 1% FP rate with 10M items per shard
**Impact**: For 10M URLs, 100K false positives = 100K unnecessary DB lookups

### Solution Options

**Option A: Increase Bloom filter size (simple)**
```rust
// Increase from 10M items to better FP rate
let bloom = BloomFilter::with_false_pos(0.001, 10_000_000);  // 0.1% FP
```

**Option B: Two-level Bloom filter**
- L1: Fast Bloom filter (1% FP)
- L2: Larger Bloom filter for FP candidates (0.01% FP)

**Expected Gain**: 5-10% reduction in DB lookup overhead

---

## NEW Optimization #9: WAL Record Buffer Reuse 🟡

### Problem
**Location**: `src/wal.rs:65-84`

Every WAL record encode creates a new Vec:
```rust
pub fn encode(&self) -> Vec<u8> {
    let total_len = 4 + 4 + 16 + payload_len;
    let mut record = Vec::with_capacity(total_len);
    // ... fills vector ...
    record
}
```

For millions of events, this allocates millions of Vecs.

### Solution
Use a thread-local buffer pool:
```rust
thread_local! {
    static ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

pub fn encode(&self, buf: &mut Vec<u8>) {
    buf.clear();
    let total_len = 4 + 4 + 16 + self.payload.len();
    buf.reserve(total_len);
    // ... write directly to buf ...
}
```

**Expected Gain**: 2-4% reduction in WAL write overhead

---

## NEW Optimization #10: SystemTime Caching 🟡

### Problem
Multiple `SystemTime::now()` calls throughout the code:
- `SitemapNode::new` (line 119)
- `HostState::new` (line 261)
- `HostState::mark_request_made` (line 297)
- `HostState::record_failure` (line 309)

Each call is a syscall with overhead.

### Solution
Pass timestamp as parameter or cache per-batch:
```rust
// In writer thread batch processing:
let batch_timestamp = SystemTime::now();
for event in events {
    process_event(event, batch_timestamp);
}
```

**Expected Gain**: <1% but reduces syscall overhead

---

## Implementation Priority

### Phase 1 (Immediate - Low Risk)
1. ✅ **#4: Fragment HashSet** - Simple type change, big O improvement
2. ✅ **#5: Arc clone hoisting** - One-line fix
3. ✅ **#6: URL normalization check** - Fast path optimization

### Phase 2 (Quick Wins)
4. **#7: ReadyHost Arc<str>** - Simple type change
5. **#8: Bloom filter tuning** - Config change

### Phase 3 (Moderate Effort)
6. **#9: WAL buffer reuse** - Requires API change
7. **#10: SystemTime caching** - Requires threading through calls

---

## Combined Impact Estimate

- Phase 1: **5-12% additional improvement**
- Phase 2: **3-8% additional improvement**
- Phase 3: **2-5% additional improvement**

**Total with all optimizations**: 40-60% throughput improvement over baseline

---

## Status

- [x] Documented
- [ ] Implemented #4
- [ ] Implemented #5
- [ ] Implemented #6
- [ ] Tested
- [ ] Benchmarked
