#!/bin/bash
# Comprehensive stress test with real-world challenging sites
# Tests bot detection, rate limiting, queue management, and memory stability

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Real-World Stress Test Suite${NC}"
echo -e "${BLUE}Testing: Bot Detection, Rate Limits, Queue Growth${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Build release binary
echo -e "${YELLOW}Building release binary...${NC}"
cargo build --release

BINARY="./target/release/rust_sitemap"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="/tmp/stress_test_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo -e "${GREEN}Results directory: ${RESULTS_DIR}${NC}"
echo ""

# Test configuration
MAX_URLS=5000
WORKERS=256
TIMEOUT=600  # 10 minutes per test

# Function to run a single test with monitoring
run_test() {
    local TEST_NAME="$1"
    local START_URL="$2"
    local TEST_DIR="${RESULTS_DIR}/${TEST_NAME}"

    mkdir -p "$TEST_DIR"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Test: ${TEST_NAME}${NC}"
    echo -e "${BLUE}URL: ${START_URL}${NC}"
    echo -e "${BLUE}========================================${NC}"

    # Start memory monitoring in background
    MONITOR_PID=""
    (
        while true; do
            ps aux | grep "rust_sitemap crawl" | grep -v grep | awk '{print $6}' >> "${TEST_DIR}/memory_rss.log" 2>/dev/null || true
            sleep 2
        done
    ) &
    MONITOR_PID=$!

    # Run the crawl with timeout
    local START_TIME=$(date +%s)

    timeout ${TIMEOUT} ${BINARY} crawl \
        --start-url "${START_URL}" \
        --data-dir "${TEST_DIR}/data" \
        --max-urls ${MAX_URLS} \
        --workers ${WORKERS} \
        2>&1 | tee "${TEST_DIR}/output.log" || true

    local END_TIME=$(date +%s)
    local DURATION=$((END_TIME - START_TIME))

    # Stop memory monitoring
    if [ -n "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
    fi

    # Analyze results
    echo ""
    echo -e "${YELLOW}Test Summary for ${TEST_NAME}:${NC}"
    echo "Duration: ${DURATION}s"

    # Count results from crawler state
    local URL_COUNT=$(grep -c "Progress:" "${TEST_DIR}/output.log" | tail -1 || echo "0")
    if grep "Progress:" "${TEST_DIR}/output.log" > /dev/null 2>&1; then
        local PROCESSED=$(grep "Progress:" "${TEST_DIR}/output.log" | tail -1 | grep -oP '\d+ total' | grep -oP '\d+' || echo "0")
        echo "URLs processed: ${PROCESSED}"
        if [ "$DURATION" -gt 0 ]; then
            echo "Throughput: $((PROCESSED / DURATION)) URLs/sec"
        fi
    else
        echo "URLs processed: 0 (no progress logs)"
    fi

    # Memory stats
    if [ -f "${TEST_DIR}/memory_rss.log" ]; then
        local MAX_MEM=$(sort -n "${TEST_DIR}/memory_rss.log" | tail -1)
        local AVG_MEM=$(awk '{sum+=$1} END {print sum/NR}' "${TEST_DIR}/memory_rss.log")
        echo "Peak memory: $((MAX_MEM / 1024)) MB"
        echo "Avg memory: $((${AVG_MEM%.*} / 1024)) MB"
    fi

    # Error analysis
    local ERRORS_403=$(grep -c "403" "${TEST_DIR}/output.log" || echo "0")
    local ERRORS_429=$(grep -c "429" "${TEST_DIR}/output.log" || echo "0")
    local ERRORS_TIMEOUT=$(grep -c "timeout" "${TEST_DIR}/output.log" || echo "0")
    local ERRORS_BLOCKED=$(grep -c "blocked\|Blocked\|BLOCKED" "${TEST_DIR}/output.log" || echo "0")
    local CHANNEL_ERRORS=$(grep -c "channel closed" "${TEST_DIR}/output.log" || echo "0")

    echo ""
    echo -e "${YELLOW}Error Analysis:${NC}"
    echo "403 Forbidden: ${ERRORS_403}"
    echo "429 Rate Limited: ${ERRORS_429}"
    echo "Timeouts: ${ERRORS_TIMEOUT}"
    echo "Blocked hosts: ${ERRORS_BLOCKED}"
    echo "Channel errors: ${CHANNEL_ERRORS}"

    # Queue analysis
    local FINAL_QUEUED=$(grep "queued URLs" "${TEST_DIR}/output.log" | tail -1 | grep -oP '\d+ queued' | grep -oP '\d+' || echo "0")
    local FINAL_WITH_WORK=$(grep "with work" "${TEST_DIR}/output.log" | tail -1 | grep -oP '\d+ with work' | grep -oP '\d+' || echo "0")

    echo ""
    echo -e "${YELLOW}Queue Status:${NC}"
    echo "Final queued: ${FINAL_QUEUED}"
    echo "Hosts with work: ${FINAL_WITH_WORK}"

    if [ "${FINAL_QUEUED}" -gt "$((MAX_URLS * 10))" ]; then
        echo -e "${RED}WARNING: Queue explosion detected! (${FINAL_QUEUED} > $((MAX_URLS * 10)))${NC}"
    fi

    echo ""
    echo -e "${GREEN}Test ${TEST_NAME} complete${NC}"
    echo ""
}

# Test Suite: Bot Detection & Rate Limiting
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}PHASE 1: Bot Detection & Rate Limiting${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Test 1: Reddit (aggressive rate limiting)
run_test "reddit" "https://www.reddit.com/r/programming"

# Test 2: Medium (Cloudflare protection)
run_test "medium" "https://medium.com"

# Test 3: HackerNews (high link density, simple structure)
run_test "hackernews" "https://news.ycombinator.com"

# Test Suite: High Volume Sites
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}PHASE 2: High Volume Queue Management${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Test 4: Wikipedia (known URL explosion)
run_test "wikipedia" "https://en.wikipedia.org/wiki/Web_crawler"

# Test 5: Stack Overflow (high internal link density)
run_test "stackoverflow" "https://stackoverflow.com/questions/tagged/rust"

# Test Suite: Combined Stress
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}PHASE 3: Combined Stress Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Test 6: Python Docs (moderate difficulty, good baseline)
run_test "python_docs" "https://docs.python.org/3/"

# Generate final report
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}FINAL REPORT${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

REPORT_FILE="${RESULTS_DIR}/STRESS_TEST_REPORT.txt"

cat > "${REPORT_FILE}" <<EOF
Stress Test Report
Generated: $(date)
Results Directory: ${RESULTS_DIR}

========================================
TEST RESULTS SUMMARY
========================================

EOF

for TEST_DIR in "${RESULTS_DIR}"/*; do
    if [ -d "$TEST_DIR" ]; then
        TEST_NAME=$(basename "$TEST_DIR")
        echo "Test: ${TEST_NAME}" >> "${REPORT_FILE}"

        if [ -f "${TEST_DIR}/output.log" ]; then
            # Success rate
            PROCESSED=$(grep "Progress:" "${TEST_DIR}/output.log" | tail -1 | grep -oP '\d+ total' | grep -oP '\d+' || echo "0")
            SUCCESS=$(grep "Progress:" "${TEST_DIR}/output.log" | tail -1 | grep -oP '\d+ success' | grep -oP '\d+' || echo "0")

            if [ "$PROCESSED" -gt 0 ]; then
                SUCCESS_RATE=$(awk "BEGIN {printf \"%.1f\", ($SUCCESS / $PROCESSED) * 100}")
                echo "  Success Rate: ${SUCCESS_RATE}%" >> "${REPORT_FILE}"
            fi

            # Memory peak
            if [ -f "${TEST_DIR}/memory_rss.log" ]; then
                MAX_MEM=$(sort -n "${TEST_DIR}/memory_rss.log" | tail -1)
                echo "  Peak Memory: $((MAX_MEM / 1024)) MB" >> "${REPORT_FILE}"
            fi

            # Errors
            ERRORS_403=$(grep -c "403" "${TEST_DIR}/output.log" || echo "0")
            ERRORS_429=$(grep -c "429" "${TEST_DIR}/output.log" || echo "0")
            echo "  Rate Limited (429): ${ERRORS_429}" >> "${REPORT_FILE}"
            echo "  Forbidden (403): ${ERRORS_403}" >> "${REPORT_FILE}"

        fi
        echo "" >> "${REPORT_FILE}"
    fi
done

echo ""
cat "${REPORT_FILE}"
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Stress test complete!${NC}"
echo -e "${GREEN}Results: ${RESULTS_DIR}${NC}"
echo -e "${GREEN}Report: ${REPORT_FILE}${NC}"
echo -e "${GREEN}========================================${NC}"
