#!/bin/bash
# run_recovery_scenarios.sh — Measure recovery time under 3 scenarios
#
# Scenario 1: Insert WITHOUT crash → recovery time
#   (clean pmemInodePool load + convert to mainIndex, no log replay needed)
#
# Scenario 2: Insert WITH crash → recovery WITHOUT log replay
#   (insert with ENABLE_LOG_REPLAY=1 so WAL is written, then rebuild
#    with ENABLE_LOG_REPLAY=0 for recovery — same crash state as S3)
#
# Scenario 3: Insert WITH crash → recovery WITH log replay
#   (build with ENABLE_LOG_REPLAY=1, pmemInodePool copy + WAL log replay)
#
# Usage: bash run_recovery_scenarios.sh <threads> <pmem_path> [crash_after_sec]

set -uo pipefail

THREADS=${1:?Usage: $0 <threads> <pmem_path> [crash_after_sec]}
PMEM=${2:?Usage: $0 <threads> <pmem_path> [crash_after_sec]}
CRASH_AFTER=${3:-30}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Kill any stale project processes
pkill -9 -f './project' 2>/dev/null || true
sleep 2

RESULT_DIR="$SCRIPT_DIR/recovery_scenario_results"
mkdir -p "$RESULT_DIR"
TS=$(date +%Y%m%d_%H%M%S)
CSV="$RESULT_DIR/recovery_scenarios_${TS}.csv"

echo "SCENARIO,ENABLE_LOG_REPLAY,CRASH,INODE_COUNT,VNODE_COUNT,VNODE_LOAD_MS,BLOOM_REBUILD_MS,INODE_COPY_MS,MAININDEX_REBUILD_MS,BASE_RECOVERY_MS,TOTAL_RECOVERY_MS,LOG_REPLAY_MS,LOG_ENTRIES,YCSB_C_THROUGHPUT,YCSB_C_ELAPSED" > "$CSV"

# ---- Helper Functions ----

clean_pmem() {
    rm -f "${PMEM}"/pmemBFPool "${PMEM}"/pmemVnodePool "${PMEM}"/pmemInodePool "${PMEM}"/ckpt_log 2>/dev/null || true
}

build_project() {
    local log_replay="$1"
    echo "  Building with ENABLE_LOG_REPLAY=${log_replay}..."
    make clean -s 2>/dev/null
    if ! make -j"$(nproc)" -s \
        ENABLE_DT_ONLY_RECLAIM=1 \
        ENABLE_LOG_REPLAY="${log_replay}" \
        CXXFLAGS_EXTRA="-DDIVERGENCE_THRESHOLD=1.01 -DPERSISTENT_THRESHOLD=8388608 -g"; then
        echo "  BUILD FAILED"
        return 1
    fi
    echo "  Build succeeded"
    return 0
}

wait_for_pools() {
    local pid="$1"
    local log="$2"
    local pool_wait=0
    while ! grep -q "Populating" "$log" 2>/dev/null; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "  ERROR: process died during pool creation"
            return 1
        fi
        sleep 2
        pool_wait=$((pool_wait + 2))
        if [ $pool_wait -ge 300 ]; then
            echo "  ERROR: pool creation timeout (${pool_wait}s)"
            kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
            return 1
        fi
    done
    echo "  Pools ready (~${pool_wait}s)"
    return 0
}

# Wait for a background process to finish, with timeout
wait_with_timeout() {
    local pid="$1"
    local timeout="$2"
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge "$timeout" ]; then
            echo "  TIMEOUT: process didn't finish in ${timeout}s"
            kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
            return 1
        fi
    done
    wait "$pid" 2>/dev/null || true
    return 0
}

parse_and_record() {
    local scenario="$1"
    local log_replay_flag="$2"
    local crashed="$3"
    local recovery_log="$4"

    # [Recovery] Copied 19977 inodes from PMem to DRAM
    local inode_count
    inode_count=$(grep -oP 'Copied \K\d+' "$recovery_log" | head -1)

    # [Init] pmemVnodePool currentIdx=1909376
    local vnode_count
    vnode_count=$(grep -oP 'currentIdx=\K\d+' "$recovery_log" | head -1)

    # [Recovery] vnode_load_time: 1234.567 ms
    local vnode_load_ms
    vnode_load_ms=$(grep -oP 'vnode_load_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery] bloom_rebuild_time: 1234.567 ms
    local bloom_rebuild_ms
    bloom_rebuild_ms=$(grep -oP 'bloom_rebuild_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery] inode_copy_time: 12.345 ms
    local inode_copy_ms
    inode_copy_ms=$(grep -oP 'inode_copy_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery] mainindex_rebuild_time: 5.678 ms
    local mainindex_rebuild_ms
    mainindex_rebuild_ms=$(grep -oP 'mainindex_rebuild_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery] base_recovery_time: 1252.590 ms (vnode_load=..., inode_copy=..., mainindex_rebuild=...)
    local base_recovery_ms
    base_recovery_ms=$(grep -oP 'base_recovery_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery]   total_recovery_time: 5647.318 ms  (includes log replay)
    local total_recovery_ms
    total_recovery_ms=$(grep -oP 'total_recovery_time: \K[0-9.]+' "$recovery_log" | head -1)

    # [Recovery]   log_replay: 42 entries (1024 bytes), 12.345 ms
    local log_replay_ms
    log_replay_ms=$(grep 'log_replay:' "$recovery_log" | grep -oP '[0-9.]+ ms' | head -1 | grep -oP '[0-9.]+')

    local log_entries
    log_entries=$(grep 'log_replay:' "$recovery_log" | grep -oP '\d+ entries' | head -1 | grep -oP '\d+')

    # YCSB_C throughput 1.67897e+07
    local ycsb_c_tput
    ycsb_c_tput=$(grep 'YCSB_C throughput' "$recovery_log" | awk '{print $NF}')

    # Elapsed_time 5.95604
    local ycsb_c_elapsed
    ycsb_c_elapsed=$(grep 'Elapsed_time' "$recovery_log" | awk '{print $NF}')

    inode_count=${inode_count:-0}
    vnode_count=${vnode_count:-0}
    vnode_load_ms=${vnode_load_ms:-0}
    bloom_rebuild_ms=${bloom_rebuild_ms:-0}
    inode_copy_ms=${inode_copy_ms:-0}
    mainindex_rebuild_ms=${mainindex_rebuild_ms:-0}
    base_recovery_ms=${base_recovery_ms:-0}
    log_replay_ms=${log_replay_ms:-0}
    total_recovery_ms=$(awk "BEGIN {printf \"%.3f\", ${base_recovery_ms} + ${log_replay_ms}}")
    log_entries=${log_entries:-0}
    ycsb_c_tput=${ycsb_c_tput:-0}
    ycsb_c_elapsed=${ycsb_c_elapsed:-0}

    echo "${scenario},${log_replay_flag},${crashed},${inode_count},${vnode_count},${vnode_load_ms},${bloom_rebuild_ms},${inode_copy_ms},${mainindex_rebuild_ms},${base_recovery_ms},${total_recovery_ms},${log_replay_ms},${log_entries},${ycsb_c_tput},${ycsb_c_elapsed}" >> "$CSV"

    echo "  --- Parsed Results ---"
    echo "    Inodes copied from PMem: ${inode_count}"
    echo "    Vnode pool size:         ${vnode_count}"
    echo "    Vnode load time:         ${vnode_load_ms} ms"
    echo "    Bloom rebuild time:      ${bloom_rebuild_ms} ms"
    echo "    Inode copy time:         ${inode_copy_ms} ms"
    echo "    MainIndex rebuild time:  ${mainindex_rebuild_ms} ms"
    echo "    Base recovery time:      ${base_recovery_ms} ms (vnode+inode+mainindex)"
    echo "    Total recovery time:     ${total_recovery_ms} ms (base + log replay)"
    echo "    Log replay time:         ${log_replay_ms} ms"
    echo "    Log entries replayed:    ${log_entries}"
    echo "    YCSB_C throughput:       ${ycsb_c_tput} ops/s"
    echo "    YCSB_C elapsed:          ${ycsb_c_elapsed} s"

    # Print key recovery lines for debugging
    echo "  --- Recovery Log Excerpts ---"
    grep -E '\[Recovery\]|\[Init\]' "$recovery_log" | head -25 || true
}

# ==================================================================
# Scenario 1: Insert WITHOUT crash → clean recovery
#   Build with ENABLE_LOG_REPLAY=1 to get internal timing.
#   Insert completes fully, then restart to measure recovery time.
#   Since there was no crash, log replay finds a clean log → skip.
#   This measures pure pmemInodePool load + mainIndex rebuild time.
# ==================================================================
scenario1() {
    echo ""
    echo "================================================================="
    echo "Scenario 1: Insert WITHOUT crash → clean recovery"
    echo "  (pmemInodePool load + mainIndex rebuild, no log replay needed)"
    echo "================================================================="

    local insert_log="$RESULT_DIR/s1_insert_${TS}.log"
    local recovery_log="$RESULT_DIR/s1_recovery_${TS}.log"

    clean_pmem

    # Build with ENABLE_LOG_REPLAY=1 for internal timing output
    if ! build_project 1; then
        echo "1_no_crash,1,no,BUILD_FAIL,,,,,,," >> "$CSV"
        return
    fi

    # Phase 1: Full insert (no crash, wait for completion)
    echo "  Phase 1: Full insert (--insert-only, waiting for completion)..."
    ./project a zipf "${THREADS}" "${PMEM}" --insert-only > "$insert_log" 2>&1
    local RET=$?

    if [ $RET -ne 0 ]; then
        echo "  ERROR: Insert failed (exit code ${RET})"
        tail -10 "$insert_log"
        echo "1_no_crash,1,no,INSERT_FAIL,,,,,,," >> "$CSV"
        return
    fi

    if [ ! -f "${PMEM}/pmemInodePool" ]; then
        echo "  ERROR: PMem pools missing after insert"
        echo "1_no_crash,1,no,NO_POOLS,,,,,,," >> "$CSV"
        return
    fi

    local insert_tput
    insert_tput=$(grep 'YCSB_INSERT throughput' "$insert_log" | awk '{print $NF}')
    echo "  Phase 1: Insert completed (throughput: ${insert_tput:-?} ops/s)"

    # Phase 2: Restart → recovery + YCSB_C
    echo "  Phase 2: Recovery + YCSB_C..."
    ./project c zipf "${THREADS}" "${PMEM}" > "$recovery_log" 2>&1 &
    local RECOVERY_PID=$!

    if ! wait_with_timeout "$RECOVERY_PID" 600; then
        echo "1_no_crash,1,no,TIMEOUT,,,,,,,,,," >> "$CSV"
        return
    fi

    parse_and_record "1_no_crash" "1" "no" "$recovery_log"

    echo "  Logs: $insert_log  $recovery_log"
    clean_pmem
}

# ==================================================================
# Scenario 2: Insert WITH crash → recovery WITHOUT log replay
#   Phase 1: Build with ENABLE_LOG_REPLAY=1 for the INSERT phase,
#            so WAL entries ARE written (same crash state as Scenario 3).
#   Phase 2: Rebuild with ENABLE_LOG_REPLAY=0 for the RECOVERY phase,
#            so the WAL exists on PMem but is NOT replayed.
#   This isolates the effect of log replay by comparing with Scenario 3.
# ==================================================================
scenario2() {
    echo ""
    echo "================================================================="
    echo "Scenario 2: Insert WITH crash → recovery WITHOUT log replay"
    echo "  (Insert with LOG_REPLAY=1, Recovery with LOG_REPLAY=0)"
    echo "================================================================="

    local insert_log="$RESULT_DIR/s2_insert_${TS}.log"
    local recovery_log="$RESULT_DIR/s2_recovery_${TS}.log"

    clean_pmem

    # Build WITH log replay for insert phase (so WAL is written to PMem)
    echo "  [Phase 1 build] ENABLE_LOG_REPLAY=1 (WAL will be written)"
    if ! build_project 1; then
        echo "2_crash_no_replay,0,yes,BUILD_FAIL,,,,,,," >> "$CSV"
        return
    fi

    # Phase 1: Insert + crash (with WAL logging active)
    echo "  Phase 1: Insert + crash after ${CRASH_AFTER}s..."
    > "$insert_log"
    ./project a zipf "${THREADS}" "${PMEM}" --insert-only > "$insert_log" 2>&1 &
    local PID=$!

    if ! wait_for_pools "$PID" "$insert_log"; then
        echo "2_crash_no_replay,0,yes,POOL_FAIL,,,,,,," >> "$CSV"
        return
    fi

    echo "  Inserting for ${CRASH_AFTER}s..."
    sleep "${CRASH_AFTER}"

    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID" 2>/dev/null
        wait "$PID" 2>/dev/null || true
        echo "  Crashed via kill -9"
    else
        echo "  WARNING: process exited before crash point"
    fi

    if [ ! -f "${PMEM}/pmemInodePool" ]; then
        echo "  ERROR: PMem pools missing after crash"
        echo "2_crash_no_replay,0,yes,NO_POOLS,,,,,,," >> "$CSV"
        return
    fi

    # Rebuild WITHOUT log replay for recovery phase
    echo "  [Phase 2 build] ENABLE_LOG_REPLAY=0 (WAL will NOT be replayed)"
    if ! build_project 0; then
        echo "2_crash_no_replay,0,yes,BUILD_FAIL,,,,,,," >> "$CSV"
        return
    fi

    # Phase 2: Recovery (no log replay) + YCSB_C
    echo "  Phase 2: Recovery (no log replay) + YCSB_C..."
    ./project c zipf "${THREADS}" "${PMEM}" > "$recovery_log" 2>&1 &
    local RECOVERY_PID=$!

    if ! wait_with_timeout "$RECOVERY_PID" 600; then
        echo "2_crash_no_replay,0,yes,TIMEOUT,,,,,,,,,," >> "$CSV"
        return
    fi

    parse_and_record "2_crash_no_replay" "0" "yes" "$recovery_log"

    echo "  Logs: $insert_log  $recovery_log"
    clean_pmem
}

# ==================================================================
# Scenario 3: Insert WITH crash → recovery WITH log replay
#   Build with ENABLE_LOG_REPLAY=1 (WAL logging + replay on recovery).
#   Insert runs for CRASH_AFTER seconds, then kill -9.
#   Recovery does pmemInodePool copy + WAL log replay.
#   Internal total_recovery_time available from program output.
# ==================================================================
scenario3() {
    echo ""
    echo "================================================================="
    echo "Scenario 3: Insert WITH crash → recovery WITH log replay"
    echo "  (ENABLE_LOG_REPLAY=1, pmemInodePool copy + WAL replay)"
    echo "================================================================="

    local insert_log="$RESULT_DIR/s3_insert_${TS}.log"
    local recovery_log="$RESULT_DIR/s3_recovery_${TS}.log"

    clean_pmem

    # Build WITH log replay
    if ! build_project 1; then
        echo "3_crash_with_replay,1,yes,BUILD_FAIL,,,,,,," >> "$CSV"
        return
    fi

    # Phase 1: Insert + crash
    echo "  Phase 1: Insert + crash after ${CRASH_AFTER}s..."
    > "$insert_log"
    ./project a zipf "${THREADS}" "${PMEM}" --insert-only > "$insert_log" 2>&1 &
    local PID=$!

    if ! wait_for_pools "$PID" "$insert_log"; then
        echo "3_crash_with_replay,1,yes,POOL_FAIL,,,,,,," >> "$CSV"
        return
    fi

    echo "  Inserting for ${CRASH_AFTER}s..."
    sleep "${CRASH_AFTER}"

    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID" 2>/dev/null
        wait "$PID" 2>/dev/null || true
        echo "  Crashed via kill -9"
    else
        echo "  WARNING: process exited before crash point"
    fi

    if [ ! -f "${PMEM}/pmemInodePool" ]; then
        echo "  ERROR: PMem pools missing after crash"
        echo "3_crash_with_replay,1,yes,NO_POOLS,,,,,,," >> "$CSV"
        return
    fi

    # Phase 2: Recovery (with log replay) + YCSB_C
    echo "  Phase 2: Recovery (with log replay) + YCSB_C..."
    ./project c zipf "${THREADS}" "${PMEM}" > "$recovery_log" 2>&1 &
    local RECOVERY_PID=$!

    if ! wait_with_timeout "$RECOVERY_PID" 600; then
        echo "3_crash_with_replay,1,yes,TIMEOUT,,,,,,,,,," >> "$CSV"
        return
    fi

    parse_and_record "3_crash_with_replay" "1" "yes" "$recovery_log"

    echo "  Logs: $insert_log  $recovery_log"
    clean_pmem
}

# ==================================================================
# Main
# ==================================================================
echo "================================================================="
echo "Recovery Scenario Test"
echo "  Threads:     ${THREADS}"
echo "  PMem path:   ${PMEM}"
echo "  Crash after: ${CRASH_AFTER}s (for scenarios 2 & 3)"
echo "  Timestamp:   ${TS}"
echo "  Results dir: ${RESULT_DIR}"
echo "================================================================="

scenario1
scenario2
scenario3

echo ""
echo "================================================================="
echo "All 3 scenarios complete. Summary:"
echo "================================================================="
column -t -s',' "$CSV"
echo ""
echo "CSV: $CSV"
echo "Detailed logs: $RESULT_DIR/"
