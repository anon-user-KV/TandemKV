#!/bin/bash
###############################################################################
# run_splitpath_stats.sh  –  Collect split-path statistics for all 8 configs
#
# For each configuration:
#   1. Build with ENABLE_SPLITPATH_STATS=1
#   2. Run insert-only with 16 threads on zipf distribution (to populate data)
#   3. Run workload A with 16 threads on zipf distribution (to collect stats)
#   4. Capture full output including [SplitPath], [RebalSGP], [Level], [ChainHop]
#
# Configurations (8 total):
#   1. TANDEMKV                             (SGP=1, FLUSH=0, coeff=3)
#   2. TANDEMKV + NO SGP                    (SGP=0, FLUSH=0, coeff=3)
#   3. TANDEMKV + IMMEDIATE PERSIST         (SGP=1, FLUSH=1, coeff=3)
#   4. TANDEMKV + NO SGP + IMM PERSIST      (SGP=0, FLUSH=1, coeff=3)
#   5. TANDEMKV + COEFFICIENT=1             (SGP=1, FLUSH=0, coeff=1)
#   6. TANDEMKV + COEFF=1 + NO SGP          (SGP=0, FLUSH=0, coeff=1)
#   7. TANDEMKV + COEFF=1 + NO SGP+IMM      (SGP=0, FLUSH=1, coeff=1)
#   8. TANDEMKV + COEFF=1 + IMM PERSIST     (SGP=1, FLUSH=1, coeff=1)
#
# Threads: 16 only
# Workload: insert-only (populate) + workload A (benchmark)
# Runs: 1 per config
###############################################################################
set -uo pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
PMEM_DIR="/mnt/pmem0"
THREADS=16
DIST="zipf"

RESULT_FILE="$PROJ_DIR/splitpath_stats_results.txt"
CSV_FILE="$PROJ_DIR/splitpath_stats_results.csv"

# ── Helpers ──────────────────────────────────────────────────────────────────

build() {              # $1=SGP $2=FLUSH $3=COEFF_ONE $4=RECLAIM
    cd "$PROJ_DIR"
    make clean >/dev/null 2>&1 || true
    if ! make -j"$(nproc)" \
            ENABLE_SGP="$1" \
            ENABLE_IMMEDIATE_FLUSH="$2" \
            ENABLE_COEFF_ONE="$3" \
            ENABLE_IMMEDIATE_RECLAIM="${4:-0}" \
            ENABLE_SPLITPATH_STATS=1 \
            2>&1 | tail -5; then
        echo "*** BUILD FAILED ***"
        return 1
    fi
}

run_insert_only() {    # $1 = dist, $2 = threads
    local dist="$1"
    local threads="$2"
    rm -rf "$PMEM_DIR"/* 2>/dev/null || true
    "$PROJ_DIR/project" a "$dist" "$threads" "$PMEM_DIR" --insert-only 2>&1
}

run_workload_a() {     # $1 = dist, $2 = threads
    local dist="$1"
    local threads="$2"
    "$PROJ_DIR/project" a "$dist" "$threads" "$PMEM_DIR" 2>&1
}

# ── Configuration table ─────────────────────────────────────────────────────
#           NAME                              SGP  FLUSH  COEFF_ONE  RECLAIM
CONFIGS=(
    "TANDEMKV                             1    0      0          0"
    "TANDEMKV+NO_SGP                      0    0      0          0"
    "TANDEMKV+IMMEDIATE_PERSIST           1    1      0          1"
    "TANDEMKV+NO_SGP+IMMEDIATE_PERSIST    0    1      0          1"
    "TANDEMKV+COEFFICIENT=1               1    0      1          0"
    "TANDEMKV+COEFF=1+NO_SGP              0    0      1          0"
    "TANDEMKV+COEFF=1+NO_SGP+IMM_PERSIST  0    1      1          1"
    "TANDEMKV+COEFF=1+IMM_PERSIST         1    1      1          1"
)

# ── Header ───────────────────────────────────────────────────────────────────
divider="================================================================================================================================="

{
echo "$divider"
echo "  Split-Path Stats Benchmark – Workload A   (threads=$THREADS, dist=$DIST)"
echo "  Phase 1: insert-only (populate), Phase 2: workload A (benchmark)"
echo "  1 run per config, ENABLE_SPLITPATH_STATS=1"
echo "  $(date)"
echo "$divider"
} | tee "$RESULT_FILE"

# Write CSV header
echo "CONFIG,INSERT_THROUGHPUT,WORKLOADA_THROUGHPUT,PATH_A,PATH_B,PATH_C,PATH_D,PATH_E,TOTAL,LOG_AVOIDED,LOG_NEEDED,AVOID_RATE" > "$CSV_FILE"

# ── Main loop ────────────────────────────────────────────────────────────────

for cfg_line in "${CONFIGS[@]}"; do
    read -r NAME SGP FLUSH COEFF_ONE RECLAIM <<< "$cfg_line"

    echo "" | tee -a "$RESULT_FILE"
    echo "$divider" | tee -a "$RESULT_FILE"
    echo ">>> Config: $NAME  (SGP=$SGP  FLUSH=$FLUSH  COEFF_ONE=$COEFF_ONE  RECLAIM=$RECLAIM)" | tee -a "$RESULT_FILE"
    echo "$divider" | tee -a "$RESULT_FILE"

    # Build with splitpath stats enabled
    build "$SGP" "$FLUSH" "$COEFF_ONE" "$RECLAIM"

    # Phase 1: Run insert-only to populate data
    echo "    [Phase 1] Running insert-only (dist=$DIST, threads=$THREADS)..." | tee -a "$RESULT_FILE"
    insert_out=$(run_insert_only "$DIST" "$THREADS")

    # Extract insert throughput
    insert_tput=$(echo "$insert_out" | grep "YCSB_INSERT throughput" | awk '{print $NF}')
    insert_tput="${insert_tput:-N/A}"
    echo "    Insert Throughput: $insert_tput" | tee -a "$RESULT_FILE"

    # Phase 2: Run workload A
    echo "    [Phase 2] Running workload A (dist=$DIST, threads=$THREADS)..." | tee -a "$RESULT_FILE"
    workload_out=$(run_workload_a "$DIST" "$THREADS")

    # Extract workload A throughput
    wa_tput=$(echo "$workload_out" | grep -E "throughput" | tail -1 | awk '{print $NF}')
    wa_tput="${wa_tput:-N/A}"
    echo "    Workload A Throughput: $wa_tput" | tee -a "$RESULT_FILE"

    # Extract and log all splitpath-related output lines from workload A
    echo "" | tee -a "$RESULT_FILE"
    echo "    ── Split-Path Stats (Workload A) ──" | tee -a "$RESULT_FILE"
    echo "$workload_out" | grep -E '^\[SplitPath\]|^\[RebalSGP\]|^\[Level |^\[ChainHop\]' | while IFS= read -r line; do
        echo "    $line" | tee -a "$RESULT_FILE"
    done

    # Parse [SplitPath] line from workload A output for CSV
    sp_line=$(echo "$workload_out" | grep '^\[SplitPath\]' || echo "")
    if [[ -n "$sp_line" ]]; then
        path_a=$(echo "$sp_line" | grep -oP 'A\(sgp_covered\+\+\)=\K[0-9]+')
        path_b=$(echo "$sp_line" | grep -oP 'B\(gp_covered\+log\)=\K[0-9]+')
        path_c=$(echo "$sp_line" | grep -oP 'C\(linkSGP\)=\K[0-9]+')
        path_d=$(echo "$sp_line" | grep -oP 'D\(activateGP\+log\)=\K[0-9]+')
        path_e=$(echo "$sp_line" | grep -oP 'E\(rebalance\)=\K[0-9]+')
        sp_total=$(echo "$sp_line" | grep -oP 'total=\K[0-9]+')
        log_avoided=$(echo "$sp_line" | grep -oP 'log_avoided=\K[0-9]+')
        log_needed=$(echo "$sp_line" | grep -oP 'log_needed=\K[0-9]+')
        avoid_rate=$(echo "$sp_line" | grep -oP 'avoid_rate=\K[0-9.]+%' || echo "N/A")
    else
        path_a="N/A"; path_b="N/A"; path_c="N/A"; path_d="N/A"; path_e="N/A"
        sp_total="N/A"; log_avoided="N/A"; log_needed="N/A"; avoid_rate="N/A"
    fi

    echo "$NAME,$insert_tput,$wa_tput,$path_a,$path_b,$path_c,$path_d,$path_e,$sp_total,$log_avoided,$log_needed,$avoid_rate" >> "$CSV_FILE"

    # Save full raw output for this config
    raw_file="$PROJ_DIR/splitpath_raw_${NAME}.txt"
    {
        echo "=== INSERT-ONLY OUTPUT ==="
        echo "$insert_out"
        echo ""
        echo "=== WORKLOAD A OUTPUT ==="
        echo "$workload_out"
    } > "$raw_file"
    echo "" | tee -a "$RESULT_FILE"
    echo "    Full output saved to: $raw_file" | tee -a "$RESULT_FILE"

done  # end CONFIG

# ── Summary table ────────────────────────────────────────────────────────────
{
echo ""
echo "$divider"
echo "  SUMMARY"
echo "$divider"
echo ""
printf "%-45s | %14s | %14s | %8s %8s %8s %8s %8s | %10s | %10s\n" \
    "CONFIGURATION" "INSERT_TPUT" "WKLD_A_TPUT" "A" "B" "C" "D" "E" "AVOID_RATE" "TOTAL"
echo "----------------------------------------------+----------------+----------------+-----------------------------------------------+------------+------------"
} | tee -a "$RESULT_FILE"

# Re-read from CSV for summary (skip header)
tail -n +2 "$CSV_FILE" | while IFS=',' read -r cfg itp wtp pa pb pc pd pe tot la ln ar; do
    printf "%-45s | %14s | %14s | %8s %8s %8s %8s %8s | %10s | %10s\n" \
        "$cfg" "$itp" "$wtp" "$pa" "$pb" "$pc" "$pd" "$pe" "$ar" "$tot"
done | tee -a "$RESULT_FILE"

{
echo ""
echo "$divider"
echo "  Finished: $(date)"
echo "$divider"
} | tee -a "$RESULT_FILE"

echo ""
echo "Results saved to: $RESULT_FILE"
echo "CSV saved to:     $CSV_FILE"
