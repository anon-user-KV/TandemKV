#!/bin/bash
###############################################################################
# run_sgp_call_stats.sh  –  Count linkInactiveSGP & insertSGPAtPos calls
#
# For each configuration:
#   1. Build with ENABLE_SPLITPATH_STATS=1
#   2. Run insert-only (load phase) with 16 threads on zipf distribution
#   3. Run workload A with 16 threads on zipf distribution
#   4. Capture [SGPCallStats] output for linkInactiveSGP / insertSGPAtPos counts
#
# Configurations (4 total, all SGP-enabled):
#   1. TANDEMKV                   (SGP=1, FLUSH=0, coeff=3)
#   2. TANDEMKV+IMMEDIATE_PERSIST (SGP=1, FLUSH=1, coeff=3)
#   3. TANDEMKV+COEFFICIENT=1     (SGP=1, FLUSH=0, coeff=1)
#   4. TANDEMKV+COEFF=1+IMM_PERSIST (SGP=1, FLUSH=1, coeff=1)
#
# Threads: 16
# Workload: insert-only (load) + workload A
# Runs: 1 per config
###############################################################################
set -uo pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
PMEM_DIR="/mnt/pmem0"
THREADS=16
DIST="zipf"

RESULT_FILE="$PROJ_DIR/sgp_call_stats_results.txt"
CSV_FILE="$PROJ_DIR/sgp_call_stats_results.csv"

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
    "TANDEMKV+IMMEDIATE_PERSIST           1    1      0          1"
    "TANDEMKV+COEFFICIENT=1               1    0      1          0"
    "TANDEMKV+COEFF=1+IMM_PERSIST         1    1      1          1"
)

# ── Header ───────────────────────────────────────────────────────────────────
divider="================================================================================================================================="

{
echo "$divider"
echo "  SGP Call Stats Benchmark   (threads=$THREADS, dist=$DIST)"
echo "  Phase 1: insert-only (load), Phase 2: workload A"
echo "  Tracking: linkInactiveSGP calls/success, insertSGPAtPos calls"
echo "  1 run per config, ENABLE_SPLITPATH_STATS=1"
echo "  $(date)"
echo "$divider"
} | tee "$RESULT_FILE"

# Write CSV header
echo "CONFIG,PHASE,THROUGHPUT,linkInactiveSGP_calls,linkInactiveSGP_success,insertSGPAtPos_calls,PATH_A,PATH_B,PATH_C,PATH_D,PATH_E,TOTAL,LOG_AVOIDED,AVOID_RATE" > "$CSV_FILE"

# ── Main loop ────────────────────────────────────────────────────────────────

for cfg_line in "${CONFIGS[@]}"; do
    read -r NAME SGP FLUSH COEFF_ONE RECLAIM <<< "$cfg_line"

    echo "" | tee -a "$RESULT_FILE"
    echo "$divider" | tee -a "$RESULT_FILE"
    echo ">>> Config: $NAME  (SGP=$SGP  FLUSH=$FLUSH  COEFF_ONE=$COEFF_ONE  RECLAIM=$RECLAIM)" | tee -a "$RESULT_FILE"
    echo "$divider" | tee -a "$RESULT_FILE"

    # Build with splitpath stats enabled
    build "$SGP" "$FLUSH" "$COEFF_ONE" "$RECLAIM"

    # ── Phase 1: insert-only (load) ──
    echo "" | tee -a "$RESULT_FILE"
    echo "    [Phase 1] Running insert-only (load) (dist=$DIST, threads=$THREADS)..." | tee -a "$RESULT_FILE"
    insert_out=$(run_insert_only "$DIST" "$THREADS")

    insert_tput=$(echo "$insert_out" | grep "YCSB_INSERT throughput" | awk '{print $NF}')
    insert_tput="${insert_tput:-N/A}"
    echo "    Insert Throughput: $insert_tput" | tee -a "$RESULT_FILE"

    # Extract SGPCallStats from insert phase
    sgp_line_ins=$(echo "$insert_out" | grep '^\[SGPCallStats\]' || echo "")
    if [[ -n "$sgp_line_ins" ]]; then
        link_calls_ins=$(echo "$sgp_line_ins" | grep -oP 'linkInactiveSGP_calls=\K[0-9]+')
        link_succ_ins=$(echo "$sgp_line_ins" | grep -oP 'linkInactiveSGP_success=\K[0-9]+')
        insert_sgp_ins=$(echo "$sgp_line_ins" | grep -oP 'insertSGPAtPos_calls=\K[0-9]+')
    else
        link_calls_ins="N/A"; link_succ_ins="N/A"; insert_sgp_ins="N/A"
    fi

    echo "    $sgp_line_ins" | tee -a "$RESULT_FILE"

    # Extract SplitPath stats from insert phase
    sp_line_ins=$(echo "$insert_out" | grep '^\[SplitPath\]' || echo "")
    if [[ -n "$sp_line_ins" ]]; then
        pa_ins=$(echo "$sp_line_ins" | grep -oP 'A\(sgp_covered\+\+\)=\K[0-9]+')
        pb_ins=$(echo "$sp_line_ins" | grep -oP 'B\(gp_covered\+log\)=\K[0-9]+')
        pc_ins=$(echo "$sp_line_ins" | grep -oP 'C\(linkSGP\)=\K[0-9]+')
        pd_ins=$(echo "$sp_line_ins" | grep -oP 'D\(activateGP\+log\)=\K[0-9]+')
        pe_ins=$(echo "$sp_line_ins" | grep -oP 'E\(rebalance\)=\K[0-9]+')
        total_ins=$(echo "$sp_line_ins" | grep -oP 'total=\K[0-9]+')
        avoided_ins=$(echo "$sp_line_ins" | grep -oP 'log_avoided=\K[0-9]+')
        rate_ins=$(echo "$sp_line_ins" | grep -oP 'avoid_rate=\K[0-9.]+%' || echo "N/A")
    else
        pa_ins="N/A"; pb_ins="N/A"; pc_ins="N/A"; pd_ins="N/A"; pe_ins="N/A"
        total_ins="N/A"; avoided_ins="N/A"; rate_ins="N/A"
    fi
    echo "    $sp_line_ins" | tee -a "$RESULT_FILE"

    echo "$NAME,INSERT,$insert_tput,$link_calls_ins,$link_succ_ins,$insert_sgp_ins,$pa_ins,$pb_ins,$pc_ins,$pd_ins,$pe_ins,$total_ins,$avoided_ins,$rate_ins" >> "$CSV_FILE"

    # ── Phase 2: workload A ──
    echo "" | tee -a "$RESULT_FILE"
    echo "    [Phase 2] Running workload A (dist=$DIST, threads=$THREADS)..." | tee -a "$RESULT_FILE"
    workload_out=$(run_workload_a "$DIST" "$THREADS")

    wa_tput=$(echo "$workload_out" | grep -E "throughput" | tail -1 | awk '{print $NF}')
    wa_tput="${wa_tput:-N/A}"
    echo "    Workload A Throughput: $wa_tput" | tee -a "$RESULT_FILE"

    # Extract SGPCallStats from workload A phase
    sgp_line_wa=$(echo "$workload_out" | grep '^\[SGPCallStats\]' || echo "")
    if [[ -n "$sgp_line_wa" ]]; then
        link_calls_wa=$(echo "$sgp_line_wa" | grep -oP 'linkInactiveSGP_calls=\K[0-9]+')
        link_succ_wa=$(echo "$sgp_line_wa" | grep -oP 'linkInactiveSGP_success=\K[0-9]+')
        insert_sgp_wa=$(echo "$sgp_line_wa" | grep -oP 'insertSGPAtPos_calls=\K[0-9]+')
    else
        link_calls_wa="N/A"; link_succ_wa="N/A"; insert_sgp_wa="N/A"
    fi

    echo "    $sgp_line_wa" | tee -a "$RESULT_FILE"

    # Extract SplitPath stats from workload A
    sp_line_wa=$(echo "$workload_out" | grep '^\[SplitPath\]' || echo "")
    if [[ -n "$sp_line_wa" ]]; then
        pa_wa=$(echo "$sp_line_wa" | grep -oP 'A\(sgp_covered\+\+\)=\K[0-9]+')
        pb_wa=$(echo "$sp_line_wa" | grep -oP 'B\(gp_covered\+log\)=\K[0-9]+')
        pc_wa=$(echo "$sp_line_wa" | grep -oP 'C\(linkSGP\)=\K[0-9]+')
        pd_wa=$(echo "$sp_line_wa" | grep -oP 'D\(activateGP\+log\)=\K[0-9]+')
        pe_wa=$(echo "$sp_line_wa" | grep -oP 'E\(rebalance\)=\K[0-9]+')
        total_wa=$(echo "$sp_line_wa" | grep -oP 'total=\K[0-9]+')
        avoided_wa=$(echo "$sp_line_wa" | grep -oP 'log_avoided=\K[0-9]+')
        rate_wa=$(echo "$sp_line_wa" | grep -oP 'avoid_rate=\K[0-9.]+%' || echo "N/A")
    else
        pa_wa="N/A"; pb_wa="N/A"; pc_wa="N/A"; pd_wa="N/A"; pe_wa="N/A"
        total_wa="N/A"; avoided_wa="N/A"; rate_wa="N/A"
    fi
    echo "    $sp_line_wa" | tee -a "$RESULT_FILE"

    echo "$NAME,WORKLOAD_A,$wa_tput,$link_calls_wa,$link_succ_wa,$insert_sgp_wa,$pa_wa,$pb_wa,$pc_wa,$pd_wa,$pe_wa,$total_wa,$avoided_wa,$rate_wa" >> "$CSV_FILE"

    # Save full raw output
    raw_file="$PROJ_DIR/sgp_call_raw_${NAME}.txt"
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
echo "  SUMMARY — SGP Function Call Counts"
echo "$divider"
echo ""
printf "%-45s | %-10s | %14s | %18s %18s %18s\n" \
    "CONFIGURATION" "PHASE" "THROUGHPUT" "linkInactive_calls" "linkInactive_succ" "insertSGPAtPos"
echo "----------------------------------------------+------------+----------------+--------------------+-------------------+-------------------"
} | tee -a "$RESULT_FILE"

tail -n +2 "$CSV_FILE" | while IFS=',' read -r cfg phase tp lc ls isp pa pb pc pd pe tot la ar; do
    printf "%-45s | %-10s | %14s | %18s %18s %18s\n" \
        "$cfg" "$phase" "$tp" "$lc" "$ls" "$isp"
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
