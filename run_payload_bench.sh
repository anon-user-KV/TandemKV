#!/bin/bash
###############################################################################
# run_100M_bench.sh  –  Variable-payload sweep on the 100M YCSB trace set.
#
#   For each (payload_size, read_copy) combo:
#     - Clear pmem
#     - Load (insert-only) using `a` workload entry point
#     - Run workloads c, b, d, a back-to-back on the populated dataset
#     - Clear pmem again at the end of the round
#
# Captures throughput from each run into both a human-readable log and a CSV.
###############################################################################
set -uo pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
PMEM_DIR="/mnt/pmem0"
WORKLOAD_DIR="$PROJ_DIR/ycsb_generator/100M"

PAYLOAD_SIZES=(8 128 256 512 1024)
READ_COPY_MODES=(1)
DIST="zipf"
THREADS=16
RUNS=1
ROUND_SEQ=("load" "c" "b" "d" "a")
ITEM_NUM=100000000   # must match the trace set

# Compute value-pool bytes for a given payload size:
#   slot = roundUp8(8 header + payload), pool = ITEM_NUM * slot * 1.2 headroom
# Pool is created fresh each round (pmem is cleared), so no cross-run leakage.
valpool_bytes() {     # $1 = payload size in bytes
    local payload="$1"
    local slot=$(( ( (8 + payload + 7) / 8 ) * 8 ))
    echo $(( ITEM_NUM * slot * 12 / 10 ))
}

RESULT_FILE="$PROJ_DIR/100M_bench_results.txt"
CSV_FILE="$PROJ_DIR/100M_bench_results.csv"

# ── Sanity checks ───────────────────────────────────────────────────────────
pmem_free_bytes() {
    df --block-size=1 "$PMEM_DIR" 2>/dev/null | awk 'NR==2{print $4}'
}

if [[ ! -x "$PROJ_DIR/project" ]]; then
    echo "error: $PROJ_DIR/project not found or not executable."
    echo "       build first:  make clean && make -j8 ENABLE_VARIABLE_PAYLOAD=1"
    exit 1
fi
if [[ ! -d "$WORKLOAD_DIR" ]]; then
    echo "error: workload dir $WORKLOAD_DIR does not exist."
    exit 1
fi
for w in load txnsa_${DIST} txnsb_${DIST} txnsc_${DIST} txnsd_${DIST}; do
    [[ -f "$WORKLOAD_DIR/${w}.trace" ]] || { echo "missing $WORKLOAD_DIR/${w}.trace"; exit 1; }
done

# Warn for any payload whose pool alone exceeds available pmem
avail=$(pmem_free_bytes)
echo "pmem available: $(( avail / 1024 / 1024 / 1024 )) GB"
for p in "${PAYLOAD_SIZES[@]}"; do
    needed=$(valpool_bytes "$p")
    if (( needed > avail )); then
        echo "WARNING: payload=${p}B needs ~$(( needed / 1024 / 1024 / 1024 )) GB value pool, only $(( avail / 1024 / 1024 / 1024 )) GB free — that cell will OOS"
    fi
done

# ── Helpers ─────────────────────────────────────────────────────────────────
clear_pmem() { rm -rf "$PMEM_DIR"/* 2>/dev/null || true; }

run_load() {                      # $1=payload $2=read_copy
    local vpool; vpool=$(valpool_bytes "$1")
    clear_pmem
    TANDEMKV_WORKLOAD_DIR="$WORKLOAD_DIR" \
    TANDEMKV_PAYLOAD_SIZE="$1" \
    TANDEMKV_READ_COPY="$2" \
    TANDEMKV_VALPOOL_BYTES="$vpool" \
    numactl --cpunodebind=0 --membind=0 \
        "$PROJ_DIR/project" a "$DIST" "$THREADS" "$PMEM_DIR" --insert-only 2>&1
}

run_workload() {                  # $1=letter $2=payload $3=read_copy
    local vpool; vpool=$(valpool_bytes "$2")
    TANDEMKV_WORKLOAD_DIR="$WORKLOAD_DIR" \
    TANDEMKV_PAYLOAD_SIZE="$2" \
    TANDEMKV_READ_COPY="$3" \
    TANDEMKV_VALPOOL_BYTES="$vpool" \
    numactl --cpunodebind=0 --membind=0 \
        "$PROJ_DIR/project" "$1" "$DIST" "$THREADS" "$PMEM_DIR" 2>&1
}

throughput_pattern() {
    case "$1" in
        load) echo "YCSB_INSERT throughput" ;;
        a)    echo "YCSB_A throughput" ;;
        b)    echo "YCSB_B throughput" ;;
        c)    echo "YCSB_C throughput" ;;
        d)    echo "YCSB_D throughput" ;;
        *)    echo "UNKNOWN" ;;
    esac
}

extract_throughput() {            # stdin = log; $1 = grep pattern
    grep "$1" | tail -1 | awk '{print $NF}'
}

avg() {
    local sum=0 n=0
    for v in "$@"; do
        [[ -z "$v" || "$v" == "N/A" ]] && continue
        sum=$(echo "$sum + $v" | bc -l); n=$((n + 1))
    done
    [[ $n -eq 0 ]] && echo "N/A" || echo "scale=2; $sum / $n" | bc -l
}

# ── Header ──────────────────────────────────────────────────────────────────
divider="================================================================================"
{
    echo "$divider"
    echo "  100M Variable-Payload Benchmark"
    echo "  workloads:    $WORKLOAD_DIR  (dist=$DIST)"
    echo "  payloads:     ${PAYLOAD_SIZES[*]}"
    echo "  read_copy:    ${READ_COPY_MODES[*]}"
    echo "  threads:      $THREADS    rounds: $RUNS"
    echo "  round seq:    ${ROUND_SEQ[*]}"
    echo "  start:        $(date)"
    echo "$divider"
} | tee "$RESULT_FILE"

echo "PAYLOAD,READ_COPY,DIST,WORKLOAD,THREADS,RUN1,AVG" > "$CSV_FILE"

# ── Main loop ───────────────────────────────────────────────────────────────
for PAYLOAD in "${PAYLOAD_SIZES[@]}"; do
    for COPY in "${READ_COPY_MODES[@]}"; do
        echo "" | tee -a "$RESULT_FILE"
        echo "--- payload=${PAYLOAD}B  read_copy=${COPY}  dist=${DIST}  threads=${THREADS} ---" \
            | tee -a "$RESULT_FILE"

        declare -A round_vals
        for WL in "${ROUND_SEQ[@]}"; do round_vals["$WL"]=""; done

        for r in $(seq 1 $RUNS); do
            echo "  == round $r/$RUNS ==" | tee -a "$RESULT_FILE"
            for WL in "${ROUND_SEQ[@]}"; do
                pattern=$(throughput_pattern "$WL")
                if [[ "$WL" == "load" ]]; then
                    echo "    [$WL] clearing pmem + loading..." | tee -a "$RESULT_FILE"
                    out=$(run_load "$PAYLOAD" "$COPY")
                else
                    echo "    [$WL] running workload $WL..." | tee -a "$RESULT_FILE"
                    out=$(run_workload "$WL" "$PAYLOAD" "$COPY")
                fi
                tput=$(echo "$out" | extract_throughput "$pattern")
                [[ -z "$tput" ]] && tput="N/A"
                round_vals["$WL"]="${round_vals[$WL]} $tput"
                echo "    [$WL] r${r}: $tput" | tee -a "$RESULT_FILE"
            done
            echo "    [cleanup] clearing pmem..." | tee -a "$RESULT_FILE"
            clear_pmem
        done

        echo "" | tee -a "$RESULT_FILE"
        for WL in "${ROUND_SEQ[@]}"; do
            read -ra vals <<< "${round_vals[$WL]}"
            v1="${vals[0]:-N/A}"
            wl_avg=$(avg "${vals[@]}")
            printf "    payload=%-5s copy=%s | %-5s | R1=%-12s AVG=%s\n" \
                "$PAYLOAD" "$COPY" "$WL" "$v1" "$wl_avg" | tee -a "$RESULT_FILE"
            echo "$PAYLOAD,$COPY,$DIST,$WL,$THREADS,$v1,$wl_avg" >> "$CSV_FILE"
        done
        unset round_vals
    done
done

# ── Summary ────────────────────────────────────────────────────────────────
{
    echo ""
    echo "$divider"
    echo "  SUMMARY"
    echo "$divider"
    printf "%-8s | %-9s | %-5s | %-8s | %14s\n" \
        "PAYLOAD" "READ_COPY" "DIST" "WORKLOAD" "AVG THROUGHPUT"
    echo "---------+-----------+-------+----------+----------------"
} | tee -a "$RESULT_FILE"

tail -n +2 "$CSV_FILE" | while IFS=',' read -r p c d w t r1 a; do
    printf "%-8s | %-9s | %-5s | %-8s | %14s\n" "$p" "$c" "$d" "$w" "$a"
done | tee -a "$RESULT_FILE"

{
    echo ""
    echo "$divider"
    echo "  finished: $(date)"
    echo "$divider"
} | tee -a "$RESULT_FILE"

echo ""
echo "log: $RESULT_FILE"
echo "csv: $CSV_FILE"
