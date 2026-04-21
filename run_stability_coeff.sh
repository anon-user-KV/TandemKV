#!/bin/bash
#
# run_stability_coeff.sh — Test different SEARCH_STABILITY_COEFFICIENT_BY_LEVEL configurations
#
# This script patches include/common.h with various coefficient arrays,
# rebuilds the project, and measures throughput under different workloads.
#
# The stability coefficient controls how aggressively the search algorithm
# tolerates divergence at each B+-tree level:
#   - Higher coefficients at lower levels (0,1) → more stable search, fewer restarts
#   - Lower coefficients → faster convergence but more retries on concurrent updates
#
# Usage: ./run_stability_coeff.sh [threads] [pmem_path]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

THREADS=${1:-16}
PMEM_PATH=${2:-/mnt/pmem0}
RESULT_DIR="stability_coeff_results"
COMMON_H="include/common.h"

mkdir -p "$RESULT_DIR"

# ---- Coefficient Configurations ----
# Format: "label | coeff_array (20 comma-separated ints)"
#
# The array has MAX_LEVEL=20 entries. Levels 0,1 are leaf/near-leaf,
# higher levels are closer to root.
#
# We test variations primarily on the first few levels since those
# are where most search activity happens.

CONFIGS=(
    # All ones — no stability boost at any level
    "all_1          | 1,1, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # All zeros — maximally aggressive (no tolerance)
    "all_0          | 0,0, 0,0,0, 0,0,0,0,0, 0,0,0,0,0, 0,0,0,0,0"

    # Current default (ENABLE_COEFF_ONE=0): {3,3,1,1,...}
    "default_3_3_1  | 3,3, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Current ENABLE_COEFF_ONE=1: {3,3,1,0,0,...}
    "coeff_one_3_3  | 3,3, 1,0,0, 0,0,0,0,0, 0,0,0,0,0, 0,0,0,0,0"

    # Mild boost at level 0-1
    "mild_2_2       | 2,2, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Strong boost at level 0-1
    "strong_5_5     | 5,5, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Very strong boost at level 0-1
    "vstrong_10_10  | 10,10, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Gradual decay: higher coefficients near leaves
    "gradual_5_4_3  | 5,4, 3,2,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Extended boost: first 4 levels with coefficient > 1
    "extend_3_3_3_3 | 3,3, 3,3,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Only level 0 boosted
    "l0_only_5      | 5,1, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Heavy at level 0 with taper
    "heavy_l0_10_3  | 10,3, 1,1,1, 1,1,1,1,1, 1,1,1,1,1, 1,1,1,1,1"

    # Uniform moderate boost across all levels
    "uniform_3      | 3,3, 3,3,3, 3,3,3,3,3, 3,3,3,3,3, 3,3,3,3,3"
)

# Workloads to test
WL_TYPES=(  "c"             "a"    )
WL_DISTS=(  "zipf"          "zipf" )
WL_EXTRA=(  "--insert-only"  ""    )
WORKLOAD_LABELS=( "INSERT_ONLY" "YCSB_A" )
NUM_WORKLOADS=${#WL_TYPES[@]}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$RESULT_DIR/stability_coeff_${TIMESTAMP}.csv"

echo "label,coeff_l0,coeff_l1,coeff_l2,coeff_l3,coeff_l4,workload,throughput,elapsed_time" > "$SUMMARY"

# ---- Backup original common.h ----
BACKUP="$COMMON_H.orig_${TIMESTAMP}"
cp "$COMMON_H" "$BACKUP"
echo "Backed up $COMMON_H → $BACKUP"

# ---- Helper: Patch common.h with a given coefficient array ----
patch_common_h() {
    local coeffs="$1"  # comma-separated, 20 values

    # Restore from backup to ensure clean state
    cp "$BACKUP" "$COMMON_H"

    # Convert comma-separated string to space-separated array
    IFS=',' read -ra CARR <<< "$coeffs"

    # Build the new array initializer with proper formatting
    local new_array=""
    new_array+="    ${CARR[0]}, ${CARR[1]},\n"
    new_array+="    ${CARR[2]}, ${CARR[3]}, ${CARR[4]},\n"
    new_array+="    ${CARR[5]}, ${CARR[6]}, ${CARR[7]}, ${CARR[8]}, ${CARR[9]},\n"
    new_array+="    ${CARR[10]}, ${CARR[11]}, ${CARR[12]}, ${CARR[13]}, ${CARR[14]},\n"
    new_array+="    ${CARR[15]}, ${CARR[16]}, ${CARR[17]}, ${CARR[18]}, ${CARR[19]}"

    # Replace the #if ENABLE_COEFF_ONE ... #endif block with a single array
    # We use awk for multi-line replacement
    awk -v new_arr="$new_array" '
    /^#if ENABLE_COEFF_ONE/ {
        # Print the array directly, bypassing the #if/#else/#endif
        print "const int SEARCH_STABILITY_COEFFICIENT_BY_LEVEL[MAX_LEVEL] = {"
        # Use printf to handle \n in new_arr
        n = split(new_arr, lines, "\\n")
        for (i = 1; i <= n; i++) print lines[i]
        print "};"
        # Skip until #endif
        while (getline > 0) {
            if ($0 ~ /^#endif/) break
        }
        next
    }
    { print }
    ' "$COMMON_H" > "${COMMON_H}.tmp" && mv "${COMMON_H}.tmp" "$COMMON_H"
}

# ---- Helper: Clean PMem state ----
clean_pmem() {
    rm -f "$PMEM_PATH"/*.pool 2>/dev/null || true
    rm -rf "$PMEM_PATH"/ckpt_log "$PMEM_PATH"/pmemBFPool \
           "$PMEM_PATH"/pmemInodePool "$PMEM_PATH"/pmemVnodePool \
           "$PMEM_PATH"/prism "$PMEM_PATH"/dl "$PMEM_PATH"/sl \
           "$PMEM_PATH"/log 2>/dev/null || true
    sync
}

# ---- Main Loop ----
total_runs=$(( ${#CONFIGS[@]} * NUM_WORKLOADS ))
echo "============================================================"
echo " Search Stability Coefficient Experiments"
echo " Threads: $THREADS | PMem: $PMEM_PATH"
echo " Configs: ${#CONFIGS[@]} | Workloads: $NUM_WORKLOADS"
echo " Total runs: $total_runs"
echo " Results: $SUMMARY"
echo "============================================================"

run_count=0

for config_line in "${CONFIGS[@]}"; do
    # Parse "label | coeff_array"
    label=$(echo "$config_line" | awk -F'|' '{print $1}' | xargs)
    coeffs=$(echo "$config_line" | awk -F'|' '{print $2}' | tr -d ' ')

    # Extract first 5 coefficients for CSV
    IFS=',' read -ra CARR <<< "$coeffs"
    c0="${CARR[0]}"; c1="${CARR[1]}"; c2="${CARR[2]}"; c3="${CARR[3]}"; c4="${CARR[4]}"

    echo ""
    echo ">>> Config: $label"
    echo "    Coefficients: [${CARR[0]},${CARR[1]},${CARR[2]},${CARR[3]},${CARR[4]},...]"

    # Patch common.h
    patch_common_h "$coeffs"

    # Build
    echo "  Building..."
    make clean -s 2>/dev/null || true
    if ! make -j"$(nproc)" \
        ENABLE_DT_ONLY_RECLAIM=1 \
        CXXFLAGS_EXTRA="-DDIVERGENCE_THRESHOLD=1.01" \
        2>&1; then
        echo "  [BUILD FAILED] Skipping $label"
        for i in $(seq 0 $((NUM_WORKLOADS - 1))); do
            echo "$label,$c0,$c1,$c2,$c3,$c4,${WORKLOAD_LABELS[$i]},BUILD_FAIL,BUILD_FAIL" >> "$SUMMARY"
        done
        continue
    fi

    if [[ ! -x ./project ]]; then
        echo "  [BINARY NOT FOUND] Skipping $label"
        for i in $(seq 0 $((NUM_WORKLOADS - 1))); do
            echo "$label,$c0,$c1,$c2,$c3,$c4,${WORKLOAD_LABELS[$i]},NO_BINARY,NO_BINARY" >> "$SUMMARY"
        done
        continue
    fi

    # Run workloads
    for i in $(seq 0 $((NUM_WORKLOADS - 1))); do
        wl_type="${WL_TYPES[$i]}"
        wl_dist="${WL_DISTS[$i]}"
        wl_extra="${WL_EXTRA[$i]}"
        wl_label="${WORKLOAD_LABELS[$i]}"
        run_count=$((run_count + 1))

        echo "  [$run_count/$total_runs] $label / $wl_label"

        clean_pmem

        outfile="$RESULT_DIR/${label}_${wl_label}_${TIMESTAMP}.log"
        timeout 600 ./project "$wl_type" "$wl_dist" "$THREADS" "$PMEM_PATH" $wl_extra \
            > "$outfile" 2>&1 || true

        # Extract throughput
        if [[ "$wl_label" == "INSERT_ONLY" ]]; then
            tput=$(grep -oP '(?<=YCSB_INSERT throughput )\S+' "$outfile" 2>/dev/null || echo "N/A")
        else
            tput=$(grep -oP '(?<=YCSB_[A-F] throughput )\S+' "$outfile" 2>/dev/null || echo "N/A")
        fi
        etime=$(grep -oP '(?<=Elapsed_time )\S+' "$outfile" 2>/dev/null | head -1 || echo "N/A")

        echo "    throughput=$tput  elapsed=${etime}s"
        echo "$label,$c0,$c1,$c2,$c3,$c4,$wl_label,$tput,$etime" >> "$SUMMARY"
    done
done

# ---- Restore original common.h ----
cp "$BACKUP" "$COMMON_H"
echo ""
echo "Restored original $COMMON_H from $BACKUP"

# ---- Print Summary ----
echo ""
echo "============================================================"
echo " All experiments complete."
echo "============================================================"
echo ""

for wl in "${WORKLOAD_LABELS[@]}"; do
    echo "--- $wl ---"
    head -1 "$SUMMARY"
    grep ",$wl," "$SUMMARY" | sort
    echo ""
done

echo "CSV: $SUMMARY"
echo "Logs: $RESULT_DIR/"
