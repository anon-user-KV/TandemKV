# TandemKV

**[→ Interactive documentation](https://anon-user-kv.github.io/TandemKV/)**

Off-critical-path index maintenance for durable DRAM–NVM key-value stores.
Prototype source and reproduction scripts for the SIGMOD '27 submission.

## Requirements
- Linux with a DAX-capable PMEM mount (default: `/mnt/pmem0`)
- PMDK ≥ 1.9 (`libpmemobj`)
- g++ with C++17, `make`, `numactl`

## Build
```cd TandemKV; make -j${n_proc}```

Compile-time flags (passed as `make VAR=1`): `ENABLE_SGP`, `ENABLE_COEFF_ONE`,
`ENABLE_IMMEDIATE_FLUSH`, `ENABLE_IMMEDIATE_RECLAIM`, `ENABLE_SPLITPATH_STATS`.

## Run a single workload
```./project <workload>  <dist>  <threads> <pmem_dir> [--insert-only]```


## Reproduce paper results
| Paper question | Scripts |
|---------------|---|
| Q1 Throughput | `run_full_bench.sh`, `run_full_bench_scan.sh` |
| Q2 Ablation   | `run_splitpath_stats.sh`, `run_sgp_call_stats.sh` |
| Q3 Knob sweep | `run_dt_sweep.sh`, `run_threshold.sh`, `run_rt_dt.sh`, `run_dt_only.sh`, `run_stability_coeff.sh` |
| Q4 Recovery   | `run_recovery_scenarios.sh`, `run_crash_recovery.sh` |

See the [interactive documentation](https://anon-user-kv.github.io/TandemKV/) for
design overview, module map, and full reproduction guide.

