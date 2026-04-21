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

## Generate YCSB traces
The binary reads pre-generated YCSB trace files from the directory pointed to
by the `TANDEMKV_WORKLOAD_DIR` environment variable. The generator is bundled
in `ycsb_generator/`.

cd ycsb_generator
make

generate 50M-key load + txns{a..f} for {zipf, unif}; outputs *.trace in 0.99/
./generator.sh

or generate a single workload manually:
./ycsb_generator workload:a-f dist:zipf|unif <num_items>
./ycsb_generator a zipf 5000000



Edit `ITEM_NUM` in `generator.sh` (default 50M) for the full-paper scale, or
use a smaller value (e.g. 5M) for quick smoke tests. Move the resulting
`*.trace` files into one directory and point `TANDEMKV_WORKLOAD_DIR` at it.

Expected contents of `$TANDEMKV_WORKLOAD_DIR`:
`load.trace`, `txns{a,b,c,d,e,f}_{zipf,unif}.trace`.

## Run a single workload
export TANDEMKV_WORKLOAD_DIR=/path/to/workloads
```./project <workload>  <dist>  <threads> <pmem_dir> [--insert-only]```


## Reproduce paper results
| Paper question | Scripts |
|---------------|---|
| Q1 Throughput | `run_full_bench.sh`, `run_full_bench_scan.sh` |
| Q2 Ablation   | `run_splitpath_stats.sh`, `run_sgp_call_stats.sh` |
| Q3 Knob sweep | `run_dt_sweep.sh`, `run_threshold.sh`, `run_rt_dt.sh`, `run_dt_only.sh`, `run_stability_coeff.sh` |
| Q4 Recovery   | `run_recovery_scenarios.sh`, `run_crash_recovery.sh` |

All reproduction scripts expect `TANDEMKV_WORKLOAD_DIR` to be set and a
writable DAX mount at `/mnt/pmem*`.

See the [interactive documentation](https://anon-user-kv.github.io/TandemKV/) for
design overview, module map, and full reproduction guide.

See the [interactive documentation](https://anon-user-kv.github.io/TandemKV/) for
design overview, module map, and full reproduction guide.
Want me to write it into 
