# TandemKV

**[→ Interactive documentation](https://anon-user-kv.github.io/TandemKV/)**

Off-critical-path index maintenance for durable DRAM–NVM key-value stores.
Prototype source and reproduction scripts for the SIGMOD '27 submission.

## Requirements
- Linux with a DAX-capable PMEM mount (default: `/mnt/pmem0`)
- PMDK ≥ 1.9 (`libpmemobj`)
- g++ with C++17, `make`, `numactl`

## Build
cd TandemKV; make

Compile-time flags (passed as `make VAR=1`): `ENABLE_SGP`, `ENABLE_COEFF_ONE`,
`ENABLE_IMMEDIATE_FLUSH`, `ENABLE_IMMEDIATE_RECLAIM`, `ENABLE_SPLITPATH_STATS`.

## Run a single workload
./project <workload> <dist> <threads> <pmem_dir> [--insert-only]
