# BX CPU Parallel CDC Dedup (Linux)

This is a standalone project under `BX/code` and does not modify or depend on the repository's existing modules.

## Features

- `V0`: serial baseline
- `V1`: multithread + global index lock
- `V2`: multithread + sharded index
- `V3`: pipeline (`Reader -> Chunker -> Hasher -> Indexer`) + bounded queues + sharded index + batch transfer
- CDC chunking (`--chunk_mode cdc`) and fixed chunking (`--chunk_mode fixed`)
- Dataset modes: generated A/B/C or `--input_dir`
- CSV/JSON output for report plotting
- Optional correctness check against V0 (`--validate_with_v0`)

## Build (Linux)

```bash
cd BX/code
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binary:

```bash
./build/bx_dedup
```

## Quick Start

Run V0 baseline:

```bash
./build/bx_dedup --version v0 --gen_dataset A --file_count 512 --total_bytes 512MB
```

Run V1/V2/V3:

```bash
./build/bx_dedup --version v1 --gen_dataset B --worker_threads 8 --file_count 1024 --total_bytes 2GB
./build/bx_dedup --version v2 --gen_dataset B --worker_threads 8 --num_shards 32 --file_count 1024 --total_bytes 2GB
./build/bx_dedup --version v3 --gen_dataset C --chunker_threads 4 --hasher_threads 8 --indexer_threads 8 --num_shards 64 --queue_capacity 256 --batch_size 128 --file_count 1024 --total_bytes 2GB
```

CDC vs fixed on shifted dataset C:

```bash
./build/bx_dedup --version v3 --chunk_mode fixed --gen_dataset C --avg_chunk 8KB --file_count 1024 --total_bytes 2GB
./build/bx_dedup --version v3 --chunk_mode cdc   --gen_dataset C --avg_chunk 8KB --file_count 1024 --total_bytes 2GB
```

CSV/JSON output:

```bash
./build/bx_dedup --version v3 --gen_dataset C --csv_output out/results.csv --json_output out/last.json
```

Validation example:

```bash
./build/bx_dedup --version v3 --gen_dataset A --validate_with_v0
```

## Main CLI Options

- `--version v0|v1|v2|v3`
- `--chunk_mode cdc|fixed`
- `--input_dir <dir>` or `--gen_dataset A|B|C`
- `--file_count <N>`
- `--total_bytes <size>` (`KB/MB/GB` supported)
- `--min_chunk --avg_chunk --max_chunk`
- `--worker_threads` (V1/V2)
- `--reader_threads --chunker_threads --hasher_threads --indexer_threads` (V3)
- `--num_shards --queue_capacity --batch_size`
- `--repeat <N>`
- `--csv_output <path> --json_output <path>`

## Bench Script

See `scripts/run_bench.sh` for batch runs that produce report-ready CSV.

## Reliable Large-Load Perf Suite

Use `scripts/run_perf_suite.sh` to run `v0/v1/v2/v3` repeatedly under large load and auto-generate reports.

```bash
cd BX/code
chmod +x scripts/run_perf_suite.sh
./scripts/run_perf_suite.sh
```

Default profile:

- multiple warmup runs + measured runs (`RUNS=7`)
- large generated dataset (`TOTAL_BYTES=4GB`, `FILE_COUNT=2048`)
- datasets `A,B,C`
- auto outputs:
  - `out/perf_<timestamp>/raw_runs.csv`
  - `out/perf_<timestamp>/summary_stats.csv`
  - `out/perf_<timestamp>/report_overview.md`
  - `out/perf_<timestamp>/report_v0.md` / `report_v1.md` / `report_v2.md` / `report_v3.md`

You can override parameters via env vars:

```bash
RUNS=10 TOTAL_BYTES=8GB FILE_COUNT=4096 DATASETS=A,B,C NUM_SHARDS=64 ./scripts/run_perf_suite.sh
```
