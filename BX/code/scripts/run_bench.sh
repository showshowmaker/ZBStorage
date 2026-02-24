#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

BIN="./build/bx_dedup"
OUT_DIR="out"
CSV="$OUT_DIR/bench.csv"
mkdir -p "$OUT_DIR"
rm -f "$CSV"

# Group 1: version comparison
for v in v0 v1 v2 v3; do
  "$BIN" --version "$v" --gen_dataset A --file_count 1024 --total_bytes 2GB \
    --worker_threads 8 --chunker_threads 4 --hasher_threads 8 --indexer_threads 8 \
    --num_shards 32 --queue_capacity 256 --batch_size 128 --repeat 3 --csv_output "$CSV"
done

# Group 2: scalability on v3
for t in 1 2 4 8 16; do
  "$BIN" --version v3 --gen_dataset B --chunker_threads "$t" --hasher_threads "$t" --indexer_threads "$t" \
    --num_shards 64 --file_count 1024 --total_bytes 2GB --repeat 3 --csv_output "$CSV"
done

# Group 3: shard count impact
for s in 1 8 16 32 64; do
  "$BIN" --version v3 --gen_dataset B --chunker_threads 4 --hasher_threads 8 --indexer_threads 8 \
    --num_shards "$s" --file_count 1024 --total_bytes 2GB --repeat 3 --csv_output "$CSV"
done

# Group 4: avg chunk size impact
for c in 4KB 8KB 16KB; do
  "$BIN" --version v3 --gen_dataset C --avg_chunk "$c" --min_chunk 2KB --max_chunk 64KB \
    --chunker_threads 4 --hasher_threads 8 --indexer_threads 8 --num_shards 64 \
    --file_count 1024 --total_bytes 2GB --repeat 3 --csv_output "$CSV"
done

# Group 5: fixed vs cdc on shifted dataset C
for mode in fixed cdc; do
  "$BIN" --version v3 --chunk_mode "$mode" --gen_dataset C --avg_chunk 8KB \
    --chunker_threads 4 --hasher_threads 8 --indexer_threads 8 --num_shards 64 \
    --file_count 1024 --total_bytes 2GB --repeat 3 --csv_output "$CSV"
done

echo "Bench completed: $CSV"
