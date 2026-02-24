#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUNS="${RUNS:-7}"
WARMUP="${WARMUP:-1}"
TOTAL_BYTES="${TOTAL_BYTES:-4GB}"
FILE_COUNT="${FILE_COUNT:-2048}"
DATASETS="${DATASETS:-A,B,C}"
CHUNK_MODE="${CHUNK_MODE:-cdc}"
SEED="${SEED:-7}"
NUM_SHARDS="${NUM_SHARDS:-64}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-256}"
BATCH_SIZE="${BATCH_SIZE:-128}"

NPROC="$(nproc)"
WORKER_THREADS="${WORKER_THREADS:-$NPROC}"
CHUNKER_THREADS="${CHUNKER_THREADS:-$(( (NPROC + 3) / 4 ))}"
HASHER_THREADS="${HASHER_THREADS:-$(( (NPROC + 1) / 2 ))}"
INDEXER_THREADS="${INDEXER_THREADS:-$(( (NPROC + 1) / 2 ))}"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-out/perf_${TIMESTAMP}}"
RAW_CSV="${OUT_DIR}/raw_runs.csv"
LOG_FILE="${OUT_DIR}/run.log"
META_FILE="${OUT_DIR}/meta.env"

mkdir -p "$OUT_DIR"
rm -f "$RAW_CSV" "$LOG_FILE" "$META_FILE"

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "[1/4] build bx_dedup" | tee -a "$LOG_FILE"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release >>"$LOG_FILE" 2>&1
cmake --build build -j >>"$LOG_FILE" 2>&1

BIN="./build/bx_dedup"
if [[ ! -x "$BIN" ]]; then
  echo "binary not found: $BIN" | tee -a "$LOG_FILE"
  exit 1
fi

echo "[2/4] collect environment" | tee -a "$LOG_FILE"
{
  echo "TIMESTAMP=${TIMESTAMP}"
  echo "RUNS=${RUNS}"
  echo "WARMUP=${WARMUP}"
  echo "TOTAL_BYTES=${TOTAL_BYTES}"
  echo "FILE_COUNT=${FILE_COUNT}"
  echo "DATASETS=${DATASETS}"
  echo "CHUNK_MODE=${CHUNK_MODE}"
  echo "SEED=${SEED}"
  echo "NUM_SHARDS=${NUM_SHARDS}"
  echo "QUEUE_CAPACITY=${QUEUE_CAPACITY}"
  echo "BATCH_SIZE=${BATCH_SIZE}"
  echo "WORKER_THREADS=${WORKER_THREADS}"
  echo "CHUNKER_THREADS=${CHUNKER_THREADS}"
  echo "HASHER_THREADS=${HASHER_THREADS}"
  echo "INDEXER_THREADS=${INDEXER_THREADS}"
  echo "NPROC=${NPROC}"
  echo "UNAME=$(uname -a)"
  if command -v lscpu >/dev/null 2>&1; then
    echo "--- LSCPU ---"
    lscpu
  fi
  if command -v free >/dev/null 2>&1; then
    echo "--- FREE -h ---"
    free -h
  fi
} >"$META_FILE"

IFS=',' read -r -a DATASET_ARR <<< "$DATASETS"
VERSIONS=(v0 v1 v2 v3)

echo "[3/4] run warmup (${WARMUP}) and measured runs (${RUNS})" | tee -a "$LOG_FILE"

run_once() {
  local version="$1"
  local dataset="$2"
  local csv_out="$3"

  local args=(
    --version "$version"
    --chunk_mode "$CHUNK_MODE"
    --gen_dataset "$dataset"
    --file_count "$FILE_COUNT"
    --total_bytes "$TOTAL_BYTES"
    --seed "$SEED"
    --min_chunk 2KB
    --avg_chunk 8KB
    --max_chunk 64KB
    --num_shards "$NUM_SHARDS"
    --queue_capacity "$QUEUE_CAPACITY"
    --batch_size "$BATCH_SIZE"
    --repeat 1
  )

  if [[ "$version" == "v1" || "$version" == "v2" ]]; then
    args+=(--worker_threads "$WORKER_THREADS")
  fi
  if [[ "$version" == "v3" ]]; then
    args+=(--reader_threads 1 --chunker_threads "$CHUNKER_THREADS" --hasher_threads "$HASHER_THREADS" --indexer_threads "$INDEXER_THREADS")
  fi
  if [[ -n "$csv_out" ]]; then
    args+=(--csv_output "$csv_out")
  fi

  "$BIN" "${args[@]}"
}

for dataset in "${DATASET_ARR[@]}"; do
  for version in "${VERSIONS[@]}"; do
    for ((w=1; w<=WARMUP; w++)); do
      echo "[warmup] dataset=${dataset} version=${version} round=${w}" | tee -a "$LOG_FILE"
      run_once "$version" "$dataset" "" >>"$LOG_FILE" 2>&1
    done
  done

done

for ((r=1; r<=RUNS; r++)); do
  for dataset in "${DATASET_ARR[@]}"; do
    for version in "${VERSIONS[@]}"; do
      echo "[run] round=${r}/${RUNS} dataset=${dataset} version=${version}" | tee -a "$LOG_FILE"
      run_once "$version" "$dataset" "$RAW_CSV" | tee -a "$LOG_FILE"
    done
  done
done

echo "[4/4] generate report" | tee -a "$LOG_FILE"
"$PYTHON_BIN" scripts/generate_perf_report.py \
  --input "$RAW_CSV" \
  --out_dir "$OUT_DIR" \
  --meta "$META_FILE" | tee -a "$LOG_FILE"

echo "done" | tee -a "$LOG_FILE"
echo "raw csv   : $RAW_CSV"
echo "summary   : ${OUT_DIR}/summary_stats.csv"
echo "overview  : ${OUT_DIR}/report_overview.md"
echo "v0 report : ${OUT_DIR}/report_v0.md"
echo "v1 report : ${OUT_DIR}/report_v1.md"
echo "v2 report : ${OUT_DIR}/report_v2.md"
echo "v3 report : ${OUT_DIR}/report_v3.md"
