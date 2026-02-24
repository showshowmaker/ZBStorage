#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import math
import os
import statistics
from collections import defaultdict
from typing import Dict, List, Tuple

VERSIONS = ["v0", "v1", "v2", "v3"]


def to_float(row: Dict[str, str], key: str) -> float:
    try:
        return float(row.get(key, "0") or 0)
    except ValueError:
        return 0.0


def to_int(row: Dict[str, str], key: str) -> int:
    try:
        return int(float(row.get(key, "0") or 0))
    except ValueError:
        return 0


def mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return statistics.fmean(values)


def std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


def ci95(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    return 1.96 * std(values) / math.sqrt(n)


def load_meta(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if "=" in line and not line.startswith("---"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate benchmark report from bx_dedup csv")
    p.add_argument("--input", required=True, help="raw csv file")
    p.add_argument("--out_dir", required=True, help="output directory")
    p.add_argument("--meta", default="", help="meta env file")
    return p.parse_args()


def format_float(v: float, digits: int = 3) -> str:
    return f"{v:.{digits}f}"


def write_summary_csv(path: str, rows: List[Dict[str, str]]) -> None:
    fields = [
        "version",
        "dataset",
        "runs",
        "elapsed_mean_s",
        "elapsed_std_s",
        "elapsed_ci95_s",
        "throughput_mean_mb_s",
        "throughput_std_mb_s",
        "dedup_ratio_mean",
        "dedup_ratio_std",
        "chunks_total_mean",
        "chunks_unique_mean",
        "chunks_duplicate_mean",
        "speedup_vs_v0",
        "q1_peak_mean",
        "q2_peak_mean",
        "q3_peak_mean",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def build_tables(summary_map: Dict[Tuple[str, str], Dict[str, float]], datasets: List[str]) -> List[str]:
    lines: List[str] = []
    for ds in datasets:
        lines.append(f"## Dataset `{ds}`")
        lines.append("")
        lines.append("| Version | Runs | Elapsed Mean(s) | Std(s) | CI95(s) | Throughput Mean(MB/s) | Std | Dedup Mean | Speedup vs V0 |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for v in VERSIONS:
            key = (v, ds)
            if key not in summary_map:
                lines.append(f"| {v} | 0 | - | - | - | - | - | - | - |")
                continue
            s = summary_map[key]
            lines.append(
                "| {v} | {runs} | {elapsed} | {std} | {ci95} | {tp} | {tp_std} | {dedup} | {speedup} |".format(
                    v=v,
                    runs=int(s["runs"]),
                    elapsed=format_float(s["elapsed_mean_s"]),
                    std=format_float(s["elapsed_std_s"]),
                    ci95=format_float(s["elapsed_ci95_s"]),
                    tp=format_float(s["throughput_mean_mb_s"]),
                    tp_std=format_float(s["throughput_std_mb_s"]),
                    dedup=format_float(s["dedup_ratio_mean"], 4),
                    speedup=format_float(s["speedup_vs_v0"]),
                )
            )
        lines.append("")
    return lines


def build_version_report(version: str, datasets: List[str], summary_map: Dict[Tuple[str, str], Dict[str, float]], out_path: str) -> None:
    lines: List[str] = []
    lines.append(f"# Performance Report - {version}")
    lines.append("")
    lines.append("| Dataset | Runs | Elapsed Mean(s) | Std(s) | Throughput Mean(MB/s) | Dedup Mean | Speedup vs V0 | Q1 Peak Mean | Q2 Peak Mean | Q3 Peak Mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for ds in datasets:
        s = summary_map.get((version, ds))
        if not s:
            lines.append(f"| {ds} | 0 | - | - | - | - | - | - | - | - |")
            continue
        lines.append(
            "| {ds} | {runs} | {elapsed} | {std} | {tp} | {dedup} | {speedup} | {q1} | {q2} | {q3} |".format(
                ds=ds,
                runs=int(s["runs"]),
                elapsed=format_float(s["elapsed_mean_s"]),
                std=format_float(s["elapsed_std_s"]),
                tp=format_float(s["throughput_mean_mb_s"]),
                dedup=format_float(s["dedup_ratio_mean"], 4),
                speedup=format_float(s["speedup_vs_v0"]),
                q1=format_float(s["q1_peak_mean"], 1),
                q2=format_float(s["q2_peak_mean"], 1),
                q3=format_float(s["q3_peak_mean"], 1),
            )
        )

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    datasets_seen = []
    datasets_set = set()

    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            version = row.get("version", "")
            dataset = row.get("dataset", "")
            if not version or not dataset:
                continue
            grouped[(version, dataset)].append(row)
            if dataset not in datasets_set:
                datasets_set.add(dataset)
                datasets_seen.append(dataset)

    if not grouped:
        raise SystemExit("no rows in input csv")

    datasets = datasets_seen

    summary_map: Dict[Tuple[str, str], Dict[str, float]] = {}

    for key, rows in grouped.items():
        elapsed = [to_float(r, "elapsed_sec") for r in rows]
        throughput = [to_float(r, "throughput_mb_s") for r in rows]
        dedup = [to_float(r, "dedup_ratio") for r in rows]
        chunks_total = [to_float(r, "chunks_total") for r in rows]
        chunks_unique = [to_float(r, "chunks_unique") for r in rows]
        chunks_dup = [to_float(r, "chunks_duplicate") for r in rows]
        q1 = [to_float(r, "q1_peak") for r in rows]
        q2 = [to_float(r, "q2_peak") for r in rows]
        q3 = [to_float(r, "q3_peak") for r in rows]

        summary_map[key] = {
            "runs": float(len(rows)),
            "elapsed_mean_s": mean(elapsed),
            "elapsed_std_s": std(elapsed),
            "elapsed_ci95_s": ci95(elapsed),
            "throughput_mean_mb_s": mean(throughput),
            "throughput_std_mb_s": std(throughput),
            "dedup_ratio_mean": mean(dedup),
            "dedup_ratio_std": std(dedup),
            "chunks_total_mean": mean(chunks_total),
            "chunks_unique_mean": mean(chunks_unique),
            "chunks_duplicate_mean": mean(chunks_dup),
            "q1_peak_mean": mean(q1),
            "q2_peak_mean": mean(q2),
            "q3_peak_mean": mean(q3),
            "speedup_vs_v0": 0.0,
        }

    for ds in datasets:
        base = summary_map.get(("v0", ds))
        if not base or base["elapsed_mean_s"] <= 0:
            continue
        base_elapsed = base["elapsed_mean_s"]
        for v in VERSIONS:
            k = (v, ds)
            if k in summary_map and summary_map[k]["elapsed_mean_s"] > 0:
                summary_map[k]["speedup_vs_v0"] = base_elapsed / summary_map[k]["elapsed_mean_s"]

    summary_rows: List[Dict[str, str]] = []
    for ds in datasets:
        for v in VERSIONS:
            k = (v, ds)
            if k not in summary_map:
                continue
            s = summary_map[k]
            summary_rows.append(
                {
                    "version": v,
                    "dataset": ds,
                    "runs": str(int(s["runs"])),
                    "elapsed_mean_s": format_float(s["elapsed_mean_s"], 6),
                    "elapsed_std_s": format_float(s["elapsed_std_s"], 6),
                    "elapsed_ci95_s": format_float(s["elapsed_ci95_s"], 6),
                    "throughput_mean_mb_s": format_float(s["throughput_mean_mb_s"], 6),
                    "throughput_std_mb_s": format_float(s["throughput_std_mb_s"], 6),
                    "dedup_ratio_mean": format_float(s["dedup_ratio_mean"], 6),
                    "dedup_ratio_std": format_float(s["dedup_ratio_std"], 6),
                    "chunks_total_mean": format_float(s["chunks_total_mean"], 2),
                    "chunks_unique_mean": format_float(s["chunks_unique_mean"], 2),
                    "chunks_duplicate_mean": format_float(s["chunks_duplicate_mean"], 2),
                    "speedup_vs_v0": format_float(s["speedup_vs_v0"], 6),
                    "q1_peak_mean": format_float(s["q1_peak_mean"], 2),
                    "q2_peak_mean": format_float(s["q2_peak_mean"], 2),
                    "q3_peak_mean": format_float(s["q3_peak_mean"], 2),
                }
            )

    summary_csv = os.path.join(args.out_dir, "summary_stats.csv")
    write_summary_csv(summary_csv, summary_rows)

    meta = load_meta(args.meta)
    overview = []
    overview.append("# Performance Benchmark Overview")
    overview.append("")
    overview.append(f"Generated at: {dt.datetime.now().isoformat(timespec='seconds')}")
    overview.append("")
    overview.append("## Test Configuration")
    overview.append("")
    overview.append("| Item | Value |")
    overview.append("|---|---|")
    for k in [
        "RUNS",
        "WARMUP",
        "TOTAL_BYTES",
        "FILE_COUNT",
        "DATASETS",
        "CHUNK_MODE",
        "SEED",
        "WORKER_THREADS",
        "CHUNKER_THREADS",
        "HASHER_THREADS",
        "INDEXER_THREADS",
        "NUM_SHARDS",
        "QUEUE_CAPACITY",
        "BATCH_SIZE",
        "NPROC",
        "UNAME",
    ]:
        if k in meta:
            overview.append(f"| {k} | {meta[k]} |")
    overview.append("")
    overview.append("## Reliability Notes")
    overview.append("")
    overview.append("- Each metric is aggregated from multiple independent process runs.")
    overview.append("- `elapsed_std_s` and `elapsed_ci95_s` are included to show stability.")
    overview.append("- Use `speedup_vs_v0` to compare parallel versions against baseline.")
    overview.append("")

    overview.extend(build_tables(summary_map, datasets))

    overview_path = os.path.join(args.out_dir, "report_overview.md")
    with open(overview_path, "w", encoding="utf-8") as f:
        f.write("\n".join(overview) + "\n")

    for v in VERSIONS:
        path = os.path.join(args.out_dir, f"report_{v}.md")
        build_version_report(v, datasets, summary_map, path)

    print(f"report generated: {overview_path}")
    print(f"summary generated: {summary_csv}")


if __name__ == "__main__":
    main()
