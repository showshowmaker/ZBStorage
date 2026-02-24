#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "bx/config.h"
#include "bx/dataset.h"
#include "bx/engine.h"
#include "bx/stats.h"

namespace bx {

RunStats AverageStats(const std::vector<RunStats>& runs) {
  RunStats out;
  if (runs.empty()) {
    return out;
  }
  out.version = runs[0].version;
  out.dataset = runs[0].dataset;
  out.files_total = runs[0].files_total;
  out.bytes_input = runs[0].bytes_input;

  for (const auto& r : runs) {
    out.chunks_total += r.chunks_total;
    out.chunks_unique += r.chunks_unique;
    out.chunks_duplicate += r.chunks_duplicate;
    out.bytes_unique += r.bytes_unique;
    out.dedup_ratio += r.dedup_ratio;
    out.elapsed_sec += r.elapsed_sec;
    out.throughput_mb_s += r.throughput_mb_s;
    out.chunk_time_ns += r.chunk_time_ns;
    out.hash_time_ns += r.hash_time_ns;
    out.index_time_ns += r.index_time_ns;
    out.pop_wait_ns += r.pop_wait_ns;
    out.push_wait_ns += r.push_wait_ns;
  }

  const std::uint64_t n = static_cast<std::uint64_t>(runs.size());
  out.chunks_total /= n;
  out.chunks_unique /= n;
  out.chunks_duplicate /= n;
  out.bytes_unique /= n;
  out.dedup_ratio /= static_cast<double>(n);
  out.elapsed_sec /= static_cast<double>(n);
  out.throughput_mb_s /= static_cast<double>(n);
  out.chunk_time_ns /= n;
  out.hash_time_ns /= n;
  out.index_time_ns /= n;
  out.pop_wait_ns /= n;
  out.push_wait_ns /= n;
  return out;
}

}  // namespace bx

int main(int argc, char** argv) {
  std::string error;
  const auto config_opt = bx::ParseConfig(argc, argv, &error);
  if (!config_opt.has_value()) {
    std::cerr << "Argument error: " << error << "\n\n" << bx::BuildHelpText();
    return EXIT_FAILURE;
  }
  bx::Config config = *config_opt;

  if (config.print_help) {
    std::cout << bx::BuildHelpText();
    return EXIT_SUCCESS;
  }

  const std::vector<bx::FileTask> dataset = bx::BuildDataset(config);
  if (dataset.empty()) {
    std::cerr << "No input files found/generated.\n";
    return EXIT_FAILURE;
  }

  std::vector<bx::RunStats> runs;
  bx::RunResult last;

  for (std::size_t i = 0; i < config.repeat; ++i) {
    bx::RunResult run = bx::ExecuteOnce(config, dataset);
    runs.push_back(run.stats);
    last = run;

    std::cout << "==== run " << (i + 1) << "/" << config.repeat << " ====\n";
    std::cout << bx::FormatRunStats(run.stats, run.shard_hits, run.shard_inserts, run.queue_peaks) << '\n';

    if (!config.csv_output.empty()) {
      std::string csv_err;
      if (!bx::AppendCsv(config, run.stats, run.queue_peaks, &csv_err)) {
        std::cerr << "CSV write error: " << csv_err << "\n";
      }
    }
  }

  const bx::RunStats avg = bx::AverageStats(runs);
  std::cout << "==== average(" << config.repeat << ") ====\n";
  std::cout << bx::FormatRunStats(avg, last.shard_hits, last.shard_inserts, last.queue_peaks) << '\n';

  if (config.validate_with_v0 && config.version != bx::Version::kV0) {
    bx::Config base = config;
    base.version = bx::Version::kV0;
    const bx::RunResult v0 = bx::ExecuteOnce(base, dataset);

    std::string diff;
    if (bx::ValidateStatsEqual(v0.stats, last.stats, &diff)) {
      std::cout << "Validation against v0: PASS\n";
    } else {
      std::cout << "Validation against v0: FAIL\n" << diff;
    }
  }

  if (!config.json_output.empty()) {
    std::string json_err;
    if (!bx::WriteJson(config, avg, last.shard_hits, last.shard_inserts, last.queue_peaks, &json_err)) {
      std::cerr << "JSON write error: " << json_err << "\n";
    }
  }

  return EXIT_SUCCESS;
}
