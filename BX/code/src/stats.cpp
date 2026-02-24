#include "bx/stats.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace bx {

RunStats MergeStats(const Config& config, const std::vector<ThreadLocalStats>& thread_stats,
                    std::uint64_t bytes_input, std::uint64_t files_total, double elapsed_sec) {
  RunStats out;
  out.version = VersionToString(config.version);
  out.dataset = DatasetTypeToString(config.dataset_type);
  out.files_total = files_total;
  out.bytes_input = bytes_input;
  out.elapsed_sec = elapsed_sec;

  for (const auto& ts : thread_stats) {
    out.chunks_total += ts.chunks_total;
    out.chunks_unique += ts.chunks_unique;
    out.chunks_duplicate += ts.chunks_duplicate;
    out.bytes_unique += ts.unique_bytes;
    out.chunk_time_ns += ts.chunk_time_ns;
    out.hash_time_ns += ts.hash_time_ns;
    out.index_time_ns += ts.index_time_ns;
    out.pop_wait_ns += ts.pop_wait_ns;
    out.push_wait_ns += ts.push_wait_ns;
  }

  if (bytes_input > 0) {
    out.dedup_ratio = 1.0 - (static_cast<double>(out.bytes_unique) / static_cast<double>(bytes_input));
  }
  if (elapsed_sec > 0.0) {
    out.throughput_mb_s = (static_cast<double>(bytes_input) / (1024.0 * 1024.0)) / elapsed_sec;
  }

  return out;
}

std::string FormatRunStats(const RunStats& stats,
                           const std::vector<std::uint64_t>& shard_hits,
                           const std::vector<std::uint64_t>& shard_inserts,
                           const std::vector<std::size_t>& queue_peaks) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(3);
  oss << "version=" << stats.version << " dataset=" << stats.dataset << "\n";
  oss << "files=" << stats.files_total << " bytes_input=" << stats.bytes_input << "\n";
  oss << "chunks_total=" << stats.chunks_total << " unique=" << stats.chunks_unique
      << " duplicate=" << stats.chunks_duplicate << "\n";
  oss << "bytes_unique=" << stats.bytes_unique << " dedup_ratio=" << stats.dedup_ratio << "\n";
  oss << "elapsed_sec=" << stats.elapsed_sec << " throughput_mb_s=" << stats.throughput_mb_s << "\n";
  oss << "stage_time_ms chunk=" << (stats.chunk_time_ns / 1e6) << " hash=" << (stats.hash_time_ns / 1e6)
      << " index=" << (stats.index_time_ns / 1e6) << "\n";
  oss << "queue_wait_ms pop=" << (stats.pop_wait_ns / 1e6)
      << " push=" << (stats.push_wait_ns / 1e6) << "\n";

  oss << "shard_hits=";
  for (std::size_t i = 0; i < shard_hits.size(); ++i) {
    if (i) {
      oss << ',';
    }
    oss << shard_hits[i];
  }
  oss << "\nshard_inserts=";
  for (std::size_t i = 0; i < shard_inserts.size(); ++i) {
    if (i) {
      oss << ',';
    }
    oss << shard_inserts[i];
  }
  oss << "\nqueue_peaks=";
  for (std::size_t i = 0; i < queue_peaks.size(); ++i) {
    if (i) {
      oss << ',';
    }
    oss << queue_peaks[i];
  }
  oss << '\n';
  return oss.str();
}

bool AppendCsv(const Config& config, const RunStats& stats,
               const std::vector<std::size_t>& queue_peaks,
               std::string* error) {
  if (config.csv_output.empty()) {
    return true;
  }

  namespace fs = std::filesystem;
  fs::path p(config.csv_output);
  if (!p.parent_path().empty()) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
  }

  const bool need_header = !fs::exists(p);
  std::ofstream fout(config.csv_output, std::ios::app);
  if (!fout) {
    *error = "Cannot open csv file: " + config.csv_output;
    return false;
  }

  if (need_header) {
    fout << "version,dataset,chunk_mode,files,bytes_input,chunks_total,chunks_unique,chunks_duplicate,bytes_unique,dedup_ratio,elapsed_sec,throughput_mb_s,chunk_ms,hash_ms,index_ms,pop_wait_ms,push_wait_ms,q1_peak,q2_peak,q3_peak\n";
  }

  fout << stats.version << ',' << stats.dataset << ',' << ChunkModeToString(config.chunk_mode) << ','
       << stats.files_total << ',' << stats.bytes_input << ',' << stats.chunks_total << ','
       << stats.chunks_unique << ',' << stats.chunks_duplicate << ',' << stats.bytes_unique << ','
       << std::fixed << std::setprecision(6) << stats.dedup_ratio << ',' << stats.elapsed_sec << ','
       << stats.throughput_mb_s << ',' << (stats.chunk_time_ns / 1e6) << ','
       << (stats.hash_time_ns / 1e6) << ',' << (stats.index_time_ns / 1e6) << ','
       << (stats.pop_wait_ns / 1e6) << ',' << (stats.push_wait_ns / 1e6) << ',';

  const std::size_t q1 = queue_peaks.size() > 0 ? queue_peaks[0] : 0;
  const std::size_t q2 = queue_peaks.size() > 1 ? queue_peaks[1] : 0;
  const std::size_t q3 = queue_peaks.size() > 2 ? queue_peaks[2] : 0;
  fout << q1 << ',' << q2 << ',' << q3 << '\n';

  return true;
}

bool WriteJson(const Config& config, const RunStats& stats,
               const std::vector<std::uint64_t>& shard_hits,
               const std::vector<std::uint64_t>& shard_inserts,
               const std::vector<std::size_t>& queue_peaks,
               std::string* error) {
  if (config.json_output.empty()) {
    return true;
  }

  namespace fs = std::filesystem;
  fs::path p(config.json_output);
  if (!p.parent_path().empty()) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
  }

  std::ofstream fout(config.json_output);
  if (!fout) {
    *error = "Cannot open json file: " + config.json_output;
    return false;
  }

  fout << "{\n";
  fout << "  \"version\": \"" << stats.version << "\",\n";
  fout << "  \"dataset\": \"" << stats.dataset << "\",\n";
  fout << "  \"chunk_mode\": \"" << ChunkModeToString(config.chunk_mode) << "\",\n";
  fout << "  \"files_total\": " << stats.files_total << ",\n";
  fout << "  \"bytes_input\": " << stats.bytes_input << ",\n";
  fout << "  \"chunks_total\": " << stats.chunks_total << ",\n";
  fout << "  \"chunks_unique\": " << stats.chunks_unique << ",\n";
  fout << "  \"chunks_duplicate\": " << stats.chunks_duplicate << ",\n";
  fout << "  \"bytes_unique\": " << stats.bytes_unique << ",\n";
  fout << "  \"dedup_ratio\": " << std::setprecision(8) << stats.dedup_ratio << ",\n";
  fout << "  \"elapsed_sec\": " << std::setprecision(8) << stats.elapsed_sec << ",\n";
  fout << "  \"throughput_mb_s\": " << std::setprecision(8) << stats.throughput_mb_s << ",\n";
  fout << "  \"queue_peaks\": [";
  for (std::size_t i = 0; i < queue_peaks.size(); ++i) {
    if (i) {
      fout << ", ";
    }
    fout << queue_peaks[i];
  }
  fout << "],\n";

  fout << "  \"shard_hits\": [";
  for (std::size_t i = 0; i < shard_hits.size(); ++i) {
    if (i) {
      fout << ", ";
    }
    fout << shard_hits[i];
  }
  fout << "],\n";

  fout << "  \"shard_inserts\": [";
  for (std::size_t i = 0; i < shard_inserts.size(); ++i) {
    if (i) {
      fout << ", ";
    }
    fout << shard_inserts[i];
  }
  fout << "]\n";
  fout << "}\n";

  return true;
}

bool ValidateStatsEqual(const RunStats& lhs, const RunStats& rhs, std::string* diff) {
  std::ostringstream oss;
  bool ok = true;

  if (lhs.bytes_input != rhs.bytes_input) {
    ok = false;
    oss << "bytes_input mismatch: " << lhs.bytes_input << " vs " << rhs.bytes_input << '\n';
  }
  if (lhs.chunks_total != rhs.chunks_total) {
    ok = false;
    oss << "chunks_total mismatch: " << lhs.chunks_total << " vs " << rhs.chunks_total << '\n';
  }
  if (lhs.chunks_unique != rhs.chunks_unique) {
    ok = false;
    oss << "chunks_unique mismatch: " << lhs.chunks_unique << " vs " << rhs.chunks_unique << '\n';
  }
  if (lhs.chunks_duplicate != rhs.chunks_duplicate) {
    ok = false;
    oss << "chunks_duplicate mismatch: " << lhs.chunks_duplicate << " vs " << rhs.chunks_duplicate << '\n';
  }
  if (lhs.bytes_unique != rhs.bytes_unique) {
    ok = false;
    oss << "bytes_unique mismatch: " << lhs.bytes_unique << " vs " << rhs.bytes_unique << '\n';
  }

  if (diff != nullptr) {
    *diff = oss.str();
  }
  return ok;
}

}  // namespace bx
