#pragma once

#include <string>
#include <vector>

#include "bx/config.h"
#include "bx/types.h"

namespace bx {

RunStats MergeStats(const Config& config, const std::vector<ThreadLocalStats>& thread_stats,
                    std::uint64_t bytes_input, std::uint64_t files_total, double elapsed_sec);

std::string FormatRunStats(const RunStats& stats,
                           const std::vector<std::uint64_t>& shard_hits,
                           const std::vector<std::uint64_t>& shard_inserts,
                           const std::vector<std::size_t>& queue_peaks);

bool AppendCsv(const Config& config, const RunStats& stats,
               const std::vector<std::size_t>& queue_peaks,
               std::string* error);

bool WriteJson(const Config& config, const RunStats& stats,
               const std::vector<std::uint64_t>& shard_hits,
               const std::vector<std::uint64_t>& shard_inserts,
               const std::vector<std::size_t>& queue_peaks,
               std::string* error);

bool ValidateStatsEqual(const RunStats& lhs, const RunStats& rhs, std::string* diff);

}  // namespace bx
