#pragma once

#include <vector>

#include "bx/config.h"
#include "bx/types.h"

namespace bx {

struct RunResult {
  RunStats stats;
  std::vector<std::uint64_t> shard_hits;
  std::vector<std::uint64_t> shard_inserts;
  std::vector<std::size_t> queue_peaks;
};

RunResult ExecuteOnce(const Config& config, const std::vector<FileTask>& dataset);

}  // namespace bx
