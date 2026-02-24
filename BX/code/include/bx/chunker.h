#pragma once

#include <vector>

#include "bx/config.h"
#include "bx/types.h"

namespace bx {

class Chunker {
 public:
  explicit Chunker(const Config& config);

  std::vector<ChunkDesc> Split(const FileTask& file) const;

 private:
  std::vector<ChunkDesc> SplitCDC(const FileTask& file) const;
  std::vector<ChunkDesc> SplitFixed(const FileTask& file) const;

  std::size_t min_chunk_;
  std::size_t avg_chunk_;
  std::size_t max_chunk_;
  std::size_t window_size_;
  ChunkMode mode_;
};

}  // namespace bx
