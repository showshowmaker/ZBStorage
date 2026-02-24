#include "bx/chunker.h"

#include <algorithm>
#include <array>
#include <cstdint>

namespace bx {
namespace {

const std::array<std::uint64_t, 256>& GearTable() {
  static const std::array<std::uint64_t, 256> table = [] {
    std::array<std::uint64_t, 256> t{};
    std::uint64_t x = 0x9e3779b97f4a7c15ULL;
    for (std::size_t i = 0; i < t.size(); ++i) {
      x ^= x >> 12U;
      x ^= x << 25U;
      x ^= x >> 27U;
      t[i] = x * 0x2545F4914F6CDD1DULL;
    }
    return t;
  }();
  return table;
}

std::size_t MaskBitsForAvg(std::size_t avg_chunk) {
  std::size_t bits = 0;
  std::size_t x = std::max<std::size_t>(2, avg_chunk);
  while ((1ULL << bits) < x && bits < 63) {
    ++bits;
  }
  return std::max<std::size_t>(1, bits - 1);
}

}  // namespace

Chunker::Chunker(const Config& config)
    : min_chunk_(config.min_chunk),
      avg_chunk_(config.avg_chunk),
      max_chunk_(config.max_chunk),
      window_size_(std::max<std::size_t>(1, config.window_size)),
      mode_(config.chunk_mode) {}

std::vector<ChunkDesc> Chunker::Split(const FileTask& file) const {
  if (mode_ == ChunkMode::kFixed) {
    return SplitFixed(file);
  }
  return SplitCDC(file);
}

std::vector<ChunkDesc> Chunker::SplitFixed(const FileTask& file) const {
  std::vector<ChunkDesc> out;
  if (!file.buffer || file.buffer->empty()) {
    return out;
  }

  const std::size_t step = std::max<std::size_t>(1, avg_chunk_);
  std::size_t off = 0;
  std::size_t idx = 0;
  while (off < file.buffer->size()) {
    const std::size_t len = std::min(step, file.buffer->size() - off);
    ChunkDesc c;
    c.file_id = file.file_id;
    c.chunk_idx_in_file = idx++;
    c.offset = off;
    c.length = len;
    c.data_owner = file.buffer;
    out.push_back(std::move(c));
    off += len;
  }
  return out;
}

std::vector<ChunkDesc> Chunker::SplitCDC(const FileTask& file) const {
  std::vector<ChunkDesc> out;
  if (!file.buffer || file.buffer->empty()) {
    return out;
  }

  const auto& data = *file.buffer;
  const auto& gear = GearTable();
  const std::size_t bits = MaskBitsForAvg(avg_chunk_);
  const std::uint64_t mask = (bits >= 63) ? ~0ULL : ((1ULL << bits) - 1ULL);

  std::uint64_t rolling = 0;
  std::size_t start = 0;
  std::size_t idx = 0;

  for (std::size_t i = 0; i < data.size(); ++i) {
    const std::uint8_t b = data[i];
    rolling = (rolling << 1U) + gear[b];

    const std::size_t len = i - start + 1;
    bool cut = false;
    if (len >= min_chunk_) {
      cut = ((rolling & mask) == 0);
    }
    if (len >= max_chunk_) {
      cut = true;
    }

    if (cut) {
      ChunkDesc c;
      c.file_id = file.file_id;
      c.chunk_idx_in_file = idx++;
      c.offset = start;
      c.length = len;
      c.data_owner = file.buffer;
      out.push_back(std::move(c));
      start = i + 1;
      rolling = 0;
    }
  }

  if (start < data.size()) {
    ChunkDesc c;
    c.file_id = file.file_id;
    c.chunk_idx_in_file = idx;
    c.offset = start;
    c.length = data.size() - start;
    c.data_owner = file.buffer;
    out.push_back(std::move(c));
  }

  (void)window_size_;
  return out;
}

}  // namespace bx
