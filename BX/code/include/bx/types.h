#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace bx {

enum class DatasetType {
  kInputDir,
  kLowRedundancy,
  kHighRedundancy,
  kShiftedRedundancy,
};

enum class Version {
  kV0,
  kV1,
  kV2,
  kV3,
};

enum class ChunkMode {
  kCDC,
  kFixed,
};

struct FileTask {
  std::size_t file_id = 0;
  std::string path;
  std::shared_ptr<std::vector<std::uint8_t>> buffer;
  std::size_t size_bytes = 0;
  std::string dataset_tag;
  std::size_t seq_no = 0;
};

struct ChunkDesc {
  std::size_t file_id = 0;
  std::size_t chunk_idx_in_file = 0;
  std::size_t offset = 0;
  std::size_t length = 0;
  std::shared_ptr<std::vector<std::uint8_t>> data_owner;
};

struct ChunkBatch {
  std::size_t batch_id = 0;
  std::vector<ChunkDesc> chunks;
};

struct Fingerprint {
  std::uint64_t hash = 0;
  std::uint32_t length = 0;

  bool operator==(const Fingerprint& rhs) const {
    return hash == rhs.hash && length == rhs.length;
  }
};

struct FingerprintHasher {
  std::size_t operator()(const Fingerprint& fp) const {
    std::uint64_t x = fp.hash ^ (static_cast<std::uint64_t>(fp.length) << 1U);
    x ^= x >> 33U;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33U;
    return static_cast<std::size_t>(x);
  }
};

struct FingerprintChunk {
  ChunkDesc chunk;
  Fingerprint fingerprint;
};

struct FingerprintBatch {
  std::size_t batch_id = 0;
  std::vector<FingerprintChunk> chunks;
};

struct IndexRecord {
  std::uint64_t canonical_chunk_id = 0;
  std::size_t first_seen_file_id = 0;
  std::size_t length = 0;
  std::uint64_t ref_count = 0;
};

struct ThreadLocalStats {
  std::uint64_t files_processed = 0;
  std::uint64_t bytes_processed = 0;
  std::uint64_t chunks_total = 0;
  std::uint64_t chunks_unique = 0;
  std::uint64_t chunks_duplicate = 0;
  std::uint64_t unique_bytes = 0;
  std::uint64_t chunk_time_ns = 0;
  std::uint64_t hash_time_ns = 0;
  std::uint64_t index_time_ns = 0;
  std::uint64_t pop_wait_ns = 0;
  std::uint64_t push_wait_ns = 0;
};

struct RunStats {
  std::string version;
  std::string dataset;
  std::uint64_t files_total = 0;
  std::uint64_t bytes_input = 0;
  std::uint64_t chunks_total = 0;
  std::uint64_t chunks_unique = 0;
  std::uint64_t chunks_duplicate = 0;
  std::uint64_t bytes_unique = 0;
  double dedup_ratio = 0.0;
  double elapsed_sec = 0.0;
  double throughput_mb_s = 0.0;
  std::uint64_t chunk_time_ns = 0;
  std::uint64_t hash_time_ns = 0;
  std::uint64_t index_time_ns = 0;
  std::uint64_t pop_wait_ns = 0;
  std::uint64_t push_wait_ns = 0;
};

}  // namespace bx
