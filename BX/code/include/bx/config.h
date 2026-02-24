#pragma once

#include <optional>
#include <string>

#include "bx/types.h"

namespace bx {

struct Config {
  Version version = Version::kV0;
  ChunkMode chunk_mode = ChunkMode::kCDC;
  DatasetType dataset_type = DatasetType::kLowRedundancy;

  std::string input_dir;
  std::size_t file_count = 1024;
  std::size_t total_bytes = 512ULL * 1024ULL * 1024ULL;
  std::uint64_t seed = 7;

  std::size_t min_chunk = 2ULL * 1024ULL;
  std::size_t avg_chunk = 8ULL * 1024ULL;
  std::size_t max_chunk = 64ULL * 1024ULL;
  std::size_t window_size = 64;

  std::size_t worker_threads = 4;
  std::size_t reader_threads = 1;
  std::size_t chunker_threads = 2;
  std::size_t hasher_threads = 4;
  std::size_t indexer_threads = 4;

  std::size_t num_shards = 32;
  std::size_t queue_capacity = 256;
  std::size_t batch_size = 128;

  std::size_t repeat = 1;
  std::string csv_output;
  std::string json_output;
  bool validate_with_v0 = false;
  bool print_help = false;
};

std::optional<Config> ParseConfig(int argc, char** argv, std::string* error);
std::string BuildHelpText();
std::string VersionToString(Version version);
std::string DatasetTypeToString(DatasetType type);
std::string ChunkModeToString(ChunkMode mode);

}  // namespace bx
