#include "bx/config.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>
#include <unordered_map>

namespace bx {
namespace {

bool ParseUInt64(const std::string& text, std::uint64_t* out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  std::size_t idx = 0;
  try {
    value = std::stoull(text, &idx);
  } catch (...) {
    return false;
  }
  if (idx != text.size()) {
    return false;
  }
  *out = value;
  return true;
}

bool ParseSize(const std::string& text, std::size_t* out) {
  if (text.empty()) {
    return false;
  }
  std::string lower;
  lower.reserve(text.size());
  for (char c : text) {
    lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }

  std::uint64_t mul = 1;
  if (lower.size() > 2 && lower.substr(lower.size() - 2) == "kb") {
    mul = 1024ULL;
    lower = lower.substr(0, lower.size() - 2);
  } else if (lower.size() > 2 && lower.substr(lower.size() - 2) == "mb") {
    mul = 1024ULL * 1024ULL;
    lower = lower.substr(0, lower.size() - 2);
  } else if (lower.size() > 2 && lower.substr(lower.size() - 2) == "gb") {
    mul = 1024ULL * 1024ULL * 1024ULL;
    lower = lower.substr(0, lower.size() - 2);
  } else if (!lower.empty() && lower.back() == 'k') {
    mul = 1024ULL;
    lower.pop_back();
  } else if (!lower.empty() && lower.back() == 'm') {
    mul = 1024ULL * 1024ULL;
    lower.pop_back();
  } else if (!lower.empty() && lower.back() == 'g') {
    mul = 1024ULL * 1024ULL * 1024ULL;
    lower.pop_back();
  }

  std::uint64_t base = 0;
  if (!ParseUInt64(lower, &base)) {
    return false;
  }
  *out = static_cast<std::size_t>(base * mul);
  return true;
}

bool ParseVersion(const std::string& text, Version* out) {
  if (text == "v0") {
    *out = Version::kV0;
    return true;
  }
  if (text == "v1") {
    *out = Version::kV1;
    return true;
  }
  if (text == "v2") {
    *out = Version::kV2;
    return true;
  }
  if (text == "v3") {
    *out = Version::kV3;
    return true;
  }
  return false;
}

bool ParseDataset(const std::string& text, DatasetType* out) {
  std::string x;
  x.reserve(text.size());
  for (char c : text) {
    x.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  if (x == "a" || x == "low") {
    *out = DatasetType::kLowRedundancy;
    return true;
  }
  if (x == "b" || x == "high") {
    *out = DatasetType::kHighRedundancy;
    return true;
  }
  if (x == "c" || x == "shift" || x == "shifted") {
    *out = DatasetType::kShiftedRedundancy;
    return true;
  }
  return false;
}

bool ParseChunkMode(const std::string& text, ChunkMode* out) {
  if (text == "cdc") {
    *out = ChunkMode::kCDC;
    return true;
  }
  if (text == "fixed") {
    *out = ChunkMode::kFixed;
    return true;
  }
  return false;
}

bool NeedsValue(const std::string& key, int i, int argc, std::string* error) {
  if (i + 1 >= argc) {
    *error = "Missing value for option: " + key;
    return false;
  }
  return true;
}

}  // namespace

std::string VersionToString(Version version) {
  switch (version) {
    case Version::kV0:
      return "v0";
    case Version::kV1:
      return "v1";
    case Version::kV2:
      return "v2";
    case Version::kV3:
      return "v3";
  }
  return "unknown";
}

std::string DatasetTypeToString(DatasetType type) {
  switch (type) {
    case DatasetType::kInputDir:
      return "input_dir";
    case DatasetType::kLowRedundancy:
      return "A_low";
    case DatasetType::kHighRedundancy:
      return "B_high";
    case DatasetType::kShiftedRedundancy:
      return "C_shifted";
  }
  return "unknown";
}

std::string ChunkModeToString(ChunkMode mode) {
  switch (mode) {
    case ChunkMode::kCDC:
      return "cdc";
    case ChunkMode::kFixed:
      return "fixed";
  }
  return "unknown";
}

std::optional<Config> ParseConfig(int argc, char** argv, std::string* error) {
  Config config;
  bool dataset_set = false;

  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    if (key == "--help" || key == "-h") {
      config.print_help = true;
      continue;
    }
    if (key == "--validate_with_v0") {
      config.validate_with_v0 = true;
      continue;
    }

    if (!NeedsValue(key, i, argc, error)) {
      return std::nullopt;
    }
    const std::string value = argv[++i];

    if (key == "--version") {
      if (!ParseVersion(value, &config.version)) {
        *error = "Invalid --version: " + value;
        return std::nullopt;
      }
    } else if (key == "--chunk_mode") {
      if (!ParseChunkMode(value, &config.chunk_mode)) {
        *error = "Invalid --chunk_mode: " + value;
        return std::nullopt;
      }
    } else if (key == "--input_dir") {
      config.input_dir = value;
      config.dataset_type = DatasetType::kInputDir;
      dataset_set = true;
    } else if (key == "--gen_dataset") {
      if (!ParseDataset(value, &config.dataset_type)) {
        *error = "Invalid --gen_dataset: " + value;
        return std::nullopt;
      }
      dataset_set = true;
    } else if (key == "--file_count") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --file_count: " + value;
        return std::nullopt;
      }
      config.file_count = x;
    } else if (key == "--total_bytes") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --total_bytes: " + value;
        return std::nullopt;
      }
      config.total_bytes = x;
    } else if (key == "--seed") {
      std::uint64_t x = 0;
      if (!ParseUInt64(value, &x)) {
        *error = "Invalid --seed: " + value;
        return std::nullopt;
      }
      config.seed = x;
    } else if (key == "--min_chunk") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --min_chunk: " + value;
        return std::nullopt;
      }
      config.min_chunk = x;
    } else if (key == "--avg_chunk") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --avg_chunk: " + value;
        return std::nullopt;
      }
      config.avg_chunk = x;
    } else if (key == "--max_chunk") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --max_chunk: " + value;
        return std::nullopt;
      }
      config.max_chunk = x;
    } else if (key == "--window_size") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --window_size: " + value;
        return std::nullopt;
      }
      config.window_size = x;
    } else if (key == "--worker_threads") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --worker_threads: " + value;
        return std::nullopt;
      }
      config.worker_threads = std::max<std::size_t>(1, x);
    } else if (key == "--reader_threads") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --reader_threads: " + value;
        return std::nullopt;
      }
      config.reader_threads = std::max<std::size_t>(1, x);
    } else if (key == "--chunker_threads") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --chunker_threads: " + value;
        return std::nullopt;
      }
      config.chunker_threads = std::max<std::size_t>(1, x);
    } else if (key == "--hasher_threads") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --hasher_threads: " + value;
        return std::nullopt;
      }
      config.hasher_threads = std::max<std::size_t>(1, x);
    } else if (key == "--indexer_threads") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --indexer_threads: " + value;
        return std::nullopt;
      }
      config.indexer_threads = std::max<std::size_t>(1, x);
    } else if (key == "--num_shards") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --num_shards: " + value;
        return std::nullopt;
      }
      config.num_shards = std::max<std::size_t>(1, x);
    } else if (key == "--queue_capacity") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --queue_capacity: " + value;
        return std::nullopt;
      }
      config.queue_capacity = std::max<std::size_t>(1, x);
    } else if (key == "--batch_size") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --batch_size: " + value;
        return std::nullopt;
      }
      config.batch_size = std::max<std::size_t>(1, x);
    } else if (key == "--repeat") {
      std::size_t x = 0;
      if (!ParseSize(value, &x)) {
        *error = "Invalid --repeat: " + value;
        return std::nullopt;
      }
      config.repeat = std::max<std::size_t>(1, x);
    } else if (key == "--csv_output") {
      config.csv_output = value;
    } else if (key == "--json_output") {
      config.json_output = value;
    } else {
      *error = "Unknown option: " + key;
      return std::nullopt;
    }
  }

  if (!dataset_set && config.input_dir.empty()) {
    config.dataset_type = DatasetType::kLowRedundancy;
  }
  if (!config.input_dir.empty() && config.dataset_type != DatasetType::kInputDir) {
    *error = "Cannot use --input_dir with --gen_dataset together";
    return std::nullopt;
  }
  if (config.min_chunk == 0 || config.avg_chunk == 0 || config.max_chunk == 0) {
    *error = "Chunk sizes must be positive";
    return std::nullopt;
  }
  if (!(config.min_chunk <= config.avg_chunk && config.avg_chunk <= config.max_chunk)) {
    *error = "Chunk sizes must satisfy min <= avg <= max";
    return std::nullopt;
  }

  return config;
}

std::string BuildHelpText() {
  std::ostringstream oss;
  oss << "Usage: bx_dedup [options]\n"
      << "  --version v0|v1|v2|v3\n"
      << "  --chunk_mode cdc|fixed\n"
      << "  --input_dir <path> | --gen_dataset A|B|C\n"
      << "  --file_count N\n"
      << "  --total_bytes 512MB\n"
      << "  --min_chunk 2KB --avg_chunk 8KB --max_chunk 64KB\n"
      << "  --worker_threads N\n"
      << "  --reader_threads N --chunker_threads N --hasher_threads N --indexer_threads N\n"
      << "  --num_shards N --queue_capacity N --batch_size N\n"
      << "  --repeat N --csv_output result.csv --json_output result.json\n"
      << "  --validate_with_v0\n";
  return oss.str();
}

}  // namespace bx
