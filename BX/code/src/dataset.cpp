#include "bx/dataset.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace bx {
namespace {

std::vector<std::uint8_t> RandomBytes(std::mt19937_64* rng, std::size_t n) {
  std::vector<std::uint8_t> out(n);
  std::uniform_int_distribution<int> dist(0, 255);
  for (std::size_t i = 0; i < n; ++i) {
    out[i] = static_cast<std::uint8_t>(dist(*rng));
  }
  return out;
}

std::size_t JitteredSize(std::size_t base, std::mt19937_64* rng) {
  std::uniform_int_distribution<int> pct(-25, 25);
  const int j = pct(*rng);
  const std::int64_t size = static_cast<std::int64_t>(base) +
                            static_cast<std::int64_t>(base) * j / 100;
  return static_cast<std::size_t>(std::max<std::int64_t>(1024, size));
}

void AppendRandom(std::vector<std::uint8_t>* dst, std::size_t bytes, std::mt19937_64* rng) {
  auto tmp = RandomBytes(rng, bytes);
  dst->insert(dst->end(), tmp.begin(), tmp.end());
}

void AppendFromTemplate(std::vector<std::uint8_t>* dst, const std::vector<std::vector<std::uint8_t>>& pool,
                        std::size_t bytes, std::mt19937_64* rng) {
  if (pool.empty()) {
    AppendRandom(dst, bytes, rng);
    return;
  }
  std::uniform_int_distribution<std::size_t> pick(0, pool.size() - 1);
  std::size_t remain = bytes;
  while (remain > 0) {
    const auto& src = pool[pick(*rng)];
    const std::size_t n = std::min(remain, src.size());
    dst->insert(dst->end(), src.begin(), src.begin() + static_cast<std::ptrdiff_t>(n));
    remain -= n;
  }
}

std::vector<std::uint8_t> BuildLowRedundancyFile(std::size_t target,
                                                 const std::vector<std::vector<std::uint8_t>>& pool,
                                                 std::mt19937_64* rng) {
  std::vector<std::uint8_t> out;
  out.reserve(target);
  std::uniform_int_distribution<int> pick(0, 99);

  while (out.size() < target) {
    const std::size_t chunk = std::min<std::size_t>(4096, target - out.size());
    if (pick(*rng) < 10) {
      AppendFromTemplate(&out, pool, chunk, rng);
    } else {
      AppendRandom(&out, chunk, rng);
    }
  }
  return out;
}

std::vector<std::uint8_t> BuildHighRedundancyFile(std::size_t target,
                                                  const std::vector<std::vector<std::uint8_t>>& pool,
                                                  std::mt19937_64* rng) {
  std::vector<std::uint8_t> out;
  out.reserve(target);
  std::uniform_int_distribution<int> pick(0, 99);

  while (out.size() < target) {
    const std::size_t chunk = std::min<std::size_t>(4096, target - out.size());
    if (pick(*rng) < 80) {
      AppendFromTemplate(&out, pool, chunk, rng);
    } else {
      AppendRandom(&out, chunk, rng);
    }
  }
  return out;
}

void ApplyShiftEdits(std::vector<std::uint8_t>* data, std::mt19937_64* rng) {
  if (data->empty()) {
    return;
  }
  std::uniform_int_distribution<int> op_dist(0, 2);
  std::uniform_int_distribution<int> size_dist(8, 64);
  const std::size_t edits = std::max<std::size_t>(2, data->size() / (256 * 1024));

  for (std::size_t e = 0; e < edits; ++e) {
    const int op = op_dist(*rng);
    const std::size_t span = static_cast<std::size_t>(size_dist(*rng));
    std::uniform_int_distribution<std::size_t> pos_dist(0, data->size() - 1);
    const std::size_t pos = pos_dist(*rng);

    if (op == 0) {
      auto bytes = RandomBytes(rng, span);
      data->insert(data->begin() + static_cast<std::ptrdiff_t>(pos), bytes.begin(), bytes.end());
    } else if (op == 1) {
      const std::size_t del = std::min(span, data->size() - pos);
      data->erase(data->begin() + static_cast<std::ptrdiff_t>(pos),
                  data->begin() + static_cast<std::ptrdiff_t>(pos + del));
      if (data->empty()) {
        data->push_back(0);
      }
    } else {
      const std::size_t rep = std::min(span, data->size() - pos);
      auto bytes = RandomBytes(rng, rep);
      std::copy(bytes.begin(), bytes.end(), data->begin() + static_cast<std::ptrdiff_t>(pos));
    }
  }
}

std::vector<FileTask> BuildGeneratedDataset(const Config& config) {
  std::vector<FileTask> files;
  const std::size_t file_count = std::max<std::size_t>(1, config.file_count);
  files.reserve(file_count);

  std::mt19937_64 rng(config.seed);
  const std::size_t avg_file_size = std::max<std::size_t>(1024, config.total_bytes / file_count);

  std::vector<std::vector<std::uint8_t>> small_pool;
  const std::size_t pool_size = (config.dataset_type == DatasetType::kHighRedundancy) ? 8 : 64;
  small_pool.reserve(pool_size);
  for (std::size_t i = 0; i < pool_size; ++i) {
    small_pool.push_back(RandomBytes(&rng, 4096));
  }

  for (std::size_t i = 0; i < file_count; ++i) {
    const std::size_t target = JitteredSize(avg_file_size, &rng);
    auto owner = std::make_shared<std::vector<std::uint8_t>>();

    if (config.dataset_type == DatasetType::kLowRedundancy) {
      *owner = BuildLowRedundancyFile(target, small_pool, &rng);
    } else if (config.dataset_type == DatasetType::kHighRedundancy) {
      *owner = BuildHighRedundancyFile(target, small_pool, &rng);
    } else {
      *owner = BuildHighRedundancyFile(target, small_pool, &rng);
      ApplyShiftEdits(owner.get(), &rng);
    }

    FileTask task;
    task.file_id = i;
    task.path = "gen_file_" + std::to_string(i);
    task.size_bytes = owner->size();
    task.buffer = std::move(owner);
    task.dataset_tag = DatasetTypeToString(config.dataset_type);
    task.seq_no = i;
    files.push_back(std::move(task));
  }

  return files;
}

std::vector<FileTask> BuildInputDirDataset(const Config& config) {
  namespace fs = std::filesystem;
  std::vector<FileTask> files;
  if (config.input_dir.empty()) {
    return files;
  }

  std::error_code ec;
  if (!fs::exists(config.input_dir, ec)) {
    return files;
  }

  std::size_t file_id = 0;
  for (fs::recursive_directory_iterator it(config.input_dir, ec), end; it != end && !ec; it.increment(ec)) {
    if (!it->is_regular_file(ec)) {
      continue;
    }

    std::ifstream fin(it->path(), std::ios::binary);
    if (!fin) {
      continue;
    }

    auto owner = std::make_shared<std::vector<std::uint8_t>>();
    fin.seekg(0, std::ios::end);
    const std::streamsize n = fin.tellg();
    if (n <= 0) {
      continue;
    }
    fin.seekg(0, std::ios::beg);
    owner->resize(static_cast<std::size_t>(n));
    fin.read(reinterpret_cast<char*>(owner->data()), n);
    if (fin.gcount() != n) {
      continue;
    }

    FileTask task;
    task.file_id = file_id;
    task.path = it->path().string();
    task.size_bytes = owner->size();
    task.buffer = std::move(owner);
    task.dataset_tag = "input";
    task.seq_no = file_id;
    files.push_back(std::move(task));
    ++file_id;

    if (config.file_count > 0 && files.size() >= config.file_count) {
      break;
    }
  }

  return files;
}

}  // namespace

std::vector<FileTask> BuildDataset(const Config& config) {
  if (config.dataset_type == DatasetType::kInputDir) {
    return BuildInputDirDataset(config);
  }
  return BuildGeneratedDataset(config);
}

}  // namespace bx
