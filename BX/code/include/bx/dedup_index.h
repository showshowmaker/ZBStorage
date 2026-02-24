#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "bx/config.h"
#include "bx/types.h"

namespace bx {

class IDedupIndex {
 public:
  virtual ~IDedupIndex() = default;
  virtual bool CheckAndInsert(const FingerprintChunk& chunk) = 0;
  virtual std::size_t ShardCount() const = 0;
  virtual std::vector<std::uint64_t> SnapshotShardHits() const = 0;
  virtual std::vector<std::uint64_t> SnapshotShardInserts() const = 0;
};

std::unique_ptr<IDedupIndex> CreateIndex(const Config& config);

class GlobalLockIndex : public IDedupIndex {
 public:
  bool CheckAndInsert(const FingerprintChunk& chunk) override;
  std::size_t ShardCount() const override { return 1; }
  std::vector<std::uint64_t> SnapshotShardHits() const override;
  std::vector<std::uint64_t> SnapshotShardInserts() const override;

 private:
  mutable std::mutex mu_;
  std::unordered_map<Fingerprint, IndexRecord, FingerprintHasher> map_;
  std::uint64_t hits_ = 0;
  std::uint64_t inserts_ = 0;
};

class ShardedIndex : public IDedupIndex {
 public:
  explicit ShardedIndex(std::size_t shard_count);
  bool CheckAndInsert(const FingerprintChunk& chunk) override;
  std::size_t ShardCount() const override { return shards_.size(); }
  std::vector<std::uint64_t> SnapshotShardHits() const override;
  std::vector<std::uint64_t> SnapshotShardInserts() const override;

 private:
  struct Shard {
    mutable std::mutex mu;
    std::unordered_map<Fingerprint, IndexRecord, FingerprintHasher> map;
    std::uint64_t hits = 0;
    std::uint64_t inserts = 0;
  };

  std::vector<Shard> shards_;
};

}  // namespace bx
