#include "bx/dedup_index.h"

#include <functional>

namespace bx {

std::unique_ptr<IDedupIndex> CreateIndex(const Config& config) {
  if (config.version == Version::kV2 || config.version == Version::kV3) {
    return std::make_unique<ShardedIndex>(config.num_shards);
  }
  return std::make_unique<GlobalLockIndex>();
}

bool GlobalLockIndex::CheckAndInsert(const FingerprintChunk& chunk) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(chunk.fingerprint);
  if (it == map_.end()) {
    IndexRecord rec;
    rec.canonical_chunk_id = chunk.fingerprint.hash;
    rec.length = chunk.chunk.length;
    rec.first_seen_file_id = chunk.chunk.file_id;
    rec.ref_count = 1;
    map_.emplace(chunk.fingerprint, rec);
    ++inserts_;
    return true;
  }
  ++it->second.ref_count;
  ++hits_;
  return false;
}

std::vector<std::uint64_t> GlobalLockIndex::SnapshotShardHits() const {
  std::lock_guard<std::mutex> lock(mu_);
  return {hits_};
}

std::vector<std::uint64_t> GlobalLockIndex::SnapshotShardInserts() const {
  std::lock_guard<std::mutex> lock(mu_);
  return {inserts_};
}

ShardedIndex::ShardedIndex(std::size_t shard_count) : shards_(std::max<std::size_t>(1, shard_count)) {}

bool ShardedIndex::CheckAndInsert(const FingerprintChunk& chunk) {
  const std::size_t shard_id = FingerprintHasher{}(chunk.fingerprint) % shards_.size();
  Shard& shard = shards_[shard_id];

  std::lock_guard<std::mutex> lock(shard.mu);
  auto it = shard.map.find(chunk.fingerprint);
  if (it == shard.map.end()) {
    IndexRecord rec;
    rec.canonical_chunk_id = chunk.fingerprint.hash;
    rec.length = chunk.chunk.length;
    rec.first_seen_file_id = chunk.chunk.file_id;
    rec.ref_count = 1;
    shard.map.emplace(chunk.fingerprint, rec);
    ++shard.inserts;
    return true;
  }
  ++it->second.ref_count;
  ++shard.hits;
  return false;
}

std::vector<std::uint64_t> ShardedIndex::SnapshotShardHits() const {
  std::vector<std::uint64_t> out;
  out.reserve(shards_.size());
  for (const Shard& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mu);
    out.push_back(shard.hits);
  }
  return out;
}

std::vector<std::uint64_t> ShardedIndex::SnapshotShardInserts() const {
  std::vector<std::uint64_t> out;
  out.reserve(shards_.size());
  for (const Shard& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mu);
    out.push_back(shard.inserts);
  }
  return out;
}

}  // namespace bx
