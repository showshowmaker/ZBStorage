#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ArchiveCandidateQueue.h"

namespace zb::mds {

struct StagedArchiveChunk {
    ArchiveCandidateEntry candidate;
    std::string lease_id;
    std::string op_id;
    std::string chunk_key;
    uint64_t version{0};
    uint64_t size_bytes{0};
    std::string data_file;
    bool done{false};
};

class ArchiveBatchStager {
public:
    struct Options {
        uint64_t disc_size_bytes{1ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL};
        bool strict_full_disc{true};
        uint64_t max_batch_age_ms{0};
    };

    bool Init(const std::string& staging_dir, Options options, std::string* error);

    bool StageChunk(const ArchiveCandidateEntry& candidate,
                    const std::string& lease_id,
                    const std::string& op_id,
                    const std::string& chunk_key,
                    uint64_t version,
                    const std::string& data,
                    bool* inserted,
                    bool* deferred,
                    std::string* error);
    bool HasSealedBatch() const;
    bool SealIfReady(std::string* error);
    std::vector<StagedArchiveChunk> SnapshotSealedBatch() const;
    bool ReadChunkData(const StagedArchiveChunk& staged, std::string* data, std::string* error) const;
    bool UpdateLease(const std::string& chunk_id,
                     const std::string& lease_id,
                     const std::string& op_id,
                     uint64_t version,
                     std::string* error);
    bool MarkChunkDone(const std::string& chunk_id, std::string* error);
    bool RemoveChunk(const std::string& chunk_id, std::string* error);
    bool ResetIfDrained(std::string* error);
    bool ContainsChunk(const std::string& chunk_id) const;
    uint64_t CurrentBytes() const;
    uint64_t DiscSizeBytes() const;
    std::string CurrentBatchId() const;

private:
    static uint64_t NowMilliseconds();
    static std::string BuildBatchId(uint64_t now_ms);
    static std::vector<std::string> SplitTabs(const std::string& line);

    bool LoadManifestLocked(std::string* error);
    bool PersistManifestLocked(std::string* error) const;
    bool EnsureDirsLocked(std::string* error) const;
    bool IsReadyToSealLocked() const;
    std::string ChunkFilePathLocked(const std::string& chunk_id) const;
    bool RemoveChunkFileNoThrow(const std::string& path) const;

    mutable std::mutex mu_;
    bool inited_{false};
    std::string staging_dir_;
    std::string chunk_dir_;
    std::string manifest_path_;
    Options options_;

    std::string batch_id_;
    uint64_t created_ts_ms_{0};
    bool sealed_{false};
    uint64_t total_bytes_{0};
    std::unordered_map<std::string, StagedArchiveChunk> chunks_;
    std::vector<std::string> order_;
};

} // namespace zb::mds
