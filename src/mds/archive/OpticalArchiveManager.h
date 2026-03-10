#pragma once

#include <brpc/channel.h>
#include <rocksdb/write_batch.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ArchiveCandidateQueue.h"
#include "ArchiveBatchStager.h"
#include "ArchiveLeaseManager.h"
#include "../allocator/NodeStateCache.h"
#include "../storage/RocksMetaStore.h"
#include "mds.pb.h"

namespace zb::mds {

class OpticalArchiveManager {
public:
    struct Options {
        uint64_t archive_trigger_bytes{10ULL * 1024ULL * 1024ULL * 1024ULL};
        uint64_t archive_target_bytes{8ULL * 1024ULL * 1024ULL * 1024ULL};
        uint64_t cold_file_ttl_sec{3600};
        uint32_t max_chunks_per_round{64};
        uint64_t default_chunk_size{4ULL * 1024ULL * 1024ULL};
    };

    OpticalArchiveManager(RocksMetaStore* store,
                          NodeStateCache* cache,
                          ArchiveCandidateQueue* candidate_queue,
                          ArchiveBatchStager* batch_stager,
                          ArchiveLeaseManager* lease_manager,
                          Options options);

    // Runs one archive + eviction round. Returns true if the round completed.
    bool RunOnce(std::string* error);

private:
    enum class ArchiveByCandidateResult {
        kArchived,
        kSkipped,
        kFailed,
    };

    bool ShouldArchiveNow(const std::vector<NodeInfo>& nodes);
    bool LoadInodeAttr(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool ReadChunkFromReplica(const zb::rpc::ReplicaLocation& source,
                              uint64_t read_size,
                              std::string* data,
                              std::string* error);
    bool WriteChunkToOptical(const NodeSelection& optical,
                             const std::string& chunk_id,
                             const std::string& op_id,
                             const std::string& data,
                             uint64_t inode_id,
                             uint32_t chunk_index,
                             const zb::rpc::InodeAttr* inode_attr,
                             zb::rpc::ReplicaLocation* optical_location,
                             std::string* error);
    bool DeleteDiskReplica(const zb::rpc::ReplicaLocation& replica, std::string* error);
    bool ResolveCandidateSource(const ArchiveCandidateEntry& candidate,
                                std::string* chunk_key,
                                zb::rpc::ReplicaLocation* source_disk,
                                uint64_t* chunk_size,
                                std::string* error);
    bool PersistOpticalReplica(const std::string& chunk_key,
                               const NodeSelection& optical,
                               const std::string& chunk_id,
                               uint64_t data_size,
                               const zb::rpc::ReplicaLocation* optical_location,
                               rocksdb::WriteBatch* update_batch,
                               std::string* error);
    bool BurnSealedBatch(const NodeSelection& optical,
                         uint32_t* archived_count,
                         std::unordered_set<std::string>* touched_chunk_keys,
                         std::string* error);
    ArchiveByCandidateResult ArchiveByCandidate(const ArchiveCandidateEntry& candidate,
                                                const NodeSelection& optical,
                                                const std::string& lease_id,
                                                const std::string& op_id,
                                                std::unordered_set<std::string>* touched_chunk_keys,
                                                std::string* error);
    bool ReconcileArchiveStates(uint32_t max_records, std::string* error);
    bool ProcessReverseChunkRepairTasks(uint32_t max_records, std::string* error);
    bool EnqueueReverseChunkRepair(const std::string& chunk_id, std::string* error);
    bool FindChunkKeyByChunkId(const std::string& chunk_id, std::string* chunk_key, std::string* error);
    bool HasReadyOpticalReplica(const std::string& chunk_key,
                                const std::string& chunk_id,
                                bool* has_ready,
                                std::string* error);
    bool IsOpticalWriteCommitted(const std::string& chunk_id, const std::string& op_id, bool* committed, std::string* error);
    bool MarkOpticalWriteCommitted(const std::string& chunk_id, const std::string& op_id, std::string* error);
    bool UpdateSourceArchiveState(const ArchiveCandidateEntry& candidate,
                                  const std::string& archive_state,
                                  uint64_t version,
                                  std::string* error);
    brpc::Channel* GetChannel(const std::string& address, std::string* error);

    RocksMetaStore* store_{};
    NodeStateCache* cache_{};
    ArchiveCandidateQueue* candidate_queue_{};
    ArchiveBatchStager* batch_stager_{};
    ArchiveLeaseManager* lease_manager_{};
    Options options_;
    bool archive_mode_{false};

    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

} // namespace zb::mds
