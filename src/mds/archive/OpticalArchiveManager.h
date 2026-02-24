#pragma once

#include <brpc/channel.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

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

    OpticalArchiveManager(RocksMetaStore* store, NodeStateCache* cache, Options options);

    // Runs one archive + eviction round. Returns true if the round completed.
    bool RunOnce(std::string* error);

private:
    bool ShouldArchiveNow(const std::vector<NodeInfo>& nodes);
    bool LoadInodeAttr(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool ReadChunkFromReplica(const zb::rpc::ReplicaLocation& source,
                              uint64_t read_size,
                              std::string* data,
                              std::string* error);
    bool WriteChunkToOptical(const NodeSelection& optical,
                             const std::string& chunk_id,
                             const std::string& data,
                             std::string* error);
    bool DeleteDiskReplica(const zb::rpc::ReplicaLocation& replica, std::string* error);
    brpc::Channel* GetChannel(const std::string& address, std::string* error);

    RocksMetaStore* store_{};
    NodeStateCache* cache_{};
    Options options_;
    bool archive_mode_{false};

    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

} // namespace zb::mds
