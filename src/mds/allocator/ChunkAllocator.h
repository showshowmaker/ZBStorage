#pragma once

#include <string>
#include <vector>

#include "PGManager.h"
#include "NodeStateCache.h"
#include "mds.pb.h"

namespace zb::mds {

class ChunkAllocator {
public:
    explicit ChunkAllocator(NodeStateCache* cache, PGManager* pg_manager = nullptr, bool enable_pg_layout = false);

    bool AllocateChunk(uint32_t replica,
                       const std::string& chunk_id,
                       std::vector<zb::rpc::ReplicaLocation>* out);
    bool AllocateObjectByPg(uint32_t replica,
                            const std::string& object_id,
                            uint64_t epoch,
                            std::vector<zb::rpc::ReplicaLocation>* out,
                            std::string* error);
    void SetPgManager(PGManager* pg_manager);
    void SetPgEnabled(bool enabled);

private:
    NodeStateCache* cache_{};
    PGManager* pg_manager_{};
    bool enable_pg_layout_{false};
};

} // namespace zb::mds
