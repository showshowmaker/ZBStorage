#pragma once

#include <string>
#include <vector>

#include "NodeStateCache.h"
#include "mds.pb.h"

namespace zb::mds {

class ChunkAllocator {
public:
    explicit ChunkAllocator(NodeStateCache* cache);

    bool AllocateChunk(uint32_t replica,
                       const std::string& chunk_id,
                       std::vector<zb::rpc::ReplicaLocation>* out);

private:
    static std::string PickDisk(NodeInfo* node);

    NodeStateCache* cache_{};
};

} // namespace zb::mds
