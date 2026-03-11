#pragma once

#include <string>
#include <vector>

#include "PGManager.h"
#include "NodeStateCache.h"
#include "mds.pb.h"

namespace zb::mds {

class ObjectAllocator {
public:
    explicit ObjectAllocator(NodeStateCache* cache, PGManager* pg_manager = nullptr);

    bool AllocateObject(uint32_t replica,
                        const std::string& object_id,
                        std::vector<zb::rpc::ReplicaLocation>* out);
    bool AllocateObjectByPg(uint32_t replica,
                            const std::string& object_id,
                            uint64_t epoch,
                            std::vector<zb::rpc::ReplicaLocation>* out,
                            std::string* error);
    void SetPgManager(PGManager* pg_manager);

private:
    NodeStateCache* cache_{};
    PGManager* pg_manager_{};
};

} // namespace zb::mds
