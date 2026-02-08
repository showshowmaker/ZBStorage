#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "../config/MdsConfig.h"

namespace zb::mds {

class NodeStateCache {
public:
    explicit NodeStateCache(std::vector<NodeInfo> nodes);

    std::vector<NodeInfo> Snapshot() const;
    std::vector<NodeInfo*> PickNodes(uint32_t count);

private:
    mutable std::mutex mu_;
    std::vector<NodeInfo> nodes_;
    size_t next_node_index_{0};
};

} // namespace zb::mds
