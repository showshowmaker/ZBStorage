#include "NodeStateCache.h"

namespace zb::mds {

NodeStateCache::NodeStateCache(std::vector<NodeInfo> nodes) : nodes_(std::move(nodes)) {}

std::vector<NodeInfo> NodeStateCache::Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return nodes_;
}

std::vector<NodeInfo*> NodeStateCache::PickNodes(uint32_t count) {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<NodeInfo*> picked;
    if (nodes_.empty() || count == 0) {
        return picked;
    }
    for (uint32_t i = 0; i < count; ++i) {
        const size_t index = next_node_index_ % nodes_.size();
        picked.push_back(&nodes_[index]);
        next_node_index_ = (next_node_index_ + 1) % nodes_.size();
    }
    return picked;
}

} // namespace zb::mds
