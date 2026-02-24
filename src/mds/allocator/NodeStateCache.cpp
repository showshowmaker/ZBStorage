#include "NodeStateCache.h"

#include <algorithm>
#include <unordered_set>

namespace zb::mds {

NodeStateCache::NodeStateCache(std::vector<NodeInfo> nodes) : nodes_(std::move(nodes)) {}

std::vector<NodeInfo> NodeStateCache::Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return nodes_;
}

void NodeStateCache::ReplaceNodes(std::vector<NodeInfo> nodes) {
    std::lock_guard<std::mutex> lock(mu_);
    nodes_ = std::move(nodes);
    next_node_index_ = 0;
    repeat_remaining_ = 0;
}

std::vector<NodeSelection> NodeStateCache::PickNodes(uint32_t count) {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<NodeSelection> picked;
    if (nodes_.empty() || count == 0) {
        return picked;
    }
    // Default allocator excludes optical tier and only picks allocatable primaries.
    NodeType filter = NodeType::kOptical;
    bool strict_filter = false;
    picked.reserve(count);
    std::unordered_set<std::string> selected;
    size_t unique_target = std::min(static_cast<size_t>(count), EstimateLogicalNodeCountLocked(filter, strict_filter));
    size_t max_attempts = std::max<size_t>(64, unique_target * EstimateWeightSumLocked(filter, strict_filter) * 2);
    max_attempts = std::min<size_t>(max_attempts, 1000000);

    for (size_t attempt = 0; attempt < max_attempts && picked.size() < unique_target; ++attempt) {
        NodeSelection selection = NextSelectionLocked(filter, strict_filter);
        if (selection.node_id.empty()) {
            break;
        }
        if (selected.insert(selection.node_id).second) {
            picked.push_back(std::move(selection));
        }
    }

    while (picked.size() < count) {
        NodeSelection selection = NextSelectionLocked(filter, strict_filter);
        if (selection.node_id.empty()) {
            break;
        }
        picked.push_back(std::move(selection));
    }

    return picked;
}

std::vector<NodeSelection> NodeStateCache::PickNodesByType(uint32_t count, NodeType type) {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<NodeSelection> picked;
    if (nodes_.empty() || count == 0) {
        return picked;
    }

    picked.reserve(count);
    std::unordered_set<std::string> selected;
    size_t unique_target = std::min(static_cast<size_t>(count), EstimateLogicalNodeCountLocked(type, true));
    size_t max_attempts = std::max<size_t>(64, unique_target * EstimateWeightSumLocked(type, true) * 2);
    max_attempts = std::min<size_t>(max_attempts, 1000000);

    for (size_t attempt = 0; attempt < max_attempts && picked.size() < unique_target; ++attempt) {
        NodeSelection selection = NextSelectionLocked(type, true);
        if (selection.node_id.empty()) {
            break;
        }
        if (selected.insert(selection.node_id).second) {
            picked.push_back(std::move(selection));
        }
    }

    while (picked.size() < count) {
        NodeSelection selection = NextSelectionLocked(type, true);
        if (selection.node_id.empty()) {
            break;
        }
        picked.push_back(std::move(selection));
    }

    return picked;
}

NodeSelection NodeStateCache::NextSelectionLocked(NodeType type_filter, bool strict_type_filter) {
    NodeSelection selection;
    if (nodes_.empty()) {
        return selection;
    }

    NodeInfo* node = nullptr;
    size_t scanned = 0;
    while (scanned < nodes_.size()) {
        NodeInfo* candidate = &nodes_[next_node_index_ % nodes_.size()];
        if (IsNodeAllocatable(*candidate, type_filter, strict_type_filter)) {
            node = candidate;
            break;
        }
        next_node_index_ = (next_node_index_ + 1) % nodes_.size();
        repeat_remaining_ = 0;
        ++scanned;
    }
    if (!node) {
        return selection;
    }
    if (repeat_remaining_ == 0) {
        repeat_remaining_ = std::max<uint32_t>(1, node->weight);
    }

    selection.address = node->address;
    selection.type = node->type;
    selection.group_id = node->group_id.empty() ? node->node_id : node->group_id;
    selection.epoch = node->epoch;
    selection.secondary_node_id = node->secondary_node_id;
    selection.secondary_address = node->secondary_address;
    selection.sync_ready = node->sync_ready;

    uint64_t virtual_index = 0;
    if (node->type == NodeType::kVirtual) {
        uint32_t total_virtual_nodes = std::max<uint32_t>(1, node->virtual_node_count);
        virtual_index = node->next_virtual_index % total_virtual_nodes;
        node->next_virtual_index = (node->next_virtual_index + 1) % total_virtual_nodes;
        selection.node_id = BuildVirtualNodeId(node->node_id, virtual_index);
    } else {
        selection.node_id = node->node_id;
    }

    selection.disk_id = PickDiskLocked(node, virtual_index);

    --repeat_remaining_;
    if (repeat_remaining_ == 0) {
        next_node_index_ = (next_node_index_ + 1) % nodes_.size();
    }

    return selection;
}

std::string NodeStateCache::PickDiskLocked(NodeInfo* node, uint64_t virtual_index) {
    if (!node || node->disks.empty()) {
        return "disk-01";
    }

    size_t base = node->next_disk_index % node->disks.size();
    size_t shift = static_cast<size_t>(virtual_index % node->disks.size());
    size_t index = (base + shift) % node->disks.size();
    node->next_disk_index = (node->next_disk_index + 1) % node->disks.size();
    return node->disks[index].disk_id;
}

std::string NodeStateCache::BuildVirtualNodeId(const std::string& base_node_id, uint64_t virtual_index) {
    return base_node_id + "-v" + std::to_string(virtual_index);
}

size_t NodeStateCache::EstimateLogicalNodeCountLocked(NodeType type_filter, bool strict_type_filter) const {
    size_t total = 0;
    for (const auto& node : nodes_) {
        if (!IsNodeAllocatable(node, type_filter, strict_type_filter)) {
            continue;
        }
        if (node.type == NodeType::kVirtual) {
            total += std::max<uint32_t>(1, node.virtual_node_count);
        } else {
            ++total;
        }
    }
    return total;
}

size_t NodeStateCache::EstimateWeightSumLocked(NodeType type_filter, bool strict_type_filter) const {
    size_t total = 0;
    for (const auto& node : nodes_) {
        if (!IsNodeAllocatable(node, type_filter, strict_type_filter)) {
            continue;
        }
        total += std::max<uint32_t>(1, node.weight);
    }
    return std::max<size_t>(1, total);
}

bool NodeStateCache::IsNodeAllocatable(const NodeInfo& node, NodeType type_filter, bool strict_type_filter) {
    if (!node.allocatable || !node.is_primary) {
        return false;
    }
    if (strict_type_filter) {
        return node.type == type_filter;
    }
    return node.type != type_filter;
}

} // namespace zb::mds
