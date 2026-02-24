#include "ChunkAllocator.h"

namespace zb::mds {

ChunkAllocator::ChunkAllocator(NodeStateCache* cache) : cache_(cache) {}

bool ChunkAllocator::AllocateChunk(uint32_t replica,
                                   const std::string& chunk_id,
                                   std::vector<zb::rpc::ReplicaLocation>* out) {
    if (!cache_ || !out || replica == 0) {
        return false;
    }

    out->clear();
    std::vector<NodeSelection> nodes = cache_->PickNodes(replica);
    if (nodes.empty()) {
        return false;
    }

    for (const auto& node : nodes) {
        zb::rpc::ReplicaLocation location;
        location.set_node_id(node.node_id);
        location.set_node_address(node.address);
        location.set_disk_id(node.disk_id);
        location.set_chunk_id(chunk_id);
        location.set_group_id(node.group_id);
        location.set_epoch(node.epoch);
        location.set_primary_node_id(node.node_id);
        location.set_primary_address(node.address);
        location.set_secondary_node_id(node.secondary_node_id);
        location.set_secondary_address(node.secondary_address);
        location.set_sync_ready(node.sync_ready);
        location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        location.set_replica_state(zb::rpc::REPLICA_READY);
        out->push_back(location);
    }

    return true;
}

} // namespace zb::mds
