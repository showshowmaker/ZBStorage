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
    std::vector<NodeInfo*> nodes = cache_->PickNodes(replica);
    if (nodes.empty()) {
        return false;
    }

    for (auto* node : nodes) {
        if (!node) {
            continue;
        }
        zb::rpc::ReplicaLocation location;
        location.set_node_id(node->node_id);
        location.set_node_address(node->address);
        location.set_disk_id(PickDisk(node));
        location.set_chunk_id(chunk_id);
        out->push_back(location);
    }

    return true;
}

std::string ChunkAllocator::PickDisk(NodeInfo* node) {
    if (!node || node->disks.empty()) {
        return "disk-01";
    }
    size_t index = node->next_disk_index % node->disks.size();
    node->next_disk_index = (node->next_disk_index + 1) % node->disks.size();
    return node->disks[index].disk_id;
}

} // namespace zb::mds
