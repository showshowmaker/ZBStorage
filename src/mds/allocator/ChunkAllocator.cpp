#include "ChunkAllocator.h"

namespace zb::mds {

ChunkAllocator::ChunkAllocator(NodeStateCache* cache, PGManager* pg_manager, bool enable_pg_layout)
    : cache_(cache), pg_manager_(pg_manager), enable_pg_layout_(enable_pg_layout) {}

bool ChunkAllocator::AllocateChunk(uint32_t replica,
                                   const std::string& chunk_id,
                                   std::vector<zb::rpc::ReplicaLocation>* out) {
    if (!out || replica == 0) {
        return false;
    }

    if (enable_pg_layout_ && pg_manager_) {
        std::string error;
        return AllocateObjectByPg(replica, chunk_id, pg_manager_->CurrentEpoch(), out, &error);
    }

    if (!cache_) {
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

bool ChunkAllocator::AllocateObjectByPg(uint32_t replica,
                                        const std::string& object_id,
                                        uint64_t epoch,
                                        std::vector<zb::rpc::ReplicaLocation>* out,
                                        std::string* error) {
    if (!out || replica == 0) {
        if (error) {
            *error = "invalid allocate arguments";
        }
        return false;
    }
    if (!pg_manager_) {
        if (error) {
            *error = "pg manager is unavailable";
        }
        return false;
    }
    if (epoch == 0) {
        epoch = pg_manager_->CurrentEpoch();
    }
    if (epoch == 0) {
        if (error) {
            *error = "pg epoch is unavailable";
        }
        return false;
    }

    const uint32_t pg_id = pg_manager_->ObjectToPg(object_id, epoch);
    PgReplicaSet pg_set;
    if (!pg_manager_->ResolvePg(pg_id, epoch, &pg_set, error)) {
        return false;
    }
    if (pg_set.members.size() < replica) {
        if (error) {
            *error = "insufficient pg replicas, required=" + std::to_string(replica) +
                     ", actual=" + std::to_string(pg_set.members.size());
        }
        return false;
    }

    out->clear();
    out->reserve(replica);
    for (size_t i = 0; i < replica; ++i) {
        const auto& member = pg_set.members[i];
        zb::rpc::ReplicaLocation location;
        location.set_node_id(member.node_id);
        location.set_node_address(member.node_address);
        location.set_disk_id(member.disk_id);
        location.set_chunk_id(object_id);
        location.set_group_id(member.group_id);
        location.set_epoch(member.epoch);
        location.set_primary_node_id(member.primary_node_id);
        location.set_primary_address(member.primary_address);
        location.set_secondary_node_id(member.secondary_node_id);
        location.set_secondary_address(member.secondary_address);
        location.set_sync_ready(member.sync_ready);
        location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        location.set_replica_state(zb::rpc::REPLICA_READY);
        out->push_back(std::move(location));
    }
    return true;
}

void ChunkAllocator::SetPgManager(PGManager* pg_manager) {
    pg_manager_ = pg_manager;
}

void ChunkAllocator::SetPgEnabled(bool enabled) {
    enable_pg_layout_ = enabled;
}

} // namespace zb::mds
