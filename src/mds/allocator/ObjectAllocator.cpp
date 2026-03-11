#include "ObjectAllocator.h"

namespace zb::mds {

ObjectAllocator::ObjectAllocator(NodeStateCache* cache, PGManager* pg_manager)
    : cache_(cache), pg_manager_(pg_manager) {}

bool ObjectAllocator::AllocateObject(uint32_t replica,
                                     const std::string& object_id,
                                     std::vector<zb::rpc::ReplicaLocation>* out) {
    if (!out || replica == 0) {
        return false;
    }

    if (!pg_manager_) {
        return false;
    }
    std::string error;
    return AllocateObjectByPg(replica, object_id, pg_manager_->CurrentEpoch(), out, &error);
}

bool ObjectAllocator::AllocateObjectByPg(uint32_t replica,
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
        location.set_object_id(object_id);
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

void ObjectAllocator::SetPgManager(PGManager* pg_manager) {
    pg_manager_ = pg_manager;
}

} // namespace zb::mds
