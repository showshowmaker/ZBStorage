#include "ClusterState.h"

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>

namespace zb::scheduler {

ClusterState::ClusterState(FailureDetector detector) : detector_(std::move(detector)) {}

HeartbeatAssignment ClusterState::ReportHeartbeat(const zb::rpc::HeartbeatRequest& request) {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t now_ms = request.report_ts_ms() ? request.report_ts_ms() : NowMs();

    NodeState& node = nodes_[request.node_id()];
    if (node.node_id.empty()) {
        node.node_id = request.node_id();
        node.admin_state = zb::rpc::NODE_ADMIN_ENABLED;
        node.desired_admin_state = zb::rpc::NODE_ADMIN_ENABLED;
        node.power_state = zb::rpc::NODE_POWER_ON;
        node.desired_power_state = zb::rpc::NODE_POWER_ON;
    }

    node.node_type = request.node_type();
    node.address = request.address();
    node.weight = std::max<uint32_t>(1, request.weight());
    node.virtual_node_count = std::max<uint32_t>(1, request.virtual_node_count());
    node.group_id = request.group_id().empty() ? request.node_id() : request.group_id();
    node.role = request.role();
    node.peer_node_id = request.peer_node_id();
    node.peer_address = request.peer_address();
    node.applied_lsn = request.applied_lsn();
    node.last_heartbeat_ms = now_ms;
    node.health_state = zb::rpc::NODE_HEALTH_HEALTHY;
    node.power_state = zb::rpc::NODE_POWER_ON;

    node.disks.clear();
    for (const auto& disk : request.disks()) {
        DiskState disk_state;
        disk_state.disk_id = disk.disk_id();
        disk_state.capacity_bytes = disk.capacity_bytes();
        disk_state.free_bytes = disk.free_bytes();
        disk_state.is_healthy = disk.is_healthy();
        disk_state.last_update_ms = now_ms;
        node.disks[disk_state.disk_id] = std::move(disk_state);
    }

    bool changed = true;
    EnsureGroupLocked(node.group_id);
    GroupState& group = groups_[node.group_id];
    if (group.primary_node_id.empty()) {
        if (node.role == zb::rpc::NODE_ROLE_SECONDARY) {
            group.secondary_node_id = node.node_id;
        } else {
            group.primary_node_id = node.node_id;
        }
    } else if (group.primary_node_id == node.node_id) {
        // keep primary
    } else if (group.secondary_node_id.empty() || group.secondary_node_id == node.node_id) {
        group.secondary_node_id = node.node_id;
    }

    ReconcileGroupLocked(node.group_id, &changed);
    if (changed) {
        ++generation_;
    }

    return BuildAssignmentLocked(nodes_[request.node_id()]);
}

uint64_t ClusterState::TickHealth() {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t now_ms = NowMs();
    bool changed = false;

    for (auto& item : nodes_) {
        NodeState& node = item.second;
        zb::rpc::NodeHealthState health = detector_.Evaluate(now_ms,
                                                             node.last_heartbeat_ms,
                                                             node.desired_power_state);
        if (health != node.health_state) {
            node.health_state = health;
            changed = true;
        }

        zb::rpc::NodePowerState original_power = node.power_state;
        if (node.desired_power_state == zb::rpc::NODE_POWER_OFF) {
            if (node.last_heartbeat_ms == 0 || now_ms - node.last_heartbeat_ms >= detector_.dead_timeout_ms()) {
                node.power_state = zb::rpc::NODE_POWER_OFF;
            }
        } else if (node.health_state == zb::rpc::NODE_HEALTH_HEALTHY) {
            node.power_state = zb::rpc::NODE_POWER_ON;
        }
        if (node.power_state != original_power) {
            changed = true;
        }
    }

    std::vector<std::string> group_ids;
    group_ids.reserve(groups_.size());
    for (const auto& item : groups_) {
        group_ids.push_back(item.first);
    }
    for (const auto& group_id : group_ids) {
        MaybeFailoverGroupLocked(group_id, &changed);
        ReconcileGroupLocked(group_id, &changed);
    }

    if (changed) {
        ++generation_;
    }
    return generation_;
}

uint64_t ClusterState::SetNodeAdminState(const std::string& node_id,
                                         zb::rpc::NodeAdminState state,
                                         std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return generation_;
    }
    it->second.admin_state = state;
    it->second.desired_admin_state = state;

    bool changed = true;
    MaybeFailoverGroupLocked(it->second.group_id, &changed);
    ReconcileGroupLocked(it->second.group_id, &changed);
    ++generation_;
    return generation_;
}

uint64_t ClusterState::SetDesiredPowerState(const std::string& node_id,
                                            zb::rpc::NodePowerState state,
                                            std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return generation_;
    }
    it->second.desired_power_state = state;
    ++generation_;
    return generation_;
}

uint64_t ClusterState::SetCurrentPowerState(const std::string& node_id,
                                            zb::rpc::NodePowerState state,
                                            std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return generation_;
    }
    it->second.power_state = state;
    ++generation_;
    return generation_;
}

bool ClusterState::Snapshot(uint64_t min_generation,
                            uint64_t* generation,
                            std::vector<NodeState>* nodes) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (generation) {
        *generation = generation_;
    }
    if (!nodes) {
        return true;
    }
    nodes->clear();
    if (generation_ < min_generation) {
        return true;
    }
    nodes->reserve(nodes_.size());
    for (const auto& item : nodes_) {
        nodes->push_back(item.second);
    }
    return true;
}

bool ClusterState::GetNode(const std::string& node_id, NodeState* node) const {
    if (!node) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    *node = it->second;
    return true;
}

bool ClusterState::CreateOperation(const std::string& node_id,
                                   zb::rpc::NodeOperationType type,
                                   const std::string& message,
                                   NodeOperationState* out,
                                   std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (nodes_.find(node_id) == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    NodeOperationState op;
    op.operation_id = "op-" + std::to_string(next_operation_id_++);
    op.node_id = node_id;
    op.operation_type = type;
    op.status = zb::rpc::NODE_OP_RUNNING;
    op.message = message;
    op.start_ts_ms = NowMs();
    operations_[op.operation_id] = op;
    if (out) {
        *out = op;
    }
    return true;
}

bool ClusterState::UpdateOperation(const std::string& operation_id,
                                   zb::rpc::NodeOperationStatus status,
                                   const std::string& message,
                                   std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = operations_.find(operation_id);
    if (it == operations_.end()) {
        if (error) {
            *error = "operation not found: " + operation_id;
        }
        return false;
    }
    it->second.status = status;
    it->second.message = message;
    if (status == zb::rpc::NODE_OP_SUCCEEDED || status == zb::rpc::NODE_OP_FAILED) {
        it->second.finish_ts_ms = NowMs();
    }
    return true;
}

bool ClusterState::GetOperation(const std::string& operation_id, NodeOperationState* out) const {
    if (!out) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = operations_.find(operation_id);
    if (it == operations_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

bool ClusterState::IsNodeEligibleAsPrimaryLocked(const NodeState& node) const {
    return node.health_state == zb::rpc::NODE_HEALTH_HEALTHY &&
           node.admin_state == zb::rpc::NODE_ADMIN_ENABLED &&
           node.power_state == zb::rpc::NODE_POWER_ON;
}

bool ClusterState::IsNodeEligibleAsSecondaryLocked(const NodeState& node) const {
    return node.health_state == zb::rpc::NODE_HEALTH_HEALTHY &&
           node.power_state == zb::rpc::NODE_POWER_ON &&
           node.admin_state != zb::rpc::NODE_ADMIN_DISABLED;
}

void ClusterState::EnsureGroupLocked(const std::string& group_id) {
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return;
    }
    GroupState group;
    group.group_id = group_id;
    group.epoch = 1;
    groups_[group_id] = std::move(group);
}

void ClusterState::ReconcileGroupLocked(const std::string& group_id, bool* changed) {
    auto group_it = groups_.find(group_id);
    if (group_it == groups_.end()) {
        return;
    }
    GroupState& group = group_it->second;

    if (!group.primary_node_id.empty() && nodes_.find(group.primary_node_id) == nodes_.end()) {
        group.primary_node_id.clear();
        if (changed) {
            *changed = true;
        }
    }
    if (!group.secondary_node_id.empty() && nodes_.find(group.secondary_node_id) == nodes_.end()) {
        group.secondary_node_id.clear();
        if (changed) {
            *changed = true;
        }
    }

    if (group.primary_node_id.empty()) {
        for (const auto& item : nodes_) {
            if (item.second.group_id == group_id) {
                group.primary_node_id = item.first;
                if (changed) {
                    *changed = true;
                }
                break;
            }
        }
    }

    if (group.secondary_node_id.empty()) {
        for (const auto& item : nodes_) {
            if (item.second.group_id != group_id || item.first == group.primary_node_id) {
                continue;
            }
            group.secondary_node_id = item.first;
            if (changed) {
                *changed = true;
            }
            break;
        }
    }

    if (group.primary_node_id == group.secondary_node_id) {
        group.secondary_node_id.clear();
        if (changed) {
            *changed = true;
        }
    }

    group.sync_ready = false;
    auto sec_it = nodes_.find(group.secondary_node_id);
    if (sec_it != nodes_.end() && IsNodeEligibleAsSecondaryLocked(sec_it->second)) {
        group.sync_ready = true;
    }

    for (auto& item : nodes_) {
        NodeState& node = item.second;
        if (node.group_id != group_id) {
            continue;
        }
        zb::rpc::NodeRole new_role = zb::rpc::NODE_ROLE_UNKNOWN;
        if (node.node_id == group.primary_node_id) {
            new_role = zb::rpc::NODE_ROLE_PRIMARY;
        } else if (node.node_id == group.secondary_node_id) {
            new_role = zb::rpc::NODE_ROLE_SECONDARY;
        }
        if (node.role != new_role) {
            node.role = new_role;
            if (changed) {
                *changed = true;
            }
        }
        if (node.epoch != group.epoch) {
            node.epoch = group.epoch;
            if (changed) {
                *changed = true;
            }
        }
        if (node.sync_ready != group.sync_ready) {
            node.sync_ready = group.sync_ready;
            if (changed) {
                *changed = true;
            }
        }
    }
}

void ClusterState::MaybeFailoverGroupLocked(const std::string& group_id, bool* changed) {
    auto group_it = groups_.find(group_id);
    if (group_it == groups_.end()) {
        return;
    }
    GroupState& group = group_it->second;

    auto primary_it = nodes_.find(group.primary_node_id);
    auto secondary_it = nodes_.find(group.secondary_node_id);
    bool primary_ok = primary_it != nodes_.end() && IsNodeEligibleAsPrimaryLocked(primary_it->second);
    bool secondary_ok = secondary_it != nodes_.end() && IsNodeEligibleAsSecondaryLocked(secondary_it->second);
    if (primary_ok || !secondary_ok) {
        return;
    }

    const std::string old_primary = group.primary_node_id;
    group.primary_node_id = group.secondary_node_id;
    group.secondary_node_id = old_primary;
    ++group.epoch;
    if (changed) {
        *changed = true;
    }
}

HeartbeatAssignment ClusterState::BuildAssignmentLocked(const NodeState& node) const {
    HeartbeatAssignment assignment;
    assignment.generation = generation_;
    assignment.group_id = node.group_id;
    assignment.assigned_role = node.role;
    assignment.epoch = node.epoch;

    auto group_it = groups_.find(node.group_id);
    if (group_it == groups_.end()) {
        return assignment;
    }
    const GroupState& group = group_it->second;
    assignment.primary_node_id = group.primary_node_id;
    assignment.secondary_node_id = group.secondary_node_id;
    assignment.epoch = group.epoch;

    auto p_it = nodes_.find(group.primary_node_id);
    if (p_it != nodes_.end()) {
        assignment.primary_address = p_it->second.address;
    }
    auto s_it = nodes_.find(group.secondary_node_id);
    if (s_it != nodes_.end()) {
        assignment.secondary_address = s_it->second.address;
    }
    return assignment;
}

uint64_t ClusterState::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::scheduler
