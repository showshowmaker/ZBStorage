#include "SchedulerServiceImpl.h"

#include <brpc/controller.h>

namespace zb::scheduler {

SchedulerServiceImpl::SchedulerServiceImpl(ClusterState* state, LifecycleManager* lifecycle)
    : state_(state), lifecycle_(lifecycle) {}

void SchedulerServiceImpl::ReportHeartbeat(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::HeartbeatRequest* request,
                                           zb::rpc::HeartbeatReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid heartbeat request");
        return;
    }

    HeartbeatAssignment assignment = state_->ReportHeartbeat(*request);
    response->set_generation(assignment.generation);
    response->set_assigned_role(assignment.assigned_role);
    response->set_epoch(assignment.epoch);
    response->set_group_id(assignment.group_id);
    response->set_primary_node_id(assignment.primary_node_id);
    response->set_primary_address(assignment.primary_address);
    response->set_secondary_node_id(assignment.secondary_node_id);
    response->set_secondary_address(assignment.secondary_address);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetClusterView(google::protobuf::RpcController* cntl_base,
                                          const zb::rpc::GetClusterViewRequest* request,
                                          zb::rpc::GetClusterViewReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INTERNAL_ERROR,
                   "service not initialized");
        return;
    }

    uint64_t generation = 0;
    std::vector<NodeState> nodes;
    state_->Snapshot(request->min_generation(), &generation, &nodes);
    response->set_generation(generation);
    for (const auto& node : nodes) {
        FillNodeView(node, response->add_nodes());
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::SetNodeAdminState(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::SetNodeAdminStateRequest* request,
                                             zb::rpc::SetNodeAdminStateReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    std::string error;
    uint64_t generation = state_->SetNodeAdminState(request->node_id(), request->admin_state(), &error);
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, error);
        return;
    }
    response->set_generation(generation);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::StartNode(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::StartNodeRequest* request,
                                     zb::rpc::NodeOperationReply* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->StartNode(request->node_id(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::StopNode(google::protobuf::RpcController* cntl_base,
                                    const zb::rpc::StopNodeRequest* request,
                                    zb::rpc::NodeOperationReply* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->StopNode(request->node_id(), request->force(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::RebootNode(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::RebootNodeRequest* request,
                                      zb::rpc::NodeOperationReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->RebootNode(request->node_id(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetOperationStatus(google::protobuf::RpcController* cntl_base,
                                              const zb::rpc::GetOperationStatusRequest* request,
                                              zb::rpc::NodeOperationReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->operation_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }

    NodeOperationState op;
    if (!state_->GetOperation(request->operation_id(), &op)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, "operation not found");
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::FillStatus(zb::rpc::SchedulerStatus* status,
                                      zb::rpc::SchedulerStatusCode code,
                                      const std::string& message) {
    if (!status) {
        return;
    }
    status->set_code(code);
    status->set_message(message);
}

void SchedulerServiceImpl::FillNodeView(const NodeState& node, zb::rpc::NodeView* out) {
    if (!out) {
        return;
    }
    out->set_node_id(node.node_id);
    out->set_node_type(node.node_type);
    out->set_address(node.address);
    out->set_weight(node.weight);
    out->set_virtual_node_count(node.virtual_node_count);
    out->set_health_state(node.health_state);
    out->set_admin_state(node.admin_state);
    out->set_power_state(node.power_state);
    out->set_desired_admin_state(node.desired_admin_state);
    out->set_desired_power_state(node.desired_power_state);
    out->set_last_heartbeat_ms(node.last_heartbeat_ms);
    out->set_group_id(node.group_id);
    out->set_role(node.role);
    out->set_epoch(node.epoch);
    out->set_applied_lsn(node.applied_lsn);
    out->set_peer_node_id(node.peer_node_id);
    out->set_peer_address(node.peer_address);
    out->set_sync_ready(node.sync_ready);
    for (const auto& item : node.disks) {
        const DiskState& disk = item.second;
        zb::rpc::NodeDiskView* d = out->add_disks();
        d->set_disk_id(disk.disk_id);
        d->set_capacity_bytes(disk.capacity_bytes);
        d->set_free_bytes(disk.free_bytes);
        d->set_is_healthy(disk.is_healthy);
        d->set_last_update_ms(disk.last_update_ms);
    }
}

void SchedulerServiceImpl::FillOperation(const NodeOperationState& op, zb::rpc::NodeOperation* out) {
    if (!out) {
        return;
    }
    out->set_operation_id(op.operation_id);
    out->set_node_id(op.node_id);
    out->set_operation_type(op.operation_type);
    out->set_status(op.status);
    out->set_message(op.message);
    out->set_start_ts_ms(op.start_ts_ms);
    out->set_finish_ts_ms(op.finish_ts_ms);
}

} // namespace zb::scheduler
