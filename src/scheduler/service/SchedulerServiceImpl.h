#pragma once

#include "../lifecycle/LifecycleManager.h"
#include "../model/ClusterState.h"
#include "scheduler.pb.h"

namespace zb::scheduler {

class SchedulerServiceImpl : public zb::rpc::SchedulerService {
public:
    SchedulerServiceImpl(ClusterState* state, LifecycleManager* lifecycle);

    void ReportHeartbeat(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::HeartbeatRequest* request,
                         zb::rpc::HeartbeatReply* response,
                         google::protobuf::Closure* done) override;

    void GetClusterView(google::protobuf::RpcController* cntl_base,
                        const zb::rpc::GetClusterViewRequest* request,
                        zb::rpc::GetClusterViewReply* response,
                        google::protobuf::Closure* done) override;

    void SetNodeAdminState(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::SetNodeAdminStateRequest* request,
                           zb::rpc::SetNodeAdminStateReply* response,
                           google::protobuf::Closure* done) override;

    void StartNode(google::protobuf::RpcController* cntl_base,
                   const zb::rpc::StartNodeRequest* request,
                   zb::rpc::NodeOperationReply* response,
                   google::protobuf::Closure* done) override;

    void StopNode(google::protobuf::RpcController* cntl_base,
                  const zb::rpc::StopNodeRequest* request,
                  zb::rpc::NodeOperationReply* response,
                  google::protobuf::Closure* done) override;

    void RebootNode(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::RebootNodeRequest* request,
                    zb::rpc::NodeOperationReply* response,
                    google::protobuf::Closure* done) override;

    void GetOperationStatus(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::GetOperationStatusRequest* request,
                            zb::rpc::NodeOperationReply* response,
                            google::protobuf::Closure* done) override;

private:
    static void FillStatus(zb::rpc::SchedulerStatus* status,
                           zb::rpc::SchedulerStatusCode code,
                           const std::string& message);
    static void FillNodeView(const NodeState& node, zb::rpc::NodeView* out);
    static void FillOperation(const NodeOperationState& op, zb::rpc::NodeOperation* out);

    ClusterState* state_{};
    LifecycleManager* lifecycle_{};
};

} // namespace zb::scheduler
