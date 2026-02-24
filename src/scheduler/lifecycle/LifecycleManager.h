#pragma once

#include <string>

#include "../model/ClusterState.h"
#include "NodeActuator.h"

namespace zb::scheduler {

class LifecycleManager {
public:
    LifecycleManager(ClusterState* state, NodeActuator* actuator);

    bool StartNode(const std::string& node_id,
                   const std::string& reason,
                   NodeOperationState* op,
                   std::string* error);
    bool StopNode(const std::string& node_id,
                  bool force,
                  const std::string& reason,
                  NodeOperationState* op,
                  std::string* error);
    bool RebootNode(const std::string& node_id,
                    const std::string& reason,
                    NodeOperationState* op,
                    std::string* error);

private:
    bool RunOperation(const std::string& node_id,
                      zb::rpc::NodeOperationType type,
                      bool force,
                      const std::string& reason,
                      NodeOperationState* op,
                      std::string* error);

    ClusterState* state_{};
    NodeActuator* actuator_{};
};

} // namespace zb::scheduler
