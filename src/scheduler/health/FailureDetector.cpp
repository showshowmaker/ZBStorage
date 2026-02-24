#include "FailureDetector.h"

namespace zb::scheduler {

FailureDetector::FailureDetector(uint64_t suspect_timeout_ms, uint64_t dead_timeout_ms)
    : suspect_timeout_ms_(suspect_timeout_ms),
      dead_timeout_ms_(dead_timeout_ms) {}

zb::rpc::NodeHealthState FailureDetector::Evaluate(uint64_t now_ms,
                                                   uint64_t last_heartbeat_ms,
                                                   zb::rpc::NodePowerState desired_power_state) const {
    if (desired_power_state == zb::rpc::NODE_POWER_OFF) {
        return zb::rpc::NODE_HEALTH_HEALTHY;
    }
    if (last_heartbeat_ms == 0 || now_ms <= last_heartbeat_ms) {
        return zb::rpc::NODE_HEALTH_SUSPECT;
    }
    uint64_t elapsed = now_ms - last_heartbeat_ms;
    if (elapsed >= dead_timeout_ms_) {
        return zb::rpc::NODE_HEALTH_DEAD;
    }
    if (elapsed >= suspect_timeout_ms_) {
        return zb::rpc::NODE_HEALTH_SUSPECT;
    }
    return zb::rpc::NODE_HEALTH_HEALTHY;
}

} // namespace zb::scheduler
