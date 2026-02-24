#pragma once

#include <cstdint>

#include "scheduler.pb.h"

namespace zb::scheduler {

class FailureDetector {
public:
    FailureDetector(uint64_t suspect_timeout_ms, uint64_t dead_timeout_ms);

    zb::rpc::NodeHealthState Evaluate(uint64_t now_ms,
                                      uint64_t last_heartbeat_ms,
                                      zb::rpc::NodePowerState desired_power_state) const;

    uint64_t dead_timeout_ms() const { return dead_timeout_ms_; }
    uint64_t suspect_timeout_ms() const { return suspect_timeout_ms_; }

private:
    uint64_t suspect_timeout_ms_{0};
    uint64_t dead_timeout_ms_{0};
};

} // namespace zb::scheduler
