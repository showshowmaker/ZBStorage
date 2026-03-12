#pragma once

#include <cstdint>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct OpticalCapacityPlan {
    uint64_t total_disc_count{0};
    uint64_t count_100gb{0};
    uint64_t count_1tb{0};
    uint64_t count_10tb{0};
    uint64_t target_eb{0};
    long double total_tb{0.0L};
    long double total_eb{0.0L};
    long double capacity_gap_tb{0.0L};
    std::string note;
};

class CapacityPlanner {
public:
    static bool BuildOpticalCapacityPlan(const ClusterScaleConfig& cluster,
                                         const OpticalCapacityPlanConfig& cfg,
                                         OpticalCapacityPlan* out,
                                         std::string* error);
};

} // namespace zb::meta_gen

