#include "capacity_planner.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace zb::meta_gen {

namespace {

constexpr long double kTbPerEb = 1000000.0L;

bool SolveStrictThreeTier(uint64_t disc_count,
                          uint64_t target_eb,
                          uint64_t min_100gb,
                          uint64_t min_1tb,
                          uint64_t min_10tb,
                          uint64_t* out_100gb,
                          uint64_t* out_1tb,
                          uint64_t* out_10tb) {
    if (!out_100gb || !out_1tb || !out_10tb) {
        return false;
    }

    // Unit: 0.1 TB.
    // 100GB => 1, 1TB => 10, 10TB => 100.
    const long double target_tb_ld = static_cast<long double>(target_eb) * kTbPerEb;
    const long double target_units_ld = target_tb_ld * 10.0L;
    if (target_units_ld < 0.0L ||
        target_units_ld > static_cast<long double>(std::numeric_limits<uint64_t>::max())) {
        return false;
    }
    const uint64_t target_units = static_cast<uint64_t>(target_units_ld);

    if (min_100gb + min_1tb + min_10tb > disc_count) {
        return false;
    }

    if (target_units < disc_count) {
        return false;
    }
    const uint64_t rhs = target_units - disc_count; // 9*y + 99*z
    if (rhs % 9ULL != 0ULL) {
        return false;
    }
    const uint64_t rhs9 = rhs / 9ULL; // y + 11*z

    const uint64_t z_min = min_10tb;
    uint64_t z_max = std::min<uint64_t>(disc_count, rhs9 / 11ULL);
    if (z_max < z_min) {
        return false;
    }

    // Prefer more 10TB discs to minimize tiny-disc metadata fanout.
    for (uint64_t z = z_max + 1; z > z_min; --z) {
        const uint64_t cur_z = z - 1;
        if (rhs9 < 11ULL * cur_z) {
            continue;
        }
        const uint64_t y = rhs9 - 11ULL * cur_z;
        if (y > disc_count - cur_z) {
            continue;
        }
        const uint64_t x = disc_count - cur_z - y;
        if (x < min_100gb || y < min_1tb || cur_z < min_10tb) {
            continue;
        }
        *out_100gb = x;
        *out_1tb = y;
        *out_10tb = cur_z;
        return true;
    }
    return false;
}

} // namespace

bool CapacityPlanner::BuildOpticalCapacityPlan(const ClusterScaleConfig& cluster,
                                               const OpticalCapacityPlanConfig& cfg,
                                               OpticalCapacityPlan* out,
                                               std::string* error) {
    if (!out) {
        if (error) {
            *error = "capacity plan output is null";
        }
        return false;
    }

    OpticalCapacityPlan plan;
    plan.total_disc_count = cluster.optical_node_count * cluster.discs_per_optical_node;
    plan.target_eb = cfg.target_eb;

    uint64_t c100 = 0;
    uint64_t c1 = 0;
    uint64_t c10 = 0;
    if (!SolveStrictThreeTier(plan.total_disc_count,
                              cfg.target_eb,
                              cfg.min_100gb_discs,
                              cfg.min_1tb_discs,
                              cfg.min_10tb_discs,
                              &c100,
                              &c1,
                              &c10)) {
        if (error) {
            *error = "unable to find an exact three-tier optical capacity solution";
        }
        return false;
    }

    plan.count_100gb = c100;
    plan.count_1tb = c1;
    plan.count_10tb = c10;
    plan.total_tb = static_cast<long double>(c100) * 0.1L +
                    static_cast<long double>(c1) * 1.0L +
                    static_cast<long double>(c10) * 10.0L;
    plan.total_eb = plan.total_tb / kTbPerEb;
    const long double target_tb = static_cast<long double>(cfg.target_eb) * kTbPerEb;
    plan.capacity_gap_tb = plan.total_tb - target_tb;
    if (plan.count_100gb == 0 && plan.count_1tb == 0) {
        plan.note = "exact target reached; only 10TB discs are feasible under strict mode";
    } else {
        plan.note = "exact target reached with mixed tiers";
    }

    *out = std::move(plan);
    return true;
}

} // namespace zb::meta_gen
