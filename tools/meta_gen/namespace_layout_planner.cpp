#include "namespace_layout_planner.h"

#include <limits>
#include <utility>

namespace zb::meta_gen {

namespace {

bool SafeMul(uint64_t a, uint64_t b, uint64_t* out) {
    if (!out) {
        return false;
    }
    if (a == 0 || b == 0) {
        *out = 0;
        return true;
    }
    if (a > std::numeric_limits<uint64_t>::max() / b) {
        return false;
    }
    *out = a * b;
    return true;
}

bool PowU64(uint64_t base, uint32_t exp, uint64_t* out) {
    if (!out) {
        return false;
    }
    uint64_t v = 1;
    for (uint32_t i = 0; i < exp; ++i) {
        if (!SafeMul(v, base, &v)) {
            return false;
        }
    }
    *out = v;
    return true;
}

bool SumGeom(uint64_t base, uint32_t depth, uint64_t* out) {
    if (!out) {
        return false;
    }
    uint64_t sum = 0;
    uint64_t cur = 1;
    for (uint32_t i = 0; i <= depth; ++i) {
        if (sum > std::numeric_limits<uint64_t>::max() - cur) {
            return false;
        }
        sum += cur;
        if (i == depth) {
            break;
        }
        if (!SafeMul(cur, base, &cur)) {
            return false;
        }
    }
    *out = sum;
    return true;
}

} // namespace

bool NamespaceLayoutPlanner::BuildPlan(const ClusterScaleConfig& cluster,
                                       const DirectoryLayoutConfig& cfg,
                                       NamespaceLayoutPlan* out,
                                       std::string* error) {
    if (!out) {
        if (error) {
            *error = "layout plan output is null";
        }
        return false;
    }
    if (cluster.namespace_count == 0) {
        if (error) {
            *error = "namespace_count is zero";
        }
        return false;
    }
    if (cfg.branch_factor < 2) {
        if (error) {
            *error = "branch_factor must be >= 2";
        }
        return false;
    }
    if (cfg.max_depth == 0) {
        if (error) {
            *error = "max_depth is zero";
        }
        return false;
    }
    if (cfg.max_files_per_leaf == 0) {
        if (error) {
            *error = "max_files_per_leaf is zero";
        }
        return false;
    }

    NamespaceLayoutPlan plan;
    plan.files_per_namespace = cluster.total_files / static_cast<uint64_t>(cluster.namespace_count);
    if (cluster.total_files % static_cast<uint64_t>(cluster.namespace_count) != 0ULL) {
        ++plan.files_per_namespace;
    }
    plan.branch_factor = cfg.branch_factor;

    uint32_t chosen_depth = cfg.max_depth;
    uint64_t chosen_leaves = 0;
    for (uint32_t d = 1; d <= cfg.max_depth; ++d) {
        uint64_t leaves = 0;
        if (!PowU64(cfg.branch_factor, d, &leaves)) {
            break;
        }
        const uint64_t files_per_leaf =
            (plan.files_per_namespace + leaves - 1ULL) / leaves;
        if (files_per_leaf <= cfg.max_files_per_leaf) {
            chosen_depth = d;
            chosen_leaves = leaves;
            break;
        }
        chosen_leaves = leaves;
    }
    if (chosen_leaves == 0) {
        if (error) {
            *error = "failed to compute leaf directory count";
        }
        return false;
    }

    plan.depth = chosen_depth;
    plan.leaf_dir_count = chosen_leaves;
    plan.files_per_leaf_base = plan.files_per_namespace / chosen_leaves;
    plan.files_per_leaf_remainder = plan.files_per_namespace % chosen_leaves;

    if (!SumGeom(cfg.branch_factor, chosen_depth, &plan.total_dir_count)) {
        if (error) {
            *error = "directory count overflow in layout planner";
        }
        return false;
    }

    *out = std::move(plan);
    return true;
}

} // namespace zb::meta_gen
