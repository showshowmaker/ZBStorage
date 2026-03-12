#pragma once

#include <cstdint>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct NamespaceLayoutPlan {
    uint64_t files_per_namespace{0};
    uint32_t depth{0};
    uint32_t branch_factor{0};
    uint64_t leaf_dir_count{0};
    uint64_t files_per_leaf_base{0};
    uint64_t files_per_leaf_remainder{0};
    uint64_t total_dir_count{0};
};

class NamespaceLayoutPlanner {
public:
    static bool BuildPlan(const ClusterScaleConfig& cluster,
                          const DirectoryLayoutConfig& cfg,
                          NamespaceLayoutPlan* out,
                          std::string* error);
};

} // namespace zb::meta_gen

