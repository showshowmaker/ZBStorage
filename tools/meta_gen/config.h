#pragma once

#include <cstdint>
#include <string>

namespace zb::meta_gen {

enum class OpticalCapacityMode {
    kStrictExactTarget = 0,
    kMixedThreeTiers = 1,
};

struct ClusterScaleConfig {
    uint64_t optical_node_count{10000};
    uint64_t discs_per_optical_node{10000};

    uint64_t virtual_node_count{100};
    uint64_t virtual_disks_per_node{16};
    uint64_t virtual_disk_size_tb{2};
    uint64_t real_node_count{1};
    uint64_t real_disks_per_node{24};
    uint64_t real_disk_size_tb{2};

    uint64_t total_files{100000000000ULL};
    uint32_t namespace_count{1000};
    double disk_replica_ratio{0.5};
    uint64_t file_size_estimate_samples{200000};
};

struct OpticalCapacityPlanConfig {
    OpticalCapacityMode mode{OpticalCapacityMode::kStrictExactTarget};
    uint64_t target_eb{1000};
    uint64_t min_100gb_discs{0};
    uint64_t min_1tb_discs{0};
    uint64_t min_10tb_discs{0};
};

struct DirectoryLayoutConfig {
    uint32_t branch_factor{64};
    uint32_t max_depth{4};
    uint32_t max_files_per_leaf{2048};
};

struct FileSizeSamplerConfig {
    uint64_t min_bytes{1024ULL};
    uint64_t max_bytes{2ULL * 1024ULL * 1024ULL * 1024ULL};
    uint64_t seed{0x9E3779B97F4A7C15ULL};
};

inline ClusterScaleConfig DefaultClusterScaleConfig() {
    return ClusterScaleConfig{};
}

inline OpticalCapacityPlanConfig DefaultOpticalCapacityPlanConfig() {
    return OpticalCapacityPlanConfig{};
}

inline DirectoryLayoutConfig DefaultDirectoryLayoutConfig() {
    return DirectoryLayoutConfig{};
}

inline FileSizeSamplerConfig DefaultFileSizeSamplerConfig() {
    return FileSizeSamplerConfig{};
}

} // namespace zb::meta_gen
