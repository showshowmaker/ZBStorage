#pragma once

#include <cstdint>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct OpticalMetaGenConfig {
    std::string output_dir;
    uint64_t now_seconds{0};
    uint64_t target_optical_eb{1000};
    uint64_t min_100gb_discs{0};
    uint64_t min_1tb_discs{0};
    uint64_t min_10tb_discs{0};
    uint64_t progress_interval_files{1000000};
    uint64_t progress_interval_sec{30};
};

struct OpticalMetaGenStats {
    uint64_t total_files{0};
    uint64_t manifest_records{0};
    uint64_t total_discs{0};
    uint64_t used_discs{0};
    uint64_t count_100gb{0};
    uint64_t count_1tb{0};
    uint64_t count_10tb{0};
};

class OpticalMetaGenerator {
public:
    static bool Generate(const ClusterScaleConfig& cluster,
                         const DirectoryLayoutConfig& dir_cfg,
                         const FileSizeSamplerConfig& file_cfg,
                         const OpticalMetaGenConfig& gen_cfg,
                         OpticalMetaGenStats* out,
                         std::string* error);
};

} // namespace zb::meta_gen
