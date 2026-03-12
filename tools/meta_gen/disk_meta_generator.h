#pragma once

#include <cstdint>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct DiskMetaGenConfig {
    std::string output_dir;
    uint64_t object_unit_size{4ULL * 1024ULL * 1024ULL};
    uint64_t now_seconds{0};
    std::string txid_prefix{"seed"};
};

struct DiskMetaGenStats {
    uint64_t total_files{0};
    uint64_t files_with_disk_meta{0};
    uint64_t real_node_file_meta_records{0};
    uint64_t virtual_node_file_meta_records{0};
    long double disk_budget_bytes{0.0L};
    long double disk_used_bytes{0.0L};
};

class DiskMetaGenerator {
public:
    static bool Generate(const ClusterScaleConfig& cluster,
                         const DirectoryLayoutConfig& dir_cfg,
                         const FileSizeSamplerConfig& file_cfg,
                         const DiskMetaGenConfig& gen_cfg,
                         DiskMetaGenStats* out,
                         std::string* error);
};

} // namespace zb::meta_gen
