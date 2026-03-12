#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "config.h"

namespace zb::meta_gen {

struct MdsSstGenConfig {
    std::string output_dir;
    std::string sst_prefix{"mds_meta"};
    uint64_t max_kv_per_sst{1000000};

    uint64_t object_unit_size{4ULL * 1024ULL * 1024ULL};
    uint64_t now_seconds{0};
};

struct MdsSstGenStats {
    uint64_t inode_count{0};
    uint64_t dentry_count{0};
    uint64_t anchor_count{0};
    uint64_t kv_count{0};
    uint64_t sst_count{0};
    uint64_t files_with_disk_anchor{0};
    uint64_t files_with_optical_anchor{0};
    uint64_t next_inode{0};
    long double sampled_total_bytes{0.0L};
    long double disk_budget_bytes{0.0L};
    long double disk_used_bytes{0.0L};
    std::vector<std::string> sst_files;
};

class MdsSstGenerator {
public:
    static bool Generate(const ClusterScaleConfig& cluster,
                         const DirectoryLayoutConfig& dir_cfg,
                         const FileSizeSamplerConfig& file_cfg,
                         const MdsSstGenConfig& gen_cfg,
                         MdsSstGenStats* out,
                         std::string* error);
};

} // namespace zb::meta_gen
