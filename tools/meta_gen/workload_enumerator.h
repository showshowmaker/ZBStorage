#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct FileWorkItem {
    uint32_t namespace_id{0};
    uint64_t inode_id{0};
    uint64_t file_ordinal_in_namespace{0};
    uint64_t global_file_ordinal{0};
    uint64_t file_size{0};
    std::string file_id;
    std::string file_path;
};

class WorkloadEnumerator {
public:
    using FileCallback = std::function<bool(const FileWorkItem&)>;

    static bool EnumerateFiles(const ClusterScaleConfig& cluster,
                               const DirectoryLayoutConfig& dir_cfg,
                               const FileSizeSamplerConfig& file_cfg,
                               const FileCallback& on_file,
                               std::string* error);
};

} // namespace zb::meta_gen
