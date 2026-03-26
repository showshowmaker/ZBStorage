#pragma once

#include <cstdint>
#include <string>

namespace zb::mds {

class MasstreeBulkMetaGenerator {
public:
    struct Request {
        std::string output_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        uint64_t inode_start{0};
        uint64_t file_count{100000000ULL};
        uint32_t max_files_per_leaf_dir{2048};
        uint32_t max_subdirs_per_dir{256};
    };

    struct Result {
        std::string staging_dir;
        std::string manifest_path;
        std::string inode_records_path;
        std::string dentry_records_path;
        std::string verify_manifest_path;
        uint64_t root_inode_id{0};
        uint64_t inode_min{0};
        uint64_t inode_max{0};
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t level1_dir_count{0};
        uint64_t leaf_dir_count{0};
        uint64_t avg_file_size_bytes{0};
        std::string total_file_bytes;
    };

    bool Generate(const Request& request,
                  Result* result,
                  std::string* error) const;
};

} // namespace zb::mds
