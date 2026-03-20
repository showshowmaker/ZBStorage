#pragma once

#include <cstdint>
#include <string>

namespace zb::mds {

struct ArchiveManifest {
    std::string namespace_id;
    std::string path_prefix;
    std::string generation_id;
    std::string manifest_path;
    std::string inode_root;
    std::string inode_index_root;
    std::string inode_bloom_root;
    std::string dentry_root;
    std::string dentry_index_root;
    uint64_t root_inode_id{0};
    uint64_t inode_min{0};
    uint64_t inode_max{0};
    uint64_t inode_count{0};
    uint64_t page_size_bytes{0};

    static bool LoadFromFile(const std::string& manifest_path,
                             ArchiveManifest* manifest,
                             std::string* error);
};

} // namespace zb::mds
