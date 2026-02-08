#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::mds {

struct DiskInfo {
    std::string disk_id;
};

struct NodeInfo {
    std::string node_id;
    std::string address;
    std::vector<DiskInfo> disks;
    size_t next_disk_index{0};
};

struct MdsConfig {
    std::string db_path;
    uint64_t chunk_size{4 * 1024 * 1024};
    uint32_t replica{2};
    std::vector<NodeInfo> nodes;

    static MdsConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::mds
