#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::optical_node {

struct OpticalNodeConfig {
    std::string node_id;
    std::string node_address;
    std::string scheduler_addr;
    std::string group_id;
    std::string node_role;
    std::string peer_node_id;
    std::string peer_address;
    bool replication_enabled{false};
    uint32_t replication_timeout_ms{2000};
    uint32_t node_weight{1};
    uint32_t heartbeat_interval_ms{2000};

    std::vector<std::string> disk_ids;
    std::string archive_root{"/tmp/zb_optical"};
    uint64_t max_image_size_bytes{1024ULL * 1024ULL * 1024ULL};
    uint64_t disk_capacity_bytes{10ULL * 1024ULL * 1024ULL * 1024ULL};
    std::string mount_point_prefix{"/optical"};

    static OpticalNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::optical_node
