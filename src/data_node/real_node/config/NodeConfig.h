#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::real_node {

struct DiskSpec {
    std::string id;
    std::string mount_point;
};

struct NodeConfig {
    // Raw values loaded from config file.
    std::string disks_env;
    std::string data_root;
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

    static NodeConfig LoadFromFile(const std::string& path, std::string* error);
    std::vector<DiskSpec> ParseDisksEnv(std::string* error) const;
};

} // namespace zb::real_node
