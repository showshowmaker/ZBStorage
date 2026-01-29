#pragma once

#include <string>
#include <vector>

namespace zb::real_node {

struct DiskSpec {
    std::string id;
    std::string mount_point;
};

struct NodeConfig {
    // Raw environment values for traceability.
    std::string disks_env;
    std::string data_root;

    static NodeConfig LoadFromEnv();
    std::vector<DiskSpec> ParseDisksEnv(std::string* error) const;
};

} // namespace zb::real_node
