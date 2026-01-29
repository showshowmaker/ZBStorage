#pragma once

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

    static NodeConfig LoadFromFile(const std::string& path, std::string* error);
    std::vector<DiskSpec> ParseDisksEnv(std::string* error) const;
};

} // namespace zb::real_node
