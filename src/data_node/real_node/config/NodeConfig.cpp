#include "NodeConfig.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace zb::real_node {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

} // namespace

NodeConfig NodeConfig::LoadFromEnv() {
    NodeConfig cfg;
    if (const char* disks = std::getenv("ZB_DISKS")) {
        cfg.disks_env = disks;
    }
    if (const char* data_root = std::getenv("DATA_ROOT")) {
        cfg.data_root = data_root;
    }
    return cfg;
}

std::vector<DiskSpec> NodeConfig::ParseDisksEnv(std::string* error) const {
    std::vector<DiskSpec> specs;
    if (disks_env.empty()) {
        return specs;
    }

    size_t start = 0;
    while (start < disks_env.size()) {
        size_t end = disks_env.find(';', start);
        if (end == std::string::npos) {
            end = disks_env.size();
        }
        std::string token = Trim(disks_env.substr(start, end - start));
        if (!token.empty()) {
            size_t sep = token.find(':');
            if (sep == std::string::npos) {
                if (error) {
                    *error = "Invalid ZB_DISKS entry: " + token;
                }
                return {};
            }
            DiskSpec spec;
            spec.id = Trim(token.substr(0, sep));
            spec.mount_point = Trim(token.substr(sep + 1));
            if (spec.id.empty() || spec.mount_point.empty()) {
                if (error) {
                    *error = "Invalid ZB_DISKS entry (empty field): " + token;
                }
                return {};
            }
            specs.push_back(std::move(spec));
        }
        start = end + 1;
    }

    return specs;
}

} // namespace zb::real_node
