#include "MdsConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <unordered_map>

namespace zb::mds {

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

std::vector<std::string> Split(const std::string& input, char delimiter) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(input);
    while (std::getline(stream, token, delimiter)) {
        token = Trim(token);
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

} // namespace

MdsConfig MdsConfig::LoadFromFile(const std::string& path, std::string* error) {
    MdsConfig cfg;
    std::ifstream input(path);
    if (!input) {
        if (error) {
            *error = "Failed to open config file: " + path;
        }
        return cfg;
    }

    std::unordered_map<std::string, std::vector<std::string>> disks_by_node;

    std::string line;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "Invalid config line " + std::to_string(line_no) + ": " + line;
            }
            return {};
        }
        std::string key = Trim(trimmed.substr(0, eq));
        std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "MDS_DB_PATH") {
            cfg.db_path = value;
        } else if (key == "CHUNK_SIZE") {
            cfg.chunk_size = static_cast<uint64_t>(std::stoull(value));
        } else if (key == "REPLICA") {
            cfg.replica = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "NODES") {
            auto nodes = Split(value, ';');
            cfg.nodes.clear();
            for (const auto& item : nodes) {
                size_t at = item.find('@');
                if (at == std::string::npos) {
                    if (error) {
                        *error = "Invalid NODES entry (expected node_id@address): " + item;
                    }
                    return {};
                }
                NodeInfo node;
                node.node_id = Trim(item.substr(0, at));
                node.address = Trim(item.substr(at + 1));
                if (node.node_id.empty() || node.address.empty()) {
                    if (error) {
                        *error = "Invalid NODES entry: " + item;
                    }
                    return {};
                }
                cfg.nodes.push_back(std::move(node));
            }
        } else if (key == "DISKS") {
            auto node_entries = Split(value, ';');
            for (const auto& entry : node_entries) {
                size_t sep = entry.find(':');
                if (sep == std::string::npos) {
                    if (error) {
                        *error = "Invalid DISKS entry (expected node_id:disk1,disk2): " + entry;
                    }
                    return {};
                }
                std::string node_id = Trim(entry.substr(0, sep));
                std::string disks_value = Trim(entry.substr(sep + 1));
                auto disk_list = Split(disks_value, ',');
                disks_by_node[node_id] = disk_list;
            }
        }
    }

    for (auto& node : cfg.nodes) {
        auto it = disks_by_node.find(node.node_id);
        if (it != disks_by_node.end()) {
            for (const auto& disk_id : it->second) {
                node.disks.push_back({disk_id});
            }
        }
    }

    if (cfg.db_path.empty() && error) {
        *error = "MDS_DB_PATH is required";
        return {};
    }
    if (cfg.nodes.empty() && error) {
        *error = "NODES is required";
        return {};
    }

    return cfg;
}

} // namespace zb::mds
