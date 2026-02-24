#include "MdsConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
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

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool ParsePositiveUint32(const std::string& text, uint32_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        uint64_t value = std::stoull(text);
        if (value == 0 || value > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        *out = static_cast<uint32_t>(value);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseNodeType(const std::string& text, NodeType* type) {
    if (!type) {
        return false;
    }
    std::string lowered = ToLower(Trim(text));
    if (lowered == "real") {
        *type = NodeType::kReal;
        return true;
    }
    if (lowered == "virtual") {
        *type = NodeType::kVirtual;
        return true;
    }
    if (lowered == "optical") {
        *type = NodeType::kOptical;
        return true;
    }
    return false;
}

bool ParseBool(const std::string& text, bool* out) {
    if (!out) {
        return false;
    }
    const std::string lowered = ToLower(Trim(text));
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        *out = true;
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseNodeEntry(const std::string& item, NodeInfo* node, std::string* error) {
    if (!node) {
        return false;
    }

    std::vector<std::string> parts = Split(item, ',');
    if (parts.empty()) {
        if (error) {
            *error = "Empty NODES entry";
        }
        return false;
    }

    size_t at = parts[0].find('@');
    if (at == std::string::npos) {
        if (error) {
            *error = "Invalid NODES entry (expected node_id@address): " + item;
        }
        return false;
    }

    node->node_id = Trim(parts[0].substr(0, at));
    node->address = Trim(parts[0].substr(at + 1));
    node->group_id = node->node_id;
    node->type = NodeType::kReal;
    node->weight = 1;
    node->virtual_node_count = 1;

    if (node->node_id.empty() || node->address.empty()) {
        if (error) {
            *error = "Invalid NODES entry: " + item;
        }
        return false;
    }

    size_t positional_index = 0;
    for (size_t i = 1; i < parts.size(); ++i) {
        std::string token = Trim(parts[i]);
        if (token.empty()) {
            continue;
        }

        std::string key;
        std::string value;
        size_t eq = token.find('=');
        if (eq == std::string::npos) {
            if (positional_index == 0) {
                key = "type";
            } else if (positional_index == 1) {
                key = "weight";
            } else if (positional_index == 2) {
                key = "virtual_node_count";
            } else {
                if (error) {
                    *error = "Too many positional fields in NODES entry: " + item;
                }
                return false;
            }
            value = token;
            ++positional_index;
        } else {
            key = ToLower(Trim(token.substr(0, eq)));
            value = Trim(token.substr(eq + 1));
        }

        if (key == "type" || key == "node_type") {
            if (!ParseNodeType(value, &node->type)) {
                if (error) {
                    *error = "Invalid node type in NODES entry: " + item;
                }
                return false;
            }
        } else if (key == "weight") {
            if (!ParsePositiveUint32(value, &node->weight)) {
                if (error) {
                    *error = "Invalid weight in NODES entry: " + item;
                }
                return false;
            }
        } else if (key == "virtual_node_count" || key == "vnode_count") {
            if (!ParsePositiveUint32(value, &node->virtual_node_count)) {
                if (error) {
                    *error = "Invalid virtual_node_count in NODES entry: " + item;
                }
                return false;
            }
        } else {
            if (error) {
                *error = "Unknown NODES option '" + key + "' in entry: " + item;
            }
            return false;
        }
    }

    if (node->type == NodeType::kReal) {
        node->virtual_node_count = 1;
    }

    return true;
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
        } else if (key == "SCHEDULER_ADDR") {
            cfg.scheduler_address = value;
        } else if (key == "SCHEDULER_REFRESH_MS") {
            cfg.scheduler_refresh_ms = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "CHUNK_SIZE") {
            cfg.chunk_size = static_cast<uint64_t>(std::stoull(value));
        } else if (key == "REPLICA") {
            cfg.replica = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "ENABLE_OPTICAL_ARCHIVE") {
            if (!ParseBool(value, &cfg.enable_optical_archive)) {
                if (error) {
                    *error = "Invalid ENABLE_OPTICAL_ARCHIVE value: " + value;
                }
                return {};
            }
        } else if (key == "ARCHIVE_TRIGGER_BYTES") {
            cfg.archive_trigger_bytes = static_cast<uint64_t>(std::stoull(value));
        } else if (key == "ARCHIVE_TARGET_BYTES") {
            cfg.archive_target_bytes = static_cast<uint64_t>(std::stoull(value));
        } else if (key == "COLD_FILE_TTL_SEC") {
            cfg.cold_file_ttl_sec = static_cast<uint64_t>(std::stoull(value));
        } else if (key == "ARCHIVE_SCAN_INTERVAL_MS") {
            cfg.archive_scan_interval_ms = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "ARCHIVE_MAX_CHUNKS_PER_ROUND") {
            cfg.archive_max_chunks_per_round = static_cast<uint32_t>(std::stoul(value));
        } else if (key == "NODES") {
            auto nodes = Split(value, ';');
            cfg.nodes.clear();
            std::unordered_map<std::string, bool> seen_node_ids;
            for (const auto& item : nodes) {
                NodeInfo node;
                if (!ParseNodeEntry(item, &node, error)) {
                    return {};
                }
                if (seen_node_ids[node.node_id]) {
                    if (error) {
                        *error = "Duplicated node id in NODES: " + node.node_id;
                    }
                    return {};
                }
                seen_node_ids[node.node_id] = true;
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
    if (cfg.archive_target_bytes > cfg.archive_trigger_bytes) {
        cfg.archive_target_bytes = cfg.archive_trigger_bytes;
    }
    if (cfg.archive_max_chunks_per_round == 0) {
        cfg.archive_max_chunks_per_round = 1;
    }
    if (cfg.nodes.empty() && cfg.scheduler_address.empty() && error) {
        *error = "NODES is required when SCHEDULER_ADDR is not set";
        return {};
    }

    return cfg;
}

} // namespace zb::mds
