#include "VirtualNodeConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace zb::virtual_node {

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

bool ParseUint64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = std::stoull(text);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseUint32(const std::string& text, uint32_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint32_t>(std::stoul(text));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseBool(const std::string& value, bool* out) {
    if (!out) {
        return false;
    }
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
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

} // namespace

VirtualNodeConfig VirtualNodeConfig::LoadFromFile(const std::string& path, std::string* error) {
    VirtualNodeConfig cfg;
    std::ifstream input(path);
    if (!input) {
        if (error) {
            *error = "Failed to open config file: " + path;
        }
        return cfg;
    }

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
        if (key == "NODE_ID") {
            cfg.node_id = value;
        } else if (key == "NODE_ADDRESS") {
            cfg.node_address = value;
        } else if (key == "SCHEDULER_ADDR") {
            cfg.scheduler_addr = value;
        } else if (key == "GROUP_ID") {
            cfg.group_id = value;
        } else if (key == "NODE_ROLE") {
            cfg.node_role = value;
        } else if (key == "PEER_NODE_ID") {
            cfg.peer_node_id = value;
        } else if (key == "PEER_ADDRESS") {
            cfg.peer_address = value;
        } else if (key == "REPLICATION_ENABLED") {
            if (!ParseBool(value, &cfg.replication_enabled)) {
                if (error) {
                    *error = "Invalid REPLICATION_ENABLED at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "REPLICATION_TIMEOUT_MS") {
            if (!ParseUint32(value, &cfg.replication_timeout_ms)) {
                if (error) {
                    *error = "Invalid REPLICATION_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "NODE_WEIGHT") {
            if (!ParseUint32(value, &cfg.node_weight)) {
                if (error) {
                    *error = "Invalid NODE_WEIGHT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "VIRTUAL_NODE_COUNT") {
            if (!ParseUint32(value, &cfg.virtual_node_count)) {
                if (error) {
                    *error = "Invalid VIRTUAL_NODE_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "HEARTBEAT_INTERVAL_MS") {
            if (!ParseUint32(value, &cfg.heartbeat_interval_ms)) {
                if (error) {
                    *error = "Invalid HEARTBEAT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISKS") {
            cfg.disk_ids = Split(value, ',');
        } else if (key == "READ_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.read_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "WRITE_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.write_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "READ_MBPS") {
            uint64_t mbps = 0;
            if (!ParseUint64(value, &mbps)) {
                if (error) {
                    *error = "Invalid READ_MBPS at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.read_bytes_per_sec = mbps * 1024ULL * 1024ULL;
        } else if (key == "WRITE_MBPS") {
            uint64_t mbps = 0;
            if (!ParseUint64(value, &mbps)) {
                if (error) {
                    *error = "Invalid WRITE_MBPS at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.write_bytes_per_sec = mbps * 1024ULL * 1024ULL;
        } else if (key == "READ_BASE_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.read_base_latency_ms)) {
                if (error) {
                    *error = "Invalid READ_BASE_LATENCY_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "WRITE_BASE_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.write_base_latency_ms)) {
                if (error) {
                    *error = "Invalid WRITE_BASE_LATENCY_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "JITTER_MS") {
            if (!ParseUint32(value, &cfg.jitter_ms)) {
                if (error) {
                    *error = "Invalid JITTER_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISK_CAPACITY_BYTES") {
            if (!ParseUint64(value, &cfg.disk_capacity_bytes)) {
                if (error) {
                    *error = "Invalid DISK_CAPACITY_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "MOUNT_POINT_PREFIX") {
            cfg.mount_point_prefix = value;
        }
    }

    if (cfg.disk_ids.empty()) {
        cfg.disk_ids.push_back("disk-01");
    }
    if (cfg.node_weight == 0) {
        cfg.node_weight = 1;
    }
    if (cfg.node_role.empty()) {
        cfg.node_role = "PRIMARY";
    }
    if (cfg.virtual_node_count == 0) {
        cfg.virtual_node_count = 1;
    }
    if (cfg.heartbeat_interval_ms == 0) {
        cfg.heartbeat_interval_ms = 2000;
    }
    if (cfg.mount_point_prefix.empty()) {
        cfg.mount_point_prefix = "/virtual";
    }
    return cfg;
}

} // namespace zb::virtual_node
