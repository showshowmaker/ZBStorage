#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../../msg/storage_node_messages.h"

namespace zb::real_node {

struct DiskContext {
    std::string id;
    std::string mount_point;
    uint64_t capacity_bytes{0};
    uint64_t free_bytes{0};
    bool is_healthy{false};
};

class DiskManager {
public:
    DiskManager() = default;

    zb::msg::Status Init(const std::string& config_str);
    zb::msg::Status InitFromConfig(const std::string& config_str);
    zb::msg::Status InitFromDataRoot(const std::string& data_root);
    zb::msg::Status Refresh();

    std::string GetMountPoint(const std::string& disk_id) const;
    bool IsHealthy(const std::string& disk_id) const;
    std::vector<zb::msg::DiskReport> GetReport() const;

private:
    static bool LoadDiskIdFromFile(const std::string& mount_point, std::string* out_id);
    static bool RefreshStats(DiskContext* disk);

    std::unordered_map<std::string, DiskContext> disks_;
};

} // namespace zb::real_node
