#include "DiskManager.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

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

zb::msg::Status DiskManager::Init(const std::string& config_str) {
    return InitFromConfig(config_str);
}

zb::msg::Status DiskManager::InitFromConfig(const std::string& config_str) {
    disks_.clear();
    if (config_str.empty()) {
        return zb::msg::Status::InvalidArgument("Empty disk config string");
    }

    size_t start = 0;
    while (start < config_str.size()) {
        size_t end = config_str.find(';', start);
        if (end == std::string::npos) {
            end = config_str.size();
        }
        std::string token = Trim(config_str.substr(start, end - start));
        if (!token.empty()) {
            size_t sep = token.find(':');
            if (sep == std::string::npos) {
                return zb::msg::Status::InvalidArgument("Invalid disk config entry: " + token);
            }
            std::string id = Trim(token.substr(0, sep));
            std::string mount_point = Trim(token.substr(sep + 1));
            if (id.empty() || mount_point.empty()) {
                return zb::msg::Status::InvalidArgument("Invalid disk config entry (empty field): " + token);
            }

            DiskContext disk;
            disk.id = id;
            disk.mount_point = mount_point;
            disk.is_healthy = RefreshStats(&disk);
            disks_[disk.id] = disk;
        }
        start = end + 1;
    }

    if (disks_.empty()) {
        return zb::msg::Status::InvalidArgument("No valid disk entries found in config");
    }

    return zb::msg::Status::Ok();
}

zb::msg::Status DiskManager::InitFromDataRoot(const std::string& data_root) {
    disks_.clear();
    if (data_root.empty()) {
        return zb::msg::Status::InvalidArgument("DATA_ROOT is empty");
    }

    fs::path root(data_root);
    if (!fs::exists(root) || !fs::is_directory(root)) {
        return zb::msg::Status::NotFound("DATA_ROOT not found: " + data_root);
    }

    for (const auto& entry : fs::directory_iterator(root)) {
        if (!entry.is_directory()) {
            continue;
        }
        std::string mount_point = entry.path().string();
        std::string disk_id;
        if (!LoadDiskIdFromFile(mount_point, &disk_id)) {
            disk_id = entry.path().filename().string();
        }

        DiskContext disk;
        disk.id = disk_id;
        disk.mount_point = mount_point;
        disk.is_healthy = RefreshStats(&disk);
        disks_[disk.id] = disk;
    }

    if (disks_.empty()) {
        return zb::msg::Status::NotFound("No disks found under DATA_ROOT: " + data_root);
    }

    return zb::msg::Status::Ok();
}

zb::msg::Status DiskManager::Refresh() {
    if (disks_.empty()) {
        return zb::msg::Status::NotFound("No disks initialized");
    }
    for (auto& [id, disk] : disks_) {
        disk.is_healthy = RefreshStats(&disk);
    }
    return zb::msg::Status::Ok();
}

std::string DiskManager::GetMountPoint(const std::string& disk_id) const {
    auto it = disks_.find(disk_id);
    if (it == disks_.end() || !it->second.is_healthy) {
        return {};
    }
    return it->second.mount_point;
}

bool DiskManager::IsHealthy(const std::string& disk_id) const {
    auto it = disks_.find(disk_id);
    return it != disks_.end() && it->second.is_healthy;
}

std::vector<zb::msg::DiskReport> DiskManager::GetReport() const {
    std::vector<zb::msg::DiskReport> reports;
    reports.reserve(disks_.size());
    for (const auto& [id, disk] : disks_) {
        zb::msg::DiskReport report;
        report.id = disk.id;
        report.mount_point = disk.mount_point;
        report.capacity_bytes = disk.capacity_bytes;
        report.free_bytes = disk.free_bytes;
        report.is_healthy = disk.is_healthy;
        reports.push_back(std::move(report));
    }
    return reports;
}

bool DiskManager::LoadDiskIdFromFile(const std::string& mount_point, std::string* out_id) {
    fs::path id_path = fs::path(mount_point) / ".disk_id";
    std::ifstream input(id_path.string());
    if (!input) {
        return false;
    }
    std::string id;
    std::getline(input, id);
    id = Trim(id);
    if (id.empty()) {
        return false;
    }
    *out_id = id;
    return true;
}

bool DiskManager::RefreshStats(DiskContext* disk) {
    if (!disk) {
        return false;
    }
    try {
        fs::space_info info = fs::space(disk->mount_point);
        disk->capacity_bytes = info.capacity;
        disk->free_bytes = info.available;
        return true;
    } catch (const fs::filesystem_error&) {
        disk->capacity_bytes = 0;
        disk->free_bytes = 0;
        return false;
    }
}

} // namespace zb::real_node
