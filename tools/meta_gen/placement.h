#pragma once

#include <cstdint>
#include <string>

#include "config.h"

namespace zb::meta_gen {

struct DiskPlacement {
    bool valid{false};
    bool is_real_node{false};
    uint64_t node_index{0};
    uint64_t disk_index{0};
    std::string node_id;
    std::string disk_id;
};

struct OpticalPlacement {
    bool valid{false};
    uint64_t node_index{0};
    uint64_t disc_index{0};
    std::string node_id;
    std::string disc_id;
};

inline std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

inline DiskPlacement PickDiskPlacement(const ClusterScaleConfig& cluster, uint64_t inode_id) {
    DiskPlacement p;
    const uint64_t real_total = cluster.real_node_count * cluster.real_disks_per_node;
    const uint64_t virtual_total = cluster.virtual_node_count * cluster.virtual_disks_per_node;
    const uint64_t total = real_total + virtual_total;
    if (total == 0) {
        return p;
    }
    uint64_t slot = inode_id % total;
    if (slot < real_total && cluster.real_node_count > 0 && cluster.real_disks_per_node > 0) {
        p.valid = true;
        p.is_real_node = true;
        p.node_index = slot / cluster.real_disks_per_node;
        p.disk_index = slot % cluster.real_disks_per_node;
        p.node_id = "real-" + std::to_string(p.node_index + 1);
        p.disk_id = "disk" + std::to_string(p.disk_index);
        return p;
    }
    slot -= real_total;
    if (cluster.virtual_node_count == 0 || cluster.virtual_disks_per_node == 0) {
        return p;
    }
    p.valid = true;
    p.is_real_node = false;
    p.node_index = slot / cluster.virtual_disks_per_node;
    p.disk_index = slot % cluster.virtual_disks_per_node;
    p.node_id = "virtual-" + std::to_string(p.node_index + 1);
    p.disk_id = "disk" + std::to_string(p.disk_index);
    return p;
}

inline OpticalPlacement PickOpticalPlacement(const ClusterScaleConfig& cluster, uint64_t inode_id) {
    OpticalPlacement p;
    if (cluster.optical_node_count == 0 || cluster.discs_per_optical_node == 0) {
        return p;
    }
    p.valid = true;
    p.node_index = inode_id % cluster.optical_node_count;
    p.disc_index = (inode_id / cluster.optical_node_count) % cluster.discs_per_optical_node;
    p.node_id = "optical-" + std::to_string(p.node_index + 1);
    p.disc_id = "odisk" + std::to_string(p.disc_index);
    return p;
}

} // namespace zb::meta_gen
