#pragma once

#include <cctype>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

#include "mds.pb.h"
#include "UnifiedInodeRecord.h"

namespace zb::mds {

constexpr uint8_t kUnifiedFlagVirtualDiskNode = 0x01;

inline bool TryParseTrailingUint64(const std::string& text, uint64_t* value) {
    if (!value || text.empty()) {
        return false;
    }
    size_t end = text.size();
    size_t begin = end;
    while (begin > 0 && std::isdigit(static_cast<unsigned char>(text[begin - 1])) != 0) {
        --begin;
    }
    if (begin == end) {
        return false;
    }
    try {
        *value = static_cast<uint64_t>(std::stoull(text.substr(begin, end - begin)));
        return true;
    } catch (...) {
        return false;
    }
}

inline bool TryParseTrailingUint32(const std::string& text, uint32_t* value) {
    uint64_t parsed = 0;
    if (!TryParseTrailingUint64(text, &parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *value = static_cast<uint32_t>(parsed);
    return true;
}

inline bool IsVirtualDiskNodeId(const std::string& node_id) {
    return node_id.find("-v") != std::string::npos;
}

inline std::string FormatRealNodeId(uint64_t numeric_id) {
    std::ostringstream oss;
    oss << "node-real-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

inline std::string FormatVirtualNodeId(uint64_t numeric_id) {
    return "vpool-v" + std::to_string(numeric_id);
}

inline std::string FormatDiskIdForTier(uint32_t numeric_id, bool is_virtual) {
    if (is_virtual) {
        return "disk" + std::to_string(numeric_id);
    }
    std::ostringstream oss;
    oss << "disk-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

inline std::string FormatOpticalNodeId(uint64_t numeric_id) {
    return "optical-node-" + std::to_string(numeric_id);
}

inline std::string FormatOpticalDiskId(uint64_t numeric_id) {
    return "disk-" + std::to_string(numeric_id);
}

inline std::string FormatOpticalImageId(uint64_t node_id, uint64_t disk_id, uint32_t image_id) {
    return "img-" + std::to_string(node_id) + "-" + std::to_string(disk_id) + "-" + std::to_string(image_id);
}

inline void ApplyAttrToUnifiedRecord(const zb::rpc::InodeAttr& attr, UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->version = unified_inode_layout::kVersion;
    record->inode_type = static_cast<uint8_t>(attr.type());
    record->inode_id = attr.inode_id();
    record->size_bytes = attr.size();
    record->atime = attr.atime();
    record->mtime = attr.mtime();
    record->ctime = attr.ctime();
    record->version_no = attr.version();
    record->blksize = attr.blksize();
    record->blocks_512 = attr.blocks_512();
    record->rdev_major = attr.rdev_major();
    record->rdev_minor = attr.rdev_minor();
    record->object_unit_size = attr.object_unit_size();
    record->mode = attr.mode();
    record->uid = attr.uid();
    record->gid = attr.gid();
    record->nlink = attr.nlink();
    record->replica = attr.replica();
    record->file_archive_state = static_cast<uint32_t>(attr.file_archive_state());
    if (!attr.file_name().empty() || record->file_name.empty()) {
        record->file_name = attr.file_name();
        record->file_name_len = static_cast<uint16_t>(record->file_name.size());
    }
}

inline void ClearLocationInAttr(zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    attr->set_storage_tier(zb::rpc::INODE_STORAGE_NONE);
    attr->set_disk_node_id(0);
    attr->set_disk_id(0);
    attr->set_optical_node_id(0);
    attr->set_optical_disk_id(0);
    attr->set_optical_image_id(0);
    attr->set_disk_node_is_virtual(false);
}

inline void ApplyDiskLocationToAttr(const zb::rpc::DiskFileLocation& location, zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    ClearLocationInAttr(attr);
    attr->set_storage_tier(zb::rpc::INODE_STORAGE_DISK);
    uint64_t disk_node_id = 0;
    uint32_t disk_id = 0;
    const bool is_virtual = IsVirtualDiskNodeId(location.node_id());
    (void)TryParseTrailingUint64(location.node_id(), &disk_node_id);
    (void)TryParseTrailingUint32(location.disk_id(), &disk_id);
    attr->set_disk_node_id(disk_node_id);
    attr->set_disk_id(disk_id);
    attr->set_disk_node_is_virtual(is_virtual);
}

inline void ApplyOpticalLocationToAttr(const zb::rpc::OpticalFileLocation& location, zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    ClearLocationInAttr(attr);
    attr->set_storage_tier(zb::rpc::INODE_STORAGE_OPTICAL);
    uint64_t optical_node_id = 0;
    uint64_t optical_disk_id = 0;
    uint32_t optical_image_id = 0;
    (void)TryParseTrailingUint64(location.node_id(), &optical_node_id);
    (void)TryParseTrailingUint64(location.disk_id(), &optical_disk_id);
    (void)TryParseTrailingUint32(location.image_id(), &optical_image_id);
    attr->set_optical_node_id(optical_node_id);
    attr->set_optical_disk_id(optical_disk_id);
    attr->set_optical_image_id(optical_image_id);
}

inline bool PopulateDiskFileLocationFromInodeAttr(const zb::rpc::InodeAttr& attr,
                                                  zb::rpc::DiskFileLocation* location) {
    if (!location ||
        attr.storage_tier() != zb::rpc::INODE_STORAGE_DISK ||
        attr.disk_node_id() == 0) {
        return false;
    }
    location->Clear();
    const bool is_virtual = attr.disk_node_is_virtual();
    location->set_node_id(is_virtual ? FormatVirtualNodeId(attr.disk_node_id())
                                     : FormatRealNodeId(attr.disk_node_id()));
    location->set_disk_id(FormatDiskIdForTier(attr.disk_id(), is_virtual));
    return true;
}

inline bool PopulateOpticalFileLocationFromInodeAttr(const zb::rpc::InodeAttr& attr,
                                                     uint64_t inode_id,
                                                     zb::rpc::OpticalFileLocation* location) {
    if (!location ||
        inode_id == 0 ||
        attr.storage_tier() != zb::rpc::INODE_STORAGE_OPTICAL ||
        attr.optical_node_id() == 0) {
        return false;
    }
    location->Clear();
    location->set_node_id(FormatOpticalNodeId(attr.optical_node_id()));
    location->set_disk_id(FormatOpticalDiskId(attr.optical_disk_id()));
    location->set_image_id(FormatOpticalImageId(attr.optical_node_id(),
                                                attr.optical_disk_id(),
                                                attr.optical_image_id()));
    location->set_file_id("obj-" + std::to_string(inode_id) + "-0");
    return true;
}

inline void ClearDiskLocationInUnifiedRecord(UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->disk_node_id = 0;
    record->disk_id = 0;
    if (record->location_format == static_cast<uint8_t>(UnifiedLocationFormat::kDisk)) {
        record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kNone);
        record->storage_loc0 = 0;
        record->storage_loc1 = 0;
    }
    record->flags &= ~kUnifiedFlagVirtualDiskNode;
}

inline void ClearOpticalLocationInUnifiedRecord(UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->optical_node_id = 0;
    record->optical_disk_id = 0;
    record->optical_image_id = 0;
    if (record->location_format == static_cast<uint8_t>(UnifiedLocationFormat::kOpticalPacked)) {
        record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kNone);
        record->storage_loc0 = 0;
        record->storage_loc1 = 0;
    }
}

inline void ApplyDiskLocationToUnifiedRecord(const zb::rpc::DiskFileLocation& location,
                                             UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kDisk);
    ClearOpticalLocationInUnifiedRecord(record);
    ClearDiskLocationInUnifiedRecord(record);
    if (IsVirtualDiskNodeId(location.node_id())) {
        record->flags |= kUnifiedFlagVirtualDiskNode;
    }
    (void)TryParseTrailingUint64(location.node_id(), &record->disk_node_id);
    (void)TryParseTrailingUint32(location.disk_id(), &record->disk_id);
    record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kDisk);
    record->storage_loc0 = record->disk_node_id;
    record->storage_loc1 = static_cast<uint64_t>(record->disk_id);
}

inline void ApplyOpticalLocationToUnifiedRecord(const zb::rpc::OpticalFileLocation& location,
                                                UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kOptical);
    ClearDiskLocationInUnifiedRecord(record);
    (void)TryParseTrailingUint64(location.node_id(), &record->optical_node_id);
    (void)TryParseTrailingUint64(location.disk_id(), &record->optical_disk_id);
    (void)TryParseTrailingUint32(location.image_id(), &record->optical_image_id);
    record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kOpticalPacked);
    record->storage_loc0 = record->optical_node_id;
    record->storage_loc1 = (record->optical_disk_id << 32U) |
                           static_cast<uint64_t>(record->optical_image_id);
}

inline bool PopulateDiskFileLocationFromUnifiedRecord(const UnifiedInodeRecord& record,
                                                      zb::rpc::DiskFileLocation* location) {
    if (!location ||
        record.storage_tier != static_cast<uint8_t>(UnifiedStorageTier::kDisk) ||
        record.disk_node_id == 0) {
        return false;
    }
    const bool is_virtual = (record.flags & kUnifiedFlagVirtualDiskNode) != 0;
    location->Clear();
    location->set_node_id(is_virtual ? FormatVirtualNodeId(record.disk_node_id)
                                     : FormatRealNodeId(record.disk_node_id));
    location->set_disk_id(FormatDiskIdForTier(record.disk_id, is_virtual));
    return true;
}

inline bool PopulateOpticalFileLocationFromUnifiedRecord(const UnifiedInodeRecord& record,
                                                         zb::rpc::OpticalFileLocation* location) {
    if (!location ||
        record.storage_tier != static_cast<uint8_t>(UnifiedStorageTier::kOptical) ||
        record.optical_node_id == 0) {
        return false;
    }
    location->Clear();
    location->set_node_id(FormatOpticalNodeId(record.optical_node_id));
    location->set_disk_id(FormatOpticalDiskId(record.optical_disk_id));
    location->set_image_id(FormatOpticalImageId(record.optical_node_id,
                                                record.optical_disk_id,
                                                record.optical_image_id));
    return true;
}

} // namespace zb::mds
