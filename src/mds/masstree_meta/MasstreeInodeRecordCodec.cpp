#include "MasstreeInodeRecordCodec.h"

#include <utility>

#include "MasstreeOpticalProfile.h"

namespace zb::mds {

namespace {

bool IsUnifiedPayload(std::string_view data) {
    return data.size() == unified_inode_layout::kRecordSize &&
           static_cast<uint8_t>(data[unified_inode_layout::kVersionOffset]) == unified_inode_layout::kVersion;
}

} // namespace

bool MasstreeInodeRecordCodec::Encode(const MasstreeInodeRecord& record,
                                      std::string* data,
                                      std::string* error) {
    return EncodeUnifiedInodeRecord(record.inode, data, error);
}

bool MasstreeInodeRecordCodec::Decode(const std::string& data,
                                      MasstreeInodeRecord* record,
                                      std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree inode record decode output is null";
        }
        return false;
    }
    if (!IsUnifiedPayload(data)) {
        if (error) {
            *error = "unsupported masstree inode record format";
        }
        return false;
    }
    UnifiedInodeRecord inode;
    if (!DecodeUnifiedInodeRecord(data, &inode, error)) {
        return false;
    }
    record->inode = std::move(inode);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeInodeRecordCodec::HasOpticalImage(const std::string& data,
                                               bool* has_optical_image,
                                               std::string* error) {
    if (!has_optical_image) {
        if (error) {
            *error = "masstree inode record optical flag output is null";
        }
        return false;
    }
    UnifiedInodeRecord inode;
    if (!DecodeUnifiedInodeRecord(data, &inode, error)) {
        return false;
    }
    *has_optical_image = inode.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kOptical);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeInodeRecordCodec::RebaseEncoded(const std::string& data,
                                             uint64_t inode_id,
                                             bool has_optical_image,
                                             uint64_t optical_image_global_id,
                                             std::string* rebased,
                                             std::string* error) {
    if (!rebased) {
        if (error) {
            *error = "masstree inode record rebased output is null";
        }
        return false;
    }
    UnifiedInodeRecord inode;
    if (!DecodeUnifiedInodeRecord(data, &inode, error)) {
        return false;
    }
    const uint64_t old_inode_id = inode.inode_id;
    inode.inode_id = inode_id;
    if (inode.parent_inode_id != 0) {
        inode.parent_inode_id += (inode_id - old_inode_id);
    }
    if (has_optical_image) {
        const MasstreeOpticalProfile profile = MasstreeOpticalProfile::Fixed();
        uint32_t node_index = 0;
        uint32_t disk_index = 0;
        uint32_t image_index_in_disk = 0;
        if (!profile.DecodeGlobalImageId(optical_image_global_id,
                                         &node_index,
                                         &disk_index,
                                         &image_index_in_disk)) {
            if (error) {
                *error = "invalid rebased optical image id";
            }
            return false;
        }
        inode.storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kOptical);
        inode.optical_node_id = node_index;
        inode.optical_disk_id = disk_index;
        inode.optical_image_id = image_index_in_disk;
        inode.disk_node_id = 0;
        inode.disk_id = 0;
        inode.location_format = static_cast<uint8_t>(UnifiedLocationFormat::kOpticalPacked);
        inode.storage_loc0 = inode.optical_node_id;
        inode.storage_loc1 = (inode.optical_disk_id << 32U) | static_cast<uint64_t>(inode.optical_image_id);
    } else if (inode.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kOptical)) {
        inode.storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kNone);
        inode.optical_node_id = 0;
        inode.optical_disk_id = 0;
        inode.optical_image_id = 0;
        inode.location_format = static_cast<uint8_t>(UnifiedLocationFormat::kNone);
        inode.storage_loc0 = 0;
        inode.storage_loc1 = 0;
    }
    return EncodeUnifiedInodeRecord(inode, rebased, error);
}

} // namespace zb::mds
