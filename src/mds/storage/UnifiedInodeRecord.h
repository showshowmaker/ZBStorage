#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "mds.pb.h"

namespace zb::mds {

enum class UnifiedStorageTier : uint8_t {
    kNone = 0,
    kDisk = 1,
    kOptical = 2,
};

enum class UnifiedLocationFormat : uint8_t {
    kNone = 0,
    kDisk = 1,
    kOpticalPacked = 2,
};

struct UnifiedInodeRecord {
    uint8_t version{3};
    uint8_t inode_type{static_cast<uint8_t>(zb::rpc::INODE_FILE)};
    uint8_t storage_tier{static_cast<uint8_t>(UnifiedStorageTier::kNone)};
    uint8_t flags{0};

    uint16_t file_name_len{0};
    uint8_t location_format{static_cast<uint8_t>(UnifiedLocationFormat::kNone)};

    uint64_t inode_id{0};
    uint64_t parent_inode_id{0};
    uint64_t size_bytes{0};
    uint64_t atime{0};
    uint64_t mtime{0};
    uint64_t ctime{0};
    uint64_t version_no{0};

    uint32_t mode{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t nlink{0};
    uint32_t blksize{0};
    uint32_t file_archive_state{0};
    uint64_t blocks_512{0};
    uint64_t storage_loc0{0};
    uint64_t storage_loc1{0};
    uint32_t rdev_major{0};
    uint32_t rdev_minor{0};
    uint64_t object_unit_size{0};
    uint32_t replica{0};

    uint64_t disk_node_id{0};
    uint32_t disk_id{0};

    uint64_t optical_node_id{0};
    uint64_t optical_disk_id{0};
    uint32_t optical_image_id{0};

    std::string file_name;
};

namespace unified_inode_layout {
constexpr size_t kRecordSize = 256;
constexpr uint8_t kVersion = 3;

constexpr size_t kVersionOffset = 0;
constexpr size_t kInodeTypeOffset = 1;
constexpr size_t kStorageTierOffset = 2;
constexpr size_t kFlagsOffset = 3;
constexpr size_t kFileNameLenOffset = 4;
constexpr size_t kLocationFormatOffset = 5;
constexpr size_t kReserved0Offset = 6;
constexpr size_t kInodeIdOffset = 8;
constexpr size_t kParentInodeIdOffset = 16;
constexpr size_t kSizeBytesOffset = 24;
constexpr size_t kAtimeOffset = 32;
constexpr size_t kMtimeOffset = 40;
constexpr size_t kCtimeOffset = 48;
constexpr size_t kVersionNoOffset = 56;
constexpr size_t kModeOffset = 64;
constexpr size_t kUidOffset = 68;
constexpr size_t kGidOffset = 72;
constexpr size_t kNlinkOffset = 76;
constexpr size_t kBlockSizeOffset = 80;
constexpr size_t kFileArchiveStateOffset = 84;
constexpr size_t kBlocks512Offset = 88;
constexpr size_t kStorageLoc0Offset = 96;
constexpr size_t kStorageLoc1Offset = 104;
constexpr size_t kRdevMajorOffset = 112;
constexpr size_t kRdevMinorOffset = 116;
constexpr size_t kObjectUnitSizeOffset = 120;
constexpr size_t kReplicaOffset = 124;
constexpr size_t kReserved1Offset = 126;
constexpr size_t kFileNameOffset = 128;
constexpr size_t kFileNameSize = 64;
constexpr size_t kReservedOffset = 192;
constexpr size_t kReservedSize = 64;
} // namespace unified_inode_layout

bool EncodeUnifiedInodeRecord(const UnifiedInodeRecord& record, std::string* out, std::string* error);
bool DecodeUnifiedInodeRecord(std::string_view data, UnifiedInodeRecord* out, std::string* error);

void UnifiedInodeRecordToAttr(const UnifiedInodeRecord& record, zb::rpc::InodeAttr* attr);
bool AttrToUnifiedInodeRecord(const zb::rpc::InodeAttr& attr,
                              uint64_t parent_inode_id,
                              UnifiedStorageTier storage_tier,
                              UnifiedInodeRecord* record,
                              std::string* error);

} // namespace zb::mds
