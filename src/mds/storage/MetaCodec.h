#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mds.pb.h"

namespace zb::mds {

struct LayoutRootRecord {
    uint64_t inode_id{0};
    std::string layout_root_id;
    uint64_t layout_version{0};
    uint64_t file_size{0};
    uint64_t epoch{0};
    uint64_t update_ts{0};
};

struct LayoutExtentRecord {
    uint64_t logical_offset{0};
    uint64_t length{0};
    std::string object_id;
    uint64_t object_offset{0};
    uint64_t object_length{0};
    uint64_t object_version{0};
};

struct LayoutNodeRecord {
    std::string node_id;
    uint32_t level{0};
    std::vector<LayoutExtentRecord> extents;
    std::vector<std::string> child_layout_ids;
};

struct PgViewMemberRecord {
    std::string node_id;
    std::string node_address;
    std::string disk_id;
    std::string group_id;
    uint64_t epoch{0};
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
    bool sync_ready{false};
};

struct PgViewRecord {
    uint64_t epoch{0};
    uint32_t pg_id{0};
    std::vector<PgViewMemberRecord> members;
};

class MetaCodec {
public:
    static std::string EncodeUInt64(uint64_t value);
    static bool DecodeUInt64(const std::string& data, uint64_t* value);

    static std::string EncodeInodeAttr(const zb::rpc::InodeAttr& attr);
    static bool DecodeInodeAttr(const std::string& data, zb::rpc::InodeAttr* attr);

    static std::string EncodeChunkMeta(const zb::rpc::ChunkMeta& meta);
    static bool DecodeChunkMeta(const std::string& data, zb::rpc::ChunkMeta* meta);

    static std::string EncodeLayoutRoot(const LayoutRootRecord& root);
    static bool DecodeLayoutRoot(const std::string& data, LayoutRootRecord* root);

    static std::string EncodeLayoutNode(const LayoutNodeRecord& node);
    static bool DecodeLayoutNode(const std::string& data, LayoutNodeRecord* node);

    static std::string EncodePgView(const PgViewRecord& view);
    static bool DecodePgView(const std::string& data, PgViewRecord* view);
};

} // namespace zb::mds
