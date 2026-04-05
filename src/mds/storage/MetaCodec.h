#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "UnifiedInodeRecord.h"
#include "mds.pb.h"

namespace zb::mds {

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

    static bool EncodeUnifiedInodeRecord(const UnifiedInodeRecord& record, std::string* out, std::string* error);
    static bool DecodeUnifiedInodeRecord(const std::string& data, UnifiedInodeRecord* record, std::string* error);
    static bool DecodeUnifiedInodeAttr(const std::string& data, zb::rpc::InodeAttr* attr, std::string* error);

    static std::string EncodePgView(const PgViewRecord& view);
    static bool DecodePgView(const std::string& data, PgViewRecord* view);
};

} // namespace zb::mds
