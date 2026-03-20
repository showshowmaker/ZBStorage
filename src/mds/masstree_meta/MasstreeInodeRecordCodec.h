#pragma once

#include <cstdint>
#include <string>

#include "mds.pb.h"

namespace zb::mds {

struct MasstreeInodeRecord {
    zb::rpc::InodeAttr attr;
    bool has_optical_image{false};
    uint64_t optical_image_global_id{0};
};

class MasstreeInodeRecordCodec {
public:
    static bool Encode(const MasstreeInodeRecord& record, std::string* data, std::string* error);
    static bool Decode(const std::string& data, MasstreeInodeRecord* record, std::string* error);
};

} // namespace zb::mds
