#pragma once

#include <cstdint>
#include <string>

#include "../storage/UnifiedInodeRecord.h"

namespace zb::mds {

struct MasstreeInodeRecord {
    UnifiedInodeRecord inode;
};

class MasstreeInodeRecordCodec {
public:
    static bool Encode(const MasstreeInodeRecord& record, std::string* data, std::string* error);
    static bool Decode(const std::string& data, MasstreeInodeRecord* record, std::string* error);
    static bool HasOpticalImage(const std::string& data, bool* has_optical_image, std::string* error);
    static bool RebaseEncoded(const std::string& data,
                              uint64_t inode_id,
                              bool has_optical_image,
                              uint64_t optical_image_global_id,
                              std::string* rebased,
                              std::string* error);
};

} // namespace zb::mds
