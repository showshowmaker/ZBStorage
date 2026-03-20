#pragma once

#include <cstdint>

#include "mds.pb.h"

namespace zb::mds {

struct MasstreePackedDentryValue {
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

inline uint64_t PackMasstreeDentryValue(uint64_t child_inode, zb::rpc::InodeType type) {
    return (child_inode << 8U) | (static_cast<uint64_t>(type) & 0xFFU);
}

inline bool UnpackMasstreeDentryValue(uint64_t packed, MasstreePackedDentryValue* value) {
    if (!value) {
        return false;
    }
    value->child_inode = packed >> 8U;
    value->type = static_cast<zb::rpc::InodeType>(packed & 0xFFU);
    return value->child_inode != 0;
}

} // namespace zb::mds
