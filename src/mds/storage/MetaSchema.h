#pragma once

#include <string>

namespace zb::mds {

inline std::string PathKey(const std::string& path) {
    return "P/" + path;
}

inline std::string InodeKey(uint64_t inode_id) {
    return "I/" + std::to_string(inode_id);
}

inline std::string ChunkKey(uint64_t inode_id, uint32_t index) {
    return "C/" + std::to_string(inode_id) + "/" + std::to_string(index);
}

inline std::string NextInodeKey() {
    return "X/next_inode";
}

} // namespace zb::mds
