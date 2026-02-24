#pragma once

#include <string>

namespace zb::mds {

inline std::string InodeKey(uint64_t inode_id) {
    return "I/" + std::to_string(inode_id);
}

inline std::string DentryKey(uint64_t parent_inode, const std::string& name) {
    return "D/" + std::to_string(parent_inode) + "/" + name;
}

inline std::string DentryPrefix(uint64_t parent_inode) {
    return "D/" + std::to_string(parent_inode) + "/";
}

inline std::string ChunkKey(uint64_t inode_id, uint32_t index) {
    return "C/" + std::to_string(inode_id) + "/" + std::to_string(index);
}

inline std::string ChunkPrefix(uint64_t inode_id) {
    return "C/" + std::to_string(inode_id) + "/";
}

inline bool ParseChunkKey(const std::string& key, uint64_t* inode_id, uint32_t* index) {
    if (!inode_id || !index) {
        return false;
    }
    if (key.size() < 4 || key[0] != 'C' || key[1] != '/') {
        return false;
    }
    size_t slash = key.find('/', 2);
    if (slash == std::string::npos) {
        return false;
    }
    try {
        *inode_id = static_cast<uint64_t>(std::stoull(key.substr(2, slash - 2)));
        *index = static_cast<uint32_t>(std::stoul(key.substr(slash + 1)));
        return true;
    } catch (...) {
        return false;
    }
}

inline std::string HandleKey(uint64_t handle_id) {
    return "H/" + std::to_string(handle_id);
}

inline std::string NextInodeKey() {
    return "X/next_inode";
}

inline std::string NextHandleKey() {
    return "X/next_handle";
}

constexpr uint64_t kRootInodeId = 1;

} // namespace zb::mds
