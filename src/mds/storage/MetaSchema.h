#pragma once

#include <cstdint>
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

inline std::string ObjectKey(uint64_t inode_id, uint32_t index) {
    return "O/" + std::to_string(inode_id) + "/" + std::to_string(index);
}

inline std::string ObjectPrefix(uint64_t inode_id) {
    return "O/" + std::to_string(inode_id) + "/";
}

inline std::string ObjectGlobalPrefix() {
    return "O/";
}

inline std::string ObjectArchiveStateKey(const std::string& object_id) {
    return "AS/" + object_id;
}

inline std::string ArchiveStatePrefix() {
    return "AS/";
}

inline std::string ObjectArchiveStatePrefix() {
    return ArchiveStatePrefix();
}

inline std::string FileArchiveStateKey(uint64_t inode_id) {
    return "AF/" + std::to_string(inode_id);
}

inline std::string FileArchiveStatePrefix() {
    return "AF/";
}

inline std::string FileAnchorKey(uint64_t inode_id) {
    return "FA/" + std::to_string(inode_id);
}

inline std::string FileAnchorPrefix() {
    return "FA/";
}

inline std::string PathPlacementPolicyKey(const std::string& normalized_path_prefix) {
    return "PR" + normalized_path_prefix;
}

inline std::string PathPlacementPolicyPrefix() {
    return "PR/";
}

inline std::string PgViewKey(uint64_t epoch, uint32_t pg_id) {
    return "PV/" + std::to_string(epoch) + "/" + std::to_string(pg_id);
}

inline std::string PgViewPrefix(uint64_t epoch) {
    return "PV/" + std::to_string(epoch) + "/";
}

inline std::string PgViewEpochKey() {
    return "PV/current_epoch";
}

inline bool ParsePgViewKey(const std::string& key, uint64_t* epoch, uint32_t* pg_id) {
    if (!epoch || !pg_id) {
        return false;
    }
    constexpr char kPrefix[] = "PV/";
    if (key.rfind(kPrefix, 0) != 0) {
        return false;
    }
    size_t sep = key.find('/', sizeof(kPrefix) - 1);
    if (sep == std::string::npos) {
        return false;
    }
    try {
        *epoch = static_cast<uint64_t>(std::stoull(key.substr(sizeof(kPrefix) - 1, sep - (sizeof(kPrefix) - 1))));
        *pg_id = static_cast<uint32_t>(std::stoul(key.substr(sep + 1)));
        return true;
    } catch (...) {
        return false;
    }
}

inline bool ParseObjectKey(const std::string& key, uint64_t* inode_id, uint32_t* index) {
    if (!inode_id || !index) {
        return false;
    }
    if (key.size() < 4 || key[0] != 'O' || key[1] != '/') {
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
