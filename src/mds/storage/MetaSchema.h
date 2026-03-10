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

inline std::string ChunkKey(uint64_t inode_id, uint32_t index) {
    return "C/" + std::to_string(inode_id) + "/" + std::to_string(index);
}

inline std::string ChunkPrefix(uint64_t inode_id) {
    return "C/" + std::to_string(inode_id) + "/";
}

inline std::string ReverseChunkKey(const std::string& chunk_id) {
    return "RC/" + chunk_id;
}

inline std::string ArchiveStateKey(const std::string& chunk_id) {
    return "AS/" + chunk_id;
}

inline std::string ArchiveStatePrefix() {
    return "AS/";
}

inline std::string ArchiveOpticalWriteKey(const std::string& chunk_id, const std::string& op_id) {
    return "AOW/" + chunk_id + "/" + op_id;
}

inline std::string ArchiveOpticalWritePrefix() {
    return "AOW/";
}

inline std::string ArchiveReverseRepairKey(const std::string& chunk_id) {
    return "ARR/" + chunk_id;
}

inline std::string ArchiveReverseRepairPrefix() {
    return "ARR/";
}

inline std::string ArchiveImageChunkPrefix(const std::string& optical_node_id,
                                           const std::string& optical_disk_id,
                                           const std::string& image_id) {
    return "AIC/" + optical_node_id + "/" + optical_disk_id + "/" + image_id + "/";
}

inline std::string ArchiveImageChunkKey(const std::string& optical_node_id,
                                        const std::string& optical_disk_id,
                                        const std::string& image_id,
                                        const std::string& chunk_id) {
    return ArchiveImageChunkPrefix(optical_node_id, optical_disk_id, image_id) + chunk_id;
}

inline std::string LayoutRootKey(uint64_t inode_id) {
    return "LR/" + std::to_string(inode_id);
}

inline std::string LayoutRootPrefix() {
    return "LR/";
}

inline std::string LayoutObjectKey(const std::string& layout_obj_id) {
    return "LO/" + layout_obj_id;
}

inline std::string LayoutObjectPrefix() {
    return "LO/";
}

inline std::string LayoutObjectReplicaKey(const std::string& layout_obj_id, uint32_t replica_index) {
    return "LOR/" + layout_obj_id + "/" + std::to_string(replica_index);
}

inline std::string LayoutObjectReplicaPrefix(const std::string& layout_obj_id) {
    return "LOR/" + layout_obj_id + "/";
}

inline std::string LayoutObjectReplicaGlobalPrefix() {
    return "LOR/";
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

inline std::string LayoutGcSeenKey(const std::string& layout_obj_id) {
    return "LGS/" + layout_obj_id;
}

inline std::string LayoutGcSeenPrefix() {
    return "LGS/";
}

inline bool ParseLayoutRootKey(const std::string& key, uint64_t* inode_id) {
    if (!inode_id) {
        return false;
    }
    constexpr char kPrefix[] = "LR/";
    if (key.rfind(kPrefix, 0) != 0) {
        return false;
    }
    try {
        *inode_id = static_cast<uint64_t>(std::stoull(key.substr(sizeof(kPrefix) - 1)));
        return true;
    } catch (...) {
        return false;
    }
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

inline bool ParseLayoutObjectReplicaKey(const std::string& key, std::string* layout_obj_id, uint32_t* replica_index) {
    if (!layout_obj_id || !replica_index) {
        return false;
    }
    constexpr char kPrefix[] = "LOR/";
    if (key.rfind(kPrefix, 0) != 0) {
        return false;
    }
    size_t slash = key.find('/', sizeof(kPrefix) - 1);
    if (slash == std::string::npos) {
        return false;
    }
    const std::string obj_id = key.substr(sizeof(kPrefix) - 1, slash - (sizeof(kPrefix) - 1));
    if (obj_id.empty()) {
        return false;
    }
    try {
        *replica_index = static_cast<uint32_t>(std::stoul(key.substr(slash + 1)));
        *layout_obj_id = obj_id;
        return true;
    } catch (...) {
        return false;
    }
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
