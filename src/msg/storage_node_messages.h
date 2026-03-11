#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "status.h"

namespace zb::msg {

struct WriteObjectRequest {
    std::string disk_id;
    std::string object_id;
    uint64_t offset{0};
    std::string data;
    bool is_replication{false};
    uint64_t epoch{0};
    std::string archive_op_id;
    uint64_t inode_id{0};
    std::string file_id;
    std::string file_path;
    uint64_t file_size{0};
    uint64_t file_offset{0};
    uint32_t file_mode{0};
    uint32_t file_uid{0};
    uint32_t file_gid{0};
    uint64_t file_mtime{0};
    uint32_t file_object_index{0};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

struct WriteObjectReply {
    Status status;
    uint64_t bytes{0};
    std::string image_id;
    uint64_t image_offset{0};
    uint64_t image_length{0};
};

struct ReadObjectRequest {
    std::string disk_id;
    std::string object_id;
    uint64_t offset{0};
    uint64_t size{0};
    std::string image_id;
    uint64_t image_offset{0};
    uint64_t image_length{0};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

struct ReadObjectReply {
    Status status;
    uint64_t bytes{0};
    std::string data;
};

struct ReadArchivedFileRequest {
    std::string disc_id;
    uint64_t inode_id{0};
    std::string file_id;
    uint64_t offset{0};
    uint64_t size{0};
};

struct ReadArchivedFileReply {
    Status status;
    uint64_t bytes{0};
    std::string data;
};

struct DeleteObjectRequest {
    std::string disk_id;
    std::string object_id;

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

struct DeleteObjectReply {
    Status status;
};

struct DiskReport {
    std::string id;
    std::string mount_point;
    uint64_t capacity_bytes{0};
    uint64_t free_bytes{0};
    bool is_healthy{false};
};

struct DiskReportReply {
    Status status;
    std::vector<DiskReport> reports;
};

} // namespace zb::msg
