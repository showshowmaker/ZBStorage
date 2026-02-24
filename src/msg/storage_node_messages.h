#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "status.h"

namespace zb::msg {

struct WriteChunkRequest {
    std::string disk_id;
    std::string chunk_id;
    uint64_t offset{0};
    std::string data;
    bool is_replication{false};
    uint64_t epoch{0};
};

struct WriteChunkReply {
    Status status;
    uint64_t bytes{0};
};

struct ReadChunkRequest {
    std::string disk_id;
    std::string chunk_id;
    uint64_t offset{0};
    uint64_t size{0};
};

struct ReadChunkReply {
    Status status;
    uint64_t bytes{0};
    std::string data;
};

struct DeleteChunkRequest {
    std::string disk_id;
    std::string chunk_id;
};

struct DeleteChunkReply {
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
