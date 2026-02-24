#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::mds {

enum class NodeType {
    kReal = 0,
    kVirtual = 1,
    kOptical = 2,
};

struct DiskInfo {
    std::string disk_id;
    uint64_t capacity_bytes{0};
    uint64_t free_bytes{0};
    bool is_healthy{true};
};

struct NodeInfo {
    std::string node_id;
    std::string address;
    std::string group_id;
    NodeType type{NodeType::kReal};
    uint32_t weight{1};
    uint32_t virtual_node_count{1};
    uint64_t next_virtual_index{0};
    bool allocatable{true};
    bool is_primary{true};
    bool sync_ready{false};
    uint64_t epoch{1};
    std::string secondary_node_id;
    std::string secondary_address;
    std::vector<DiskInfo> disks;
    size_t next_disk_index{0};
};

struct MdsConfig {
    std::string db_path;
    std::string scheduler_address;
    uint32_t scheduler_refresh_ms{2000};
    uint64_t chunk_size{4 * 1024 * 1024};
    uint32_t replica{2};
    std::vector<NodeInfo> nodes;
    bool enable_optical_archive{false};
    uint64_t archive_trigger_bytes{10ULL * 1024ULL * 1024ULL * 1024ULL};
    uint64_t archive_target_bytes{8ULL * 1024ULL * 1024ULL * 1024ULL};
    uint64_t cold_file_ttl_sec{3600};
    uint32_t archive_scan_interval_ms{5000};
    uint32_t archive_max_chunks_per_round{64};

    static MdsConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::mds
