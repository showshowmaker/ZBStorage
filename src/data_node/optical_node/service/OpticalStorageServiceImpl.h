#pragma once

#include <brpc/channel.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "../storage/ImageStore.h"
#include "../../../msg/storage_node_messages.h"

namespace zb::optical_node {

struct ReplicationStatusSnapshot {
    bool replication_enabled{false};
    bool is_primary{true};
    uint64_t epoch{1};
    uint64_t applied_lsn{0};
    std::string node_id;
    std::string group_id;
    std::string peer_node_id;
    std::string peer_address;
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
};

class OpticalStorageServiceImpl {
public:
    explicit OpticalStorageServiceImpl(ImageStore* store);

    void ConfigureReplication(const std::string& node_id,
                              const std::string& group_id,
                              bool replication_enabled,
                              bool is_primary,
                              const std::string& peer_node_id,
                              const std::string& peer_address,
                              uint32_t replication_timeout_ms);
    void ApplySchedulerAssignment(bool is_primary,
                                  uint64_t epoch,
                                  const std::string& group_id,
                                  const std::string& primary_node_id,
                                  const std::string& primary_address,
                                  const std::string& secondary_node_id,
                                  const std::string& secondary_address);
    ReplicationStatusSnapshot GetReplicationStatus() const;

    zb::msg::WriteChunkReply WriteChunk(const zb::msg::WriteChunkRequest& request);
    zb::msg::ReadChunkReply ReadChunk(const zb::msg::ReadChunkRequest& request);
    zb::msg::DeleteChunkReply DeleteChunk(const zb::msg::DeleteChunkRequest& request);
    zb::msg::DiskReportReply GetDiskReport() const;
    zb::msg::Status UpdateArchiveState(const std::string& disk_id,
                                       const std::string& chunk_id,
                                       const std::string& archive_state,
                                       uint64_t version);

private:
    struct ArchiveOpCacheEntry {
        std::string op_id;
        uint64_t last_seen_ts_ms{0};
        std::string image_id;
        uint64_t image_offset{0};
        uint64_t image_length{0};
    };

    zb::msg::Status ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request, uint64_t epoch);
    static uint64_t NowMilliseconds();
    void PruneArchiveOpCacheLocked(uint64_t now_ms);

    ImageStore* store_{};

    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    uint32_t replication_timeout_ms_{2000};

    mutable std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> peer_channels_;

    mutable std::mutex archive_op_mu_;
    std::unordered_map<std::string, ArchiveOpCacheEntry> last_archive_op_by_chunk_;
    uint64_t archive_op_cache_touch_{0};

    mutable std::mutex cache_mu_;
    std::unordered_map<std::string, std::string> cache_chunks_;
};

} // namespace zb::optical_node
