#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <brpc/channel.h>

#include "../io/DiskManager.h"
#include "../io/IOExecutor.h"
#include "../io/LocalPathResolver.h"
#include "../../../msg/storage_node_messages.h"

namespace zb::real_node {

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

class StorageServiceImpl {
public:
    StorageServiceImpl(DiskManager* disk_manager,
                       LocalPathResolver* path_resolver,
                       IOExecutor* io_executor);

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

private:
    zb::msg::Status ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request, uint64_t epoch);

    DiskManager* disk_manager_{};
    LocalPathResolver* path_resolver_{};
    IOExecutor* io_executor_{};

    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    uint32_t replication_timeout_ms_{2000};

    mutable std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> peer_channels_;
};

} // namespace zb::real_node
