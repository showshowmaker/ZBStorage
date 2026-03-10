#pragma once

#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <brpc/channel.h>

#include "ArchiveChunkMetaStore.h"
#include "../io/DiskManager.h"
#include "../io/IOExecutor.h"
#include "../io/LocalPathResolver.h"
#include "../../common/ObjectStore.h"
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

struct ArchiveCandidateStat {
    std::string disk_id;
    std::string chunk_id;
    uint64_t last_access_ts_ms{0};
    uint64_t size_bytes{0};
    uint64_t checksum{0};
    double heat_score{0.0};
    std::string archive_state{"pending"};
    uint64_t version{0};
    double score{0.0};
    uint64_t read_ops{0};
    uint64_t write_ops{0};
};

class StorageServiceImpl {
public:
    StorageServiceImpl(DiskManager* disk_manager,
                       LocalPathResolver* path_resolver,
                       IOExecutor* io_executor);
    ~StorageServiceImpl();

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
    zb::msg::ReadArchivedFileReply ReadArchivedFile(const zb::msg::ReadArchivedFileRequest& request);
    zb::msg::DeleteChunkReply DeleteChunk(const zb::msg::DeleteChunkRequest& request);
    zb::msg::Status PutObject(const zb::data_node::ObjectWriteRequest& request);
    zb::data_node::ObjectReadResult GetObject(const zb::data_node::ObjectReadRequest& request);
    zb::msg::Status DeleteObject(const zb::data_node::ObjectDeleteRequest& request);
    zb::msg::DiskReportReply GetDiskReport() const;

    bool InitArchiveMetaStore(const std::string& meta_dir,
                              size_t max_chunks,
                              uint32_t snapshot_interval_ops,
                              bool wal_fsync,
                              std::string* error);
    bool FlushArchiveMetaSnapshot(std::string* error);
    zb::msg::Status UpdateArchiveState(const std::string& disk_id,
                                       const std::string& chunk_id,
                                       const std::string& archive_state,
                                       uint64_t version);
    void SetArchiveTrackingMaxChunks(size_t max_chunks);
    std::vector<ArchiveCandidateStat> CollectArchiveCandidates(uint32_t max_candidates, uint64_t min_age_ms) const;

private:
    struct ReplicationRepairTask {
        std::string key;
        zb::msg::WriteChunkRequest request;
        uint64_t epoch{0};
        uint32_t attempts{0};
        uint64_t generation{0};
    };

    zb::msg::Status ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request, uint64_t epoch);
    void EnqueueReplicationRepair(const zb::msg::WriteChunkRequest& request, uint64_t epoch);
    uint64_t BumpReplicationRepairGeneration(const zb::msg::WriteChunkRequest& request);
    static std::string BuildReplicationRepairKey(const zb::msg::WriteChunkRequest& request);
    void ReplicationRepairLoop();
    void TrackChunkAccess(const std::string& disk_id,
                          const std::string& chunk_id,
                          uint64_t end_offset,
                          bool is_write,
                          uint64_t checksum);
    void RemoveChunkTracking(const std::string& disk_id, const std::string& chunk_id);
    static uint64_t FastChecksum64(const std::string& data);
    static uint64_t NowMilliseconds();

    DiskManager* disk_manager_{};
    LocalPathResolver* path_resolver_{};
    IOExecutor* io_executor_{};

    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    uint32_t replication_timeout_ms_{2000};

    mutable std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> peer_channels_;
    std::mutex repl_repair_mu_;
    std::condition_variable repl_repair_cv_;
    std::deque<ReplicationRepairTask> repl_repair_queue_;
    std::unordered_map<std::string, uint64_t> repl_repair_generation_;
    std::atomic<bool> stop_repl_repair_{false};
    std::thread repl_repair_thread_;

    ArchiveChunkMetaStore archive_meta_store_;
    size_t archive_tracking_max_chunks_{500000};
};

} // namespace zb::real_node
