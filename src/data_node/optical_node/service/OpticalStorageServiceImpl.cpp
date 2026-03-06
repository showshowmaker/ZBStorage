#include "OpticalStorageServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>

#include "real_node.pb.h"

namespace zb::optical_node {

namespace {

constexpr size_t kArchiveOpCacheMaxEntries = 100000;
constexpr uint64_t kArchiveOpCacheTtlMs = 24ULL * 60ULL * 60ULL * 1000ULL;
constexpr uint64_t kArchiveOpCachePruneInterval = 1024;

} // namespace

OpticalStorageServiceImpl::OpticalStorageServiceImpl(ImageStore* store) : store_(store) {}

void OpticalStorageServiceImpl::ConfigureReplication(const std::string& node_id,
                                                     const std::string& group_id,
                                                     bool replication_enabled,
                                                     bool is_primary,
                                                     const std::string& peer_node_id,
                                                     const std::string& peer_address,
                                                     uint32_t replication_timeout_ms) {
    std::lock_guard<std::mutex> lock(repl_mu_);
    repl_.node_id = node_id;
    repl_.group_id = group_id;
    repl_.replication_enabled = replication_enabled;
    repl_.is_primary = is_primary;
    repl_.peer_node_id = peer_node_id;
    repl_.peer_address = peer_address;
    repl_.epoch = 1;
    repl_.primary_node_id = is_primary ? node_id : peer_node_id;
    repl_.primary_address = is_primary ? "" : peer_address;
    repl_.secondary_node_id = is_primary ? peer_node_id : node_id;
    repl_.secondary_address = is_primary ? peer_address : "";
    replication_timeout_ms_ = replication_timeout_ms > 0 ? replication_timeout_ms : 2000;
}

void OpticalStorageServiceImpl::ApplySchedulerAssignment(bool is_primary,
                                                         uint64_t epoch,
                                                         const std::string& group_id,
                                                         const std::string& primary_node_id,
                                                         const std::string& primary_address,
                                                         const std::string& secondary_node_id,
                                                         const std::string& secondary_address) {
    std::lock_guard<std::mutex> lock(repl_mu_);
    repl_.is_primary = is_primary;
    repl_.epoch = epoch > 0 ? epoch : repl_.epoch;
    if (!group_id.empty()) {
        repl_.group_id = group_id;
    }
    repl_.primary_node_id = primary_node_id;
    repl_.primary_address = primary_address;
    repl_.secondary_node_id = secondary_node_id;
    repl_.secondary_address = secondary_address;
    if (repl_.node_id == primary_node_id) {
        repl_.peer_node_id = secondary_node_id;
        repl_.peer_address = secondary_address;
    } else if (repl_.node_id == secondary_node_id) {
        repl_.peer_node_id = primary_node_id;
        repl_.peer_address = primary_address;
    }
}

ReplicationStatusSnapshot OpticalStorageServiceImpl::GetReplicationStatus() const {
    std::lock_guard<std::mutex> lock(repl_mu_);
    return repl_;
}

zb::msg::WriteChunkReply OpticalStorageServiceImpl::WriteChunk(const zb::msg::WriteChunkRequest& request) {
    zb::msg::WriteChunkReply reply;
    if (!store_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    ReplicationStatusSnapshot repl_snapshot = GetReplicationStatus();
    if (repl_snapshot.replication_enabled && !request.is_replication && !repl_snapshot.is_primary) {
        reply.status = zb::msg::Status::IoError("NOT_LEADER");
        return reply;
    }
    if (request.is_replication && repl_snapshot.replication_enabled &&
        request.epoch > 0 && request.epoch < repl_snapshot.epoch) {
        reply.status = zb::msg::Status::IoError("STALE_EPOCH");
        return reply;
    }
    const uint64_t now_ms = NowMilliseconds();
    if (!request.archive_op_id.empty()) {
        std::lock_guard<std::mutex> lock(archive_op_mu_);
        PruneArchiveOpCacheLocked(now_ms);
        auto it = last_archive_op_by_chunk_.find(request.chunk_id);
        if (it != last_archive_op_by_chunk_.end() &&
            it->second.op_id == request.archive_op_id &&
            now_ms >= it->second.last_seen_ts_ms &&
            now_ms - it->second.last_seen_ts_ms <= kArchiveOpCacheTtlMs) {
            reply.status = zb::msg::Status::Ok();
            reply.bytes = static_cast<uint64_t>(request.data.size());
            return reply;
        }
    }

    ImageLocation location;
    reply.status = store_->WriteChunk(request.disk_id, request.chunk_id, request.data, &location);
    if (!reply.status.ok()) {
        return reply;
    }
    reply.bytes = static_cast<uint64_t>(request.data.size());
    {
        std::lock_guard<std::mutex> lock(repl_mu_);
        ++repl_.applied_lsn;
    }

    if (repl_snapshot.replication_enabled && repl_snapshot.is_primary && !request.is_replication &&
        !repl_snapshot.peer_address.empty()) {
        zb::msg::Status repl_status = ReplicateWriteToSecondary(request, repl_snapshot.epoch);
        if (!repl_status.ok()) {
            reply.status = repl_status;
            return reply;
        }
    }
    if (!request.archive_op_id.empty()) {
        std::lock_guard<std::mutex> lock(archive_op_mu_);
        PruneArchiveOpCacheLocked(now_ms);
        ArchiveOpCacheEntry& entry = last_archive_op_by_chunk_[request.chunk_id];
        entry.op_id = request.archive_op_id;
        entry.last_seen_ts_ms = now_ms;
    }

    return reply;
}

zb::msg::ReadChunkReply OpticalStorageServiceImpl::ReadChunk(const zb::msg::ReadChunkRequest& request) {
    zb::msg::ReadChunkReply reply;
    if (!store_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    reply.status = store_->ReadChunk(request.disk_id,
                                     request.chunk_id,
                                     request.offset,
                                     request.size,
                                     &reply.data,
                                     &reply.bytes);
    return reply;
}

zb::msg::DeleteChunkReply OpticalStorageServiceImpl::DeleteChunk(const zb::msg::DeleteChunkRequest& request) {
    zb::msg::DeleteChunkReply reply;
    if (!store_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    reply.status = store_->DeleteChunk(request.disk_id, request.chunk_id);
    if (reply.status.code == zb::msg::StatusCode::kNotFound) {
        reply.status = zb::msg::Status::Ok();
    }
    return reply;
}

zb::msg::DiskReportReply OpticalStorageServiceImpl::GetDiskReport() const {
    if (!store_) {
        zb::msg::DiskReportReply reply;
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    return store_->GetDiskReport();
}

zb::msg::Status OpticalStorageServiceImpl::UpdateArchiveState(const std::string& disk_id,
                                                              const std::string& chunk_id,
                                                              const std::string& archive_state,
                                                              uint64_t version) {
    (void)disk_id;
    (void)chunk_id;
    (void)archive_state;
    (void)version;
    return zb::msg::Status::Ok();
}

zb::msg::Status OpticalStorageServiceImpl::ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request,
                                                                     uint64_t epoch) {
    ReplicationStatusSnapshot repl_snapshot = GetReplicationStatus();
    if (!repl_snapshot.replication_enabled || repl_snapshot.peer_address.empty()) {
        return zb::msg::Status::Ok();
    }

    brpc::Channel* channel = nullptr;
    {
        std::lock_guard<std::mutex> lock(channel_mu_);
        auto it = peer_channels_.find(repl_snapshot.peer_address);
        if (it == peer_channels_.end()) {
            auto new_channel = std::make_unique<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = static_cast<int>(replication_timeout_ms_);
            options.max_retry = 0;
            if (new_channel->Init(repl_snapshot.peer_address.c_str(), &options) != 0) {
                return zb::msg::Status::IoError("Failed to connect secondary " + repl_snapshot.peer_address);
            }
            channel = new_channel.get();
            peer_channels_[repl_snapshot.peer_address] = std::move(new_channel);
        } else {
            channel = it->second.get();
        }
    }

    if (!channel) {
        return zb::msg::Status::IoError("secondary channel unavailable");
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteChunkRequest replicate_req;
    replicate_req.set_disk_id(request.disk_id);
    replicate_req.set_chunk_id(request.chunk_id);
    replicate_req.set_offset(request.offset);
    replicate_req.set_data(request.data);
    replicate_req.set_is_replication(true);
    replicate_req.set_epoch(epoch);
    replicate_req.set_archive_op_id(request.archive_op_id);

    zb::rpc::WriteChunkReply replicate_resp;
    brpc::Controller cntl;
    stub.WriteChunk(&cntl, &replicate_req, &replicate_resp, nullptr);
    if (cntl.Failed()) {
        return zb::msg::Status::IoError("replication rpc failed: " + cntl.ErrorText());
    }
    if (replicate_resp.status().code() != zb::rpc::STATUS_OK) {
        return zb::msg::Status::IoError("replication rejected: " + replicate_resp.status().message());
    }
    return zb::msg::Status::Ok();
}

uint64_t OpticalStorageServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void OpticalStorageServiceImpl::PruneArchiveOpCacheLocked(uint64_t now_ms) {
    ++archive_op_cache_touch_;
    const bool need_periodic_prune = (archive_op_cache_touch_ % kArchiveOpCachePruneInterval) == 0;
    if (!need_periodic_prune && last_archive_op_by_chunk_.size() <= kArchiveOpCacheMaxEntries) {
        return;
    }

    for (auto it = last_archive_op_by_chunk_.begin(); it != last_archive_op_by_chunk_.end();) {
        const ArchiveOpCacheEntry& entry = it->second;
        const bool expired = now_ms >= entry.last_seen_ts_ms &&
                             now_ms - entry.last_seen_ts_ms > kArchiveOpCacheTtlMs;
        if (expired) {
            it = last_archive_op_by_chunk_.erase(it);
        } else {
            ++it;
        }
    }
    if (last_archive_op_by_chunk_.size() <= kArchiveOpCacheMaxEntries) {
        return;
    }

    std::vector<std::pair<std::string, uint64_t>> candidates;
    candidates.reserve(last_archive_op_by_chunk_.size());
    for (const auto& item : last_archive_op_by_chunk_) {
        candidates.emplace_back(item.first, item.second.last_seen_ts_ms);
    }
    std::sort(candidates.begin(),
              candidates.end(),
              [](const std::pair<std::string, uint64_t>& a,
                 const std::pair<std::string, uint64_t>& b) {
                  return a.second < b.second;
              });
    const size_t remove_count = candidates.size() - kArchiveOpCacheMaxEntries;
    for (size_t i = 0; i < remove_count; ++i) {
        last_archive_op_by_chunk_.erase(candidates[i].first);
    }
}

} // namespace zb::optical_node
