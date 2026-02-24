#include "StorageServiceImpl.h"

#include <brpc/controller.h>

#include <utility>

#include "real_node.pb.h"

namespace zb::real_node {

StorageServiceImpl::StorageServiceImpl(DiskManager* disk_manager,
                                       LocalPathResolver* path_resolver,
                                       IOExecutor* io_executor)
    : disk_manager_(disk_manager),
      path_resolver_(path_resolver),
      io_executor_(io_executor) {}

void StorageServiceImpl::ConfigureReplication(const std::string& node_id,
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

void StorageServiceImpl::ApplySchedulerAssignment(bool is_primary,
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

ReplicationStatusSnapshot StorageServiceImpl::GetReplicationStatus() const {
    std::lock_guard<std::mutex> lock(repl_mu_);
    return repl_;
}

zb::msg::WriteChunkReply StorageServiceImpl::WriteChunk(const zb::msg::WriteChunkRequest& request) {
    zb::msg::WriteChunkReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
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

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, request.chunk_id, true);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }

    uint64_t bytes_written = 0;
    reply.status = io_executor_->Write(path, request.offset, request.data, &bytes_written);
    reply.bytes = bytes_written;
    if (!reply.status.ok()) {
        return reply;
    }

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
    return reply;
}

zb::msg::ReadChunkReply StorageServiceImpl::ReadChunk(const zb::msg::ReadChunkRequest& request) {
    zb::msg::ReadChunkReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, request.chunk_id, false);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }

    uint64_t bytes_read = 0;
    reply.status = io_executor_->Read(path, request.offset, request.size, &reply.data, &bytes_read);
    reply.bytes = bytes_read;
    return reply;
}

zb::msg::DeleteChunkReply StorageServiceImpl::DeleteChunk(const zb::msg::DeleteChunkRequest& request) {
    zb::msg::DeleteChunkReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, request.chunk_id, false);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }
    reply.status = io_executor_->Delete(path);
    if (reply.status.code == zb::msg::StatusCode::kNotFound) {
        reply.status = zb::msg::Status::Ok();
    }
    return reply;
}

zb::msg::DiskReportReply StorageServiceImpl::GetDiskReport() const {
    zb::msg::DiskReportReply reply;
    if (!disk_manager_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    reply.reports = disk_manager_->GetReport();
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::Status StorageServiceImpl::ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request,
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

} // namespace zb::real_node
