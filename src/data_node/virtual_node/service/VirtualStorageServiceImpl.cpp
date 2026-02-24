#include "VirtualStorageServiceImpl.h"

#include <brpc/controller.h>

#include <chrono>
#include <cmath>
#include <thread>
#include <utility>

#include "real_node.pb.h"

namespace zb::virtual_node {

VirtualStorageServiceImpl::VirtualStorageServiceImpl(VirtualNodeConfig config)
    : config_(std::move(config)),
      rng_(std::random_device{}()) {
    for (const auto& disk_id : config_.disk_ids) {
        if (!disk_id.empty()) {
            disk_set_.insert(disk_id);
        }
    }
    if (disk_set_.empty()) {
        disk_set_.insert("disk-01");
    }
}

void VirtualStorageServiceImpl::ConfigureReplication(const std::string& node_id,
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

void VirtualStorageServiceImpl::ApplySchedulerAssignment(bool is_primary,
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

ReplicationStatusSnapshot VirtualStorageServiceImpl::GetReplicationStatus() const {
    std::lock_guard<std::mutex> lock(repl_mu_);
    return repl_;
}

zb::msg::WriteChunkReply VirtualStorageServiceImpl::WriteChunk(const zb::msg::WriteChunkRequest& request) {
    zb::msg::WriteChunkReply reply;
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }
    if (!ValidateDisk(request.disk_id)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
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

    reply.bytes = static_cast<uint64_t>(request.data.size());
    SimulateIo(reply.bytes, false);
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

    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::ReadChunkReply VirtualStorageServiceImpl::ReadChunk(const zb::msg::ReadChunkRequest& request) {
    zb::msg::ReadChunkReply reply;
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }
    if (!ValidateDisk(request.disk_id)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
        return reply;
    }

    SimulateIo(request.size, true);
    reply.bytes = request.size;
    reply.data.assign(static_cast<size_t>(request.size), 'x');
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::DeleteChunkReply VirtualStorageServiceImpl::DeleteChunk(const zb::msg::DeleteChunkRequest& request) {
    zb::msg::DeleteChunkReply reply;
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }
    if (!ValidateDisk(request.disk_id)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
        return reply;
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::DiskReportReply VirtualStorageServiceImpl::GetDiskReport() const {
    zb::msg::DiskReportReply reply;
    for (const auto& disk_id : disk_set_) {
        zb::msg::DiskReport report;
        report.id = disk_id;
        report.mount_point = config_.mount_point_prefix + "/" + disk_id;
        report.capacity_bytes = config_.disk_capacity_bytes;
        report.free_bytes = config_.disk_capacity_bytes;
        report.is_healthy = true;
        reply.reports.push_back(std::move(report));
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

bool VirtualStorageServiceImpl::ValidateDisk(const std::string& disk_id) const {
    return disk_set_.find(disk_id) != disk_set_.end();
}

void VirtualStorageServiceImpl::SimulateIo(uint64_t bytes, bool is_read) {
    uint64_t bytes_per_sec = is_read ? config_.read_bytes_per_sec : config_.write_bytes_per_sec;
    uint32_t base_latency_ms = is_read ? config_.read_base_latency_ms : config_.write_base_latency_ms;

    uint64_t transfer_ms = 0;
    if (bytes_per_sec > 0 && bytes > 0) {
        double seconds = static_cast<double>(bytes) / static_cast<double>(bytes_per_sec);
        transfer_ms = static_cast<uint64_t>(std::ceil(seconds * 1000.0));
    }

    uint64_t delay_ms = static_cast<uint64_t>(base_latency_ms) + transfer_ms + RandomJitterMs();
    if (delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
}

uint32_t VirtualStorageServiceImpl::RandomJitterMs() {
    if (config_.jitter_ms == 0) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(rng_mu_);
    std::uniform_int_distribution<uint32_t> dist(0, config_.jitter_ms);
    return dist(rng_);
}

zb::msg::Status VirtualStorageServiceImpl::ReplicateWriteToSecondary(const zb::msg::WriteChunkRequest& request,
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

} // namespace zb::virtual_node
