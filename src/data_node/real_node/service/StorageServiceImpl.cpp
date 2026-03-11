#include "StorageServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <utility>

#include "real_node.pb.h"

namespace zb::real_node {

namespace {

constexpr uint32_t kReplicationRepairMaxRetry = 8;
constexpr int kReplicationRepairRetryIntervalMs = 500;
constexpr size_t kReplicationRepairMaxQueue = 10000;

} // namespace

StorageServiceImpl::StorageServiceImpl(DiskManager* disk_manager,
                                       LocalPathResolver* path_resolver,
                                       IOExecutor* io_executor)
    : disk_manager_(disk_manager),
      path_resolver_(path_resolver),
      io_executor_(io_executor) {
    repl_repair_thread_ = std::thread([this]() { ReplicationRepairLoop(); });
}

StorageServiceImpl::~StorageServiceImpl() {
    stop_repl_repair_.store(true);
    repl_repair_cv_.notify_all();
    if (repl_repair_thread_.joinable()) {
        repl_repair_thread_.join();
    }
}

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

zb::msg::WriteObjectReply StorageServiceImpl::WriteObject(const zb::msg::WriteObjectRequest& request) {
    zb::msg::WriteObjectReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
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

    std::string path = path_resolver_->Resolve(mount_point, object_id, true);
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
    TrackObjectAccess(request.disk_id,
                      object_id,
                      request.offset + bytes_written,
                      true,
                      FastChecksum64(request.data));

    {
        std::lock_guard<std::mutex> lock(repl_mu_);
        ++repl_.applied_lsn;
    }

    if (repl_snapshot.replication_enabled && repl_snapshot.is_primary && !request.is_replication &&
        !repl_snapshot.peer_address.empty()) {
        const uint64_t generation = BumpReplicationRepairGeneration(request);
        zb::msg::Status repl_status = ReplicateWriteToSecondary(request, repl_snapshot.epoch);
        if (!repl_status.ok()) {
            EnqueueReplicationRepair(request, repl_snapshot.epoch);
            reply.status = repl_status;
            return reply;
        }
        std::lock_guard<std::mutex> lock(repl_repair_mu_);
        const std::string key = BuildReplicationRepairKey(request);
        auto it = repl_repair_generation_.find(key);
        if (it != repl_repair_generation_.end() && it->second == generation) {
            repl_repair_generation_.erase(it);
        }
    }
    return reply;
}

zb::msg::ReadObjectReply StorageServiceImpl::ReadObject(const zb::msg::ReadObjectRequest& request) {
    zb::msg::ReadObjectReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, object_id, false);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }

    uint64_t bytes_read = 0;
    reply.status = io_executor_->Read(path, request.offset, request.size, &reply.data, &bytes_read);
    reply.bytes = bytes_read;
    if (reply.status.ok()) {
        TrackObjectAccess(request.disk_id, object_id, request.offset + bytes_read, false, 0);
    }
    return reply;
}

zb::msg::ReadArchivedFileReply StorageServiceImpl::ReadArchivedFile(const zb::msg::ReadArchivedFileRequest& request) {
    (void)request;
    zb::msg::ReadArchivedFileReply reply;
    reply.status = zb::msg::Status::InvalidArgument("real node does not support archived-file read");
    return reply;
}

zb::msg::DeleteObjectReply StorageServiceImpl::DeleteObject(const zb::msg::DeleteObjectRequest& request) {
    zb::msg::DeleteObjectReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, object_id, false);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }
    reply.status = io_executor_->Delete(path);
    if (reply.status.code == zb::msg::StatusCode::kNotFound) {
        reply.status = zb::msg::Status::Ok();
    }
    if (reply.status.ok()) {
        RemoveObjectTracking(request.disk_id, object_id);
    }
    return reply;
}

zb::msg::Status StorageServiceImpl::PutObject(const zb::data_node::ObjectWriteRequest& request) {
    zb::msg::WriteObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    object_request.offset = request.offset;
    object_request.data.assign(request.data.data(), request.data.size());
    object_request.epoch = request.epoch;
    object_request.is_replication = request.is_replication;
    return WriteObject(object_request).status;
}

zb::data_node::ObjectReadResult StorageServiceImpl::GetObject(const zb::data_node::ObjectReadRequest& request) {
    zb::msg::ReadObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    object_request.offset = request.offset;
    object_request.size = request.size;
    const zb::msg::ReadObjectReply reply = ReadObject(object_request);
    zb::data_node::ObjectReadResult result;
    result.status = reply.status;
    if (reply.status.ok()) {
        result.data = reply.data;
    }
    return result;
}

zb::msg::Status StorageServiceImpl::DeleteObject(const zb::data_node::ObjectDeleteRequest& request) {
    zb::msg::DeleteObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    return DeleteObject(object_request).status;
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

zb::msg::Status StorageServiceImpl::ReplicateWriteToSecondary(const zb::msg::WriteObjectRequest& request,
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
    zb::rpc::WriteObjectRequest replicate_req;
    const std::string object_id = request.ArchiveObjectId();
    replicate_req.set_disk_id(request.disk_id);
    replicate_req.set_object_id(object_id);
    replicate_req.set_offset(request.offset);
    replicate_req.set_data(request.data);
    replicate_req.set_is_replication(true);
    replicate_req.set_epoch(epoch);
    replicate_req.set_archive_op_id(request.archive_op_id);
    replicate_req.set_inode_id(request.inode_id);
    replicate_req.set_file_id(request.file_id);
    replicate_req.set_file_path(request.file_path);
    replicate_req.set_file_size(request.file_size);
    replicate_req.set_file_offset(request.file_offset);
    replicate_req.set_file_mode(request.file_mode);
    replicate_req.set_file_uid(request.file_uid);
    replicate_req.set_file_gid(request.file_gid);
    replicate_req.set_file_mtime(request.file_mtime);
    replicate_req.set_file_object_index(request.file_object_index);

    zb::rpc::WriteObjectReply replicate_resp;
    brpc::Controller cntl;
    stub.WriteObject(&cntl, &replicate_req, &replicate_resp, nullptr);
    if (cntl.Failed()) {
        return zb::msg::Status::IoError("replication rpc failed: " + cntl.ErrorText());
    }
    if (replicate_resp.status().code() != zb::rpc::STATUS_OK) {
        return zb::msg::Status::IoError("replication rejected: " + replicate_resp.status().message());
    }
    return zb::msg::Status::Ok();
}

void StorageServiceImpl::EnqueueReplicationRepair(const zb::msg::WriteObjectRequest& request, uint64_t epoch) {
    std::lock_guard<std::mutex> lock(repl_repair_mu_);
    const std::string key = BuildReplicationRepairKey(request);
    uint64_t generation = 1;
    auto gen_it = repl_repair_generation_.find(key);
    if (gen_it != repl_repair_generation_.end()) {
        generation = gen_it->second;
    } else {
        repl_repair_generation_[key] = generation;
    }
    for (auto qit = repl_repair_queue_.begin(); qit != repl_repair_queue_.end();) {
        if (qit->key == key) {
            qit = repl_repair_queue_.erase(qit);
        } else {
            ++qit;
        }
    }
    if (repl_repair_queue_.size() >= kReplicationRepairMaxQueue) {
        const ReplicationRepairTask dropped = std::move(repl_repair_queue_.front());
        repl_repair_queue_.pop_front();
        auto dropped_it = repl_repair_generation_.find(dropped.key);
        if (dropped_it != repl_repair_generation_.end() && dropped_it->second == dropped.generation) {
            repl_repair_generation_.erase(dropped_it);
        }
    }
    ReplicationRepairTask task;
    task.key = key;
    task.request = request;
    task.epoch = epoch;
    task.attempts = 0;
    task.generation = generation;
    repl_repair_queue_.push_back(std::move(task));
    repl_repair_cv_.notify_one();
}

uint64_t StorageServiceImpl::BumpReplicationRepairGeneration(const zb::msg::WriteObjectRequest& request) {
    std::lock_guard<std::mutex> lock(repl_repair_mu_);
    const std::string key = BuildReplicationRepairKey(request);
    uint64_t& generation = repl_repair_generation_[key];
    generation += 1;
    return generation;
}

std::string StorageServiceImpl::BuildReplicationRepairKey(const zb::msg::WriteObjectRequest& request) {
    return request.disk_id + "|" + request.ArchiveObjectId() + "|" +
           std::to_string(request.offset) + "|" + std::to_string(request.data.size());
}

void StorageServiceImpl::ReplicationRepairLoop() {
    while (true) {
        ReplicationRepairTask task;
        {
            std::unique_lock<std::mutex> lock(repl_repair_mu_);
            repl_repair_cv_.wait(lock, [this]() {
                return stop_repl_repair_.load() || !repl_repair_queue_.empty();
            });
            if (stop_repl_repair_.load() && repl_repair_queue_.empty()) {
                break;
            }
            task = std::move(repl_repair_queue_.front());
            repl_repair_queue_.pop_front();
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second != task.generation) {
                continue;
            }
        }

        zb::msg::Status status = ReplicateWriteToSecondary(task.request, task.epoch);
        if (status.ok()) {
            std::lock_guard<std::mutex> lock(repl_repair_mu_);
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second == task.generation) {
                repl_repair_generation_.erase(gen_it);
            }
            continue;
        }
        if (task.attempts + 1 >= kReplicationRepairMaxRetry) {
            std::lock_guard<std::mutex> lock(repl_repair_mu_);
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second == task.generation) {
                repl_repair_generation_.erase(gen_it);
            }
            continue;
        }

        ++task.attempts;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kReplicationRepairRetryIntervalMs * static_cast<int>(task.attempts)));
        std::lock_guard<std::mutex> lock(repl_repair_mu_);
        if (stop_repl_repair_.load()) {
            continue;
        }
        auto gen_it = repl_repair_generation_.find(task.key);
        if (gen_it != repl_repair_generation_.end() && gen_it->second != task.generation) {
            continue;
        }
        if (repl_repair_queue_.size() >= kReplicationRepairMaxQueue) {
            const ReplicationRepairTask dropped = std::move(repl_repair_queue_.front());
            repl_repair_queue_.pop_front();
            auto dropped_it = repl_repair_generation_.find(dropped.key);
            if (dropped_it != repl_repair_generation_.end() && dropped_it->second == dropped.generation) {
                repl_repair_generation_.erase(dropped_it);
            }
        }
        repl_repair_queue_.push_back(std::move(task));
        repl_repair_cv_.notify_one();
    }
}

void StorageServiceImpl::SetArchiveTrackingMaxObjects(size_t max_objects) {
    archive_tracking_max_objects_ = max_objects > 0 ? max_objects : 1;
    archive_meta_store_.SetMaxObjects(archive_tracking_max_objects_);
}

std::vector<ArchiveCandidateStat> StorageServiceImpl::CollectArchiveCandidates(uint32_t max_candidates,
                                                                               uint64_t min_age_ms) const {
    const std::vector<ArchiveCandidateView> views =
        archive_meta_store_.CollectCandidates(max_candidates, min_age_ms, NowMilliseconds());
    std::vector<ArchiveCandidateStat> out;
    out.reserve(views.size());
    for (const auto& view : views) {
        ArchiveCandidateStat candidate;
        candidate.disk_id = view.disk_id;
        candidate.SetArchiveObjectId(view.ArchiveObjectId());
        candidate.last_access_ts_ms = view.last_access_ts_ms;
        candidate.size_bytes = view.size_bytes;
        candidate.checksum = view.checksum;
        candidate.heat_score = view.heat_score;
        candidate.archive_state = view.archive_state;
        candidate.version = view.version;
        candidate.score = view.score;
        candidate.read_ops = view.read_ops;
        candidate.write_ops = view.write_ops;
        out.push_back(std::move(candidate));
    }
    return out;
}

void StorageServiceImpl::TrackObjectAccess(const std::string& disk_id,
                                           const std::string& object_id,
                                           uint64_t end_offset,
                                           bool is_write,
                                           uint64_t checksum) {
    archive_meta_store_.TrackObjectAccess(disk_id, object_id, end_offset, is_write, checksum, NowMilliseconds());
}

void StorageServiceImpl::RemoveObjectTracking(const std::string& disk_id, const std::string& object_id) {
    archive_meta_store_.RemoveObject(disk_id, object_id);
}

bool StorageServiceImpl::InitArchiveMetaStore(const std::string& meta_dir,
                                              size_t max_objects,
                                              uint32_t snapshot_interval_ops,
                                              bool wal_fsync,
                                              std::string* error) {
    archive_tracking_max_objects_ = max_objects > 0 ? max_objects : 1;
    return archive_meta_store_.Init(meta_dir,
                                    archive_tracking_max_objects_,
                                    snapshot_interval_ops,
                                    error,
                                    wal_fsync);
}

bool StorageServiceImpl::FlushArchiveMetaSnapshot(std::string* error) {
    return archive_meta_store_.FlushSnapshot(error);
}

zb::msg::Status StorageServiceImpl::UpdateArchiveState(const std::string& disk_id,
                                                       const std::string& object_id,
                                                       const std::string& archive_state,
                                                       uint64_t version) {
    if (disk_id.empty() || object_id.empty() || archive_state.empty()) {
        return zb::msg::Status::InvalidArgument("disk_id/object_id/archive_state is empty");
    }
    if (!archive_meta_store_.UpdateObjectArchiveState(disk_id, object_id, archive_state, version)) {
        return zb::msg::Status::IoError("failed to update archive state");
    }
    return zb::msg::Status::Ok();
}

uint64_t StorageServiceImpl::FastChecksum64(const std::string& data) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : data) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t StorageServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::real_node
