#include "VirtualStorageServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <thread>
#include <utility>

#include "real_node.pb.h"

namespace zb::virtual_node {

namespace {

constexpr uint32_t kReplicationRepairMaxRetry = 8;
constexpr int kReplicationRepairRetryIntervalMs = 500;
constexpr size_t kReplicationRepairMaxQueue = 10000;

} // namespace

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
    for (const auto& disk_id : disk_set_) {
        disk_used_bytes_[disk_id] = 0;
    }
    repl_repair_thread_ = std::thread([this]() { ReplicationRepairLoop(); });
}

VirtualStorageServiceImpl::~VirtualStorageServiceImpl() {
    stop_repl_repair_.store(true);
    repl_repair_cv_.notify_all();
    if (repl_repair_thread_.joinable()) {
        repl_repair_thread_.join();
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
        std::lock_guard<std::mutex> lock(chunk_mu_);
        const std::string key = BuildChunkKey(request.disk_id, request.chunk_id);
        std::string& blob = chunk_data_[key];
        if (request.offset > blob.size()) {
            blob.resize(static_cast<size_t>(request.offset), '\0');
        }
        const size_t write_begin = static_cast<size_t>(request.offset);
        const size_t write_end = write_begin + request.data.size();
        if (blob.size() < write_end) {
            blob.resize(write_end, '\0');
        }
        std::copy(request.data.begin(), request.data.end(), blob.begin() + write_begin);

        const uint64_t old_size = chunk_sizes_[key];
        const uint64_t new_size = static_cast<uint64_t>(blob.size());
        if (new_size > old_size) {
            const uint64_t increase = new_size - old_size;
            const uint64_t used = disk_used_bytes_[request.disk_id];
            if (used + increase > config_.disk_capacity_bytes) {
                // rollback write growth when capacity exceeded
                blob.resize(static_cast<size_t>(old_size));
                reply.status = zb::msg::Status::IoError("NO_SPACE");
                reply.bytes = 0;
                return reply;
            }
            disk_used_bytes_[request.disk_id] = used + increase;
        }
        chunk_sizes_[key] = static_cast<uint64_t>(blob.size());
    }
    TrackChunkAccess(request.disk_id,
                     request.chunk_id,
                     request.offset + reply.bytes,
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
    {
        std::lock_guard<std::mutex> lock(chunk_mu_);
        const std::string key = BuildChunkKey(request.disk_id, request.chunk_id);
        auto it = chunk_data_.find(key);
        if (it == chunk_data_.end()) {
            reply.status = zb::msg::Status::NotFound("chunk not found");
            return reply;
        }
        const std::string& blob = it->second;
        if (request.offset >= blob.size()) {
            reply.bytes = 0;
            reply.data.clear();
        } else {
            const uint64_t remain = static_cast<uint64_t>(blob.size()) - request.offset;
            const uint64_t read_len = std::min<uint64_t>(remain, request.size);
            reply.bytes = read_len;
            reply.data.assign(blob.data() + static_cast<size_t>(request.offset), static_cast<size_t>(read_len));
        }
    }
    TrackChunkAccess(request.disk_id, request.chunk_id, request.offset + reply.bytes, false, 0);
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
    {
        std::lock_guard<std::mutex> lock(chunk_mu_);
        const std::string key = BuildChunkKey(request.disk_id, request.chunk_id);
        auto it = chunk_data_.find(key);
        if (it != chunk_data_.end()) {
            const uint64_t size = static_cast<uint64_t>(it->second.size());
            chunk_data_.erase(it);
            chunk_sizes_.erase(key);
            uint64_t& used = disk_used_bytes_[request.disk_id];
            used = used > size ? (used - size) : 0;
        }
    }
    RemoveChunkTracking(request.disk_id, request.chunk_id);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::DiskReportReply VirtualStorageServiceImpl::GetDiskReport() const {
    zb::msg::DiskReportReply reply;
    std::unordered_map<std::string, uint64_t> used_snapshot;
    {
        std::lock_guard<std::mutex> lock(chunk_mu_);
        used_snapshot = disk_used_bytes_;
    }
    for (const auto& disk_id : disk_set_) {
        zb::msg::DiskReport report;
        report.id = disk_id;
        report.mount_point = config_.mount_point_prefix + "/" + disk_id;
        report.capacity_bytes = config_.disk_capacity_bytes;
        const uint64_t used = used_snapshot[disk_id];
        report.free_bytes = used >= report.capacity_bytes ? 0 : (report.capacity_bytes - used);
        report.is_healthy = true;
        reply.reports.push_back(std::move(report));
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

bool VirtualStorageServiceImpl::InitArchiveMetaStore(const std::string& meta_dir,
                                                     size_t max_chunks,
                                                     uint32_t snapshot_interval_ops,
                                                     bool wal_fsync,
                                                     std::string* error) {
    archive_tracking_max_chunks_ = max_chunks > 0 ? max_chunks : 1;
    return archive_meta_store_.Init(meta_dir,
                                    archive_tracking_max_chunks_,
                                    snapshot_interval_ops,
                                    error,
                                    wal_fsync);
}

bool VirtualStorageServiceImpl::FlushArchiveMetaSnapshot(std::string* error) {
    return archive_meta_store_.FlushSnapshot(error);
}

zb::msg::Status VirtualStorageServiceImpl::UpdateArchiveState(const std::string& disk_id,
                                                              const std::string& chunk_id,
                                                              const std::string& archive_state,
                                                              uint64_t version) {
    if (disk_id.empty() || chunk_id.empty() || archive_state.empty()) {
        return zb::msg::Status::InvalidArgument("disk_id/chunk_id/archive_state is empty");
    }
    if (!archive_meta_store_.UpdateArchiveState(disk_id, chunk_id, archive_state, version)) {
        return zb::msg::Status::IoError("failed to update archive state");
    }
    return zb::msg::Status::Ok();
}

void VirtualStorageServiceImpl::SetArchiveTrackingMaxChunks(size_t max_chunks) {
    archive_tracking_max_chunks_ = max_chunks > 0 ? max_chunks : 1;
    archive_meta_store_.SetMaxChunks(archive_tracking_max_chunks_);
}

std::vector<ArchiveCandidateStat> VirtualStorageServiceImpl::CollectArchiveCandidates(uint32_t max_candidates,
                                                                                       uint64_t min_age_ms) const {
    const std::vector<real_node::ArchiveCandidateView> views =
        archive_meta_store_.CollectCandidates(max_candidates, min_age_ms, NowMilliseconds());
    std::vector<ArchiveCandidateStat> out;
    out.reserve(views.size());
    for (const auto& view : views) {
        ArchiveCandidateStat stat;
        stat.disk_id = view.disk_id;
        stat.chunk_id = view.chunk_id;
        stat.last_access_ts_ms = view.last_access_ts_ms;
        stat.size_bytes = view.size_bytes;
        stat.checksum = view.checksum;
        stat.heat_score = view.heat_score;
        stat.archive_state = view.archive_state;
        stat.version = view.version;
        stat.score = view.score;
        stat.read_ops = view.read_ops;
        stat.write_ops = view.write_ops;
        out.push_back(std::move(stat));
    }
    return out;
}

bool VirtualStorageServiceImpl::ValidateDisk(const std::string& disk_id) const {
    return disk_set_.find(disk_id) != disk_set_.end();
}

void VirtualStorageServiceImpl::TrackChunkAccess(const std::string& disk_id,
                                                 const std::string& chunk_id,
                                                 uint64_t end_offset,
                                                 bool is_write,
                                                 uint64_t checksum) {
    archive_meta_store_.TrackAccess(disk_id, chunk_id, end_offset, is_write, checksum, NowMilliseconds());
}

void VirtualStorageServiceImpl::RemoveChunkTracking(const std::string& disk_id, const std::string& chunk_id) {
    archive_meta_store_.RemoveChunk(disk_id, chunk_id);
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

void VirtualStorageServiceImpl::EnqueueReplicationRepair(const zb::msg::WriteChunkRequest& request, uint64_t epoch) {
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

uint64_t VirtualStorageServiceImpl::BumpReplicationRepairGeneration(const zb::msg::WriteChunkRequest& request) {
    std::lock_guard<std::mutex> lock(repl_repair_mu_);
    const std::string key = BuildReplicationRepairKey(request);
    uint64_t& generation = repl_repair_generation_[key];
    generation += 1;
    return generation;
}

std::string VirtualStorageServiceImpl::BuildReplicationRepairKey(const zb::msg::WriteChunkRequest& request) {
    return request.disk_id + "|" + request.chunk_id + "|" +
           std::to_string(request.offset) + "|" + std::to_string(request.data.size());
}

void VirtualStorageServiceImpl::ReplicationRepairLoop() {
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

uint64_t VirtualStorageServiceImpl::FastChecksum64(const std::string& data) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : data) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t VirtualStorageServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string VirtualStorageServiceImpl::BuildChunkKey(const std::string& disk_id, const std::string& chunk_id) {
    return disk_id + "|" + chunk_id;
}

} // namespace zb::virtual_node
