#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mds.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_int32(timeout_ms, 5000, "RPC timeout in ms");
DEFINE_int32(max_retry, 1, "RPC max retry");
DEFINE_string(base_dir, "/optical_stress", "Base directory in distributed filesystem");
DEFINE_bool(auto_suffix, true, "Append timestamp suffix to base_dir to avoid conflicts");
DEFINE_uint32(file_count, 64, "Number of files to write concurrently");
DEFINE_uint64(write_size, 1048576, "Bytes per write");
DEFINE_uint64(object_unit_size, 4194304, "Object unit size used by Create");
DEFINE_uint32(replica, 1, "Replica count used by Create");
DEFINE_uint64(duration_sec, 1800, "Stress write duration in seconds");
DEFINE_uint64(target_total_bytes, 0, "Optional total bytes to write, 0 means unlimited by bytes");
DEFINE_uint32(report_interval_sec, 10, "Report interval in seconds");
DEFINE_uint32(sample_files, 8, "Number of files sampled for resolve-read check per report");
DEFINE_bool(require_optical, false, "Deprecated flag in simplified metadata mode; ignored");
DEFINE_uint64(cooldown_sec, 0, "Cooldown time after writes for archive/evict to catch up");
DEFINE_bool(require_optical_only_after_cooldown, false,
            "Deprecated flag in simplified metadata mode; ignored");
DEFINE_uint32(cluster_view_refresh_ms, 2000, "Scheduler cluster-view refresh interval in ms");

namespace {

uint64_t NowMilliseconds() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

struct FileState {
    std::string path;
    uint64_t inode_id{0};
    uint64_t object_unit_size{0};
    uint64_t file_meta_version{0};
    uint64_t size{0};
    uint64_t writes{0};
    zb::rpc::ReplicaLocation anchor;
};

struct ResolveStats {
    uint64_t files_scanned{0};
    uint64_t objects_total{0};
};

class SchedulerAddressBook {
public:
    bool Resolve(const std::string& node_id, std::string* address, std::string* err) {
        if (node_id.empty() || !address) {
            if (err) {
                *err = "node_id/address invalid";
            }
            return false;
        }
        const uint64_t now_ms = NowMilliseconds();
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = node_to_addr_.find(node_id);
            if (it == node_to_addr_.end()) {
                const std::string base = BaseNodeId(node_id);
                if (base != node_id) {
                    it = node_to_addr_.find(base);
                }
            }
            if (it != node_to_addr_.end() &&
                last_refresh_ms_ > 0 &&
                now_ms - last_refresh_ms_ <= std::max<uint64_t>(1, FLAGS_cluster_view_refresh_ms)) {
                *address = it->second;
                return true;
            }
        }
        if (!Refresh(err)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mu_);
        auto it = node_to_addr_.find(node_id);
        if (it == node_to_addr_.end()) {
            const std::string base = BaseNodeId(node_id);
            if (base != node_id) {
                it = node_to_addr_.find(base);
            }
        }
        if (it == node_to_addr_.end()) {
            if (err) {
                *err = "node_id not found in scheduler view: " + node_id;
            }
            return false;
        }
        *address = it->second;
        return true;
    }

private:
    static std::string BaseNodeId(const std::string& node_id) {
        const size_t pos = node_id.rfind("-v");
        if (pos == std::string::npos || pos + 2 >= node_id.size()) {
            return node_id;
        }
        for (size_t i = pos + 2; i < node_id.size(); ++i) {
            if (!std::isdigit(static_cast<unsigned char>(node_id[i]))) {
                return node_id;
            }
        }
        return node_id.substr(0, pos);
    }

    bool Refresh(std::string* err) {
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        if (channel.Init(FLAGS_scheduler.c_str(), &options) != 0) {
            if (err) {
                *err = "failed to init scheduler channel";
            }
            return false;
        }
        zb::rpc::SchedulerService_Stub stub(&channel);
        zb::rpc::GetClusterViewRequest req;
        req.set_min_generation(0);
        zb::rpc::GetClusterViewReply resp;
        brpc::Controller cntl;
        stub.GetClusterView(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (err) {
                *err = cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::SCHED_OK) {
            if (err) {
                *err = resp.status().message();
            }
            return false;
        }
        std::unordered_map<std::string, std::string> updated;
        for (const auto& node : resp.nodes()) {
            if (node.node_id().empty() || node.address().empty()) {
                continue;
            }
            updated[node.node_id()] = node.address();
        }
        std::lock_guard<std::mutex> lock(mu_);
        node_to_addr_.swap(updated);
        last_refresh_ms_ = NowMilliseconds();
        return true;
    }

    std::mutex mu_;
    std::unordered_map<std::string, std::string> node_to_addr_;
    uint64_t last_refresh_ms_{0};
};

SchedulerAddressBook& AddressBook() {
    static SchedulerAddressBook book;
    return book;
}

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

zb::rpc::ReplicaLocation BuildObjectReplicaFromAnchor(const zb::rpc::ReplicaLocation& anchor,
                                                      uint64_t inode_id,
                                                      uint32_t object_index) {
    zb::rpc::ReplicaLocation replica = anchor;
    replica.set_object_id(BuildStableObjectId(inode_id, object_index));
    replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    return replica;
}

class MdsClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool Mkdir(const std::string& path, std::string* err) {
        zb::rpc::MkdirRequest req;
        req.set_path(path);
        req.set_mode(0755);
        req.set_uid(0);
        req.set_gid(0);
        zb::rpc::MkdirReply resp;
        brpc::Controller cntl;
        stub_.Mkdir(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (err) {
                *err = cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() == zb::rpc::MDS_OK || resp.status().code() == zb::rpc::MDS_ALREADY_EXISTS) {
            return true;
        }
        if (err) {
            *err = resp.status().message();
        }
        return false;
    }

    bool Create(const std::string& path, uint32_t replica, uint64_t object_unit_size, uint64_t* inode_id, std::string* err) {
        zb::rpc::CreateRequest req;
        req.set_path(path);
        req.set_mode(0644);
        req.set_uid(0);
        req.set_gid(0);
        req.set_replica(replica);
        req.set_object_unit_size(object_unit_size);
        zb::rpc::CreateReply resp;
        brpc::Controller cntl;
        stub_.Create(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (err) {
                *err = cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (err) {
                *err = resp.status().message();
            }
            return false;
        }
        if (inode_id) {
            *inode_id = resp.location().attr().inode_id();
        }
        return true;
    }

    bool GetFileLocation(uint64_t inode_id, zb::rpc::ReplicaLocation* anchor, std::string* err) {
        zb::rpc::GetFileLocationRequest req;
        req.set_inode_id(inode_id);
        zb::rpc::GetFileLocationReply resp;
        brpc::Controller cntl;
        stub_.GetFileLocation(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (err) {
                *err = cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (err) {
                *err = resp.status().message();
            }
            return false;
        }
        if (anchor) {
            const auto& view = resp.location();
            if (!view.disk_location().node_id().empty() && !view.disk_location().disk_id().empty()) {
                anchor->set_node_id(view.disk_location().node_id());
                anchor->set_node_address(view.disk_location().node_address());
                anchor->set_disk_id(view.disk_location().disk_id());
                anchor->set_object_id("obj-" + std::to_string(view.attr().inode_id()) + "-0");
                anchor->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
                anchor->set_replica_state(zb::rpc::REPLICA_READY);
            } else if (!view.optical_location().node_id().empty() && !view.optical_location().disk_id().empty()) {
                anchor->set_node_id(view.optical_location().node_id());
                anchor->set_node_address(view.optical_location().node_address());
                anchor->set_disk_id(view.optical_location().disk_id());
                anchor->set_object_id(view.optical_location().file_id());
                anchor->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
                anchor->set_replica_state(zb::rpc::REPLICA_READY);
                anchor->set_image_id(view.optical_location().image_id());
            } else {
                if (err) {
                    *err = "GetFileLocation returned empty location";
                }
                return false;
            }
        }
        return true;
    }

private:
    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class DataNodeClient {
public:
    bool ResolveFileRead(const zb::rpc::ReplicaLocation& replica,
                         uint64_t inode_id,
                         uint64_t offset,
                         uint64_t size,
                         uint64_t object_unit_size_hint,
                         zb::rpc::FileMeta* meta,
                         std::vector<zb::rpc::FileObjectSlice>* slices,
                         std::string* err) {
        std::vector<std::string> addresses = CollectAddresses(replica);
        if (addresses.empty()) {
            if (err) {
                *err = "no endpoint in replica";
            }
            return false;
        }
        std::string last_error = "ResolveFileRead failed";
        for (const auto& address : addresses) {
            brpc::Channel* ch = GetChannel(address, &last_error);
            if (!ch) {
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(ch);
            zb::rpc::ResolveFileReadRequest req;
            req.set_inode_id(inode_id);
            req.set_offset(offset);
            req.set_size(size);
            req.set_disk_id(replica.disk_id());
            req.set_object_unit_size_hint(object_unit_size_hint);
            zb::rpc::ResolveFileReadReply resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.ResolveFileRead(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (resp.status().code() == zb::rpc::STATUS_OK) {
                if (meta) {
                    *meta = resp.meta();
                }
                if (slices) {
                    slices->assign(resp.slices().begin(), resp.slices().end());
                }
                return true;
            }
            last_error = resp.status().message();
        }
        if (err) {
            *err = last_error;
        }
        return false;
    }

    bool CommitFileWrite(const zb::rpc::ReplicaLocation& replica,
                        uint64_t inode_id,
                        const std::string& txid,
                        uint64_t file_size,
                        uint64_t object_unit_size,
                        uint64_t expected_version,
                        bool allow_create,
                        zb::rpc::FileMeta* committed,
                        std::string* err) {
        std::vector<std::string> addresses = CollectAddresses(replica);
        if (addresses.empty()) {
            if (err) {
                *err = "no endpoint in replica";
            }
            return false;
        }
        std::string last_error = "CommitFileWrite failed";
        for (const auto& address : addresses) {
            brpc::Channel* ch = GetChannel(address, &last_error);
            if (!ch) {
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(ch);
            zb::rpc::CommitFileWriteRequest req;
            req.set_inode_id(inode_id);
            req.set_txid(txid);
            req.set_file_size(file_size);
            req.set_object_unit_size(object_unit_size);
            req.set_expected_version(expected_version);
            req.set_allow_create(allow_create);
            req.set_mtime_sec(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()));
            zb::rpc::CommitFileWriteReply resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.CommitFileWrite(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (resp.status().code() == zb::rpc::STATUS_OK) {
                if (committed) {
                    *committed = resp.meta();
                }
                return true;
            }
            last_error = resp.status().message();
        }
        if (err) {
            *err = last_error;
        }
        return false;
    }

    bool WriteReplica(const zb::rpc::ReplicaLocation& replica,
                      uint64_t offset,
                      const std::string& data,
                      std::string* err) {
        std::vector<std::string> addresses = CollectAddresses(replica);

        if (addresses.empty()) {
            if (err) {
                *err = "no endpoint in replica";
            }
            return false;
        }

        std::string last_error = "write failed";
        for (const auto& address : addresses) {
            brpc::Channel* ch = GetChannel(address, &last_error);
            if (!ch) {
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(ch);
            zb::rpc::WriteObjectRequest req;
            req.set_disk_id(replica.disk_id());
            req.set_object_id(ReplicaObjectId(replica));
            req.set_offset(offset);
            req.set_data(data);
            req.set_epoch(replica.epoch());
            zb::rpc::WriteObjectReply resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.WriteObject(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (resp.status().code() == zb::rpc::STATUS_OK) {
                return true;
            }
            last_error = resp.status().message();
        }

        if (err) {
            *err = last_error;
        }
        return false;
    }

private:
    static std::vector<std::string> CollectAddresses(const zb::rpc::ReplicaLocation& replica) {
        std::vector<std::string> addresses;
        auto push_unique = [&addresses](const std::string& address) {
            if (address.empty()) {
                return;
            }
            if (std::find(addresses.begin(), addresses.end(), address) == addresses.end()) {
                addresses.push_back(address);
            }
        };
        auto resolve_and_push = [&push_unique](const std::string& node_id) {
            if (node_id.empty()) {
                return;
            }
            std::string address;
            std::string ignore_error;
            if (AddressBook().Resolve(node_id, &address, &ignore_error)) {
                push_unique(address);
            }
        };
        resolve_and_push(replica.primary_node_id());
        resolve_and_push(replica.node_id());
        resolve_and_push(replica.secondary_node_id());
        return addresses;
    }

    brpc::Channel* GetChannel(const std::string& address, std::string* err) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = channels_.find(address);
        if (it != channels_.end()) {
            return it->second.get();
        }
        auto channel = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        if (channel->Init(address.c_str(), &options) != 0) {
            if (err) {
                *err = "failed to init channel to " + address;
            }
            return nullptr;
        }
        brpc::Channel* raw = channel.get();
        channels_[address] = std::move(channel);
        return raw;
    }

    std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream ss(path);
    while (std::getline(ss, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool EnsureDirRecursive(MdsClient* mds, const std::string& path, std::string* err) {
    if (!mds) {
        if (err) {
            *err = "mds client is null";
        }
        return false;
    }
    if (path.empty() || path == "/") {
        return true;
    }
    std::vector<std::string> parts = SplitPath(path);
    std::string cur;
    for (const auto& part : parts) {
        cur += "/" + part;
        if (!mds->Mkdir(cur, err)) {
            return false;
        }
    }
    return true;
}

std::string BuildWritePayload(size_t size, uint64_t seq) {
    std::string data(size, 'a');
    if (data.empty()) {
        return data;
    }
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<char>('a' + ((seq + i) % 26));
    }
    return data;
}

bool EnsureFileMeta(DataNodeClient* data_client, FileState* file, std::string* err) {
    if (!data_client || !file || file->inode_id == 0 ||
        file->anchor.node_id().empty() || file->anchor.disk_id().empty()) {
        if (err) {
            *err = "invalid file meta context";
        }
        return false;
    }
    if (file->object_unit_size == 0) {
        file->object_unit_size = FLAGS_object_unit_size;
    }
    if (file->file_meta_version > 0) {
        return true;
    }
    zb::rpc::FileMeta meta;
    std::vector<zb::rpc::FileObjectSlice> ignored_slices;
    if (data_client->ResolveFileRead(file->anchor,
                                     file->inode_id,
                                     0,
                                     0,
                                     file->object_unit_size,
                                     &meta,
                                     &ignored_slices,
                                     err)) {
        file->size = meta.file_size();
        file->object_unit_size = meta.object_unit_size() > 0 ? meta.object_unit_size() : file->object_unit_size;
        file->file_meta_version = meta.version();
        return true;
    }

    zb::rpc::FileMeta committed;
    const std::string txid =
        "bootstrap-" + std::to_string(file->inode_id) + "-" + std::to_string(static_cast<uint64_t>(NowMilliseconds()));
    if (!data_client->CommitFileWrite(file->anchor,
                                      file->inode_id,
                                      txid,
                                      file->size,
                                      file->object_unit_size,
                                      0,
                                      true,
                                      &committed,
                                      err)) {
        return false;
    }
    file->size = committed.file_size();
    file->object_unit_size = committed.object_unit_size() > 0 ? committed.object_unit_size() : file->object_unit_size;
    file->file_meta_version = committed.version();
    return true;
}

bool WriteByAnchor(DataNodeClient* data_client,
                   const FileState& file,
                   uint64_t file_offset,
                   const std::string& payload,
                   std::string* err) {
    if (!data_client || file.object_unit_size == 0) {
        if (err) {
            *err = "data client is null or object_unit_size is zero";
        }
        return false;
    }

    const uint64_t object_unit_size = file.object_unit_size;
    const uint64_t write_size = static_cast<uint64_t>(payload.size());
    if (write_size == 0) {
        return true;
    }
    if (file_offset > std::numeric_limits<uint64_t>::max() - write_size) {
        if (err) {
            *err = "file offset + write size overflow";
        }
        return false;
    }
    const uint64_t write_end_global = file_offset + write_size;
    const uint64_t start_index = file_offset / object_unit_size;
    const uint64_t end_index = (write_end_global - 1) / object_unit_size;
    for (uint64_t i = start_index; i <= end_index; ++i) {
        const uint32_t object_index = static_cast<uint32_t>(i);
        const uint64_t object_start = static_cast<uint64_t>(object_index) * object_unit_size;
        const uint64_t object_end = object_start + object_unit_size;
        const uint64_t write_start = std::max<uint64_t>(object_start, file_offset);
        const uint64_t write_end = std::min<uint64_t>(object_end, write_end_global);
        if (write_end <= write_start) {
            continue;
        }

        const uint64_t object_off = write_start - object_start;
        const uint64_t write_len = write_end - write_start;
        const uint64_t payload_off = write_start - file_offset;
        const std::string piece = payload.substr(static_cast<size_t>(payload_off), static_cast<size_t>(write_len));

        const zb::rpc::ReplicaLocation replica =
            BuildObjectReplicaFromAnchor(file.anchor, file.inode_id, object_index);
        if (!data_client->WriteReplica(replica, object_off, piece, err)) {
            return false;
        }
    }
    return true;
}

ResolveStats CollectResolveStats(DataNodeClient* data_client, const std::vector<FileState>& files) {
    ResolveStats stats;
    if (!data_client || files.empty()) {
        return stats;
    }

    const size_t limit = std::min<size_t>(files.size(), FLAGS_sample_files == 0 ? files.size() : FLAGS_sample_files);
    for (size_t i = 0; i < limit; ++i) {
        const FileState& f = files[i];
        if (f.inode_id == 0 || f.size == 0) {
            continue;
        }
        zb::rpc::FileMeta meta;
        std::vector<zb::rpc::FileObjectSlice> slices;
        std::string err;
        if (!data_client->ResolveFileRead(f.anchor,
                                          f.inode_id,
                                          0,
                                          f.size,
                                          f.object_unit_size,
                                          &meta,
                                          &slices,
                                          &err)) {
            continue;
        }

        ++stats.files_scanned;
        stats.objects_total += static_cast<uint64_t>(slices.size());
    }
    return stats;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_file_count == 0 || FLAGS_write_size == 0 || FLAGS_object_unit_size == 0 || FLAGS_duration_sec == 0) {
        std::cerr << "invalid flags: file_count/write_size/object_unit_size/duration_sec must be > 0" << std::endl;
        return 1;
    }

    MdsClient mds;
    if (!mds.Init(FLAGS_mds)) {
        std::cerr << "failed to connect MDS: " << FLAGS_mds << std::endl;
        return 1;
    }
    DataNodeClient data_client;

    uint64_t ts = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count());
    std::string base_dir = FLAGS_base_dir;
    if (FLAGS_auto_suffix) {
        base_dir += "_" + std::to_string(ts);
    }
    std::string err;
    if (!EnsureDirRecursive(&mds, base_dir, &err)) {
        std::cerr << "failed to create dir " << base_dir << ": " << err << std::endl;
        return 1;
    }

    std::vector<FileState> files;
    files.reserve(FLAGS_file_count);
    for (uint32_t i = 0; i < FLAGS_file_count; ++i) {
        FileState state;
        state.path = base_dir + "/f_" + std::to_string(i) + ".bin";
        state.object_unit_size = FLAGS_object_unit_size;
        if (!mds.Create(state.path, FLAGS_replica, FLAGS_object_unit_size, &state.inode_id, &err)) {
            std::cerr << "Create failed: " << state.path << " err=" << err << std::endl;
            return 1;
        }
        if (!mds.GetFileLocation(state.inode_id, &state.anchor, &err)) {
            std::cerr << "GetFileLocation failed: " << state.path << " err=" << err << std::endl;
            return 1;
        }
        files.push_back(std::move(state));
    }

    std::cout << "Started optical stress test, base_dir=" << base_dir
              << " files=" << files.size()
              << " write_size=" << FLAGS_write_size
              << " duration_sec=" << FLAGS_duration_sec << std::endl;

    const auto begin = std::chrono::steady_clock::now();
    auto next_report = begin + std::chrono::seconds(std::max<uint32_t>(1, FLAGS_report_interval_sec));

    uint64_t seq = 0;
    uint64_t total_bytes = 0;
    uint64_t write_ops = 0;
    uint64_t failures = 0;
    size_t file_index = 0;
    while (true) {
        const auto now = std::chrono::steady_clock::now();
        const uint64_t elapsed_sec =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now - begin).count());
        if (elapsed_sec >= FLAGS_duration_sec) {
            break;
        }
        if (FLAGS_target_total_bytes > 0 && total_bytes >= FLAGS_target_total_bytes) {
            break;
        }

        FileState& f = files[file_index];
        file_index = (file_index + 1) % files.size();

        if (!EnsureFileMeta(&data_client, &f, &err)) {
            ++failures;
            std::cerr << "EnsureFileMeta failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }
        const std::string payload = BuildWritePayload(static_cast<size_t>(FLAGS_write_size), seq++);
        const uint64_t offset = f.size;

        if (!WriteByAnchor(&data_client, f, offset, payload, &err)) {
            ++failures;
            std::cerr << "object write failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }

        const uint64_t new_size = offset + FLAGS_write_size;
        zb::rpc::FileMeta committed_meta;
        const std::string txid =
            "write-" + std::to_string(f.inode_id) + "-" + std::to_string(seq) + "-" +
            std::to_string(static_cast<uint64_t>(NowMilliseconds()));
        if (!data_client.CommitFileWrite(f.anchor,
                                         f.inode_id,
                                         txid,
                                         new_size,
                                         f.object_unit_size,
                                         f.file_meta_version,
                                         true,
                                         &committed_meta,
                                         &err)) {
            ++failures;
            std::cerr << "CommitFileWrite failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }

        f.size = committed_meta.file_size();
        if (committed_meta.object_unit_size() > 0) {
            f.object_unit_size = committed_meta.object_unit_size();
        }
        f.file_meta_version = committed_meta.version();
        ++f.writes;
        ++write_ops;
        total_bytes += FLAGS_write_size;

        if (now >= next_report) {
            ResolveStats stats = CollectResolveStats(&data_client, files);
            std::cout << "[report] elapsed_sec=" << elapsed_sec
                      << " total_bytes=" << total_bytes
                      << " write_ops=" << write_ops
                      << " failures=" << failures
                      << " sampled_files=" << stats.files_scanned
                      << " sampled_objects=" << stats.objects_total
                      << std::endl;
            next_report = now + std::chrono::seconds(std::max<uint32_t>(1, FLAGS_report_interval_sec));
        }
    }

    if (FLAGS_cooldown_sec > 0) {
        std::cout << "cooldown " << FLAGS_cooldown_sec << "s for archive/evict..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cooldown_sec));
        ResolveStats stats = CollectResolveStats(&data_client, files);
        std::cout << "[cooldown-check] sampled_files=" << stats.files_scanned
                  << " sampled_objects=" << stats.objects_total
                  << std::endl;
    }

    std::cout << "Finished: total_bytes=" << total_bytes
              << " write_ops=" << write_ops
              << " failures=" << failures
              << " base_dir=" << base_dir
              << std::endl;

    if (FLAGS_require_optical || FLAGS_require_optical_only_after_cooldown) {
        std::cout << "Note: require_optical flags are deprecated and ignored in simplified metadata mode."
                  << std::endl;
    }
    if (failures > 0) {
        std::cerr << "FAILED: write failures encountered=" << failures << std::endl;
        return 4;
    }
    return 0;
}
