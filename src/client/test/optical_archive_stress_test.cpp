#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
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

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_int32(timeout_ms, 5000, "RPC timeout in ms");
DEFINE_int32(max_retry, 1, "RPC max retry");
DEFINE_string(base_dir, "/optical_stress", "Base directory in distributed filesystem");
DEFINE_bool(auto_suffix, true, "Append timestamp suffix to base_dir to avoid conflicts");
DEFINE_uint32(file_count, 64, "Number of files to write concurrently");
DEFINE_uint64(write_size, 1048576, "Bytes per write");
DEFINE_uint64(chunk_size, 4194304, "Chunk size used by Create");
DEFINE_uint32(replica, 1, "Replica count used by Create");
DEFINE_uint64(duration_sec, 1800, "Stress write duration in seconds");
DEFINE_uint64(target_total_bytes, 0, "Optional total bytes to write, 0 means unlimited by bytes");
DEFINE_uint32(report_interval_sec, 10, "Report interval in seconds");
DEFINE_uint32(sample_files, 8, "Number of files sampled for layout check per report");
DEFINE_bool(require_optical, true, "Fail if no optical replica is observed");
DEFINE_uint64(cooldown_sec, 0, "Cooldown time after writes for archive/evict to catch up");
DEFINE_bool(require_optical_only_after_cooldown, false,
            "After cooldown, require at least one chunk with optical replica only (no disk replica)");

namespace {

struct FileState {
    std::string path;
    uint64_t inode_id{0};
    uint64_t size{0};
    uint64_t writes{0};
};

struct LayoutStats {
    uint64_t files_scanned{0};
    uint64_t chunks_total{0};
    uint64_t chunks_with_optical{0};
    uint64_t chunks_with_disk{0};
    uint64_t chunks_optical_only{0};
};

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

    bool Create(const std::string& path, uint32_t replica, uint64_t chunk_size, uint64_t* inode_id, std::string* err) {
        zb::rpc::CreateRequest req;
        req.set_path(path);
        req.set_mode(0644);
        req.set_uid(0);
        req.set_gid(0);
        req.set_replica(replica);
        req.set_chunk_size(chunk_size);
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
            *inode_id = resp.attr().inode_id();
        }
        return true;
    }

    bool AllocateWrite(uint64_t inode_id, uint64_t offset, uint64_t size, zb::rpc::FileLayout* layout, std::string* err) {
        zb::rpc::AllocateWriteRequest req;
        req.set_inode_id(inode_id);
        req.set_offset(offset);
        req.set_size(size);
        zb::rpc::AllocateWriteReply resp;
        brpc::Controller cntl;
        stub_.AllocateWrite(&cntl, &req, &resp, nullptr);
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
        if (layout) {
            *layout = resp.layout();
        }
        return true;
    }

    bool CommitWrite(uint64_t inode_id, uint64_t new_size, std::string* err) {
        zb::rpc::CommitWriteRequest req;
        req.set_inode_id(inode_id);
        req.set_new_size(new_size);
        zb::rpc::CommitWriteReply resp;
        brpc::Controller cntl;
        stub_.CommitWrite(&cntl, &req, &resp, nullptr);
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
        return true;
    }

    bool GetLayout(uint64_t inode_id, uint64_t offset, uint64_t size, zb::rpc::FileLayout* layout, std::string* err) {
        zb::rpc::GetLayoutRequest req;
        req.set_inode_id(inode_id);
        req.set_offset(offset);
        req.set_size(size);
        zb::rpc::GetLayoutReply resp;
        brpc::Controller cntl;
        stub_.GetLayout(&cntl, &req, &resp, nullptr);
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
        if (layout) {
            *layout = resp.layout();
        }
        return true;
    }

private:
    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class DataNodeClient {
public:
    bool WriteReplica(const zb::rpc::ReplicaLocation& replica,
                      uint64_t offset,
                      const std::string& data,
                      std::string* err) {
        std::vector<std::string> addresses;
        if (!replica.primary_address().empty()) {
            addresses.push_back(replica.primary_address());
        }
        if (!replica.node_address().empty() &&
            std::find(addresses.begin(), addresses.end(), replica.node_address()) == addresses.end()) {
            addresses.push_back(replica.node_address());
        }
        if (!replica.secondary_address().empty() &&
            std::find(addresses.begin(), addresses.end(), replica.secondary_address()) == addresses.end()) {
            addresses.push_back(replica.secondary_address());
        }

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
            zb::rpc::WriteChunkRequest req;
            req.set_disk_id(replica.disk_id());
            req.set_chunk_id(replica.chunk_id());
            req.set_offset(offset);
            req.set_data(data);
            req.set_epoch(replica.epoch());
            zb::rpc::WriteChunkReply resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.WriteChunk(&cntl, &req, &resp, nullptr);
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

bool WriteByLayout(DataNodeClient* data_client,
                   const zb::rpc::FileLayout& layout,
                   uint64_t file_offset,
                   const std::string& payload,
                   std::string* err) {
    if (!data_client) {
        if (err) {
            *err = "data client is null";
        }
        return false;
    }

    const uint64_t chunk_size = layout.chunk_size();
    const uint64_t write_size = static_cast<uint64_t>(payload.size());
    for (const auto& chunk : layout.chunks()) {
        const uint64_t chunk_start = static_cast<uint64_t>(chunk.index()) * chunk_size;
        const uint64_t chunk_end = chunk_start + chunk_size;
        const uint64_t write_start = std::max<uint64_t>(chunk_start, file_offset);
        const uint64_t write_end = std::min<uint64_t>(chunk_end, file_offset + write_size);
        if (write_end <= write_start) {
            continue;
        }

        const uint64_t chunk_off = write_start - chunk_start;
        const uint64_t write_len = write_end - write_start;
        const uint64_t payload_off = write_start - file_offset;
        const std::string piece = payload.substr(static_cast<size_t>(payload_off), static_cast<size_t>(write_len));

        for (const auto& replica : chunk.replicas()) {
            if (!data_client->WriteReplica(replica, chunk_off, piece, err)) {
                return false;
            }
        }
    }
    return true;
}

LayoutStats CollectLayoutStats(MdsClient* mds, const std::vector<FileState>& files) {
    LayoutStats stats;
    if (!mds || files.empty()) {
        return stats;
    }

    const size_t limit = std::min<size_t>(files.size(), FLAGS_sample_files == 0 ? files.size() : FLAGS_sample_files);
    for (size_t i = 0; i < limit; ++i) {
        const FileState& f = files[i];
        if (f.inode_id == 0 || f.size == 0) {
            continue;
        }
        zb::rpc::FileLayout layout;
        std::string err;
        if (!mds->GetLayout(f.inode_id, 0, f.size, &layout, &err)) {
            continue;
        }

        ++stats.files_scanned;
        for (const auto& chunk : layout.chunks()) {
            bool has_optical = false;
            bool has_disk = false;
            for (const auto& replica : chunk.replicas()) {
                if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL) {
                    has_optical = true;
                } else {
                    has_disk = true;
                }
            }
            ++stats.chunks_total;
            if (has_optical) {
                ++stats.chunks_with_optical;
            }
            if (has_disk) {
                ++stats.chunks_with_disk;
            }
            if (has_optical && !has_disk) {
                ++stats.chunks_optical_only;
            }
        }
    }
    return stats;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_file_count == 0 || FLAGS_write_size == 0 || FLAGS_chunk_size == 0 || FLAGS_duration_sec == 0) {
        std::cerr << "invalid flags: file_count/write_size/chunk_size/duration_sec must be > 0" << std::endl;
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
        if (!mds.Create(state.path, FLAGS_replica, FLAGS_chunk_size, &state.inode_id, &err)) {
            std::cerr << "Create failed: " << state.path << " err=" << err << std::endl;
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
    bool observed_optical = false;
    bool observed_optical_only = false;

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

        const std::string payload = BuildWritePayload(static_cast<size_t>(FLAGS_write_size), seq++);
        const uint64_t offset = f.size;

        zb::rpc::FileLayout layout;
        if (!mds.AllocateWrite(f.inode_id, offset, FLAGS_write_size, &layout, &err)) {
            ++failures;
            std::cerr << "AllocateWrite failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }

        if (!WriteByLayout(&data_client, layout, offset, payload, &err)) {
            ++failures;
            std::cerr << "chunk write failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }

        const uint64_t new_size = offset + FLAGS_write_size;
        if (!mds.CommitWrite(f.inode_id, new_size, &err)) {
            ++failures;
            std::cerr << "CommitWrite failed inode=" << f.inode_id << " err=" << err << std::endl;
            continue;
        }

        f.size = new_size;
        ++f.writes;
        ++write_ops;
        total_bytes += FLAGS_write_size;

        if (now >= next_report) {
            LayoutStats stats = CollectLayoutStats(&mds, files);
            observed_optical = observed_optical || (stats.chunks_with_optical > 0);
            observed_optical_only = observed_optical_only || (stats.chunks_optical_only > 0);
            std::cout << "[report] elapsed_sec=" << elapsed_sec
                      << " total_bytes=" << total_bytes
                      << " write_ops=" << write_ops
                      << " failures=" << failures
                      << " sampled_files=" << stats.files_scanned
                      << " sampled_chunks=" << stats.chunks_total
                      << " chunks_with_optical=" << stats.chunks_with_optical
                      << " chunks_optical_only=" << stats.chunks_optical_only
                      << std::endl;
            next_report = now + std::chrono::seconds(std::max<uint32_t>(1, FLAGS_report_interval_sec));
        }
    }

    if (FLAGS_cooldown_sec > 0) {
        std::cout << "cooldown " << FLAGS_cooldown_sec << "s for archive/evict..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_cooldown_sec));
        LayoutStats stats = CollectLayoutStats(&mds, files);
        observed_optical = observed_optical || (stats.chunks_with_optical > 0);
        observed_optical_only = observed_optical_only || (stats.chunks_optical_only > 0);
        std::cout << "[cooldown-check] sampled_files=" << stats.files_scanned
                  << " sampled_chunks=" << stats.chunks_total
                  << " chunks_with_optical=" << stats.chunks_with_optical
                  << " chunks_optical_only=" << stats.chunks_optical_only
                  << std::endl;
    }

    std::cout << "Finished: total_bytes=" << total_bytes
              << " write_ops=" << write_ops
              << " failures=" << failures
              << " observed_optical=" << (observed_optical ? "true" : "false")
              << " observed_optical_only=" << (observed_optical_only ? "true" : "false")
              << " base_dir=" << base_dir
              << std::endl;

    if (FLAGS_require_optical && !observed_optical) {
        std::cerr << "FAILED: no optical replica observed in sampled layout." << std::endl;
        return 2;
    }
    if (FLAGS_require_optical_only_after_cooldown && !observed_optical_only) {
        std::cerr << "FAILED: no optical-only chunk observed after cooldown." << std::endl;
        return 3;
    }
    if (failures > 0) {
        std::cerr << "FAILED: write failures encountered=" << failures << std::endl;
        return 4;
    }
    return 0;
}
