#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mds.pb.h"
#include "real_node.pb.h"

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_uint32(default_replica, 1, "Default replica for create");
DEFINE_uint64(default_chunk_size, 4194304, "Default chunk size for create");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");
DEFINE_int32(repair_max_retry, 8, "Max retry count for asynchronous replica repair");
DEFINE_int32(repair_retry_interval_ms, 500, "Base retry interval(ms) for asynchronous replica repair");
DEFINE_int32(repair_max_queue, 10000, "Max in-memory queue size for asynchronous replica repair");

namespace zb::client::fuse_client {

namespace {

int StatusToErrno(const zb::rpc::MdsStatus& status) {
    switch (status.code()) {
        case zb::rpc::MDS_OK:
            return 0;
        case zb::rpc::MDS_INVALID_ARGUMENT:
            return EINVAL;
        case zb::rpc::MDS_NOT_FOUND:
            return ENOENT;
        case zb::rpc::MDS_ALREADY_EXISTS:
            return EEXIST;
        case zb::rpc::MDS_NOT_EMPTY:
            return ENOTEMPTY;
        case zb::rpc::MDS_STALE_EPOCH:
            return EAGAIN;
        default:
            return EIO;
    }
}

void FillStat(const zb::rpc::InodeAttr& attr, struct stat* st) {
    std::memset(st, 0, sizeof(*st));
    st->st_ino = static_cast<ino_t>(attr.inode_id());
    st->st_mode = static_cast<mode_t>(attr.mode());
    if (attr.type() == zb::rpc::INODE_DIR) {
        st->st_mode |= S_IFDIR;
    } else {
        st->st_mode |= S_IFREG;
    }
    st->st_uid = static_cast<uid_t>(attr.uid());
    st->st_gid = static_cast<gid_t>(attr.gid());
    st->st_nlink = attr.nlink() ? static_cast<nlink_t>(attr.nlink()) : 1;
    st->st_size = static_cast<off_t>(attr.size());
    st->st_atim.tv_sec = static_cast<time_t>(attr.atime());
    st->st_mtim.tv_sec = static_cast<time_t>(attr.mtime());
    st->st_ctim.tv_sec = static_cast<time_t>(attr.ctime());
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

    bool Lookup(const char* path, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::LookupRequest request;
        request.set_path(path);
        zb::rpc::LookupReply reply;
        brpc::Controller cntl;
        stub_.Lookup(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Getattr(uint64_t inode_id, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::GetattrRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetattrReply reply;
        brpc::Controller cntl;
        stub_.Getattr(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Open(const char* path, uint32_t flags, uint64_t* handle, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::OpenRequest request;
        request.set_path(path);
        request.set_flags(flags);
        zb::rpc::OpenReply reply;
        brpc::Controller cntl;
        stub_.Open(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (handle) {
            *handle = reply.handle_id();
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Close(uint64_t handle_id, zb::rpc::MdsStatus* status) {
        zb::rpc::CloseRequest request;
        request.set_handle_id(handle_id);
        zb::rpc::CloseReply reply;
        brpc::Controller cntl;
        stub_.Close(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Create(const char* path, uint32_t mode, uint32_t uid, uint32_t gid,
                zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::CreateRequest request;
        request.set_path(path);
        request.set_mode(mode);
        request.set_uid(uid);
        request.set_gid(gid);
        request.set_replica(FLAGS_default_replica);
        request.set_chunk_size(FLAGS_default_chunk_size);
        zb::rpc::CreateReply reply;
        brpc::Controller cntl;
        stub_.Create(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Mkdir(const char* path, uint32_t mode, uint32_t uid, uint32_t gid, zb::rpc::InodeAttr* attr,
               zb::rpc::MdsStatus* status) {
        zb::rpc::MkdirRequest request;
        request.set_path(path);
        request.set_mode(mode);
        request.set_uid(uid);
        request.set_gid(gid);
        zb::rpc::MkdirReply reply;
        brpc::Controller cntl;
        stub_.Mkdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Readdir(const char* path, std::vector<zb::rpc::Dentry>* entries, zb::rpc::MdsStatus* status) {
        zb::rpc::ReaddirRequest request;
        request.set_path(path);
        zb::rpc::ReaddirReply reply;
        brpc::Controller cntl;
        stub_.Readdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (entries) {
            entries->assign(reply.entries().begin(), reply.entries().end());
        }
        return true;
    }

    bool Rename(const char* old_path, const char* new_path, zb::rpc::MdsStatus* status) {
        zb::rpc::RenameRequest request;
        request.set_old_path(old_path);
        request.set_new_path(new_path);
        zb::rpc::RenameReply reply;
        brpc::Controller cntl;
        stub_.Rename(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Unlink(const char* path, zb::rpc::MdsStatus* status) {
        zb::rpc::UnlinkRequest request;
        request.set_path(path);
        zb::rpc::UnlinkReply reply;
        brpc::Controller cntl;
        stub_.Unlink(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Rmdir(const char* path, zb::rpc::MdsStatus* status) {
        zb::rpc::RmdirRequest request;
        request.set_path(path);
        zb::rpc::RmdirReply reply;
        brpc::Controller cntl;
        stub_.Rmdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool AllocateWrite(uint64_t inode_id, uint64_t offset, uint64_t size, zb::rpc::FileLayout* layout,
                       zb::rpc::MdsStatus* status) {
        zb::rpc::AllocateWriteRequest request;
        request.set_inode_id(inode_id);
        request.set_offset(offset);
        request.set_size(size);
        zb::rpc::AllocateWriteReply reply;
        brpc::Controller cntl;
        stub_.AllocateWrite(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (layout) {
            *layout = reply.layout();
        }
        return true;
    }

    bool GetLayout(uint64_t inode_id,
                   uint64_t offset,
                   uint64_t size,
                   zb::rpc::FileLayout* layout,
                   zb::rpc::OpticalReadPlan* optical_plan,
                   zb::rpc::MdsStatus* status) {
        zb::rpc::GetLayoutRequest request;
        request.set_inode_id(inode_id);
        request.set_offset(offset);
        request.set_size(size);
        zb::rpc::GetLayoutReply reply;
        brpc::Controller cntl;
        stub_.GetLayout(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (layout) {
            *layout = reply.layout();
        }
        if (optical_plan) {
            *optical_plan = reply.optical_plan();
        }
        return true;
    }

    bool GetLayoutRoot(uint64_t inode_id,
                       zb::rpc::LayoutRoot* root,
                       bool* from_legacy,
                       zb::rpc::MdsStatus* status) {
        zb::rpc::GetLayoutRootRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetLayoutRootReply reply;
        brpc::Controller cntl;
        stub_.GetLayoutRoot(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (root) {
            *root = reply.root();
        }
        if (from_legacy) {
            *from_legacy = reply.from_legacy();
        }
        if (reply.root().inode_id() != 0) {
            UpdateCachedEpoch(reply.root().inode_id(), reply.root().epoch());
        }
        return true;
    }

    bool ResolveLayout(uint64_t inode_id,
                       uint64_t offset,
                       uint64_t size,
                       zb::rpc::ResolveLayoutReply* out,
                       zb::rpc::MdsStatus* status) {
        if (!out) {
            if (status) {
                status->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
                status->set_message("ResolveLayout output is null");
            }
            return false;
        }
        zb::rpc::ResolveLayoutRequest request;
        request.set_inode_id(inode_id);
        request.set_offset(offset);
        request.set_size(size);
        if (uint64_t cached_epoch = GetCachedEpoch(inode_id); cached_epoch > 0) {
            (void)cached_epoch;
        }

        zb::rpc::ResolveLayoutReply reply;
        brpc::Controller cntl;
        stub_.ResolveLayout(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (reply.status().code() == zb::rpc::MDS_OK) {
            *out = reply;
            if (status) {
                *status = reply.status();
            }
            UpdateCachedEpoch(inode_id, reply.root().epoch());
            return true;
        }

        if (IsStaleEpochStatus(reply.status())) {
            InvalidateCachedEpoch(inode_id);
            zb::rpc::LayoutRoot refreshed_root;
            zb::rpc::MdsStatus refresh_status;
            bool from_legacy = false;
            if (GetLayoutRoot(inode_id, &refreshed_root, &from_legacy, &refresh_status)) {
                zb::rpc::ResolveLayoutReply retry_reply;
                brpc::Controller retry_cntl;
                stub_.ResolveLayout(&retry_cntl, &request, &retry_reply, nullptr);
                if (!retry_cntl.Failed()) {
                    if (status) {
                        *status = retry_reply.status();
                    }
                    if (retry_reply.status().code() == zb::rpc::MDS_OK) {
                        *out = retry_reply;
                        UpdateCachedEpoch(inode_id, retry_reply.root().epoch());
                        return true;
                    }
                } else {
                    SetRpcFailureStatus(retry_cntl, status);
                    return false;
                }
            }
        }

        if (status) {
            *status = reply.status();
        }
        return false;
    }

    bool CommitLayoutRoot(uint64_t inode_id,
                          uint64_t expected_layout_version,
                          uint64_t new_size,
                          const zb::rpc::LayoutRoot* base_root,
                          zb::rpc::LayoutRoot* committed_root,
                          zb::rpc::MdsStatus* status) {
        zb::rpc::CommitLayoutRootRequest request;
        request.set_inode_id(inode_id);
        request.set_expected_layout_version(expected_layout_version);
        request.set_update_inode_size(true);
        request.set_new_size(new_size);

        zb::rpc::LayoutRoot* root = request.mutable_root();
        root->set_inode_id(inode_id);
        root->set_layout_version(expected_layout_version > 0 ? expected_layout_version + 1 : 1);
        root->set_file_size(new_size);
        root->set_update_ts(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count()));
        if (base_root) {
            if (!base_root->layout_root_id().empty()) {
                root->set_layout_root_id(base_root->layout_root_id());
            }
            root->set_epoch(base_root->epoch());
        } else {
            root->set_epoch(GetCachedEpoch(inode_id));
        }

        zb::rpc::CommitLayoutRootReply reply;
        brpc::Controller cntl;
        stub_.CommitLayoutRoot(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            if (IsStaleEpochStatus(reply.status())) {
                InvalidateCachedEpoch(inode_id);
            }
            return false;
        }
        if (committed_root) {
            *committed_root = reply.root();
        }
        UpdateCachedEpoch(inode_id, reply.root().epoch());
        return true;
    }

    bool CommitWrite(uint64_t inode_id, uint64_t new_size, zb::rpc::MdsStatus* status) {
        zb::rpc::CommitWriteRequest request;
        request.set_inode_id(inode_id);
        request.set_new_size(new_size);
        zb::rpc::CommitWriteReply reply;
        brpc::Controller cntl;
        stub_.CommitWrite(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

private:
    static void SetRpcFailureStatus(const brpc::Controller& cntl, zb::rpc::MdsStatus* status) {
        if (!status) {
            return;
        }
        status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status->set_message(cntl.ErrorText());
    }

    static bool IsStaleEpochStatus(const zb::rpc::MdsStatus& status) {
        if (status.code() == zb::rpc::MDS_STALE_EPOCH) {
            return true;
        }
        std::string lower = status.message();
        std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return lower.find("stale epoch") != std::string::npos ||
               lower.find("layout version mismatch") != std::string::npos;
    }

    void UpdateCachedEpoch(uint64_t inode_id, uint64_t epoch) {
        if (inode_id == 0 || epoch == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(epoch_mu_);
        inode_epoch_cache_[inode_id] = epoch;
    }

    void InvalidateCachedEpoch(uint64_t inode_id) {
        if (inode_id == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(epoch_mu_);
        inode_epoch_cache_.erase(inode_id);
    }

    uint64_t GetCachedEpoch(uint64_t inode_id) const {
        if (inode_id == 0) {
            return 0;
        }
        std::lock_guard<std::mutex> lock(epoch_mu_);
        auto it = inode_epoch_cache_.find(inode_id);
        return it == inode_epoch_cache_.end() ? 0 : it->second;
    }

    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
    mutable std::mutex epoch_mu_;
    std::unordered_map<uint64_t, uint64_t> inode_epoch_cache_;
};

class DataNodeClient {
public:
    bool Write(const zb::rpc::ReplicaLocation& replica, uint64_t offset, const std::string& data, std::string* error) {
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
            if (error) {
                *error = "No data node address in replica";
            }
            return false;
        }

        std::string last_error = "Unknown write error";
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_error = "Failed to init channel: " + address;
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::WriteChunkRequest request;
            request.set_disk_id(replica.disk_id());
            request.set_chunk_id(replica.chunk_id());
            request.set_offset(offset);
            request.set_data(data);
            request.set_epoch(replica.epoch());
            zb::rpc::WriteChunkReply reply;
            brpc::Controller cntl;
            stub.WriteChunk(&cntl, &request, &reply, nullptr);
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                return true;
            }
            last_error = reply.status().message();
        }
        if (error) {
            *error = last_error;
        }
        return false;
    }

    bool Read(const zb::rpc::ReplicaLocation& replica, uint64_t offset, uint64_t size, std::string* out,
              std::string* error) {
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
            if (error) {
                *error = "No data node address in replica";
            }
            return false;
        }

        std::string last_error = "Unknown read error";
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_error = "Failed to init channel: " + address;
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::ReadChunkRequest request;
            request.set_disk_id(replica.disk_id());
            request.set_chunk_id(replica.chunk_id());
            request.set_offset(offset);
            request.set_size(size);
            zb::rpc::ReadChunkReply reply;
            brpc::Controller cntl;
            stub.ReadChunk(&cntl, &request, &reply, nullptr);
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                if (out) {
                    *out = reply.data();
                }
                return true;
            }
            last_error = reply.status().message();
        }
        if (error) {
            *error = last_error;
        }
        return false;
    }

    bool ReadArchivedFile(const zb::rpc::OpticalReadPlan& plan,
                          uint64_t offset,
                          uint64_t size,
                          std::string* out,
                          std::string* error) {
        if (!plan.enabled() || plan.node_address().empty() || plan.disc_id().empty()) {
            if (error) {
                *error = "invalid optical read plan";
            }
            return false;
        }
        brpc::Channel* channel = GetChannel(plan.node_address());
        if (!channel) {
            if (error) {
                *error = "Failed to init channel: " + plan.node_address();
            }
            return false;
        }

        zb::rpc::RealNodeService_Stub stub(channel);
        zb::rpc::ReadArchivedFileRequest request;
        request.set_disc_id(plan.disc_id());
        request.set_inode_id(plan.inode_id());
        request.set_file_id(plan.file_id());
        request.set_offset(offset);
        request.set_size(size);

        zb::rpc::ReadArchivedFileReply reply;
        brpc::Controller cntl;
        stub.ReadArchivedFile(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (error) {
                *error = cntl.ErrorText();
            }
            return false;
        }
        if (reply.status().code() != zb::rpc::STATUS_OK) {
            if (error) {
                *error = reply.status().message();
            }
            return false;
        }
        if (out) {
            *out = reply.data();
        }
        return true;
    }

private:
    brpc::Channel* GetChannel(const std::string& address) {
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
            return nullptr;
        }
        brpc::Channel* ptr = channel.get();
        channels_[address] = std::move(channel);
        return ptr;
    }

    std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

struct ReplicaRepairTask {
    std::string key;
    zb::rpc::ReplicaLocation replica;
    uint64_t offset{0};
    std::string data;
    uint32_t attempts{0};
    uint64_t generation{0};
};

struct FuseState {
    struct LayoutCacheEntry {
        uint64_t inode_id{0};
        uint64_t layout_version{0};
        uint64_t file_size{0};
        zb::rpc::FileLayout layout;
    };

    MdsClient mds;
    DataNodeClient data_nodes;
    std::mutex mu;
    std::unordered_map<uint64_t, uint64_t> handle_to_inode;
    std::unordered_map<uint64_t, LayoutCacheEntry> inode_layout_cache;
    std::mutex repair_mu;
    std::condition_variable repair_cv;
    std::deque<ReplicaRepairTask> repair_queue;
    std::unordered_map<std::string, uint64_t> repair_generation;
    std::atomic<bool> stop_repair{false};
    std::thread repair_thread;
};

FuseState* GetState() {
    return static_cast<FuseState*>(fuse_get_context()->private_data);
}

bool IsLegacyFallbackStatus(const zb::rpc::MdsStatus& status) {
    if (status.code() != zb::rpc::MDS_INTERNAL_ERROR) {
        return false;
    }
    std::string lower = status.message();
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return lower.find("method") != std::string::npos &&
           lower.find("not") != std::string::npos &&
           lower.find("found") != std::string::npos;
}

void InvalidateLayoutCache(FuseState* state, uint64_t inode_id) {
    if (!state || inode_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    state->inode_layout_cache.erase(inode_id);
}

void UpdateLayoutCache(FuseState* state,
                       uint64_t inode_id,
                       const zb::rpc::LayoutRoot& root,
                       const zb::rpc::FileLayout& layout) {
    if (!state || inode_id == 0) {
        return;
    }
    FuseState::LayoutCacheEntry entry;
    entry.inode_id = inode_id;
    entry.layout_version = root.layout_version();
    entry.file_size = root.file_size();
    entry.layout = layout;
    std::lock_guard<std::mutex> lock(state->mu);
    state->inode_layout_cache[inode_id] = std::move(entry);
}

bool TryGetLayoutCache(FuseState* state,
                       uint64_t inode_id,
                       uint64_t offset,
                       uint64_t requested_size,
                       zb::rpc::FileLayout* out_layout,
                       uint64_t* file_size) {
    if (!state || inode_id == 0 || !out_layout || !file_size) {
        return false;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto it = state->inode_layout_cache.find(inode_id);
    if (it == state->inode_layout_cache.end()) {
        return false;
    }
    const auto& entry = it->second;
    if (entry.file_size == 0 || offset >= entry.file_size) {
        *file_size = entry.file_size;
        *out_layout = entry.layout;
        return true;
    }
    uint64_t chunk_size = entry.layout.chunk_size();
    if (chunk_size == 0) {
        return false;
    }
    uint64_t end = std::min(entry.file_size, offset + requested_size);
    if (end <= offset) {
        *file_size = entry.file_size;
        *out_layout = entry.layout;
        return true;
    }
    uint32_t start_idx = static_cast<uint32_t>(offset / chunk_size);
    uint32_t end_idx = static_cast<uint32_t>((end - 1) / chunk_size);
    for (uint32_t idx = start_idx; idx <= end_idx; ++idx) {
        bool found = false;
        for (const auto& chunk : entry.layout.chunks()) {
            if (chunk.index() == idx) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    *file_size = entry.file_size;
    *out_layout = entry.layout;
    return true;
}

std::string BuildRepairKey(const zb::rpc::ReplicaLocation& replica, uint64_t offset, size_t size) {
    return replica.disk_id() + "|" + replica.chunk_id() + "|" + std::to_string(offset) + "|" + std::to_string(size);
}

uint64_t BumpRepairGeneration(FuseState* state, const std::string& key) {
    if (!state || key.empty()) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(state->repair_mu);
    uint64_t& generation = state->repair_generation[key];
    generation += 1;
    return generation;
}

void EnqueueReplicaRepair(FuseState* state,
                          const zb::rpc::ReplicaLocation& replica,
                          uint64_t offset,
                          const std::string& data) {
    if (!state) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->repair_mu);
    const std::string key = BuildRepairKey(replica, offset, data.size());
    uint64_t generation = 1;
    auto it = state->repair_generation.find(key);
    if (it != state->repair_generation.end()) {
        generation = it->second;
    } else {
        state->repair_generation[key] = generation;
    }
    for (auto qit = state->repair_queue.begin(); qit != state->repair_queue.end();) {
        if (qit->key == key) {
            qit = state->repair_queue.erase(qit);
        } else {
            ++qit;
        }
    }
    const size_t max_queue = static_cast<size_t>(std::max(1, FLAGS_repair_max_queue));
    if (state->repair_queue.size() >= max_queue) {
        const ReplicaRepairTask dropped = std::move(state->repair_queue.front());
        state->repair_queue.pop_front();
        auto dropped_it = state->repair_generation.find(dropped.key);
        if (dropped_it != state->repair_generation.end() && dropped_it->second == dropped.generation) {
            state->repair_generation.erase(dropped_it);
        }
    }
    ReplicaRepairTask task;
    task.key = key;
    task.replica = replica;
    task.offset = offset;
    task.data = data;
    task.generation = generation;
    state->repair_queue.push_back(std::move(task));
    state->repair_cv.notify_one();
}

void RepairWorkerLoop(FuseState* state) {
    if (!state) {
        return;
    }
    const uint32_t max_retry = static_cast<uint32_t>(std::max(1, FLAGS_repair_max_retry));
    const int retry_interval_ms = std::max(1, FLAGS_repair_retry_interval_ms);
    while (true) {
        ReplicaRepairTask task;
        {
            std::unique_lock<std::mutex> lock(state->repair_mu);
            state->repair_cv.wait(lock, [&]() {
                return state->stop_repair.load() || !state->repair_queue.empty();
            });
            if (state->stop_repair.load() && state->repair_queue.empty()) {
                break;
            }
            task = std::move(state->repair_queue.front());
            state->repair_queue.pop_front();
            auto it = state->repair_generation.find(task.key);
            if (it != state->repair_generation.end() && it->second != task.generation) {
                continue;
            }
        }

        std::string error;
        if (state->data_nodes.Write(task.replica, task.offset, task.data, &error)) {
            std::lock_guard<std::mutex> lock(state->repair_mu);
            auto it = state->repair_generation.find(task.key);
            if (it != state->repair_generation.end() && it->second == task.generation) {
                state->repair_generation.erase(it);
            }
            continue;
        }
        if (task.attempts + 1 >= max_retry) {
            std::lock_guard<std::mutex> lock(state->repair_mu);
            auto it = state->repair_generation.find(task.key);
            if (it != state->repair_generation.end() && it->second == task.generation) {
                state->repair_generation.erase(it);
            }
            continue;
        }

        ++task.attempts;
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_ms * task.attempts));
        {
            std::lock_guard<std::mutex> lock(state->repair_mu);
            if (!state->stop_repair.load()) {
                auto it = state->repair_generation.find(task.key);
                if (it != state->repair_generation.end() && it->second != task.generation) {
                    continue;
                }
                const size_t max_queue = static_cast<size_t>(std::max(1, FLAGS_repair_max_queue));
                if (state->repair_queue.size() >= max_queue) {
                    const ReplicaRepairTask dropped = std::move(state->repair_queue.front());
                    state->repair_queue.pop_front();
                    auto dropped_it = state->repair_generation.find(dropped.key);
                    if (dropped_it != state->repair_generation.end() && dropped_it->second == dropped.generation) {
                        state->repair_generation.erase(dropped_it);
                    }
                }
                state->repair_queue.push_back(std::move(task));
                state->repair_cv.notify_one();
            }
        }
    }
}

int FuseGetattr(const char* path, struct stat* st, struct fuse_file_info* fi) {
    (void)fi;
    auto* state = GetState();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (!state->mds.Lookup(path, &attr, &status)) {
        return -StatusToErrno(status);
    }
    FillStat(attr, st);
    return 0;
}

int FuseReaddir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset,
                struct fuse_file_info* fi, enum fuse_readdir_flags flags) {
    (void)offset;
    (void)fi;
    (void)flags;
    auto* state = GetState();
    std::vector<zb::rpc::Dentry> entries;
    zb::rpc::MdsStatus status;
    if (!state->mds.Readdir(path, &entries, &status)) {
        return -StatusToErrno(status);
    }
    filler(buf, ".", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", nullptr, 0, FUSE_FILL_DIR_PLUS);
    for (const auto& entry : entries) {
        struct stat st;
        std::memset(&st, 0, sizeof(st));
        st.st_ino = static_cast<ino_t>(entry.inode_id());
        st.st_mode = (entry.type() == zb::rpc::INODE_DIR) ? S_IFDIR : S_IFREG;
        filler(buf, entry.name().c_str(), &st, 0, FUSE_FILL_DIR_PLUS);
    }
    return 0;
}

int FuseOpen(const char* path, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    uint64_t handle_id = 0;
    if (!state->mds.Open(path, static_cast<uint32_t>(fi->flags), &handle_id, &attr, &status)) {
        return -StatusToErrno(status);
    }
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode[handle_id] = attr.inode_id();
    }
    fi->fh = handle_id;
    return 0;
}

int FuseRelease(const char* path, struct fuse_file_info* fi) {
    (void)path;
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t handle_id = fi->fh;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode.erase(handle_id);
    }
    if (!state->mds.Close(handle_id, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseCreate(const char* path, mode_t mode, struct fuse_file_info* fi) {
    auto* state = GetState();
    struct fuse_context* ctx = fuse_get_context();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (!state->mds.Create(path, static_cast<uint32_t>(mode), ctx->uid, ctx->gid, &attr, &status)) {
        return -StatusToErrno(status);
    }
    uint64_t handle_id = 0;
    if (!state->mds.Open(path, static_cast<uint32_t>(fi->flags), &handle_id, &attr, &status)) {
        return -StatusToErrno(status);
    }
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode[handle_id] = attr.inode_id();
    }
    fi->fh = handle_id;
    return 0;
}

int FuseMkdir(const char* path, mode_t mode) {
    auto* state = GetState();
    struct fuse_context* ctx = fuse_get_context();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (!state->mds.Mkdir(path, static_cast<uint32_t>(mode), ctx->uid, ctx->gid, &attr, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseUnlink(const char* path) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    if (!state->mds.Unlink(path, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseRmdir(const char* path) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    if (!state->mds.Rmdir(path, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseRename(const char* from, const char* to, unsigned int flags) {
    (void)flags;
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    if (!state->mds.Rename(from, to, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

uint64_t ResolveInode(struct fuse_file_info* fi, const char* path, FuseState* state, zb::rpc::MdsStatus* status) {
    if (fi && fi->fh != 0) {
        std::lock_guard<std::mutex> lock(state->mu);
        auto it = state->handle_to_inode.find(fi->fh);
        if (it != state->handle_to_inode.end()) {
            return it->second;
        }
    }
    zb::rpc::InodeAttr attr;
    if (!state->mds.Lookup(path, &attr, status)) {
        return 0;
    }
    return attr.inode_id();
}

int FuseRead(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status);
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }

    const uint64_t request_offset = static_cast<uint64_t>(offset);
    zb::rpc::FileLayout layout;
    zb::rpc::OpticalReadPlan optical_plan;
    uint64_t file_size = 0;
    if (!TryGetLayoutCache(state, inode_id, request_offset, static_cast<uint64_t>(size), &layout, &file_size)) {
        zb::rpc::InodeAttr attr;
        if (!state->mds.Getattr(inode_id, &attr, &status)) {
            return -StatusToErrno(status);
        }
        file_size = attr.size();
        if (request_offset >= file_size) {
            return 0;
        }

        zb::rpc::ResolveLayoutReply resolved;
        if (state->mds.ResolveLayout(inode_id, 0, file_size, &resolved, &status)) {
            layout = resolved.read_plan();
            optical_plan = resolved.optical_plan();
            UpdateLayoutCache(state, inode_id, resolved.root(), layout);
        } else {
            if (!state->mds.GetLayout(inode_id,
                                      request_offset,
                                      static_cast<uint64_t>(size),
                                      &layout,
                                      &optical_plan,
                                      &status)) {
                return -StatusToErrno(status);
            }
            if (layout.chunk_size() > 0) {
                zb::rpc::LayoutRoot pseudo_root;
                pseudo_root.set_inode_id(inode_id);
                pseudo_root.set_layout_version(attr.version());
                pseudo_root.set_file_size(file_size);
                UpdateLayoutCache(state, inode_id, pseudo_root, layout);
            }
        }
    }

    if (request_offset >= file_size) {
        return 0;
    }
    uint64_t read_size = static_cast<uint64_t>(size);
    if (read_size > file_size - request_offset) {
        read_size = file_size - request_offset;
    }
    if (optical_plan.enabled()) {
        std::string data;
        std::string error;
        if (!state->data_nodes.ReadArchivedFile(optical_plan, request_offset, read_size, &data, &error)) {
            return -EIO;
        }
        if (data.size() > read_size) {
            data.resize(static_cast<size_t>(read_size));
        }
        std::memcpy(buf, data.data(), data.size());
        return static_cast<int>(data.size());
    }

    std::string output(read_size, '\0');
    uint64_t chunk_size = layout.chunk_size();
    if (chunk_size == 0) {
        return -EIO;
    }
    for (const auto& chunk : layout.chunks()) {
        uint64_t chunk_start = static_cast<uint64_t>(chunk.index()) * chunk_size;
        uint64_t chunk_end = chunk_start + chunk_size;
        uint64_t read_start = std::max<uint64_t>(chunk_start, request_offset);
        uint64_t read_end = std::min<uint64_t>(chunk_end, request_offset + read_size);
        if (read_end <= read_start) {
            continue;
        }
        uint64_t chunk_off = read_start - chunk_start;
        uint64_t read_len = read_end - read_start;

        bool read_ok = false;
        std::string data;
        std::string error;
        for (const auto& replica : chunk.replicas()) {
            if (replica.storage_tier() != zb::rpc::STORAGE_TIER_DISK ||
                replica.replica_state() != zb::rpc::REPLICA_READY) {
                continue;
            }
            if (state->data_nodes.Read(replica, chunk_off, read_len, &data, &error)) {
                read_ok = true;
                break;
            }
        }
        if (!read_ok) {
            return -EIO;
        }
        if (data.size() > read_len) {
            data.resize(read_len);
        }
        std::memcpy(output.data() + (read_start - request_offset), data.data(), data.size());
    }

    std::memcpy(buf, output.data(), output.size());
    return static_cast<int>(output.size());
}

int FuseWrite(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status);
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }

    zb::rpc::LayoutRoot base_root;
    bool from_legacy_layout = false;
    if (!state->mds.GetLayoutRoot(inode_id, &base_root, &from_legacy_layout, &status)) {
        base_root.Clear();
        base_root.set_inode_id(inode_id);
        from_legacy_layout = true;
    }
    (void)from_legacy_layout;

    zb::rpc::FileLayout layout;
    if (!state->mds.AllocateWrite(inode_id, static_cast<uint64_t>(offset), static_cast<uint64_t>(size), &layout, &status)) {
        return -StatusToErrno(status);
    }

    uint64_t chunk_size = layout.chunk_size();
    size_t remaining = size;

    for (const auto& chunk : layout.chunks()) {
        uint64_t chunk_start = static_cast<uint64_t>(chunk.index()) * chunk_size;
        uint64_t chunk_end = chunk_start + chunk_size;
        uint64_t write_start = std::max<uint64_t>(chunk_start, static_cast<uint64_t>(offset));
        uint64_t write_end = std::min<uint64_t>(chunk_end, static_cast<uint64_t>(offset + size));
        if (write_end <= write_start) {
            continue;
        }
        uint64_t chunk_off = write_start - chunk_start;
        uint64_t write_len = write_end - write_start;
        std::string data(buf + (write_start - offset), buf + (write_start - offset + write_len));

        bool wrote_disk_replica = false;
        std::vector<zb::rpc::ReplicaLocation> failed_replicas;
        for (const auto& replica : chunk.replicas()) {
            if (replica.storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
                continue;
            }
            const std::string repair_key = BuildRepairKey(replica, chunk_off, data.size());
            (void)BumpRepairGeneration(state, repair_key);
            std::string error;
            if (!state->data_nodes.Write(replica, chunk_off, data, &error)) {
                failed_replicas.push_back(replica);
                continue;
            }
            wrote_disk_replica = true;
        }
        if (!wrote_disk_replica) {
            return -EIO;
        }
        if (!failed_replicas.empty()) {
            for (const auto& failed_replica : failed_replicas) {
                EnqueueReplicaRepair(state, failed_replica, chunk_off, data);
            }
            return -EIO;
        }

        remaining -= write_len;
    }

    uint64_t new_size = static_cast<uint64_t>(offset + size);
    zb::rpc::LayoutRoot committed_root;
    const uint64_t expected_layout_version = base_root.layout_version();
    if (!state->mds.CommitLayoutRoot(inode_id,
                                     expected_layout_version,
                                     new_size,
                                     &base_root,
                                     &committed_root,
                                     &status)) {
        const bool allow_legacy_commit = IsLegacyFallbackStatus(status);
        if (!allow_legacy_commit || !state->mds.CommitWrite(inode_id, new_size, &status)) {
            return -StatusToErrno(status);
        }
    }

    InvalidateLayoutCache(state, inode_id);
    return static_cast<int>(size - remaining);
}

int FuseTruncate(const char* path, off_t size, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status);
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }
    zb::rpc::LayoutRoot base_root;
    bool from_legacy = false;
    if (state->mds.GetLayoutRoot(inode_id, &base_root, &from_legacy, &status)) {
        (void)from_legacy;
        zb::rpc::LayoutRoot committed;
        if (state->mds.CommitLayoutRoot(inode_id,
                                        base_root.layout_version(),
                                        static_cast<uint64_t>(size),
                                        &base_root,
                                        &committed,
                                        &status)) {
            InvalidateLayoutCache(state, inode_id);
            return 0;
        }
    }
    if (!IsLegacyFallbackStatus(status) ||
        !state->mds.CommitWrite(inode_id, static_cast<uint64_t>(size), &status)) {
        return -StatusToErrno(status);
    }
    InvalidateLayoutCache(state, inode_id);
    return 0;
}

} // namespace

} // namespace zb::client::fuse_client

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    zb::client::fuse_client::FuseState state;
    if (!state.mds.Init(FLAGS_mds)) {
        std::fprintf(stderr, "Failed to connect to MDS %s\n", FLAGS_mds.c_str());
        return 1;
    }
    state.repair_thread = std::thread([&state]() {
        zb::client::fuse_client::RepairWorkerLoop(&state);
    });

    static struct fuse_operations ops = {};
    ops.getattr = zb::client::fuse_client::FuseGetattr;
    ops.readdir = zb::client::fuse_client::FuseReaddir;
    ops.open = zb::client::fuse_client::FuseOpen;
    ops.release = zb::client::fuse_client::FuseRelease;
    ops.create = zb::client::fuse_client::FuseCreate;
    ops.mkdir = zb::client::fuse_client::FuseMkdir;
    ops.unlink = zb::client::fuse_client::FuseUnlink;
    ops.rmdir = zb::client::fuse_client::FuseRmdir;
    ops.rename = zb::client::fuse_client::FuseRename;
    ops.read = zb::client::fuse_client::FuseRead;
    ops.write = zb::client::fuse_client::FuseWrite;
    ops.truncate = zb::client::fuse_client::FuseTruncate;

    int ret = fuse_main(argc, argv, &ops, &state);
    state.stop_repair.store(true);
    state.repair_cv.notify_all();
    if (state.repair_thread.joinable()) {
        state.repair_thread.join();
    }
    return ret;
}
