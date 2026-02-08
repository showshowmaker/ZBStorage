#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
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

    bool GetLayout(uint64_t inode_id, uint64_t offset, uint64_t size, zb::rpc::FileLayout* layout,
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
    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class DataNodeClient {
public:
    bool Write(const zb::rpc::ReplicaLocation& replica, uint64_t offset, const std::string& data, std::string* error) {
        brpc::Channel* channel = GetChannel(replica.node_address());
        if (!channel) {
            if (error) {
                *error = "Failed to init channel";
            }
            return false;
        }
        zb::rpc::RealNodeService_Stub stub(channel);
        zb::rpc::WriteChunkRequest request;
        request.set_disk_id(replica.disk_id());
        request.set_chunk_id(replica.chunk_id());
        request.set_offset(offset);
        request.set_data(data);
        zb::rpc::WriteChunkReply reply;
        brpc::Controller cntl;
        stub.WriteChunk(&cntl, &request, &reply, nullptr);
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
        return true;
    }

    bool Read(const zb::rpc::ReplicaLocation& replica, uint64_t offset, uint64_t size, std::string* out,
              std::string* error) {
        brpc::Channel* channel = GetChannel(replica.node_address());
        if (!channel) {
            if (error) {
                *error = "Failed to init channel";
            }
            return false;
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

struct FuseState {
    MdsClient mds;
    DataNodeClient data_nodes;
    std::mutex mu;
    std::unordered_map<uint64_t, uint64_t> handle_to_inode;
};

FuseState* GetState() {
    return static_cast<FuseState*>(fuse_get_context()->private_data);
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

    zb::rpc::InodeAttr attr;
    if (!state->mds.Getattr(inode_id, &attr, &status)) {
        return -StatusToErrno(status);
    }
    if (static_cast<uint64_t>(offset) >= attr.size()) {
        return 0;
    }
    uint64_t read_size = static_cast<uint64_t>(size);
    if (read_size > attr.size() - static_cast<uint64_t>(offset)) {
        read_size = attr.size() - static_cast<uint64_t>(offset);
    }

    zb::rpc::FileLayout layout;
    if (!state->mds.GetLayout(inode_id, static_cast<uint64_t>(offset), read_size, &layout, &status)) {
        return -StatusToErrno(status);
    }

    std::string output(read_size, '\0');
    uint64_t chunk_size = layout.chunk_size();
    for (const auto& chunk : layout.chunks()) {
        uint64_t chunk_start = static_cast<uint64_t>(chunk.index()) * chunk_size;
        uint64_t chunk_end = chunk_start + chunk_size;
        uint64_t read_start = std::max<uint64_t>(chunk_start, static_cast<uint64_t>(offset));
        uint64_t read_end = std::min<uint64_t>(chunk_end, static_cast<uint64_t>(offset) + read_size);
        if (read_end <= read_start) {
            continue;
        }
        uint64_t chunk_off = read_start - chunk_start;
        uint64_t read_len = read_end - read_start;

        bool read_ok = false;
        std::string data;
        std::string error;
        for (const auto& replica : chunk.replicas()) {
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
        std::memcpy(output.data() + (read_start - static_cast<uint64_t>(offset)), data.data(), data.size());
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

        for (const auto& replica : chunk.replicas()) {
            std::string error;
            if (!state->data_nodes.Write(replica, chunk_off, data, &error)) {
                return -EIO;
            }
        }

        remaining -= write_len;
    }

    uint64_t new_size = static_cast<uint64_t>(offset + size);
    if (!state->mds.CommitWrite(inode_id, new_size, &status)) {
        return -StatusToErrno(status);
    }

    return static_cast<int>(size - remaining);
}

int FuseTruncate(const char* path, off_t size, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status);
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }
    if (!state->mds.CommitWrite(inode_id, static_cast<uint64_t>(size), &status)) {
        return -StatusToErrno(status);
    }
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

    return fuse_main(argc, argv, &ops, &state);
}
