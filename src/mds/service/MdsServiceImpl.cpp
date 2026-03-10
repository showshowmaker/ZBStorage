#include "MdsServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "real_node.pb.h"

namespace zb::mds {

namespace {

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(path);
    while (std::getline(stream, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool IsDiskReplica(const zb::rpc::ReplicaLocation& replica) {
    return replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK;
}

bool IsLegacyLayoutObjectId(const std::string& layout_obj_id) {
    return layout_obj_id.rfind("legacy:", 0) == 0;
}

void FillLayoutRootMessage(const LayoutRootRecord& root, zb::rpc::LayoutRoot* out) {
    if (!out) {
        return;
    }
    out->set_inode_id(root.inode_id);
    out->set_layout_root_id(root.layout_root_id);
    out->set_layout_version(root.layout_version);
    out->set_file_size(root.file_size);
    out->set_epoch(root.epoch);
    out->set_update_ts(root.update_ts);
}

} // namespace

MdsServiceImpl::MdsServiceImpl(RocksMetaStore* store,
                               ChunkAllocator* allocator,
                               uint64_t default_chunk_size,
                               ArchiveCandidateQueue* candidate_queue,
                               ArchiveLeaseManager* lease_manager)
    : store_(store),
      allocator_(allocator),
      default_chunk_size_(default_chunk_size),
      candidate_queue_(candidate_queue),
      lease_manager_(lease_manager) {
    std::string error;
    EnsureRoot(&error);
}

void MdsServiceImpl::SetLayoutObjectOptions(LayoutObjectOptions options) {
    if (options.replica_count == 0) {
        options.replica_count = 1;
    }
    std::lock_guard<std::mutex> lock(layout_object_mu_);
    layout_object_options_ = options;
}

void MdsServiceImpl::Lookup(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::LookupRequest* request,
                            zb::rpc::LookupReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!ResolvePath(request->path(), &inode_id, &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Getattr(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::GetattrRequest* request,
                             zb::rpc::GetattrReply* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Open(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::OpenRequest* request,
                          zb::rpc::OpenReply* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!ResolvePath(request->path(), &inode_id, &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    uint64_t handle_id = AllocateHandleId(&error);
    if (handle_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (!store_->Put(HandleKey(handle_id), MetaCodec::EncodeUInt64(inode_id), &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    response->set_handle_id(handle_id);
    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
    (void)request->flags();
}

void MdsServiceImpl::Close(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::CloseRequest* request,
                           zb::rpc::CloseReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->handle_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "handle_id is empty");
        return;
    }

    std::string error;
    rocksdb::WriteBatch batch;
    batch.Delete(HandleKey(request->handle_id()));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Create(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::CreateRequest* request,
                            zb::rpc::CreateReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !allocator_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, error.empty() ? "parent not found" : error);
        return;
    }

    if (DentryExists(parent_inode, name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "path already exists");
        return;
    }
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::InodeAttr parent_attr;
    if (!GetInode(parent_inode, &parent_attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (parent_attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "parent is not a directory");
        return;
    }

    uint64_t inode_id = AllocateInodeId(&error);
    if (inode_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t now = NowSeconds();
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_FILE);
    attr.set_mode(request->mode());
    attr.set_uid(request->uid());
    attr.set_gid(request->gid());
    attr.set_size(0);
    attr.set_atime(now);
    attr.set_mtime(now);
    attr.set_ctime(now);
    attr.set_nlink(1);
    attr.set_chunk_size(request->chunk_size() ? request->chunk_size() : default_chunk_size_);
    attr.set_replica(request->replica() ? request->replica() : 1);
    attr.set_version(1);

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(attr));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Mkdir(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::MkdirRequest* request,
                           zb::rpc::MkdirReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, error.empty() ? "parent not found" : error);
        return;
    }

    if (DentryExists(parent_inode, name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "path already exists");
        return;
    }
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::InodeAttr parent_attr;
    if (!GetInode(parent_inode, &parent_attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (parent_attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "parent is not a directory");
        return;
    }

    uint64_t inode_id = AllocateInodeId(&error);
    if (inode_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t now = NowSeconds();
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_DIR);
    attr.set_mode(request->mode());
    attr.set_uid(request->uid());
    attr.set_gid(request->gid());
    attr.set_size(0);
    attr.set_atime(now);
    attr.set_mtime(now);
    attr.set_ctime(now);
    attr.set_nlink(2);
    attr.set_chunk_size(default_chunk_size_);
    attr.set_replica(1);
    attr.set_version(1);

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(attr));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Readdir(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::ReaddirRequest* request,
                             zb::rpc::ReaddirReply* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!ResolvePath(request->path(), &inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        return;
    }
    if (attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "not a directory");
        return;
    }

    std::string prefix = DentryPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        std::string name = it->key().ToString().substr(prefix.size());
        uint64_t child_inode = 0;
        if (!MetaCodec::DecodeUInt64(it->value().ToString(), &child_inode)) {
            continue;
        }
        zb::rpc::InodeAttr child_attr;
        if (!GetInode(child_inode, &child_attr, &error)) {
            continue;
        }
        zb::rpc::Dentry* entry = response->add_entries();
        entry->set_name(name);
        entry->set_inode_id(child_inode);
        entry->set_type(child_attr.type());
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Rename(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::RenameRequest* request,
                            zb::rpc::RenameReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->old_path().empty() || request->new_path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t old_parent = 0;
    std::string old_name;
    if (!ResolveParent(request->old_path(), &old_parent, &old_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "old parent not found");
        return;
    }

    uint64_t new_parent = 0;
    std::string new_name;
    if (!ResolveParent(request->new_path(), &new_parent, &new_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "new parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(old_parent, old_name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "source not found");
        return;
    }

    if (DentryExists(new_parent, new_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "target exists");
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(old_parent, old_name));
    batch.Put(DentryKey(new_parent, new_name), inode_data);
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Unlink(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::UnlinkRequest* request,
                            zb::rpc::UnlinkReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(parent_inode, name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }

    zb::rpc::InodeAttr attr;
    if (!GetInode(inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "not a file");
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    batch.Delete(InodeKey(inode_id));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (!DeleteInodeData(inode_id, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Rmdir(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::RmdirRequest* request,
                           zb::rpc::RmdirReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    if (request->path() == "/") {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "cannot remove root");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(parent_inode, name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }

    zb::rpc::InodeAttr attr;
    if (!GetInode(inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "not a directory");
        return;
    }

    std::string prefix = DentryPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_EMPTY, "directory not empty");
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    batch.Delete(InodeKey(inode_id));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::AllocateWrite(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::AllocateWriteRequest* request,
                                   zb::rpc::AllocateWriteReply* response,
                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !allocator_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->inode_id() == 0 || request->size() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "invalid inode or size");
        return;
    }
    if (request->size() > std::numeric_limits<uint64_t>::max() - request->offset()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "offset + size overflow");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        return;
    }

    LayoutRootRecord current_root;
    bool from_legacy = false;
    if (!LoadLayoutRoot(request->inode_id(), &current_root, &from_legacy, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    const uint64_t chunk_size = attr.chunk_size() ? attr.chunk_size() : default_chunk_size_;
    if (chunk_size == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "chunk size is zero");
        return;
    }

    attr.set_atime(NowSeconds());
    error.clear();
    PutInode(attr.inode_id(), attr, &error);

    const uint64_t start = request->offset() / chunk_size;
    const uint64_t end = (request->offset() + request->size() - 1) / chunk_size;
    const uint32_t desired_replica = attr.replica() ? attr.replica() : 1;

    PendingWriteTransaction pending;
    pending.inode_id = attr.inode_id();
    pending.base_layout_version = std::max<uint64_t>(1, current_root.layout_version);
    pending.pending_layout_version = pending.base_layout_version + 1;
    pending.create_ts_ms = NowMilliseconds();

    zb::rpc::FileLayout* layout = response->mutable_layout();
    layout->set_inode_id(attr.inode_id());
    layout->set_chunk_size(chunk_size);

    for (uint64_t index = start; index <= end; ++index) {
        const uint32_t chunk_index = static_cast<uint32_t>(index);
        const std::string chunk_key = ChunkKey(attr.inode_id(), chunk_index);

        std::string chunk_data;
        std::string local_error;
        zb::rpc::ChunkMeta old_meta;
        const bool chunk_exists = store_->Get(chunk_key, &chunk_data, &local_error);
        if (chunk_exists) {
            if (!MetaCodec::DecodeChunkMeta(chunk_data, &old_meta)) {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid chunk meta");
                return;
            }
        } else if (!local_error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, local_error);
            return;
        }

        std::vector<zb::rpc::ReplicaLocation> new_replicas;
        if (!allocator_->AllocateChunk(desired_replica, GenerateChunkId(), &new_replicas) ||
            new_replicas.size() < desired_replica) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to allocate replicas");
            return;
        }

        const uint64_t chunk_start = static_cast<uint64_t>(chunk_index) * chunk_size;
        const uint64_t chunk_end = chunk_start + chunk_size;
        const uint64_t write_start = std::max<uint64_t>(chunk_start, request->offset());
        const uint64_t write_end = std::min<uint64_t>(chunk_end, request->offset() + request->size());
        const bool full_chunk_overwrite = (write_start <= chunk_start && write_end >= chunk_end);
        if (chunk_exists && !full_chunk_overwrite) {
            if (!SeedChunkForCowWrite(old_meta, new_replicas, chunk_size, &local_error)) {
                FillStatus(response->mutable_status(),
                           zb::rpc::MDS_INTERNAL_ERROR,
                           local_error.empty() ? "failed to seed old chunk data for cow write" : local_error);
                return;
            }
        }

        zb::rpc::ChunkMeta staged_meta;
        staged_meta.set_index(chunk_index);
        for (const auto& replica : new_replicas) {
            *staged_meta.add_replicas() = replica;
        }

        pending.chunk_updates[chunk_index] = staged_meta;
        *layout->add_chunks() = staged_meta;
    }

    {
        std::lock_guard<std::mutex> lock(pending_write_mu_);
        pending_writes_[attr.inode_id()] = std::move(pending);
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetLayout(google::protobuf::RpcController* cntl_base,
                               const zb::rpc::GetLayoutRequest* request,
                               zb::rpc::GetLayoutReply* response,
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->inode_id() == 0 || request->size() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "invalid inode or size");
        return;
    }

    std::string error;
    bool optical_only = false;
    if (!BuildOpticalReadPlan(request->inode_id(),
                              request->offset(),
                              request->size(),
                              response->mutable_optical_plan(),
                              &optical_only,
                              &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to build optical read plan" : error);
        return;
    }
    if (optical_only) {
        response->mutable_layout()->Clear();
        response->mutable_layout()->set_inode_id(request->inode_id());
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    if (!BuildReadPlan(request->inode_id(), request->offset(), request->size(), response->mutable_layout(), &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to build read plan" : error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::CommitWrite(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::CommitWriteRequest* request,
                                 zb::rpc::CommitWriteReply* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    std::string error;
    LayoutRootRecord committed;
    if (ConsumePendingWriteForCommit(request->inode_id(), 0, request->new_size(), &committed, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }
    if (!error.empty() && error != "pending write txn not found") {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    LayoutRootRecord root;
    bool from_legacy = false;
    error.clear();
    if (!LoadLayoutRoot(request->inode_id(), &root, &from_legacy, &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "inode not found" : error);
        return;
    }

    root.file_size = request->new_size();
    root.layout_version = root.layout_version + 1;
    root.update_ts = NowMilliseconds();
    root.epoch = 0;
    if (!StoreLayoutRootAtomic(request->inode_id(),
                               root,
                               root.layout_version - 1,
                               true,
                               request->new_size(),
                               &committed,
                               &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetLayoutRoot(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::GetLayoutRootRequest* request,
                                   zb::rpc::GetLayoutRootReply* response,
                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    LayoutRootRecord root;
    bool from_legacy = false;
    std::string error;
    if (!LoadLayoutRoot(request->inode_id(), &root, &from_legacy, &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to load layout root" : error);
        return;
    }

    zb::rpc::InodeAttr attr;
    error.clear();
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        return;
    }
    FillLayoutRootMessage(root, response->mutable_root());
    response->mutable_root()->set_chunk_size(attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_);
    response->set_from_legacy(from_legacy);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ResolveLayout(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::ResolveLayoutRequest* request,
                                   zb::rpc::ResolveLayoutReply* response,
                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->inode_id() == 0 || request->size() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "invalid inode or size");
        return;
    }

    LayoutRootRecord root;
    bool from_legacy = false;
    std::string error;
    if (!LoadLayoutRoot(request->inode_id(), &root, &from_legacy, &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to load layout root" : error);
        return;
    }

    zb::rpc::FileLayout* read_plan = response->mutable_read_plan();
    bool optical_only = false;
    if (!BuildOpticalReadPlan(request->inode_id(),
                              request->offset(),
                              request->size(),
                              response->mutable_optical_plan(),
                              &optical_only,
                              &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to build optical read plan" : error);
        return;
    }
    if (optical_only) {
        FillLayoutRootMessage(root, response->mutable_root());
        response->set_from_legacy(from_legacy);
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    if (!BuildReadPlan(request->inode_id(), request->offset(), request->size(), read_plan, &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to resolve layout" : error);
        return;
    }

    std::vector<LayoutExtentRecord> extents;
    if (!ResolveExtents(*read_plan, request->offset(), request->size(), root.layout_version, &extents, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to build extents" : error);
        return;
    }

    for (const auto& extent : extents) {
        zb::rpc::LayoutExtent* out = response->add_extents();
        out->set_logical_offset(extent.logical_offset);
        out->set_length(extent.length);
        out->set_object_id(extent.object_id);
        out->set_object_offset(extent.object_offset);
        out->set_object_length(extent.object_length);
        out->set_object_version(extent.object_version);
        const uint64_t chunk_size = read_plan->chunk_size() > 0 ? read_plan->chunk_size() : default_chunk_size_;
        out->set_chunk_index(chunk_size > 0 ? static_cast<uint32_t>(extent.logical_offset / chunk_size) : 0);
    }

    FillLayoutRootMessage(root, response->mutable_root());
    response->mutable_root()->set_chunk_size(read_plan->chunk_size());
    response->set_from_legacy(from_legacy);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::CommitLayoutRoot(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::CommitLayoutRootRequest* request,
                                      zb::rpc::CommitLayoutRootReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    LayoutRootRecord root;
    root.inode_id = request->inode_id();
    root.layout_root_id = request->root().layout_root_id();
    root.layout_version = request->root().layout_version();
    root.file_size = request->root().file_size();
    root.epoch = request->root().epoch();
    root.update_ts = request->root().update_ts();

    LayoutRootRecord committed;
    std::string error;
    if (ConsumePendingWriteForCommit(request->inode_id(),
                                     request->expected_layout_version(),
                                     request->update_inode_size() ? request->new_size() : root.file_size,
                                     &committed,
                                     &error)) {
        FillLayoutRootMessage(committed, response->mutable_root());
        zb::rpc::InodeAttr attr;
        error.clear();
        if (GetInode(request->inode_id(), &attr, &error)) {
            response->mutable_root()->set_chunk_size(attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_);
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }
    if (!error.empty() && error != "pending write txn not found") {
        if (error == "layout version mismatch") {
            FillStatus(response->mutable_status(), zb::rpc::MDS_STALE_EPOCH, error);
            return;
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    error.clear();
    if (!StoreLayoutRootAtomic(request->inode_id(),
                               root,
                               request->expected_layout_version(),
                               request->update_inode_size(),
                               request->new_size(),
                               &committed,
                               &error)) {
        if (error == "layout version mismatch") {
            FillStatus(response->mutable_status(), zb::rpc::MDS_STALE_EPOCH, error);
            return;
        }
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to commit layout root" : error);
        return;
    }
    FillLayoutRootMessage(committed, response->mutable_root());
    zb::rpc::InodeAttr attr;
    error.clear();
    if (GetInode(request->inode_id(), &attr, &error)) {
        response->mutable_root()->set_chunk_size(attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_);
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ReportNodeStatus(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::ReportNodeStatusRequest* request,
                                      zb::rpc::ReportNodeStatusReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;

    if (!response) {
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ReportArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::ReportArchiveCandidatesRequest* request,
                                             zb::rpc::ReportArchiveCandidatesReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!candidate_queue_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "archive candidate queue disabled");
        response->set_accepted(0);
        response->set_dropped(static_cast<uint32_t>(request->candidates_size()));
        return;
    }

    std::vector<ArchiveCandidateEntry> batch;
    batch.reserve(static_cast<size_t>(request->candidates_size()));
    uint32_t dropped = 0;
    const uint64_t fallback_report_ts = request->report_ts_ms() > 0 ? request->report_ts_ms() : NowMilliseconds();

    for (const auto& item : request->candidates()) {
        ArchiveCandidateEntry candidate;
        candidate.node_id = !item.node_id().empty() ? item.node_id() : request->node_id();
        candidate.node_address = !item.node_address().empty() ? item.node_address() : request->node_address();
        candidate.disk_id = item.disk_id();
        candidate.chunk_id = item.chunk_id();
        candidate.last_access_ts_ms = item.last_access_ts_ms();
        candidate.size_bytes = item.size_bytes();
        candidate.checksum = item.checksum();
        candidate.heat_score = item.heat_score();
        candidate.archive_state = item.archive_state().empty() ? "pending" : item.archive_state();
        candidate.version = item.version();
        candidate.score = item.score();
        candidate.report_ts_ms = item.report_ts_ms() > 0 ? item.report_ts_ms() : fallback_report_ts;

        if (candidate.disk_id.empty() || candidate.chunk_id.empty()) {
            ++dropped;
            continue;
        }
        batch.push_back(std::move(candidate));
    }

    const ArchiveCandidateQueue::PushResult push = candidate_queue_->PushBatch(batch);
    response->set_accepted(static_cast<uint32_t>(push.accepted));
    response->set_dropped(static_cast<uint32_t>(dropped + push.dropped));
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ClaimArchiveTask(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::ClaimArchiveTaskRequest* request,
                                      zb::rpc::ClaimArchiveTaskReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_granted(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Claim(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "claim failed" : error);
        response->set_granted(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::RenewArchiveLease(google::protobuf::RpcController* cntl_base,
                                       const zb::rpc::RenewArchiveLeaseRequest* request,
                                       zb::rpc::RenewArchiveLeaseReply* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_renewed(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Renew(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "renew failed" : error);
        response->set_renewed(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::CommitArchiveTask(google::protobuf::RpcController* cntl_base,
                                       const zb::rpc::CommitArchiveTaskRequest* request,
                                       zb::rpc::CommitArchiveTaskReply* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_committed(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Commit(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "commit failed" : error);
        response->set_committed(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

bool MdsServiceImpl::EnsureRoot(std::string* error) {
    zb::rpc::InodeAttr root;
    if (GetInode(kRootInodeId, &root, error)) {
        return true;
    }
    if (error && !error->empty()) {
        return false;
    }

    uint64_t now = NowSeconds();
    root.set_inode_id(kRootInodeId);
    root.set_type(zb::rpc::INODE_DIR);
    root.set_mode(0755);
    root.set_uid(0);
    root.set_gid(0);
    root.set_size(0);
    root.set_atime(now);
    root.set_mtime(now);
    root.set_ctime(now);
    root.set_nlink(2);
    root.set_chunk_size(default_chunk_size_);
    root.set_replica(1);
    root.set_version(1);

    return PutInode(kRootInodeId, root, error);
}

bool MdsServiceImpl::ResolvePath(const std::string& path,
                                 uint64_t* inode_id,
                                 zb::rpc::InodeAttr* attr,
                                 std::string* error) {
    if (!inode_id) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(path);
    uint64_t current = kRootInodeId;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return GetInode(current, attr, error);
        }
        return true;
    }

    for (const auto& name : parts) {
        std::string data;
        if (!store_->Get(DentryKey(current, name), &data, error)) {
            return false;
        }
        uint64_t next = 0;
        if (!MetaCodec::DecodeUInt64(data, &next)) {
            if (error) {
                *error = "invalid dentry";
            }
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return GetInode(current, attr, error);
    }
    return true;
}

bool MdsServiceImpl::ResolveParent(const std::string& path,
                                   uint64_t* parent_inode,
                                   std::string* name,
                                   std::string* error) {
    if (!parent_inode || !name) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(path);
    if (parts.empty()) {
        if (error) {
            *error = "invalid path";
        }
        return false;
    }
    *name = parts.back();
    parts.pop_back();
    uint64_t current = kRootInodeId;
    for (const auto& part : parts) {
        std::string data;
        if (!store_->Get(DentryKey(current, part), &data, error)) {
            return false;
        }
        if (!MetaCodec::DecodeUInt64(data, &current)) {
            if (error) {
                *error = "invalid dentry";
            }
            return false;
        }
    }
    *parent_inode = current;
    return true;
}

bool MdsServiceImpl::GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) {
    std::string data;
    if (!store_->Get(InodeKey(inode_id), &data, error)) {
        return false;
    }
    if (!MetaCodec::DecodeInodeAttr(data, attr)) {
        if (error) {
            *error = "invalid inode data";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error) {
    return store_->Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(attr), error);
}

bool MdsServiceImpl::PutDentry(uint64_t parent_inode, const std::string& name, uint64_t inode_id, std::string* error) {
    return store_->Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id), error);
}

bool MdsServiceImpl::DeleteDentry(uint64_t parent_inode, const std::string& name, std::string* error) {
    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    return store_->WriteBatch(&batch, error);
}

bool MdsServiceImpl::DentryExists(uint64_t parent_inode, const std::string& name, std::string* error) {
    return store_->Exists(DentryKey(parent_inode, name), error);
}

bool MdsServiceImpl::DeleteInodeData(uint64_t inode_id, std::string* error) {
    std::string prefix = ChunkPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    rocksdb::WriteBatch batch;
    std::unordered_set<std::string> chunk_ids;
    std::unordered_set<std::string> image_index_keys;
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        zb::rpc::ChunkMeta meta;
        if (MetaCodec::DecodeChunkMeta(it->value().ToString(), &meta)) {
            for (const auto& replica : meta.replicas()) {
                if (!replica.chunk_id().empty()) {
                    chunk_ids.insert(replica.chunk_id());
                }
                if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
                    !replica.node_id().empty() &&
                    !replica.disk_id().empty() &&
                    !replica.image_id().empty() &&
                    !replica.chunk_id().empty()) {
                    image_index_keys.insert(ArchiveImageChunkKey(replica.node_id(),
                                                                 replica.disk_id(),
                                                                 replica.image_id(),
                                                                 replica.chunk_id()));
                }
            }
        }
        batch.Delete(it->key());
    }
    for (const auto& chunk_id : chunk_ids) {
        batch.Delete(ReverseChunkKey(chunk_id));
        batch.Delete(ArchiveStateKey(chunk_id));
        batch.Delete(ArchiveReverseRepairKey(chunk_id));
    }
    for (const auto& image_key : image_index_keys) {
        batch.Delete(image_key);
    }
    std::string root_data;
    std::string local_error;
    if (store_->Get(LayoutRootKey(inode_id), &root_data, &local_error)) {
        LayoutRootRecord root;
        if (MetaCodec::DecodeLayoutRoot(root_data, &root) && !root.layout_root_id.empty() &&
            !IsLegacyLayoutObjectId(root.layout_root_id)) {
            batch.Delete(LayoutObjectKey(root.layout_root_id));
            const std::string replica_prefix = LayoutObjectReplicaPrefix(root.layout_root_id);
            std::unique_ptr<rocksdb::Iterator> replica_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
            for (replica_it->Seek(replica_prefix); replica_it->Valid(); replica_it->Next()) {
                const std::string key = replica_it->key().ToString();
                if (key.rfind(replica_prefix, 0) != 0) {
                    break;
                }
                batch.Delete(key);
            }
            batch.Delete(LayoutGcSeenKey(root.layout_root_id));
        }
        batch.Delete(LayoutRootKey(inode_id));
    }
    bool ok = true;
    if (batch.Count() > 0) {
        ok = store_->WriteBatch(&batch, error);
    }
    if (ok) {
        std::lock_guard<std::mutex> lock(pending_write_mu_);
        pending_writes_.erase(inode_id);
    }
    return ok;
}

bool MdsServiceImpl::LoadLayoutRoot(uint64_t inode_id,
                                    LayoutRootRecord* root,
                                    bool* from_legacy,
                                    std::string* error) {
    if (!root) {
        if (error) {
            *error = "layout root output is null";
        }
        return false;
    }

    zb::rpc::InodeAttr inode_attr;
    std::string local_error;
    if (!GetInode(inode_id, &inode_attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }

    std::string data;
    local_error.clear();
    if (store_->Get(LayoutRootKey(inode_id), &data, &local_error)) {
        if (!MetaCodec::DecodeLayoutRoot(data, root)) {
            if (error) {
                *error = "invalid layout root record";
            }
            return false;
        }
        if (root->inode_id == 0) {
            root->inode_id = inode_id;
        }
        if (root->layout_version == 0) {
            root->layout_version = std::max<uint64_t>(1, inode_attr.version());
        }
        if (root->layout_root_id.empty()) {
            root->layout_root_id = "lr-" + std::to_string(inode_id) + "-" + std::to_string(root->layout_version);
        }
        if (from_legacy) {
            *from_legacy = false;
        }
        local_error.clear();
        if (!ValidateLayoutObjectOnLoad(inode_id, *root, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "layout object validation failed" : local_error;
            }
            return false;
        }
        return true;
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }

    root->inode_id = inode_id;
    root->layout_root_id = "legacy:" + std::to_string(inode_id);
    root->layout_version = std::max<uint64_t>(1, inode_attr.version());
    root->file_size = inode_attr.size();
    root->epoch = 0;
    root->update_ts = NowMilliseconds();
    if (from_legacy) {
        *from_legacy = true;
    }
    return true;
}

bool MdsServiceImpl::StoreLayoutRootAtomic(uint64_t inode_id,
                                           const LayoutRootRecord& root,
                                           uint64_t expected_layout_version,
                                           bool update_inode_size,
                                           uint64_t new_size,
                                           LayoutRootRecord* committed,
                                           std::string* error) {
    if (!committed) {
        if (error) {
            *error = "committed output is null";
        }
        return false;
    }

    zb::rpc::InodeAttr inode_attr;
    std::string local_error;
    if (!GetInode(inode_id, &inode_attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }

    LayoutRootRecord existing;
    bool from_legacy = false;
    if (!LoadLayoutRoot(inode_id, &existing, &from_legacy, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load existing layout root" : local_error;
        }
        return false;
    }

    const uint64_t existing_version = std::max<uint64_t>(existing.layout_version, 1);
    if (expected_layout_version > 0 && existing_version != expected_layout_version) {
        if (error) {
            *error = "layout version mismatch";
        }
        return false;
    }

    LayoutRootRecord next = root;
    next.inode_id = inode_id;
    if (next.layout_version == 0) {
        next.layout_version = existing_version + 1;
    }
    if (update_inode_size) {
        next.file_size = new_size;
    } else if (next.file_size == 0 && inode_attr.size() > 0) {
        next.file_size = inode_attr.size();
    }
    if (next.update_ts == 0) {
        next.update_ts = NowMilliseconds();
    }

    if (next.layout_root_id.empty() ||
        IsLegacyLayoutObjectId(next.layout_root_id) ||
        next.layout_root_id.rfind("inline:", 0) == 0) {
        next.layout_root_id = "lr-" + std::to_string(inode_id) + "-" + std::to_string(next.layout_version);
    }

    inode_attr.set_mtime(NowSeconds());
    inode_attr.set_ctime(NowSeconds());
    if (update_inode_size) {
        inode_attr.set_size(new_size);
    } else if (next.file_size > 0 || inode_attr.size() == 0) {
        inode_attr.set_size(next.file_size);
    }
    if (inode_attr.version() < next.layout_version) {
        inode_attr.set_version(next.layout_version);
    }

    rocksdb::WriteBatch batch;
    batch.Put(LayoutRootKey(inode_id), MetaCodec::EncodeLayoutRoot(next));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(inode_attr));
    LayoutNodeRecord layout_node;
    if (!BuildLayoutNodeFromChunks(inode_id, next.layout_root_id, next.layout_version, &layout_node, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to build layout object snapshot" : local_error;
        }
        return false;
    }
    if (!StoreLayoutNodeWithReplicas(next.layout_root_id, layout_node, &batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist layout object replicas" : local_error;
        }
        return false;
    }
    if (!store_->WriteBatch(&batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist layout root" : local_error;
        }
        return false;
    }

    *committed = next;
    return true;
}

bool MdsServiceImpl::BuildLayoutNodeFromChunks(uint64_t inode_id,
                                               const std::string& layout_obj_id,
                                               uint64_t object_version,
                                               LayoutNodeRecord* node,
                                               std::string* error) {
    if (!node || inode_id == 0 || layout_obj_id.empty()) {
        if (error) {
            *error = "invalid layout object build arguments";
        }
        return false;
    }
    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found when building layout object" : local_error;
        }
        return false;
    }
    const uint64_t chunk_size = attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_;
    if (chunk_size == 0) {
        if (error) {
            *error = "chunk size is zero";
        }
        return false;
    }

    node->node_id = layout_obj_id;
    node->level = 0;
    node->extents.clear();
    node->child_layout_ids.clear();

    const std::string prefix = ChunkPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        zb::rpc::ChunkMeta chunk_meta;
        if (!MetaCodec::DecodeChunkMeta(it->value().ToString(), &chunk_meta)) {
            continue;
        }
        const zb::rpc::ReplicaLocation* source = nullptr;
        for (const auto& replica : chunk_meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY &&
                !replica.chunk_id().empty()) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            for (const auto& replica : chunk_meta.replicas()) {
                if (!replica.chunk_id().empty()) {
                    source = &replica;
                    break;
                }
            }
        }
        if (!source) {
            continue;
        }
        LayoutExtentRecord extent;
        extent.logical_offset = static_cast<uint64_t>(chunk_meta.index()) * chunk_size;
        extent.length = chunk_size;
        extent.object_id = source->chunk_id();
        extent.object_offset = 0;
        extent.object_length = chunk_size;
        extent.object_version = object_version;
        node->extents.push_back(std::move(extent));
    }

    std::sort(node->extents.begin(), node->extents.end(), [](const LayoutExtentRecord& lhs, const LayoutExtentRecord& rhs) {
        return lhs.logical_offset < rhs.logical_offset;
    });
    return true;
}

bool MdsServiceImpl::StoreLayoutNodeWithReplicas(const std::string& layout_obj_id,
                                                 const LayoutNodeRecord& node,
                                                 rocksdb::WriteBatch* batch,
                                                 std::string* error) {
    if (!batch || layout_obj_id.empty()) {
        if (error) {
            *error = "invalid layout object store arguments";
        }
        return false;
    }
    const std::string encoded = MetaCodec::EncodeLayoutNode(node);
    uint32_t replica_count = 1;
    {
        std::lock_guard<std::mutex> lock(layout_object_mu_);
        replica_count = std::max<uint32_t>(1, layout_object_options_.replica_count);
    }

    batch->Put(LayoutObjectKey(layout_obj_id), encoded);
    for (uint32_t replica = 1; replica < replica_count; ++replica) {
        batch->Put(LayoutObjectReplicaKey(layout_obj_id, replica), encoded);
    }
    return true;
}

bool MdsServiceImpl::LoadHealthyLayoutNode(uint64_t inode_id,
                                           const std::string& layout_obj_id,
                                           uint64_t object_version,
                                           LayoutNodeRecord* node,
                                           bool* recovered,
                                           std::string* error) {
    if (!node || layout_obj_id.empty()) {
        if (error) {
            *error = "invalid layout object load arguments";
        }
        return false;
    }
    if (recovered) {
        *recovered = false;
    }

    std::string data;
    std::string local_error;
    if (store_->Get(LayoutObjectKey(layout_obj_id), &data, &local_error) &&
        MetaCodec::DecodeLayoutNode(data, node)) {
        return true;
    }

    bool found_replica = false;
    uint32_t replica_count = 1;
    {
        std::lock_guard<std::mutex> lock(layout_object_mu_);
        replica_count = std::max<uint32_t>(1, layout_object_options_.replica_count);
    }
    for (uint32_t replica = 1; replica < replica_count; ++replica) {
        local_error.clear();
        if (!store_->Get(LayoutObjectReplicaKey(layout_obj_id, replica), &data, &local_error)) {
            continue;
        }
        if (MetaCodec::DecodeLayoutNode(data, node)) {
            found_replica = true;
            break;
        }
    }
    if (!found_replica) {
        if (!BuildLayoutNodeFromChunks(inode_id, layout_obj_id, object_version, node, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to rebuild layout object" : local_error;
            }
            return false;
        }
        found_replica = true;
    }
    if (!found_replica) {
        if (error) {
            *error = "layout object cannot be recovered";
        }
        return false;
    }

    rocksdb::WriteBatch repair_batch;
    if (!StoreLayoutNodeWithReplicas(layout_obj_id, *node, &repair_batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to prepare layout object repair batch" : local_error;
        }
        return false;
    }
    if (repair_batch.Count() > 0 && !store_->WriteBatch(&repair_batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist repaired layout object" : local_error;
        }
        return false;
    }
    if (recovered) {
        *recovered = true;
    }
    return true;
}

bool MdsServiceImpl::ValidateLayoutObjectOnLoad(uint64_t inode_id, const LayoutRootRecord& root, std::string* error) {
    if (root.layout_root_id.empty() || IsLegacyLayoutObjectId(root.layout_root_id)) {
        return true;
    }
    bool scrub_on_load = true;
    {
        std::lock_guard<std::mutex> lock(layout_object_mu_);
        scrub_on_load = layout_object_options_.scrub_on_load;
    }
    if (!scrub_on_load) {
        return true;
    }
    LayoutNodeRecord node;
    bool recovered = false;
    if (!LoadHealthyLayoutNode(inode_id, root.layout_root_id, root.layout_version, &node, &recovered, error)) {
        return false;
    }
    return true;
}

bool MdsServiceImpl::BuildOpticalReadPlan(uint64_t inode_id,
                                          uint64_t offset,
                                          uint64_t size,
                                          zb::rpc::OpticalReadPlan* plan,
                                          bool* optical_only,
                                          std::string* error) {
    if (!plan || !optical_only) {
        if (error) {
            *error = "invalid optical plan output";
        }
        return false;
    }
    *optical_only = false;
    plan->Clear();
    if (inode_id == 0 || size == 0) {
        return true;
    }
    if (size > std::numeric_limits<uint64_t>::max() - offset) {
        if (error) {
            *error = "offset + size overflow";
        }
        return false;
    }

    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }
    const uint64_t chunk_size = attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_;
    if (chunk_size == 0) {
        if (error) {
            *error = "chunk_size is zero";
        }
        return false;
    }

    const uint64_t start = offset / chunk_size;
    const uint64_t end = (offset + size - 1) / chunk_size;

    bool need_optical = false;
    bool all_chunks_optical_only = true;
    zb::rpc::ReplicaLocation anchor;

    for (uint64_t index = start; index <= end; ++index) {
        const std::string chunk_key = ChunkKey(inode_id, static_cast<uint32_t>(index));
        std::string chunk_data;
        local_error.clear();
        if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }

        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
            if (error) {
                *error = "invalid chunk meta";
            }
            return false;
        }

        bool has_disk = false;
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                has_disk = true;
                break;
            }
        }
        if (has_disk) {
            all_chunks_optical_only = false;
            continue;
        }

        const zb::rpc::ReplicaLocation* optical = nullptr;
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                optical = &replica;
                break;
            }
        }
        if (!optical) {
            if (error) {
                *error = "layout contains no readable replica";
            }
            return false;
        }
        need_optical = true;
        if (anchor.node_address().empty()) {
            anchor = *optical;
            continue;
        }
        if (anchor.node_address() != optical->node_address() ||
            anchor.disk_id() != optical->disk_id()) {
            if (error) {
                *error = "requested range spans multiple optical discs";
            }
            return false;
        }
    }

    if (!need_optical || !all_chunks_optical_only) {
        return true;
    }
    if (anchor.node_address().empty() || anchor.disk_id().empty()) {
        if (error) {
            *error = "invalid optical anchor";
        }
        return false;
    }

    plan->set_enabled(true);
    plan->set_node_id(anchor.node_id());
    plan->set_node_address(anchor.node_address());
    plan->set_disc_id(anchor.disk_id());
    plan->set_inode_id(inode_id);
    plan->set_file_id("inode-" + std::to_string(inode_id));
    plan->set_file_size(attr.size());
    plan->set_layout_version(attr.version());
    *optical_only = true;
    return true;
}

bool MdsServiceImpl::BuildReadPlan(uint64_t inode_id,
                                   uint64_t offset,
                                   uint64_t size,
                                   zb::rpc::FileLayout* layout,
                                   std::string* error) {
    if (!layout) {
        if (error) {
            *error = "layout output is null";
        }
        return false;
    }
    if (inode_id == 0 || size == 0) {
        if (error) {
            *error = "invalid inode or size";
        }
        return false;
    }
    if (size > std::numeric_limits<uint64_t>::max() - offset) {
        if (error) {
            *error = "offset + size overflow";
        }
        return false;
    }

    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }

    const uint64_t chunk_size = attr.chunk_size() ? attr.chunk_size() : default_chunk_size_;
    if (chunk_size == 0) {
        if (error) {
            *error = "chunk_size is zero";
        }
        return false;
    }
    attr.set_atime(NowSeconds());
    local_error.clear();
    PutInode(attr.inode_id(), attr, &local_error);

    const uint64_t start = offset / chunk_size;
    const uint64_t end = (offset + size - 1) / chunk_size;
    std::unordered_set<std::string> recalled_images;
    bool recalled_from_optical = false;
    std::vector<std::string> requested_chunk_keys;
    requested_chunk_keys.reserve(static_cast<size_t>(end - start + 1));

    layout->Clear();
    layout->set_inode_id(attr.inode_id());
    layout->set_chunk_size(chunk_size);

    for (uint64_t index = start; index <= end; ++index) {
        local_error.clear();
        const std::string chunk_key = ChunkKey(attr.inode_id(), static_cast<uint32_t>(index));
        requested_chunk_keys.push_back(chunk_key);

        std::string chunk_data;
        zb::rpc::ChunkMeta chunk_meta;
        if (store_->Get(chunk_key, &chunk_data, &local_error)) {
            if (!MetaCodec::DecodeChunkMeta(chunk_data, &chunk_meta)) {
                if (error) {
                    *error = "invalid chunk meta";
                }
                return false;
            }
            bool recalled_this_chunk = false;
            if (!EnsureChunkReadableFromDisk(chunk_key,
                                             &chunk_meta,
                                             &recalled_images,
                                             &recalled_this_chunk,
                                             &local_error)) {
                if (error) {
                    *error = local_error.empty() ? "failed to recall optical chunk data" : local_error;
                }
                return false;
            }
            if (recalled_this_chunk) {
                recalled_from_optical = true;
            }
        } else if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }

    if (recalled_from_optical) {
        if (!CacheWholeFileToDisk(attr.inode_id(), &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to cache file on disk node" : local_error;
            }
            return false;
        }
    }

    for (const auto& chunk_key : requested_chunk_keys) {
        local_error.clear();
        std::string chunk_data;
        zb::rpc::ChunkMeta chunk_meta;
        if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }
        if (!MetaCodec::DecodeChunkMeta(chunk_data, &chunk_meta)) {
            if (error) {
                *error = "invalid chunk meta";
            }
            return false;
        }

        zb::rpc::ChunkMeta read_meta;
        read_meta.set_index(chunk_meta.index());
        for (const auto& replica : chunk_meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                *read_meta.add_replicas() = replica;
            }
        }
        if (read_meta.replicas_size() == 0) {
            if (error) {
                *error = "layout contains no readable disk replica";
            }
            return false;
        }
        *layout->add_chunks() = read_meta;
    }

    return true;
}

bool MdsServiceImpl::ResolveExtents(const zb::rpc::FileLayout& read_plan,
                                    uint64_t request_offset,
                                    uint64_t request_size,
                                    uint64_t object_version,
                                    std::vector<LayoutExtentRecord>* extents,
                                    std::string* error) {
    if (!extents) {
        if (error) {
            *error = "extents output is null";
        }
        return false;
    }
    if (request_size == 0) {
        if (error) {
            *error = "request_size is zero";
        }
        return false;
    }
    if (request_size > std::numeric_limits<uint64_t>::max() - request_offset) {
        if (error) {
            *error = "request range overflow";
        }
        return false;
    }

    extents->clear();
    const uint64_t chunk_size = read_plan.chunk_size() > 0 ? read_plan.chunk_size() : default_chunk_size_;
    if (chunk_size == 0) {
        if (error) {
            *error = "chunk_size is zero";
        }
        return false;
    }
    const uint64_t request_end = request_offset + request_size;
    for (const auto& chunk : read_plan.chunks()) {
        const uint64_t chunk_start = static_cast<uint64_t>(chunk.index()) * chunk_size;
        const uint64_t chunk_end = chunk_start + chunk_size;
        const uint64_t overlap_start = std::max<uint64_t>(chunk_start, request_offset);
        const uint64_t overlap_end = std::min<uint64_t>(chunk_end, request_end);
        if (overlap_start >= overlap_end) {
            continue;
        }

        const zb::rpc::ReplicaLocation* source = nullptr;
        for (const auto& replica : chunk.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY &&
                !replica.chunk_id().empty()) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            if (error) {
                *error = "chunk has no ready disk source";
            }
            return false;
        }

        LayoutExtentRecord extent;
        extent.logical_offset = overlap_start;
        extent.length = overlap_end - overlap_start;
        extent.object_id = source->chunk_id();
        extent.object_offset = overlap_start - chunk_start;
        extent.object_length = extent.length;
        extent.object_version = object_version;
        extents->push_back(std::move(extent));
    }
    return true;
}

bool MdsServiceImpl::SelectReadableDiskReplica(const zb::rpc::ChunkMeta& chunk_meta,
                                               zb::rpc::ReplicaLocation* source) const {
    if (!source) {
        return false;
    }
    for (const auto& replica : chunk_meta.replicas()) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_DISK ||
            replica.replica_state() != zb::rpc::REPLICA_READY ||
            replica.chunk_id().empty()) {
            continue;
        }
        *source = replica;
        return true;
    }
    return false;
}

bool MdsServiceImpl::SeedChunkForCowWrite(const zb::rpc::ChunkMeta& old_meta,
                                          const std::vector<zb::rpc::ReplicaLocation>& new_replicas,
                                          uint64_t chunk_size,
                                          std::string* error) {
    if (new_replicas.empty()) {
        if (error) {
            *error = "new replicas are empty";
        }
        return false;
    }
    if (chunk_size == 0) {
        if (error) {
            *error = "chunk size is zero";
        }
        return false;
    }

    zb::rpc::ReplicaLocation source;
    if (!SelectReadableDiskReplica(old_meta, &source)) {
        return true;
    }

    std::string data;
    std::string local_error;
    if (!ReadChunkFromReplica(source, chunk_size, &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to read source chunk for cow seed" : local_error;
        }
        return false;
    }
    for (const auto& target : new_replicas) {
        if (!WriteChunkToReplica(target, target.chunk_id(), data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to write seeded chunk replica" : local_error;
            }
            return false;
        }
    }
    return true;
}

bool MdsServiceImpl::ConsumePendingWriteForCommit(uint64_t inode_id,
                                                  uint64_t expected_base_layout_version,
                                                  uint64_t new_file_size,
                                                  LayoutRootRecord* committed_root,
                                                  std::string* error) {
    if (!committed_root) {
        if (error) {
            *error = "committed_root is null";
        }
        return false;
    }

    PendingWriteTransaction txn;
    {
        std::lock_guard<std::mutex> lock(pending_write_mu_);
        auto it = pending_writes_.find(inode_id);
        if (it == pending_writes_.end()) {
            if (error) {
                *error = "pending write txn not found";
            }
            return false;
        }
        txn = it->second;
    }

    if (expected_base_layout_version > 0 && txn.base_layout_version != expected_base_layout_version) {
        if (error) {
            *error = "layout version mismatch";
        }
        return false;
    }

    rocksdb::WriteBatch batch;
    for (const auto& [chunk_index, chunk_meta] : txn.chunk_updates) {
        const std::string chunk_key = ChunkKey(inode_id, chunk_index);

        std::string old_data;
        std::string local_error;
        if (store_->Get(chunk_key, &old_data, &local_error)) {
            zb::rpc::ChunkMeta old_meta;
            if (MetaCodec::DecodeChunkMeta(old_data, &old_meta)) {
                for (const auto& old_replica : old_meta.replicas()) {
                    if (!old_replica.chunk_id().empty()) {
                        batch.Delete(ReverseChunkKey(old_replica.chunk_id()));
                    }
                }
            }
        }

        batch.Put(chunk_key, MetaCodec::EncodeChunkMeta(chunk_meta));
        for (const auto& replica : chunk_meta.replicas()) {
            if (!replica.chunk_id().empty()) {
                batch.Put(ReverseChunkKey(replica.chunk_id()), chunk_key);
            }
        }
    }
    std::string local_error;
    if (batch.Count() > 0 && !store_->WriteBatch(&batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to publish pending chunks" : local_error;
        }
        return false;
    }

    LayoutRootRecord root;
    root.inode_id = inode_id;
    root.layout_root_id = "lr-" + std::to_string(inode_id) + "-" + std::to_string(txn.pending_layout_version);
    root.layout_version = txn.pending_layout_version;
    root.file_size = new_file_size;
    root.epoch = 0;
    root.update_ts = NowMilliseconds();
    if (!StoreLayoutRootAtomic(inode_id,
                               root,
                               txn.base_layout_version,
                               true,
                               new_file_size,
                               committed_root,
                               &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to commit layout root after publishing chunks" : local_error;
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(pending_write_mu_);
        auto it = pending_writes_.find(inode_id);
        if (it != pending_writes_.end() && it->second.pending_layout_version == txn.pending_layout_version) {
            pending_writes_.erase(it);
        }
    }
    return true;
}

bool MdsServiceImpl::HasReadyDiskReplica(const zb::rpc::ChunkMeta& chunk_meta) const {
    for (const auto& replica : chunk_meta.replicas()) {
        if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
            replica.replica_state() == zb::rpc::REPLICA_READY) {
            return true;
        }
    }
    return false;
}

bool MdsServiceImpl::FindReadyOpticalReplica(const zb::rpc::ChunkMeta& chunk_meta,
                                             zb::rpc::ReplicaLocation* optical) const {
    if (!optical) {
        return false;
    }
    for (const auto& replica : chunk_meta.replicas()) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
            replica.replica_state() != zb::rpc::REPLICA_READY) {
            continue;
        }
        *optical = replica;
        return true;
    }
    return false;
}

bool MdsServiceImpl::CollectRecallTasksByImage(const std::string& seed_chunk_key,
                                               const zb::rpc::ChunkMeta& seed_meta,
                                               const zb::rpc::ReplicaLocation& optical_seed,
                                               std::vector<RecallTask>* tasks,
                                               std::string* error) {
    if (!tasks || seed_chunk_key.empty()) {
        if (error) {
            *error = "invalid recall task arguments";
        }
        return false;
    }
    tasks->clear();

    auto find_matching_optical = [&](const zb::rpc::ChunkMeta& meta, zb::rpc::ReplicaLocation* matched) -> bool {
        if (!matched) {
            return false;
        }
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
                replica.replica_state() != zb::rpc::REPLICA_READY) {
                continue;
            }
            if (!optical_seed.node_id().empty() && replica.node_id() != optical_seed.node_id()) {
                continue;
            }
            if (!optical_seed.disk_id().empty() && replica.disk_id() != optical_seed.disk_id()) {
                continue;
            }
            if (!optical_seed.image_id().empty() && replica.image_id() != optical_seed.image_id()) {
                continue;
            }
            *matched = replica;
            return true;
        }
        return false;
    };

    std::unordered_set<std::string> seen_chunk_keys;
    zb::rpc::ReplicaLocation seed_optical;
    if (find_matching_optical(seed_meta, &seed_optical)) {
        RecallTask task;
        task.chunk_key = seed_chunk_key;
        task.optical_replica = seed_optical;
        tasks->push_back(std::move(task));
        seen_chunk_keys.insert(seed_chunk_key);
    }

    if (optical_seed.node_id().empty() ||
        optical_seed.disk_id().empty() ||
        optical_seed.image_id().empty()) {
        if (tasks->empty()) {
            if (error) {
                *error = "no ready optical replica available for recall";
            }
            return false;
        }
        return true;
    }

    const std::string prefix =
        ArchiveImageChunkPrefix(optical_seed.node_id(), optical_seed.disk_id(), optical_seed.image_id());
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string index_key = it->key().ToString();
        if (index_key.rfind(prefix, 0) != 0) {
            break;
        }
        const std::string chunk_key = it->value().ToString();
        if (chunk_key.empty() || !seen_chunk_keys.insert(chunk_key).second) {
            continue;
        }

        std::string chunk_data;
        std::string local_error;
        if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
            continue;
        }
        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
            continue;
        }
        zb::rpc::ReplicaLocation matched;
        if (!find_matching_optical(meta, &matched)) {
            continue;
        }
        RecallTask task;
        task.chunk_key = chunk_key;
        task.optical_replica = matched;
        tasks->push_back(std::move(task));
    }

    if (tasks->empty()) {
        if (error) {
            *error = "no chunks found for optical image recall";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::RecallTasksToDisk(const std::vector<RecallTask>& tasks, std::string* error) {
    if (tasks.empty()) {
        return true;
    }
    const zb::rpc::ReplicaLocation& optical_seed = tasks.front().optical_replica;
    if (optical_seed.node_address().empty() || optical_seed.disk_id().empty()) {
        if (error) {
            *error = "invalid optical recall target";
        }
        return false;
    }
    zb::rpc::ReplicaLocation target_base;
    target_base.set_node_id(optical_seed.node_id());
    target_base.set_node_address(optical_seed.node_address());
    target_base.set_disk_id(optical_seed.disk_id());
    target_base.set_group_id(optical_seed.group_id());
    target_base.set_epoch(optical_seed.epoch());
    target_base.set_primary_node_id(optical_seed.primary_node_id());
    target_base.set_primary_address(optical_seed.primary_address());
    target_base.set_secondary_node_id(optical_seed.secondary_node_id());
    target_base.set_secondary_address(optical_seed.secondary_address());
    target_base.set_sync_ready(optical_seed.sync_ready());

    rocksdb::WriteBatch batch;
    for (const auto& task : tasks) {
        std::string chunk_data;
        std::string local_error;
        if (!store_->Get(task.chunk_key, &chunk_data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("missing chunk meta: " + task.chunk_key) : local_error;
            }
            return false;
        }

        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
            if (error) {
                *error = "invalid chunk meta: " + task.chunk_key;
            }
            return false;
        }
        if (HasReadyDiskReplica(meta)) {
            continue;
        }

        zb::rpc::ReplicaLocation source = task.optical_replica;
        bool found_source = false;
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
                replica.replica_state() != zb::rpc::REPLICA_READY) {
                continue;
            }
            if (!task.optical_replica.node_id().empty() && replica.node_id() != task.optical_replica.node_id()) {
                continue;
            }
            if (!task.optical_replica.disk_id().empty() && replica.disk_id() != task.optical_replica.disk_id()) {
                continue;
            }
            if (!task.optical_replica.image_id().empty() && replica.image_id() != task.optical_replica.image_id()) {
                continue;
            }
            source = replica;
            found_source = true;
            break;
        }
        if (!found_source) {
            continue;
        }

        uint64_t read_size = source.size();
        if (read_size == 0) {
            uint64_t inode_id = 0;
            uint32_t chunk_index = 0;
            if (ParseChunkKey(task.chunk_key, &inode_id, &chunk_index)) {
                zb::rpc::InodeAttr attr;
                if (GetInode(inode_id, &attr, &local_error)) {
                    read_size = attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_;
                }
            }
        }
        if (read_size == 0) {
            read_size = default_chunk_size_;
        }

        std::string data;
        if (!ReadChunkFromReplica(source, read_size, &data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("failed to read recalled chunk from optical: " + source.chunk_id())
                                             : local_error;
            }
            return false;
        }

        const std::string recalled_chunk_id = GenerateChunkId();
        if (!WriteChunkToReplica(target_base, recalled_chunk_id, data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("failed to write recalled chunk to disk: " + recalled_chunk_id)
                                             : local_error;
            }
            return false;
        }

        zb::rpc::ReplicaLocation* disk_replica = meta.add_replicas();
        disk_replica->set_node_id(target_base.node_id());
        disk_replica->set_node_address(target_base.node_address());
        disk_replica->set_disk_id(target_base.disk_id());
        disk_replica->set_chunk_id(recalled_chunk_id);
        disk_replica->set_size(static_cast<uint64_t>(data.size()));
        disk_replica->set_group_id(target_base.group_id());
        disk_replica->set_epoch(target_base.epoch());
        disk_replica->set_primary_node_id(target_base.primary_node_id());
        disk_replica->set_primary_address(target_base.primary_address());
        disk_replica->set_secondary_node_id(target_base.secondary_node_id());
        disk_replica->set_secondary_address(target_base.secondary_address());
        disk_replica->set_sync_ready(target_base.sync_ready());
        disk_replica->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        disk_replica->set_replica_state(zb::rpc::REPLICA_READY);

        batch.Put(task.chunk_key, MetaCodec::EncodeChunkMeta(meta));
        batch.Put(ReverseChunkKey(recalled_chunk_id), task.chunk_key);
    }

    if (batch.Count() == 0) {
        return true;
    }
    std::string write_error;
    if (!store_->WriteBatch(&batch, &write_error)) {
        if (error) {
            *error = write_error.empty() ? "failed to persist recalled chunk metadata" : write_error;
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::CacheWholeFileToDisk(uint64_t inode_id, std::string* error) {
    if (inode_id == 0) {
        if (error) {
            *error = "invalid inode id";
        }
        return false;
    }
    if (!allocator_) {
        if (error) {
            *error = "chunk allocator is unavailable";
        }
        return false;
    }

    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found while caching file" : local_error;
        }
        return false;
    }
    const uint64_t inode_chunk_size = attr.chunk_size() > 0 ? attr.chunk_size() : default_chunk_size_;

    std::vector<zb::rpc::ReplicaLocation> allocated;
    if (!allocator_->AllocateChunk(1, GenerateChunkId(), &allocated) || allocated.empty()) {
        if (error) {
            *error = "failed to allocate file cache target node";
        }
        return false;
    }
    const zb::rpc::ReplicaLocation target_base = allocated.front();

    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string prefix = ChunkPrefix(inode_id);
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string chunk_key = it->key().ToString();
        if (chunk_key.rfind(prefix, 0) != 0) {
            break;
        }

        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(it->value().ToString(), &meta)) {
            if (error) {
                *error = "invalid chunk meta while caching file";
            }
            return false;
        }

        bool has_target_disk = false;
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY &&
                replica.node_id() == target_base.node_id() &&
                replica.disk_id() == target_base.disk_id()) {
                has_target_disk = true;
                break;
            }
        }
        if (has_target_disk) {
            continue;
        }

        const zb::rpc::ReplicaLocation* source = nullptr;
        for (const auto& replica : meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            for (const auto& replica : meta.replicas()) {
                if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
                    replica.replica_state() == zb::rpc::REPLICA_READY) {
                    source = &replica;
                    break;
                }
            }
        }
        if (!source) {
            if (error) {
                *error = "file cache source replica not found";
            }
            return false;
        }

        uint64_t read_size = source->size() > 0 ? source->size() : inode_chunk_size;
        if (read_size == 0) {
            read_size = default_chunk_size_;
        }

        std::string data;
        if (!ReadChunkFromReplica(*source, read_size, &data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to read source chunk while caching file" : local_error;
            }
            return false;
        }

        const std::string cached_chunk_id = GenerateChunkId();
        if (!WriteChunkToReplica(target_base, cached_chunk_id, data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to write file cache chunk to disk node" : local_error;
            }
            return false;
        }

        zb::rpc::ReplicaLocation* disk_replica = meta.add_replicas();
        disk_replica->set_node_id(target_base.node_id());
        disk_replica->set_node_address(target_base.node_address());
        disk_replica->set_disk_id(target_base.disk_id());
        disk_replica->set_chunk_id(cached_chunk_id);
        disk_replica->set_size(static_cast<uint64_t>(data.size()));
        disk_replica->set_group_id(target_base.group_id());
        disk_replica->set_epoch(target_base.epoch());
        disk_replica->set_primary_node_id(target_base.primary_node_id());
        disk_replica->set_primary_address(target_base.primary_address());
        disk_replica->set_secondary_node_id(target_base.secondary_node_id());
        disk_replica->set_secondary_address(target_base.secondary_address());
        disk_replica->set_sync_ready(target_base.sync_ready());
        disk_replica->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        disk_replica->set_replica_state(zb::rpc::REPLICA_READY);

        batch.Put(chunk_key, MetaCodec::EncodeChunkMeta(meta));
        batch.Put(ReverseChunkKey(cached_chunk_id), chunk_key);
    }

    if (batch.Count() == 0) {
        return true;
    }
    if (!store_->WriteBatch(&batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist cached file metadata" : local_error;
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::EnsureChunkReadableFromDisk(const std::string& chunk_key,
                                                 zb::rpc::ChunkMeta* chunk_meta,
                                                 std::unordered_set<std::string>* recalled_images,
                                                 bool* recalled_from_optical,
                                                 std::string* error) {
    if (recalled_from_optical) {
        *recalled_from_optical = false;
    }
    if (!chunk_meta) {
        if (error) {
            *error = "chunk meta pointer is null";
        }
        return false;
    }
    if (HasReadyDiskReplica(*chunk_meta)) {
        return true;
    }

    zb::rpc::ReplicaLocation optical;
    if (!FindReadyOpticalReplica(*chunk_meta, &optical)) {
        return true;
    }

    std::string image_key;
    if (!optical.node_id().empty() && !optical.disk_id().empty() && !optical.image_id().empty()) {
        image_key = optical.node_id() + "|" + optical.disk_id() + "|" + optical.image_id();
    }

    if (image_key.empty() || !recalled_images || recalled_images->find(image_key) == recalled_images->end()) {
        std::vector<RecallTask> tasks;
        if (!CollectRecallTasksByImage(chunk_key, *chunk_meta, optical, &tasks, error)) {
            return false;
        }
        if (!RecallTasksToDisk(tasks, error)) {
            return false;
        }
        if (!image_key.empty() && recalled_images) {
            recalled_images->insert(image_key);
        }
        if (recalled_from_optical) {
            *recalled_from_optical = true;
        }
    }

    std::string updated_data;
    std::string local_error;
    if (!store_->Get(chunk_key, &updated_data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to reload chunk metadata after recall" : local_error;
        }
        return false;
    }
    if (!MetaCodec::DecodeChunkMeta(updated_data, chunk_meta)) {
        if (error) {
            *error = "invalid chunk metadata after recall";
        }
        return false;
    }
    if (!HasReadyDiskReplica(*chunk_meta)) {
        if (error) {
            *error = "optical recall completed but no disk replica available";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::ReadChunkFromReplica(const zb::rpc::ReplicaLocation& source,
                                          uint64_t read_size,
                                          std::string* data,
                                          std::string* error) {
    if (!data) {
        if (error) {
            *error = "read output buffer is null";
        }
        return false;
    }
    brpc::Channel* channel = GetDataChannel(source.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::ReadChunkRequest req;
    req.set_disk_id(source.disk_id());
    req.set_chunk_id(source.chunk_id());
    req.set_offset(0);
    req.set_size(read_size);
    if (!source.image_id().empty()) {
        req.set_image_id(source.image_id());
        req.set_image_offset(source.image_offset());
        req.set_image_length(source.image_length());
    }

    zb::rpc::ReadChunkReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    stub.ReadChunk(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    *data = resp.data();
    return true;
}

bool MdsServiceImpl::WriteChunkToReplica(const zb::rpc::ReplicaLocation& target,
                                         const std::string& chunk_id,
                                         const std::string& data,
                                         std::string* error) {
    brpc::Channel* channel = GetDataChannel(target.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteChunkRequest req;
    req.set_disk_id(target.disk_id());
    req.set_chunk_id(chunk_id);
    req.set_offset(0);
    req.set_data(data);
    req.set_epoch(target.epoch());

    zb::rpc::WriteChunkReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    stub.WriteChunk(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

brpc::Channel* MdsServiceImpl::GetDataChannel(const std::string& address, std::string* error) {
    if (address.empty()) {
        if (error) {
            *error = "empty target address";
        }
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(channel_mu_);
    auto it = channels_.find(address);
    if (it != channels_.end()) {
        return it->second.get();
    }

    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    options.max_retry = 0;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) {
            *error = "failed to init channel to " + address;
        }
        return nullptr;
    }
    brpc::Channel* raw = channel.get();
    channels_[address] = std::move(channel);
    return raw;
}

uint64_t MdsServiceImpl::AllocateInodeId(std::string* error) {
    std::string value;
    uint64_t next_id = kRootInodeId + 1;
    if (store_->Get(NextInodeKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "Invalid next inode value";
            }
            return 0;
        }
    } else if (error && !error->empty()) {
        return 0;
    }

    uint64_t allocated = next_id;
    uint64_t new_value = next_id + 1;
    if (!store_->Put(NextInodeKey(), MetaCodec::EncodeUInt64(new_value), error)) {
        return 0;
    }
    return allocated;
}

uint64_t MdsServiceImpl::AllocateHandleId(std::string* error) {
    std::string value;
    uint64_t next_id = 1;
    if (store_->Get(NextHandleKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "Invalid next handle value";
            }
            return 0;
        }
    } else if (error && !error->empty()) {
        return 0;
    }

    uint64_t allocated = next_id;
    uint64_t new_value = next_id + 1;
    if (!store_->Put(NextHandleKey(), MetaCodec::EncodeUInt64(new_value), error)) {
        return 0;
    }
    return allocated;
}

std::string MdsServiceImpl::GenerateChunkId() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static const char kHex[] = "0123456789abcdef";
    std::string out(32, '0');
    for (size_t i = 0; i < out.size(); i += 16) {
        uint64_t value = rng();
        for (size_t j = 0; j < 16; ++j) {
            out[i + j] = kHex[(value >> ((15 - j) * 4)) & 0xF];
        }
    }
    return out;
}

uint64_t MdsServiceImpl::NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

uint64_t MdsServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void MdsServiceImpl::FillStatus(zb::rpc::MdsStatus* status,
                                zb::rpc::MdsStatusCode code,
                                const std::string& message) {
    if (!status) {
        return;
    }
    status->set_code(code);
    status->set_message(message);
}

} // namespace zb::mds
