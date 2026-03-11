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

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

std::string ArchiveObjectId(const zb::rpc::ArchiveCandidate& candidate) {
    return candidate.object_id();
}

void EnsureReplicaObjectId(zb::rpc::ReplicaLocation* replica, const std::string& object_id) {
    if (!replica || object_id.empty()) {
        return;
    }
    replica->set_object_id(object_id);
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

uint32_t ResolveExtentObjectIndex(const LayoutExtentRecord& extent, uint64_t object_unit_size) {
    if (extent.object_index > 0 || extent.logical_offset == 0 || object_unit_size == 0) {
        return extent.object_index;
    }
    return static_cast<uint32_t>(extent.logical_offset / object_unit_size);
}

} // namespace

MdsServiceImpl::MdsServiceImpl(RocksMetaStore* store,
                               ObjectAllocator* allocator,
                               uint64_t default_object_unit_size,
                               ArchiveCandidateQueue* candidate_queue,
                               ArchiveLeaseManager* lease_manager)
    : store_(store),
      allocator_(allocator),
      default_object_unit_size_(default_object_unit_size),
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

void MdsServiceImpl::SetSimplifiedAnchorMetadataMode(bool enabled) {
    simplified_anchor_metadata_mode_ = enabled;
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
    attr.set_object_unit_size(request->object_unit_size() ? request->object_unit_size() : default_object_unit_size_);
    attr.set_replica(request->replica() ? request->replica() : 1);
    attr.set_version(1);

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(attr));
    if (simplified_anchor_metadata_mode_) {
        zb::rpc::ReplicaLocation anchor;
        if (!SelectFileAnchor(inode_id, attr, &anchor, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
            return;
        }
        if (!SaveFileAnchor(inode_id, anchor, &batch)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to encode file anchor");
            return;
        }
    } else {
        LayoutRootRecord root;
        root.inode_id = inode_id;
        root.layout_version = 1;
        root.layout_root_id = "lr-" + std::to_string(inode_id) + "-1";
        root.file_size = 0;
        root.epoch = 0;
        root.update_ts = NowMilliseconds();

        LayoutNodeRecord node;
        node.node_id = root.layout_root_id;
        node.level = 0;
        node.extents.clear();
        node.child_layout_ids.clear();

        batch.Put(LayoutRootKey(inode_id), MetaCodec::EncodeLayoutRoot(root));
        if (!StoreLayoutNodeWithReplicas(root.layout_root_id, node, &batch, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
            return;
        }
    }
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
    attr.set_object_unit_size(default_object_unit_size_);
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

    if (simplified_anchor_metadata_mode_) {
        attr.set_atime(NowSeconds());
        std::string put_error;
        PutInode(attr.inode_id(), attr, &put_error);
        error.clear();
        if (!BuildReadPlanFromAnchor(request->inode_id(),
                                     request->offset(),
                                     request->size(),
                                     response->mutable_layout(),
                                     &error)) {
            FillStatus(response->mutable_status(),
                       error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                       error.empty() ? "failed to build anchor write plan" : error);
            return;
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    LayoutRootRecord current_root;
    if (!LoadLayoutRoot(request->inode_id(), &current_root, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    const uint64_t object_unit_size = attr.object_unit_size() ? attr.object_unit_size() : default_object_unit_size_;
    if (object_unit_size == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "object unit size is zero");
        return;
    }

    LayoutNodeRecord base_node;
    bool recovered = false;
    if (!LoadHealthyLayoutNode(attr.inode_id(),
                               current_root.layout_root_id,
                               current_root.layout_version,
                               &base_node,
                               &recovered,
                               &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to load current layout object" : error);
        return;
    }

    attr.set_atime(NowSeconds());
    error.clear();
    PutInode(attr.inode_id(), attr, &error);

    const uint64_t start = request->offset() / object_unit_size;
    const uint64_t end = (request->offset() + request->size() - 1) / object_unit_size;
    const uint32_t desired_replica = attr.replica() ? attr.replica() : 1;

    std::unordered_map<uint32_t, LayoutExtentRecord> extent_by_index;
    extent_by_index.reserve(base_node.extents.size() + static_cast<size_t>(end - start + 1));
    for (const auto& extent : base_node.extents) {
        if (extent.object_id.empty() || extent.length == 0) {
            continue;
        }
        LayoutExtentRecord normalized = extent;
        if (normalized.object_index == 0 && normalized.logical_offset > 0) {
            normalized.object_index = static_cast<uint32_t>(normalized.logical_offset / object_unit_size);
        }
        if (normalized.length == 0) {
            normalized.length = object_unit_size;
        }
        extent_by_index[normalized.object_index] = normalized;
    }

    PendingWriteTransaction pending;
    pending.inode_id = attr.inode_id();
    pending.base_layout_version = std::max<uint64_t>(1, current_root.layout_version);
    pending.pending_layout_version = pending.base_layout_version + 1;
    pending.create_ts_ms = NowMilliseconds();

    zb::rpc::FileLayout* layout = response->mutable_layout();
    layout->set_inode_id(attr.inode_id());
    layout->set_object_unit_size(object_unit_size);

    for (uint64_t index = start; index <= end; ++index) {
        const uint32_t object_index = static_cast<uint32_t>(index);
        const uint64_t object_start = static_cast<uint64_t>(object_index) * object_unit_size;
        const uint64_t object_end = object_start + object_unit_size;
        const uint64_t write_start = std::max<uint64_t>(object_start, request->offset());
        const uint64_t write_end = std::min<uint64_t>(object_end, request->offset() + request->size());
        const bool full_object_overwrite = (write_start <= object_start && write_end >= object_end);

        const std::string object_id = GenerateObjectId();
        std::vector<zb::rpc::ReplicaLocation> new_replicas;
        std::string local_error;
        if (!ResolveObjectReplicas(desired_replica,
                                   object_id,
                                   current_root.epoch,
                                   &new_replicas,
                                   &local_error) ||
            new_replicas.size() < desired_replica) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       local_error.empty() ? "failed to resolve object replicas from PG" : local_error);
            return;
        }

        auto old_it = extent_by_index.find(object_index);
        if (old_it != extent_by_index.end() && !full_object_overwrite) {
            if (!SeedObjectForCowWrite(old_it->second, new_replicas, object_unit_size, &local_error)) {
                FillStatus(response->mutable_status(),
                           zb::rpc::MDS_INTERNAL_ERROR,
                           local_error.empty() ? "failed to seed old object for COW write" : local_error);
                return;
            }
        }

        zb::rpc::ObjectMeta staged_meta;
        staged_meta.set_index(object_index);
        for (const auto& replica : new_replicas) {
            *staged_meta.add_replicas() = replica;
        }
        *layout->add_objects() = staged_meta;

        LayoutExtentRecord updated_extent;
        updated_extent.logical_offset = object_start;
        updated_extent.length = object_unit_size;
        updated_extent.object_id = object_id;
        updated_extent.object_offset = 0;
        updated_extent.object_length = object_unit_size;
        updated_extent.object_version = pending.pending_layout_version;
        updated_extent.object_index = object_index;
        updated_extent.pg_id = 0;
        updated_extent.placement_epoch = !new_replicas.empty() ? new_replicas.front().epoch() : current_root.epoch;
        updated_extent.storage_tier = static_cast<uint32_t>(zb::rpc::STORAGE_TIER_DISK);
        extent_by_index[object_index] = std::move(updated_extent);
    }

    pending.pending_layout_node.node_id =
        "lr-" + std::to_string(attr.inode_id()) + "-" + std::to_string(pending.pending_layout_version);
    pending.pending_layout_node.level = 0;
    pending.pending_layout_node.extents.clear();
    pending.pending_layout_node.extents.reserve(extent_by_index.size());
    for (auto& [object_index, extent] : extent_by_index) {
        extent.object_index = object_index;
        if (extent.length == 0) {
            extent.length = object_unit_size;
        }
        if (extent.object_length == 0) {
            extent.object_length = extent.length;
        }
        pending.pending_layout_node.extents.push_back(std::move(extent));
    }
    std::sort(pending.pending_layout_node.extents.begin(),
              pending.pending_layout_node.extents.end(),
              [](const LayoutExtentRecord& lhs, const LayoutExtentRecord& rhs) {
                  return lhs.logical_offset < rhs.logical_offset;
              });

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
        response->set_plan_source(zb::rpc::READ_PLAN_SOURCE_OPTICAL);
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    if (!BuildReadPlanWithPolicy(request->inode_id(),
                                 request->offset(),
                                 request->size(),
                                 response->mutable_layout(),
                                 &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to build read plan" : error);
        return;
    }
    response->set_plan_source(zb::rpc::READ_PLAN_SOURCE_LAYOUT);
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
    FillStatus(response->mutable_status(),
               zb::rpc::MDS_INVALID_ARGUMENT,
               "CommitWrite is deprecated in pure layout mode; use CommitLayoutRoot");
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
    std::string error;
    if (!LoadLayoutRoot(request->inode_id(), &root, &error)) {
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
    response->mutable_root()->set_object_unit_size(attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
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
    std::string error;
    if (!LoadLayoutRoot(request->inode_id(), &root, &error)) {
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
        response->set_plan_source(zb::rpc::READ_PLAN_SOURCE_OPTICAL);
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    if (!BuildReadPlanWithPolicy(request->inode_id(),
                                 request->offset(),
                                 request->size(),
                                 read_plan,
                                 &error)) {
        FillStatus(response->mutable_status(),
                   error == "inode not found" ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to resolve layout" : error);
        return;
    }
    response->set_plan_source(zb::rpc::READ_PLAN_SOURCE_LAYOUT);

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
        out->set_object_index(extent.object_index);
        out->set_pg_id(extent.pg_id);
        out->set_placement_epoch(extent.placement_epoch);
        out->set_storage_tier(static_cast<zb::rpc::StorageTier>(extent.storage_tier));
    }

    FillLayoutRootMessage(root, response->mutable_root());
    response->mutable_root()->set_object_unit_size(read_plan->object_unit_size());
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

    if (simplified_anchor_metadata_mode_) {
        const std::string op_id = request->op_id();
        std::string error;
        if (!op_id.empty()) {
            LayoutRootRecord already_committed;
            error.clear();
            if (LoadCommittedLayoutRootByOpId(request->inode_id(), op_id, &already_committed, &error)) {
                FillLayoutRootMessage(already_committed, response->mutable_root());
                zb::rpc::InodeAttr attr;
                error.clear();
                if (GetInode(request->inode_id(), &attr, &error)) {
                    response->mutable_root()->set_object_unit_size(
                        attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
                }
                FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
                return;
            }
            if (!error.empty() && error != "commit op result not found") {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
                return;
            }
        }

        zb::rpc::InodeAttr attr;
        error.clear();
        if (!GetInode(request->inode_id(), &attr, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
            return;
        }

        const uint64_t current_version = std::max<uint64_t>(attr.version(), 1);
        if (request->expected_layout_version() > 0 &&
            request->expected_layout_version() != current_version) {
            layout_commit_conflict_total_.fetch_add(1, std::memory_order_relaxed);
            FillStatus(response->mutable_status(), zb::rpc::MDS_STALE_EPOCH, "layout version mismatch");
            return;
        }

        const uint64_t now_sec = NowSeconds();
        attr.set_mtime(now_sec);
        attr.set_ctime(now_sec);
        if (request->update_inode_size()) {
            attr.set_size(request->new_size());
        } else if (request->root().file_size() > 0 || attr.size() == 0) {
            attr.set_size(request->root().file_size());
        }
        attr.set_version(current_version + 1);

        LayoutRootRecord committed;
        committed.inode_id = request->inode_id();
        committed.layout_root_id = "fa-" + std::to_string(request->inode_id());
        committed.layout_version = attr.version();
        committed.file_size = attr.size();
        committed.epoch = 0;
        committed.update_ts = NowMilliseconds();
        zb::rpc::ReplicaLocation anchor;
        std::string anchor_error;
        if (LoadFileAnchor(request->inode_id(), &anchor, &anchor_error)) {
            committed.epoch = anchor.epoch();
        }

        rocksdb::WriteBatch batch;
        batch.Put(InodeKey(request->inode_id()), MetaCodec::EncodeInodeAttr(attr));
        if (!op_id.empty()) {
            batch.Put(LayoutCommitOpKey(request->inode_id(), op_id), MetaCodec::EncodeLayoutRoot(committed));
        }
        error.clear();
        if (!store_->WriteBatch(&batch, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
            return;
        }
        {
            std::lock_guard<std::mutex> lock(pending_write_mu_);
            pending_writes_.erase(request->inode_id());
        }
        FillLayoutRootMessage(committed, response->mutable_root());
        response->mutable_root()->set_object_unit_size(
            attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }

    LayoutRootRecord root;
    root.inode_id = request->inode_id();
    root.layout_root_id = request->root().layout_root_id();
    root.layout_version = request->root().layout_version();
    root.file_size = request->root().file_size();
    root.epoch = request->root().epoch();
    root.update_ts = request->root().update_ts();
    const std::string op_id = request->op_id();
    std::string error;

    if (!op_id.empty()) {
        LayoutRootRecord already_committed;
        error.clear();
        if (LoadCommittedLayoutRootByOpId(request->inode_id(), op_id, &already_committed, &error)) {
            layout_commit_retry_total_.fetch_add(1, std::memory_order_relaxed);
            FillLayoutRootMessage(already_committed, response->mutable_root());
            zb::rpc::InodeAttr attr;
            error.clear();
            if (GetInode(request->inode_id(), &attr, &error)) {
                response->mutable_root()->set_object_unit_size(attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
            }
            FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
            return;
        }
        if (!error.empty() && error != "commit op result not found") {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
            return;
        }
    }

    LayoutRootRecord committed;
    if (ConsumePendingWriteForCommit(request->inode_id(),
                                     op_id,
                                     request->expected_layout_version(),
                                     request->update_inode_size() ? request->new_size() : root.file_size,
                                     &committed,
                                     &error)) {
        FillLayoutRootMessage(committed, response->mutable_root());
        zb::rpc::InodeAttr attr;
        error.clear();
        if (GetInode(request->inode_id(), &attr, &error)) {
            response->mutable_root()->set_object_unit_size(attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
        return;
    }
    if (!error.empty() && error != "pending write txn not found") {
        if (error == "layout version mismatch") {
            layout_commit_conflict_total_.fetch_add(1, std::memory_order_relaxed);
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
                               op_id.empty() ? nullptr : &op_id,
                               nullptr,
                               &committed,
                               &error)) {
        if (error == "layout version mismatch") {
            layout_commit_conflict_total_.fetch_add(1, std::memory_order_relaxed);
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
        response->mutable_root()->set_object_unit_size(attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_);
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
        const std::string object_id = ArchiveObjectId(item);
        candidate.SetArchiveObjectId(object_id);
        candidate.last_access_ts_ms = item.last_access_ts_ms();
        candidate.size_bytes = item.size_bytes();
        candidate.checksum = item.checksum();
        candidate.heat_score = item.heat_score();
        candidate.archive_state = item.archive_state().empty() ? "pending" : item.archive_state();
        candidate.version = item.version();
        candidate.score = item.score();
        candidate.report_ts_ms = item.report_ts_ms() > 0 ? item.report_ts_ms() : fallback_report_ts;

        if (candidate.disk_id.empty() || candidate.ArchiveObjectId().empty()) {
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
    root.set_object_unit_size(default_object_unit_size_);
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
    std::string prefix = ObjectPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    rocksdb::WriteBatch batch;
    batch.Delete(FileAnchorKey(inode_id));
    std::unordered_set<std::string> object_ids;
    std::unordered_set<std::string> image_index_keys;
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        zb::rpc::ObjectMeta meta;
        if (MetaCodec::DecodeObjectMeta(it->value().ToString(), &meta)) {
            for (const auto& replica : meta.replicas()) {
                const std::string object_id = ReplicaObjectId(replica);
                if (!object_id.empty()) {
                    object_ids.insert(object_id);
                }
                if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
                    !replica.node_id().empty() &&
                    !replica.disk_id().empty() &&
                    !replica.image_id().empty() &&
                    !object_id.empty()) {
                    image_index_keys.insert(ArchiveImageObjectKey(replica.node_id(),
                                                                  replica.disk_id(),
                                                                  replica.image_id(),
                                                                  object_id));
                }
            }
        }
        batch.Delete(it->key());
    }
    std::string root_data;
    std::string local_error;
    if (store_->Get(LayoutRootKey(inode_id), &root_data, &local_error)) {
        LayoutRootRecord root;
        if (MetaCodec::DecodeLayoutRoot(root_data, &root) && !root.layout_root_id.empty()) {
            std::string layout_data;
            std::string layout_error;
            if (store_->Get(LayoutObjectKey(root.layout_root_id), &layout_data, &layout_error)) {
                LayoutNodeRecord node;
                if (MetaCodec::DecodeLayoutNode(layout_data, &node)) {
                    for (const auto& extent : node.extents) {
                        if (!extent.object_id.empty()) {
                            object_ids.insert(extent.object_id);
                        }
                    }
                }
            }
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
    const std::string commit_prefix = LayoutCommitOpPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> commit_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (commit_it->Seek(commit_prefix); commit_it->Valid(); commit_it->Next()) {
        const std::string key = commit_it->key().ToString();
        if (key.rfind(commit_prefix, 0) != 0) {
            break;
        }
        batch.Delete(key);
    }
    for (const auto& object_id : object_ids) {
        batch.Delete(ReverseObjectKey(object_id));
        batch.Delete(ObjectArchiveStateKey(object_id));
        batch.Delete(ArchiveReverseObjectRepairKey(object_id));
        batch.Delete(ArchiveObjectOpticalLocationKey(object_id));
        batch.Delete(ObjectOwnerKey(object_id));
    }
    for (const auto& image_key : image_index_keys) {
        batch.Delete(image_key);
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

    if (simplified_anchor_metadata_mode_) {
        root->inode_id = inode_id;
        root->layout_root_id = "fa-" + std::to_string(inode_id);
        root->layout_version = std::max<uint64_t>(1, inode_attr.version());
        root->file_size = inode_attr.size();
        root->epoch = 0;
        root->update_ts = inode_attr.mtime() > 0 ? inode_attr.mtime() * 1000 : NowMilliseconds();
        zb::rpc::ReplicaLocation anchor;
        local_error.clear();
        if (LoadFileAnchor(inode_id, &anchor, &local_error)) {
            root->epoch = anchor.epoch();
        }
        return true;
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
            if (error) {
                *error = "layout root id is empty";
            }
            return false;
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
    if (error) {
        *error = "layout root not found";
    }
    return false;
}

bool MdsServiceImpl::StoreLayoutRootAtomic(uint64_t inode_id,
                                           const LayoutRootRecord& root,
                                           uint64_t expected_layout_version,
                                           bool update_inode_size,
                                           uint64_t new_size,
                                           const std::string* commit_op_id,
                                           const LayoutNodeRecord* layout_node_override,
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
    if (!LoadLayoutRoot(inode_id, &existing, &local_error)) {
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

    if (next.layout_root_id.empty() || next.layout_root_id.rfind("inline:", 0) == 0) {
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
    const uint64_t inode_object_unit_size = inode_attr.object_unit_size() > 0 ? inode_attr.object_unit_size() : default_object_unit_size_;

    rocksdb::WriteBatch batch;
    batch.Put(LayoutRootKey(inode_id), MetaCodec::EncodeLayoutRoot(next));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(inode_attr));
    if (commit_op_id && !commit_op_id->empty()) {
        batch.Put(LayoutCommitOpKey(inode_id, *commit_op_id), MetaCodec::EncodeLayoutRoot(next));
    }

    LayoutNodeRecord layout_node;
    if (layout_node_override) {
        layout_node = *layout_node_override;
    } else if (!existing.layout_root_id.empty()) {
        bool recovered = false;
        if (!LoadHealthyLayoutNode(inode_id,
                                   existing.layout_root_id,
                                   existing.layout_version,
                                   &layout_node,
                                   &recovered,
                                   &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to load previous layout object snapshot" : local_error;
            }
            return false;
        }
    } else {
        layout_node.node_id = next.layout_root_id;
        layout_node.level = 0;
        layout_node.extents.clear();
        layout_node.child_layout_ids.clear();
    }
    layout_node.node_id = next.layout_root_id;
    std::sort(layout_node.extents.begin(),
              layout_node.extents.end(),
              [](const LayoutExtentRecord& lhs, const LayoutExtentRecord& rhs) {
                  return lhs.logical_offset < rhs.logical_offset;
              });
    for (const auto& extent : layout_node.extents) {
        if (extent.object_id.empty()) {
            continue;
        }
        const uint32_t object_index = ResolveExtentObjectIndex(extent, inode_object_unit_size);
        batch.Put(ObjectOwnerKey(extent.object_id), EncodeObjectOwnerValue(inode_id, object_index));
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

bool MdsServiceImpl::LoadCommittedLayoutRootByOpId(uint64_t inode_id,
                                                   const std::string& op_id,
                                                   LayoutRootRecord* committed,
                                                   std::string* error) {
    if (inode_id == 0 || op_id.empty() || !committed) {
        if (error) {
            *error = "invalid commit op lookup args";
        }
        return false;
    }
    std::string data;
    std::string local_error;
    if (!store_->Get(LayoutCommitOpKey(inode_id, op_id), &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "commit op result not found" : local_error;
        }
        return false;
    }
    if (!MetaCodec::DecodeLayoutRoot(data, committed)) {
        if (error) {
            *error = "invalid commit op layout root record";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::BuildLayoutNodeFromObjects(uint64_t inode_id,
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
    const uint64_t object_unit_size = attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_;
    if (object_unit_size == 0) {
        if (error) {
            *error = "object unit size is zero";
        }
        return false;
    }

    node->node_id = layout_obj_id;
    node->level = 0;
    node->extents.clear();
    node->child_layout_ids.clear();

    const std::string prefix = ObjectPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        zb::rpc::ObjectMeta object_meta;
        if (!MetaCodec::DecodeObjectMeta(it->value().ToString(), &object_meta)) {
            continue;
        }
        const zb::rpc::ReplicaLocation* source = nullptr;
        for (const auto& replica : object_meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY &&
                !ReplicaObjectId(replica).empty()) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            for (const auto& replica : object_meta.replicas()) {
                if (!ReplicaObjectId(replica).empty()) {
                    source = &replica;
                    break;
                }
            }
        }
        if (!source) {
            continue;
        }
        LayoutExtentRecord extent;
        extent.logical_offset = static_cast<uint64_t>(object_meta.index()) * object_unit_size;
        extent.length = object_unit_size;
        extent.object_id = ReplicaObjectId(*source);
        extent.object_offset = 0;
        extent.object_length = object_unit_size;
        extent.object_version = object_version;
        extent.object_index = object_meta.index();
        extent.pg_id = 0;
        extent.placement_epoch = source->epoch();
        extent.storage_tier = static_cast<uint32_t>(source->storage_tier());
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
    (void)inode_id;
    (void)object_version;
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
        if (error) {
            *error = "layout object cannot be loaded";
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
    if (root.layout_root_id.empty()) {
        if (error) {
            *error = "layout root id is empty";
        }
        return false;
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
    if (simplified_anchor_metadata_mode_) {
        return true;
    }

    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }
    LayoutRootRecord root;
    if (!LoadLayoutRoot(inode_id, &root, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load layout root" : local_error;
        }
        return false;
    }
    if (root.layout_root_id.empty()) {
        if (error) {
            *error = "layout root id is empty";
        }
        return false;
    }

    LayoutNodeRecord node;
    bool recovered = false;
    if (!LoadHealthyLayoutNode(inode_id, root.layout_root_id, root.layout_version, &node, &recovered, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load layout object" : local_error;
        }
        return false;
    }

    const uint64_t request_end = offset + size;
    bool need_optical = false;
    bool all_objects_archived = true;
    zb::rpc::ReplicaLocation anchor;

    for (const auto& extent : node.extents) {
        const uint64_t extent_start = extent.logical_offset;
        const uint64_t extent_end = extent.logical_offset + extent.length;
        if (extent_end <= offset || extent_start >= request_end) {
            continue;
        }
        if (extent.object_id.empty()) {
            if (error) {
                *error = "layout extent object_id is empty";
            }
            return false;
        }

        std::string state_data;
        local_error.clear();
        if (!store_->Get(ObjectArchiveStateKey(extent.object_id), &state_data, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            all_objects_archived = false;
            continue;
        }

        zb::rpc::ArchiveLeaseRecord record;
        if (!record.ParseFromString(state_data)) {
            if (error) {
                *error = "invalid archive state record for object " + extent.object_id;
            }
            return false;
        }
        if (record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVED) {
            all_objects_archived = false;
            continue;
        }

        std::string location_data;
        local_error.clear();
        if (!store_->Get(ArchiveObjectOpticalLocationKey(extent.object_id), &location_data, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            all_objects_archived = false;
            continue;
        }

        zb::rpc::ReplicaLocation optical;
        if (!optical.ParseFromString(location_data)) {
            if (error) {
                *error = "invalid optical location index for object " + extent.object_id;
            }
            return false;
        }
        if (optical.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
            optical.replica_state() != zb::rpc::REPLICA_READY ||
            optical.node_address().empty() ||
            optical.disk_id().empty()) {
            if (error) {
                *error = "optical location is not ready for object " + extent.object_id;
            }
            return false;
        }
        need_optical = true;
        if (anchor.node_address().empty()) {
            anchor = optical;
            continue;
        }
        if (anchor.node_address() != optical.node_address() ||
            anchor.disk_id() != optical.disk_id() ||
            anchor.image_id() != optical.image_id()) {
            if (error) {
                *error = "requested range spans multiple optical discs";
            }
            return false;
        }
    }

    if (!need_optical || !all_objects_archived) {
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

bool MdsServiceImpl::BuildReadPlanWithPolicy(uint64_t inode_id,
                                             uint64_t offset,
                                             uint64_t size,
                                             zb::rpc::FileLayout* layout,
                                             std::string* error) {
    std::string layout_error;
    if (BuildReadPlanFromLayout(inode_id, offset, size, layout, &layout_error)) {
        layout_read_hit_total_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    pg_resolve_fail_total_.fetch_add(1, std::memory_order_relaxed);

    if (error) {
        *error = layout_error.empty() ? "layout read plan unavailable in pure layout mode" : layout_error;
    }
    return false;
}

bool MdsServiceImpl::BuildReadPlanFromLayout(uint64_t inode_id,
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
    if (simplified_anchor_metadata_mode_) {
        return BuildReadPlanFromAnchor(inode_id, offset, size, layout, error);
    }

    zb::rpc::InodeAttr attr;
    std::string local_error;
    if (!GetInode(inode_id, &attr, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "inode not found" : local_error;
        }
        return false;
    }
    const uint64_t object_unit_size = attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_;
    if (object_unit_size == 0) {
        if (error) {
            *error = "object_unit_size is zero";
        }
        return false;
    }

    LayoutRootRecord root;
    if (!LoadLayoutRoot(inode_id, &root, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load layout root" : local_error;
        }
        return false;
    }
    if (root.layout_root_id.empty()) {
        if (error) {
            *error = "layout root id is empty";
        }
        return false;
    }

    LayoutNodeRecord node;
    bool recovered = false;
    if (!LoadHealthyLayoutNode(inode_id, root.layout_root_id, root.layout_version, &node, &recovered, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load layout object" : local_error;
        }
        return false;
    }

    layout->Clear();
    layout->set_inode_id(inode_id);
    layout->set_object_unit_size(object_unit_size);
    const uint64_t request_end = offset + size;

    std::unordered_map<uint32_t, zb::rpc::ObjectMeta> object_map;
    const uint32_t replica_count = attr.replica() > 0 ? attr.replica() : 1;
    for (const auto& extent : node.extents) {
        const uint64_t extent_start = extent.logical_offset;
        const uint64_t extent_end = extent.logical_offset + extent.length;
        if (extent_end <= offset || extent_start >= request_end) {
            continue;
        }
        const uint32_t object_index = extent.object_index > 0
                                          ? extent.object_index
                                          : static_cast<uint32_t>(extent.logical_offset / object_unit_size);
        if (object_map.find(object_index) != object_map.end()) {
            continue;
        }
        if (extent.object_id.empty()) {
            if (error) {
                *error = "layout extent object_id is empty";
            }
            return false;
        }

        zb::rpc::ObjectMeta object_meta;
        object_meta.set_index(object_index);
        std::vector<zb::rpc::ReplicaLocation> replicas;
        local_error.clear();
        if (!ResolveObjectReplicas(replica_count,
                                   extent.object_id,
                                   extent.placement_epoch,
                                   &replicas,
                                   &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to resolve object placement from PG" : local_error;
            }
            return false;
        }
        for (const auto& replica : replicas) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                *object_meta.add_replicas() = replica;
            }
        }
        if (object_meta.replicas_size() == 0) {
            if (error) {
                *error = "layout object has no readable disk replica from PG";
            }
            return false;
        }
        object_map.emplace(object_index, std::move(object_meta));
    }

    std::vector<uint32_t> indices;
    indices.reserve(object_map.size());
    for (const auto& item : object_map) {
        indices.push_back(item.first);
    }
    if (indices.empty()) {
        if (error) {
            *error = "layout plan contains no readable extents";
        }
        return false;
    }
    std::sort(indices.begin(), indices.end());
    for (uint32_t index : indices) {
        *layout->add_objects() = object_map[index];
    }
    return true;
}

bool MdsServiceImpl::BuildReadPlan(uint64_t inode_id,
                                   uint64_t offset,
                                   uint64_t size,
                                   zb::rpc::FileLayout* layout,
                                   std::string* error) {
    return BuildReadPlanFromLayout(inode_id, offset, size, layout, error);
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
    const uint64_t object_unit_size = read_plan.object_unit_size() > 0 ? read_plan.object_unit_size() : default_object_unit_size_;
    if (object_unit_size == 0) {
        if (error) {
            *error = "object_unit_size is zero";
        }
        return false;
    }
    const uint64_t request_end = request_offset + request_size;
    for (const auto& object_meta : read_plan.objects()) {
        const uint64_t object_start = static_cast<uint64_t>(object_meta.index()) * object_unit_size;
        const uint64_t object_end = object_start + object_unit_size;
        const uint64_t overlap_start = std::max<uint64_t>(object_start, request_offset);
        const uint64_t overlap_end = std::min<uint64_t>(object_end, request_end);
        if (overlap_start >= overlap_end) {
            continue;
        }

        const zb::rpc::ReplicaLocation* source = nullptr;
        for (const auto& replica : object_meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY &&
                !ReplicaObjectId(replica).empty()) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            if (error) {
                *error = "object segment has no ready disk source";
            }
            return false;
        }

        LayoutExtentRecord extent;
        extent.logical_offset = overlap_start;
        extent.length = overlap_end - overlap_start;
        extent.object_id = ReplicaObjectId(*source);
        extent.object_offset = overlap_start - object_start;
        extent.object_length = extent.length;
        extent.object_version = object_version;
        extent.object_index = object_meta.index();
        extent.pg_id = 0;
        extent.placement_epoch = source->epoch();
        extent.storage_tier = static_cast<uint32_t>(source->storage_tier());
        extents->push_back(std::move(extent));
    }
    return true;
}

bool MdsServiceImpl::SelectReadableDiskObjectReplica(const zb::rpc::ObjectMeta& object_meta,
                                                     zb::rpc::ReplicaLocation* source) const {
    if (!source) {
        return false;
    }
    for (const auto& replica : object_meta.replicas()) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_DISK ||
            replica.replica_state() != zb::rpc::REPLICA_READY ||
            ReplicaObjectId(replica).empty()) {
            continue;
        }
        *source = replica;
        return true;
    }
    return false;
}

bool MdsServiceImpl::ResolveObjectReplicas(uint32_t replica_count,
                                           const std::string& object_id,
                                           uint64_t placement_epoch,
                                           std::vector<zb::rpc::ReplicaLocation>* replicas,
                                           std::string* error) const {
    if (!replicas || replica_count == 0 || object_id.empty()) {
        if (error) {
            *error = "invalid object replica resolve args";
        }
        return false;
    }
    if (!allocator_) {
        if (error) {
            *error = "allocator is unavailable";
        }
        return false;
    }
    std::string local_error;
    if (!allocator_->AllocateObjectByPg(replica_count, object_id, placement_epoch, replicas, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "AllocateObjectByPg failed" : local_error;
        }
        return false;
    }
    if (replicas->size() < replica_count) {
        if (error) {
            *error = "insufficient replicas from PG resolve";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::SelectFileAnchor(uint64_t inode_id,
                                      const zb::rpc::InodeAttr& attr,
                                      zb::rpc::ReplicaLocation* anchor,
                                      std::string* error) const {
    if (!anchor || inode_id == 0) {
        if (error) {
            *error = "invalid file anchor args";
        }
        return false;
    }
    if (!allocator_) {
        if (error) {
            *error = "allocator is unavailable";
        }
        return false;
    }

    const std::string object_id = BuildStableObjectId(inode_id, 0);
    std::vector<zb::rpc::ReplicaLocation> replicas;
    std::string local_error;
    if (!ResolveObjectReplicas(1, object_id, 0, &replicas, &local_error) || replicas.empty()) {
        replicas.clear();
        if (!allocator_->AllocateObject(1, object_id, &replicas) || replicas.empty()) {
            if (error) {
                *error = local_error.empty() ? "failed to allocate anchor replica" : local_error;
            }
            return false;
        }
    }

    for (const auto& replica : replicas) {
        if (replica.node_address().empty() || replica.disk_id().empty()) {
            continue;
        }
        *anchor = replica;
        if (anchor->storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
            anchor->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        }
        anchor->set_replica_state(zb::rpc::REPLICA_READY);
        EnsureReplicaObjectId(anchor, object_id);
        return true;
    }

    (void)attr;
    if (error) {
        *error = "no usable disk replica for file anchor";
    }
    return false;
}

bool MdsServiceImpl::LoadFileAnchor(uint64_t inode_id,
                                    zb::rpc::ReplicaLocation* anchor,
                                    std::string* error) const {
    if (!anchor || inode_id == 0) {
        if (error) {
            *error = "invalid file anchor output";
        }
        return false;
    }
    std::string data;
    std::string local_error;
    if (!store_->Get(FileAnchorKey(inode_id), &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "file anchor not found" : local_error;
        }
        return false;
    }
    if (!anchor->ParseFromString(data)) {
        if (error) {
            *error = "invalid file anchor payload";
        }
        return false;
    }
    if (anchor->storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
        anchor->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    }
    anchor->set_replica_state(zb::rpc::REPLICA_READY);
    return true;
}

bool MdsServiceImpl::SaveFileAnchor(uint64_t inode_id,
                                    const zb::rpc::ReplicaLocation& anchor,
                                    rocksdb::WriteBatch* batch) const {
    if (!batch || inode_id == 0) {
        return false;
    }
    zb::rpc::ReplicaLocation normalized = anchor;
    if (normalized.storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
        normalized.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    }
    normalized.set_replica_state(zb::rpc::REPLICA_READY);
    if (normalized.object_id().empty()) {
        normalized.set_object_id(BuildStableObjectId(inode_id, 0));
    }
    std::string payload;
    if (!normalized.SerializeToString(&payload)) {
        return false;
    }
    batch->Put(FileAnchorKey(inode_id), payload);
    return true;
}

bool MdsServiceImpl::BuildReadPlanFromAnchor(uint64_t inode_id,
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
    const uint64_t object_unit_size = attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_;
    if (object_unit_size == 0) {
        if (error) {
            *error = "object_unit_size is zero";
        }
        return false;
    }

    zb::rpc::ReplicaLocation anchor;
    if (!LoadFileAnchor(inode_id, &anchor, &local_error)) {
        local_error.clear();
        if (!SelectFileAnchor(inode_id, attr, &anchor, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to load file anchor" : local_error;
            }
            return false;
        }
        rocksdb::WriteBatch batch;
        if (!SaveFileAnchor(inode_id, anchor, &batch)) {
            if (error) {
                *error = "failed to encode generated file anchor";
            }
            return false;
        }
        std::string put_error;
        if (!store_->WriteBatch(&batch, &put_error)) {
            if (error) {
                *error = put_error.empty() ? "failed to persist generated file anchor" : put_error;
            }
            return false;
        }
    }
    if (anchor.node_address().empty() || anchor.disk_id().empty()) {
        if (error) {
            *error = "file anchor is incomplete";
        }
        return false;
    }

    layout->Clear();
    layout->set_inode_id(inode_id);
    layout->set_object_unit_size(object_unit_size);

    const uint64_t start = offset / object_unit_size;
    const uint64_t end = (offset + size - 1) / object_unit_size;
    for (uint64_t i = start; i <= end; ++i) {
        const uint32_t object_index = static_cast<uint32_t>(i);
        const std::string object_id = BuildStableObjectId(inode_id, object_index);
        zb::rpc::ObjectMeta* object_meta = layout->add_objects();
        object_meta->set_index(object_index);
        zb::rpc::ReplicaLocation* replica = object_meta->add_replicas();
        FillAnchorReplica(replica, anchor, object_id);
    }
    return true;
}

std::string MdsServiceImpl::BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

void MdsServiceImpl::FillAnchorReplica(zb::rpc::ReplicaLocation* replica,
                                       const zb::rpc::ReplicaLocation& anchor,
                                       const std::string& object_id) {
    if (!replica) {
        return;
    }
    *replica = anchor;
    EnsureReplicaObjectId(replica, object_id);
    if (replica->storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
        replica->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    }
    replica->set_replica_state(zb::rpc::REPLICA_READY);
}

bool MdsServiceImpl::SeedObjectForCowWrite(const LayoutExtentRecord& old_extent,
                                           const std::vector<zb::rpc::ReplicaLocation>& new_replicas,
                                           uint64_t object_size,
                                           std::string* error) {
    if (new_replicas.empty()) {
        if (error) {
            *error = "new replicas are empty";
        }
        return false;
    }
    if (old_extent.object_id.empty()) {
        return true;
    }
    if (object_size == 0) {
        if (error) {
            *error = "object size is zero";
        }
        return false;
    }

    std::vector<zb::rpc::ReplicaLocation> old_replicas;
    std::string local_error;
    if (!ResolveObjectReplicas(static_cast<uint32_t>(new_replicas.size()),
                               old_extent.object_id,
                               old_extent.placement_epoch,
                               &old_replicas,
                               &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to resolve old object replicas" : local_error;
        }
        return false;
    }
    if (old_replicas.empty()) {
        return true;
    }

    zb::rpc::ReplicaLocation source;
    bool found_source = false;
    for (const auto& replica : old_replicas) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_DISK ||
            replica.replica_state() != zb::rpc::REPLICA_READY) {
            continue;
        }
        source = replica;
        found_source = true;
        break;
    }
    if (!found_source) {
        source = old_replicas.front();
    }

    std::string data;
    if (!ReadObjectFromReplica(source, object_size, &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to read source object for cow seed" : local_error;
        }
        return false;
    }
    for (const auto& target : new_replicas) {
        const std::string target_object_id = ReplicaObjectId(target);
        if (target_object_id.empty() || !WriteObjectToReplica(target, target_object_id, data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to write seeded object replica" : local_error;
            }
            return false;
        }
    }
    return true;
}

bool MdsServiceImpl::ConsumePendingWriteForCommit(uint64_t inode_id,
                                                  const std::string& op_id,
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
        layout_commit_conflict_total_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    std::string local_error;
    LayoutRootRecord root;
    root.inode_id = inode_id;
    root.layout_root_id = "lr-" + std::to_string(inode_id) + "-" + std::to_string(txn.pending_layout_version);
    root.layout_version = txn.pending_layout_version;
    root.file_size = new_file_size;
    root.epoch = 0;
    for (const auto& extent : txn.pending_layout_node.extents) {
        root.epoch = std::max<uint64_t>(root.epoch, extent.placement_epoch);
    }
    root.update_ts = NowMilliseconds();

    LayoutNodeRecord pending_layout_node = txn.pending_layout_node;
    pending_layout_node.node_id = root.layout_root_id;
    if (!StoreLayoutRootAtomic(inode_id,
                               root,
                               txn.base_layout_version,
                               true,
                               new_file_size,
                               op_id.empty() ? nullptr : &op_id,
                               &pending_layout_node,
                               committed_root,
                               &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to commit pending layout root" : local_error;
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

bool MdsServiceImpl::HasReadyDiskObjectReplica(const zb::rpc::ObjectMeta& object_meta) const {
    for (const auto& replica : object_meta.replicas()) {
        if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
            replica.replica_state() == zb::rpc::REPLICA_READY) {
            return true;
        }
    }
    return false;
}

bool MdsServiceImpl::FindReadyOpticalObjectReplica(const zb::rpc::ObjectMeta& object_meta,
                                                   zb::rpc::ReplicaLocation* optical) const {
    if (!optical) {
        return false;
    }
    for (const auto& replica : object_meta.replicas()) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
            replica.replica_state() != zb::rpc::REPLICA_READY) {
            continue;
        }
        *optical = replica;
        return true;
    }
    return false;
}

bool MdsServiceImpl::CollectRecallTasksByImage(const std::string& seed_object_key,
                                               const zb::rpc::ObjectMeta& seed_meta,
                                               const zb::rpc::ReplicaLocation& optical_seed,
                                               std::vector<RecallTask>* tasks,
                                               std::string* error) {
    if (!tasks || seed_object_key.empty()) {
        if (error) {
            *error = "invalid recall task arguments";
        }
        return false;
    }
    tasks->clear();

    auto find_matching_optical = [&](const zb::rpc::ObjectMeta& meta, zb::rpc::ReplicaLocation* matched) -> bool {
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

    std::unordered_set<std::string> seen_object_keys;
    zb::rpc::ReplicaLocation seed_optical;
    if (find_matching_optical(seed_meta, &seed_optical)) {
        RecallTask task;
        task.object_key = seed_object_key;
        task.optical_replica = seed_optical;
        tasks->push_back(std::move(task));
        seen_object_keys.insert(seed_object_key);
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
        ArchiveImageObjectPrefix(optical_seed.node_id(), optical_seed.disk_id(), optical_seed.image_id());
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string index_key = it->key().ToString();
        if (index_key.rfind(prefix, 0) != 0) {
            break;
        }
        const std::string object_key = it->value().ToString();
        if (object_key.empty() || !seen_object_keys.insert(object_key).second) {
            continue;
        }

        std::string object_data;
        std::string local_error;
        if (!store_->Get(object_key, &object_data, &local_error)) {
            continue;
        }
        zb::rpc::ObjectMeta meta;
        if (!MetaCodec::DecodeObjectMeta(object_data, &meta)) {
            continue;
        }
        zb::rpc::ReplicaLocation matched;
        if (!find_matching_optical(meta, &matched)) {
            continue;
        }
        RecallTask task;
        task.object_key = object_key;
        task.optical_replica = matched;
        tasks->push_back(std::move(task));
    }

    if (tasks->empty()) {
        if (error) {
            *error = "no objects found for optical image recall";
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
        std::string object_data;
        std::string local_error;
        if (!store_->Get(task.object_key, &object_data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("missing object meta: " + task.object_key) : local_error;
            }
            return false;
        }

        zb::rpc::ObjectMeta meta;
        if (!MetaCodec::DecodeObjectMeta(object_data, &meta)) {
            if (error) {
                *error = "invalid object meta: " + task.object_key;
            }
            return false;
        }
        if (HasReadyDiskObjectReplica(meta)) {
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
            uint32_t object_index = 0;
            if (ParseObjectKey(task.object_key, &inode_id, &object_index)) {
                zb::rpc::InodeAttr attr;
                if (GetInode(inode_id, &attr, &local_error)) {
                    read_size = attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_;
                }
            }
        }
        if (read_size == 0) {
            read_size = default_object_unit_size_;
        }

        std::string data;
        if (!ReadObjectFromReplica(source, read_size, &data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("failed to read recalled object from optical: " + ReplicaObjectId(source))
                                             : local_error;
            }
            return false;
        }

        const std::string recalled_object_id = GenerateObjectId();
        if (!WriteObjectToReplica(target_base, recalled_object_id, data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? ("failed to write recalled object to disk: " + recalled_object_id)
                                             : local_error;
            }
            return false;
        }

        zb::rpc::ReplicaLocation* disk_replica = meta.add_replicas();
        disk_replica->set_node_id(target_base.node_id());
        disk_replica->set_node_address(target_base.node_address());
        disk_replica->set_disk_id(target_base.disk_id());
        EnsureReplicaObjectId(disk_replica, recalled_object_id);
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

        batch.Put(task.object_key, MetaCodec::EncodeObjectMeta(meta));
        batch.Put(ReverseObjectKey(recalled_object_id), task.object_key);
        uint64_t inode_id = 0;
        uint32_t object_index = 0;
        if (ParseObjectKey(task.object_key, &inode_id, &object_index)) {
            batch.Put(ObjectOwnerKey(recalled_object_id), EncodeObjectOwnerValue(inode_id, object_index));
        }
    }

    if (batch.Count() == 0) {
        return true;
    }
    std::string write_error;
    if (!store_->WriteBatch(&batch, &write_error)) {
        if (error) {
            *error = write_error.empty() ? "failed to persist recalled object metadata" : write_error;
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
            *error = "object allocator is unavailable";
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
    const uint64_t inode_object_unit_size = attr.object_unit_size() > 0 ? attr.object_unit_size() : default_object_unit_size_;

    std::vector<zb::rpc::ReplicaLocation> allocated;
    if (!allocator_->AllocateObject(1, GenerateObjectId(), &allocated) || allocated.empty()) {
        if (error) {
            *error = "failed to allocate file cache target node";
        }
        return false;
    }
    const zb::rpc::ReplicaLocation target_base = allocated.front();

    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string prefix = ObjectPrefix(inode_id);
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string object_key = it->key().ToString();
        if (object_key.rfind(prefix, 0) != 0) {
            break;
        }

        zb::rpc::ObjectMeta object_meta;
        if (!MetaCodec::DecodeObjectMeta(it->value().ToString(), &object_meta)) {
            if (error) {
                *error = "invalid object meta while caching file";
            }
            return false;
        }

        bool has_target_disk = false;
        for (const auto& replica : object_meta.replicas()) {
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
        for (const auto& replica : object_meta.replicas()) {
            if (replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK &&
                replica.replica_state() == zb::rpc::REPLICA_READY) {
                source = &replica;
                break;
            }
        }
        if (!source) {
            for (const auto& replica : object_meta.replicas()) {
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

        uint64_t read_size = source->size() > 0 ? source->size() : inode_object_unit_size;
        if (read_size == 0) {
            read_size = default_object_unit_size_;
        }

        std::string data;
        if (!ReadObjectFromReplica(*source, read_size, &data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to read source object while caching file" : local_error;
            }
            return false;
        }

        const std::string cached_object_id = GenerateObjectId();
        if (!WriteObjectToReplica(target_base, cached_object_id, data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to write file cache object to disk node" : local_error;
            }
            return false;
        }

        zb::rpc::ReplicaLocation* disk_replica = object_meta.add_replicas();
        disk_replica->set_node_id(target_base.node_id());
        disk_replica->set_node_address(target_base.node_address());
        disk_replica->set_disk_id(target_base.disk_id());
        EnsureReplicaObjectId(disk_replica, cached_object_id);
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

        batch.Put(object_key, MetaCodec::EncodeObjectMeta(object_meta));
        batch.Put(ReverseObjectKey(cached_object_id), object_key);
        uint64_t parsed_inode_id = 0;
        uint32_t parsed_object_index = 0;
        if (ParseObjectKey(object_key, &parsed_inode_id, &parsed_object_index)) {
            batch.Put(ObjectOwnerKey(cached_object_id),
                      EncodeObjectOwnerValue(parsed_inode_id, parsed_object_index));
        }
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

bool MdsServiceImpl::EnsureObjectReadableFromDisk(const std::string& object_key,
                                                  zb::rpc::ObjectMeta* object_meta,
                                                  std::unordered_set<std::string>* recalled_images,
                                                  bool* recalled_from_optical,
                                                  std::string* error) {
    if (recalled_from_optical) {
        *recalled_from_optical = false;
    }
    if (!object_meta) {
        if (error) {
            *error = "object meta pointer is null";
        }
        return false;
    }
    if (HasReadyDiskObjectReplica(*object_meta)) {
        return true;
    }

    zb::rpc::ReplicaLocation optical;
    if (!FindReadyOpticalObjectReplica(*object_meta, &optical)) {
        return true;
    }

    std::string image_key;
    if (!optical.node_id().empty() && !optical.disk_id().empty() && !optical.image_id().empty()) {
        image_key = optical.node_id() + "|" + optical.disk_id() + "|" + optical.image_id();
    }

    if (image_key.empty() || !recalled_images || recalled_images->find(image_key) == recalled_images->end()) {
        std::vector<RecallTask> tasks;
        if (!CollectRecallTasksByImage(object_key, *object_meta, optical, &tasks, error)) {
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
    if (!store_->Get(object_key, &updated_data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to reload object metadata after recall" : local_error;
        }
        return false;
    }
    if (!MetaCodec::DecodeObjectMeta(updated_data, object_meta)) {
        if (error) {
            *error = "invalid object metadata after recall";
        }
        return false;
    }
    if (!HasReadyDiskObjectReplica(*object_meta)) {
        if (error) {
            *error = "optical recall completed but no disk replica available";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::ReadObjectFromReplica(const zb::rpc::ReplicaLocation& source,
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
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    zb::rpc::ReadObjectRequest req;
    req.set_disk_id(source.disk_id());
    req.set_object_id(ReplicaObjectId(source));
    req.set_offset(0);
    req.set_size(read_size);
    if (!source.image_id().empty()) {
        req.set_image_id(source.image_id());
        req.set_image_offset(source.image_offset());
        req.set_image_length(source.image_length());
    }

    zb::rpc::ReadObjectReply resp;
    stub.ReadObject(&cntl, &req, &resp, nullptr);
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

bool MdsServiceImpl::WriteObjectToReplica(const zb::rpc::ReplicaLocation& target,
                                          const std::string& object_id,
                                          const std::string& data,
                                          std::string* error) {
    brpc::Channel* channel = GetDataChannel(target.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    zb::rpc::WriteObjectRequest req;
    req.set_disk_id(target.disk_id());
    req.set_object_id(object_id);
    req.set_offset(0);
    req.set_data(data);
    req.set_epoch(target.epoch());

    zb::rpc::WriteObjectReply resp;
    stub.WriteObject(&cntl, &req, &resp, nullptr);
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

std::string MdsServiceImpl::GenerateObjectId() {
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
