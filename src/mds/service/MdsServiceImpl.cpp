#include "MdsServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <random>
#include <sstream>

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

} // namespace

MdsServiceImpl::MdsServiceImpl(RocksMetaStore* store, ChunkAllocator* allocator, uint64_t default_chunk_size)
    : store_(store), allocator_(allocator), default_chunk_size_(default_chunk_size) {
    std::string error;
    EnsureRoot(&error);
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

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        return;
    }

    uint64_t chunk_size = attr.chunk_size() ? attr.chunk_size() : default_chunk_size_;
    if (attr.chunk_size() == 0) {
        attr.set_chunk_size(chunk_size);
        PutInode(attr.inode_id(), attr, &error);
    }

    uint64_t start = request->offset() / chunk_size;
    uint64_t end = (request->offset() + request->size() - 1) / chunk_size;

    zb::rpc::FileLayout* layout = response->mutable_layout();
    layout->set_inode_id(attr.inode_id());
    layout->set_chunk_size(chunk_size);

    rocksdb::WriteBatch batch;
    for (uint64_t index = start; index <= end; ++index) {
        error.clear();
        std::string chunk_data;
        zb::rpc::ChunkMeta chunk_meta;
        if (store_->Get(ChunkKey(attr.inode_id(), static_cast<uint32_t>(index)), &chunk_data, &error)) {
            if (!MetaCodec::DecodeChunkMeta(chunk_data, &chunk_meta)) {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid chunk meta");
                return;
            }
        } else {
            if (!error.empty()) {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
                return;
            }
            chunk_meta.set_index(static_cast<uint32_t>(index));
            std::vector<zb::rpc::ReplicaLocation> replicas;
            uint32_t replica = attr.replica() ? attr.replica() : 1;
            if (!allocator_->AllocateChunk(replica, GenerateChunkId(), &replicas)) {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to allocate replicas");
                return;
            }
            for (const auto& replica_info : replicas) {
                *chunk_meta.add_replicas() = replica_info;
            }
            batch.Put(ChunkKey(attr.inode_id(), static_cast<uint32_t>(index)),
                      MetaCodec::EncodeChunkMeta(chunk_meta));
        }
        *layout->add_chunks() = chunk_meta;
    }

    if (batch.Count() > 0) {
        if (!store_->WriteBatch(&batch, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
            return;
        }
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
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        return;
    }

    if (request->new_size() > attr.size()) {
        attr.set_size(request->new_size());
    }
    attr.set_mtime(NowSeconds());
    if (!PutInode(attr.inode_id(), attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
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
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        batch.Delete(it->key());
    }
    if (batch.Count() == 0) {
        return true;
    }
    return store_->WriteBatch(&batch, error);
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
