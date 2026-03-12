#include "MdsServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <fcntl.h>
#include <memory>
#include <sstream>

#include "real_node.pb.h"

namespace zb::mds {

namespace {

constexpr const char* kReadOnlyOpticalMessage = "READ_ONLY_OPTICAL";
constexpr const char* kNoSpaceRealPolicyMessage = "NO_SPACE_REAL_POLICY";

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

std::string ArchiveObjectId(const zb::rpc::ArchiveCandidate& candidate) {
    return candidate.object_id();
}

bool IsWriteOpenFlags(uint32_t flags) {
    const int access_mode = static_cast<int>(flags) & O_ACCMODE;
    if (access_mode == O_WRONLY || access_mode == O_RDWR) {
        return true;
    }
#ifdef O_TRUNC
    if ((flags & static_cast<uint32_t>(O_TRUNC)) != 0U) {
        return true;
    }
#endif
#ifdef O_APPEND
    if ((flags & static_cast<uint32_t>(O_APPEND)) != 0U) {
        return true;
    }
#endif
    return false;
}

bool HasOpticalAnchor(const zb::rpc::FileAnchorSet& anchors) {
    const bool mask_has_optical = (anchors.anchor_mask() & (1U << 1U)) != 0U;
    const auto& optical = anchors.optical_anchor();
    const bool lite_has_optical = !optical.node_id().empty() && !optical.disk_id().empty();
    return mask_has_optical || lite_has_optical;
}

bool NormalizePolicyPath(const std::string& path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::string p = path;
    std::replace(p.begin(), p.end(), '\\', '/');
    if (p.empty() || p[0] != '/') {
        p.insert(p.begin(), '/');
    }
    std::string out;
    out.reserve(p.size() + 1);
    bool prev_slash = false;
    for (char ch : p) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
            out.push_back(ch);
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    if (out.empty()) {
        out = "/";
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    *normalized = std::move(out);
    return true;
}

std::vector<std::string> BuildPathPrefixCandidates(const std::string& normalized_path) {
    std::vector<std::string> out;
    if (normalized_path.empty() || normalized_path[0] != '/') {
        return out;
    }
    std::string current = normalized_path;
    while (true) {
        out.push_back(current);
        if (current == "/") {
            break;
        }
        const size_t slash = current.find_last_of('/');
        if (slash == std::string::npos || slash == 0) {
            current = "/";
        } else {
            current = current.substr(0, slash);
        }
    }
    return out;
}

void EnsureReplicaObjectId(zb::rpc::ReplicaLocation* replica, const std::string& object_id) {
    if (!replica || object_id.empty()) {
        return;
    }
    replica->set_object_id(object_id);
}

constexpr uint32_t kAnchorMaskDisk = 1U << 0U;
constexpr uint32_t kAnchorMaskOptical = 1U << 1U;

zb::rpc::FileAnchorLite ToAnchorLite(const zb::rpc::ReplicaLocation& replica) {
    zb::rpc::FileAnchorLite lite;
    lite.set_node_id(replica.node_id());
    lite.set_disk_id(replica.disk_id());
    lite.set_object_id(replica.object_id());
    lite.set_size(replica.size());
    lite.set_storage_tier(replica.storage_tier());
    lite.set_replica_state(replica.replica_state());
    lite.set_image_id(replica.image_id());
    lite.set_image_offset(replica.image_offset());
    lite.set_image_length(replica.image_length());
    return lite;
}

zb::rpc::ReplicaLocation ToReplicaLocation(const zb::rpc::FileAnchorLite& lite) {
    zb::rpc::ReplicaLocation replica;
    replica.set_node_id(lite.node_id());
    replica.set_disk_id(lite.disk_id());
    replica.set_object_id(lite.object_id());
    replica.set_size(lite.size());
    replica.set_storage_tier(lite.storage_tier());
    replica.set_replica_state(lite.replica_state());
    replica.set_image_id(lite.image_id());
    replica.set_image_offset(lite.image_offset());
    replica.set_image_length(lite.image_length());
    return replica;
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

    zb::rpc::FileAnchorSet file_anchor_set;
    if (attr.type() == zb::rpc::INODE_FILE) {
        std::string anchor_error;
        zb::rpc::ReplicaLocation file_anchor;
        if (!LoadFileAnchorSet(inode_id, &file_anchor_set, &anchor_error) ||
            !SelectPrimaryAnchor(file_anchor_set, &file_anchor)) {
            std::string select_error;
            if (!SelectFileAnchor(inode_id, attr, &file_anchor, &select_error)) {
                FillStatus(response->mutable_status(),
                           zb::rpc::MDS_INTERNAL_ERROR,
                           select_error.empty() ? "failed to resolve file anchor" : select_error);
                return;
            }
            file_anchor_set = BuildFileAnchorSetFromSingle(file_anchor);
            rocksdb::WriteBatch anchor_batch;
            if (!SaveFileAnchorSet(inode_id, file_anchor_set, &anchor_batch)) {
                FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to encode file anchor");
                return;
            }
            std::string persist_error;
            if (!store_->WriteBatch(&anchor_batch, &persist_error)) {
                FillStatus(response->mutable_status(),
                           zb::rpc::MDS_INTERNAL_ERROR,
                           persist_error.empty() ? "failed to persist file anchor" : persist_error);
                return;
            }
        }
        if (IsWriteOpenFlags(request->flags()) && HasOpticalAnchor(file_anchor_set)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, kReadOnlyOpticalMessage);
            return;
        }
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
    if (attr.type() == zb::rpc::INODE_FILE) {
        *response->mutable_file_anchor() = file_anchor_set;
    }
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

    zb::rpc::PathPlacementPolicyRecord placement_policy;
    std::string policy_error;
    const bool has_path_policy =
        MatchPathPlacementPolicy(request->path(), &placement_policy, nullptr, &policy_error);
    if (!policy_error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, policy_error);
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), MetaCodec::EncodeInodeAttr(attr));
    zb::rpc::ReplicaLocation anchor;
    bool anchor_ok = false;
    if (has_path_policy &&
        placement_policy.target() == zb::rpc::PATH_PLACEMENT_REAL_ONLY &&
        placement_policy.strict()) {
        anchor_ok = SelectFileAnchorWithPreference(inode_id, attr, NodeType::kReal, true, &anchor, &error);
    } else {
        anchor_ok = SelectFileAnchor(inode_id, attr, &anchor, &error);
    }
    if (!anchor_ok) {
        if (has_path_policy &&
            placement_policy.target() == zb::rpc::PATH_PLACEMENT_REAL_ONLY &&
            placement_policy.strict()) {
            error = kNoSpaceRealPolicyMessage;
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    zb::rpc::FileAnchorSet anchor_set = BuildFileAnchorSetFromSingle(anchor);
    if (!SaveFileAnchorSet(inode_id, anchor_set, &batch)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to encode file anchor");
        return;
    }
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    *response->mutable_attr() = attr;
    *response->mutable_file_anchor() = anchor_set;
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

    zb::rpc::FileAnchorSet anchors;
    std::string anchor_error;
    zb::rpc::ReplicaLocation cleanup_anchor;
    if (LoadFileAnchorSet(inode_id, &anchors, &anchor_error) &&
        (SelectDiskAnchor(anchors, &cleanup_anchor) || SelectPrimaryAnchor(anchors, &cleanup_anchor)) &&
        !cleanup_anchor.node_id().empty() &&
        cleanup_anchor.storage_tier() == zb::rpc::STORAGE_TIER_DISK) {
        std::string cleanup_error;
        if (!DeleteFileMetaOnAnchor(cleanup_anchor, inode_id, true, &cleanup_error)) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       cleanup_error.empty() ? "failed to delete file meta on anchor"
                                             : cleanup_error);
            return;
        }
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

void MdsServiceImpl::GetFileAnchor(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::GetFileAnchorRequest* request,
                                   zb::rpc::GetFileAnchorReply* response,
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
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "inode not found" : error);
        return;
    }
    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode is not file");
        return;
    }

    zb::rpc::FileAnchorSet anchor_set;
    zb::rpc::ReplicaLocation anchor;
    error.clear();
    if (!LoadFileAnchorSet(request->inode_id(), &anchor_set, &error) ||
        !SelectPrimaryAnchor(anchor_set, &anchor)) {
        std::string select_error;
        if (!SelectFileAnchor(request->inode_id(), attr, &anchor, &select_error)) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       select_error.empty() ? "failed to resolve file anchor" : select_error);
            return;
        }
        anchor_set = BuildFileAnchorSetFromSingle(anchor);
        rocksdb::WriteBatch batch;
        if (!SaveFileAnchorSet(request->inode_id(), anchor_set, &batch)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "failed to encode file anchor");
            return;
        }
        std::string put_error;
        if (!store_->WriteBatch(&batch, &put_error)) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       put_error.empty() ? "failed to persist file anchor" : put_error);
            return;
        }
    }

    *response->mutable_anchor() = anchor_set;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::UpdateInodeStat(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::UpdateInodeStatRequest* request,
                                     zb::rpc::UpdateInodeStatReply* response,
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
    if (request->version() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "version is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "inode not found" : error);
        return;
    }
    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode is not file");
        return;
    }
    if (request->version() < attr.version()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_STALE_EPOCH, "stale inode stat version");
        return;
    }

    zb::rpc::FileAnchorSet anchors;
    std::string anchor_error;
    if (!LoadFileAnchorSet(request->inode_id(), &anchors, &anchor_error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   anchor_error.empty() ? "failed to load file anchor set" : anchor_error);
        return;
    }
    if (HasOpticalAnchor(anchors)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, kReadOnlyOpticalMessage);
        return;
    }

    attr.set_size(request->file_size());
    if (request->object_unit_size() > 0) {
        attr.set_object_unit_size(request->object_unit_size());
    }
    attr.set_version(request->version());
    const uint64_t now_sec = NowSeconds();
    const uint64_t mtime = request->mtime() > 0 ? request->mtime() : now_sec;
    attr.set_mtime(mtime);
    attr.set_ctime(now_sec);

    if (!PutInode(request->inode_id(), attr, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to persist inode stat" : error);
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::SetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::SetPathPlacementPolicyRequest* request,
                                            zb::rpc::SetPathPlacementPolicyReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    std::string error;
    if (!SavePathPlacementPolicy(request->policy(), &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error.empty() ? "failed to set path placement policy" : error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::DeletePathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                               const zb::rpc::DeletePathPlacementPolicyRequest* request,
                                               zb::rpc::DeletePathPlacementPolicyReply* response,
                                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    std::string error;
    if (!DeletePathPlacementPolicyByPrefix(request->path_prefix(), &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error.empty() ? "failed to delete path placement policy" : error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::GetPathPlacementPolicyRequest* request,
                                            zb::rpc::GetPathPlacementPolicyReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    zb::rpc::PathPlacementPolicyRecord policy;
    std::string matched_prefix;
    std::string error;
    const bool found = MatchPathPlacementPolicy(request->path(), &policy, &matched_prefix, &error);
    if (!error.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error);
        return;
    }
    response->set_found(found);
    if (found) {
        *response->mutable_policy() = policy;
        response->set_matched_prefix(matched_prefix);
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
    rocksdb::WriteBatch batch;
    batch.Delete(FileAnchorKey(inode_id));
    // Anchor-only metadata mode: do not touch per-object/per-layout keyspaces on unlink.
    // Data object cleanup is delegated to the anchor node via DeleteFileMeta(purge_objects=true).
    return store_->WriteBatch(&batch, error);
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
    return SelectFileAnchorWithPreference(inode_id, attr, NodeType::kReal, false, anchor, error);
}

bool MdsServiceImpl::SelectFileAnchorWithPreference(uint64_t inode_id,
                                                    const zb::rpc::InodeAttr& attr,
                                                    NodeType preferred_type,
                                                    bool strict_type,
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
    if (strict_type) {
        if (!allocator_->AllocateObjectByPgWithType(1,
                                                    object_id,
                                                    0,
                                                    preferred_type,
                                                    true,
                                                    &replicas,
                                                    &local_error) || replicas.empty()) {
            if (error) {
                *error = local_error.empty() ? "no replica matches strict placement policy" : local_error;
            }
            return false;
        }
    } else if (!ResolveObjectReplicas(1, object_id, 0, &replicas, &local_error) || replicas.empty()) {
        replicas.clear();
        if (!allocator_->AllocateObject(1, object_id, &replicas) || replicas.empty()) {
            if (error) {
                *error = local_error.empty() ? "failed to allocate anchor replica" : local_error;
            }
            return false;
        }
    }

    for (const auto& replica : replicas) {
        if (replica.node_id().empty() || replica.disk_id().empty()) {
            continue;
        }
        *anchor = replica;
        if (anchor->storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
            anchor->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        }
        anchor->set_replica_state(zb::rpc::REPLICA_READY);
        EnsureReplicaObjectId(anchor, object_id);
        StripReplicaAddresses(anchor);
        return true;
    }

    (void)attr;
    if (error) {
        *error = strict_type ? "no usable replica for strict placement policy"
                             : "no usable disk replica for file anchor";
    }
    return false;
}

bool MdsServiceImpl::MatchPathPlacementPolicy(const std::string& path,
                                              zb::rpc::PathPlacementPolicyRecord* policy,
                                              std::string* matched_prefix,
                                              std::string* error) const {
    if (error) {
        error->clear();
    }
    if (policy) {
        policy->Clear();
    }
    if (matched_prefix) {
        matched_prefix->clear();
    }
    std::string normalized_path;
    if (!NormalizePolicyPath(path, &normalized_path)) {
        if (error) {
            *error = "invalid policy lookup path";
        }
        return false;
    }
    const std::vector<std::string> candidates = BuildPathPrefixCandidates(normalized_path);
    for (const auto& prefix : candidates) {
        std::string payload;
        std::string local_error;
        if (!store_->Get(PathPlacementPolicyKey(prefix), &payload, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }
        zb::rpc::PathPlacementPolicyRecord loaded;
        if (!loaded.ParseFromString(payload)) {
            if (error) {
                *error = "invalid path placement policy payload";
            }
            return false;
        }
        if (policy) {
            *policy = loaded;
        }
        if (matched_prefix) {
            *matched_prefix = prefix;
        }
        return true;
    }
    return false;
}

bool MdsServiceImpl::SavePathPlacementPolicy(const zb::rpc::PathPlacementPolicyRecord& policy, std::string* error) {
    std::string normalized_prefix;
    if (!NormalizePolicyPath(policy.path_prefix(), &normalized_prefix)) {
        if (error) {
            *error = "invalid path_prefix";
        }
        return false;
    }
    if (policy.target() == zb::rpc::PATH_PLACEMENT_UNSPECIFIED) {
        if (error) {
            *error = "policy target is unspecified";
        }
        return false;
    }
    zb::rpc::PathPlacementPolicyRecord normalized = policy;
    normalized.set_path_prefix(normalized_prefix);
    std::string payload;
    if (!normalized.SerializeToString(&payload)) {
        if (error) {
            *error = "failed to encode path placement policy";
        }
        return false;
    }
    return store_->Put(PathPlacementPolicyKey(normalized_prefix), payload, error);
}

bool MdsServiceImpl::DeletePathPlacementPolicyByPrefix(const std::string& path_prefix, std::string* error) {
    std::string normalized_prefix;
    if (!NormalizePolicyPath(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid path_prefix";
        }
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Delete(PathPlacementPolicyKey(normalized_prefix));
    return store_->WriteBatch(&batch, error);
}

bool MdsServiceImpl::LoadFileAnchorSet(uint64_t inode_id,
                                       zb::rpc::FileAnchorSet* anchors,
                                       std::string* error) const {
    if (!anchors || inode_id == 0) {
        if (error) {
            *error = "invalid file anchor set output";
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
    if (!anchors->ParseFromString(data)) {
        if (error) {
            *error = "invalid file anchor payload";
        }
        return false;
    }

    uint32_t mask = anchors->anchor_mask();
    if (!anchors->disk_anchor().node_id().empty() || !anchors->disk_anchor().disk_id().empty()) {
        mask |= kAnchorMaskDisk;
    }
    if (!anchors->optical_anchor().node_id().empty() || !anchors->optical_anchor().disk_id().empty()) {
        mask |= kAnchorMaskOptical;
    }
    anchors->set_anchor_mask(mask);
    if (anchors->version() == 0) {
        anchors->set_version(1);
    }
    if (anchors->primary_tier() != zb::rpc::STORAGE_TIER_DISK &&
        anchors->primary_tier() != zb::rpc::STORAGE_TIER_OPTICAL) {
        anchors->set_primary_tier((mask & kAnchorMaskDisk) ? zb::rpc::STORAGE_TIER_DISK
                                                            : zb::rpc::STORAGE_TIER_OPTICAL);
    }
    if ((mask & kAnchorMaskDisk) != 0U) {
        anchors->mutable_disk_anchor()->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        anchors->mutable_disk_anchor()->set_replica_state(zb::rpc::REPLICA_READY);
    }
    if ((mask & kAnchorMaskOptical) != 0U) {
        anchors->mutable_optical_anchor()->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        anchors->mutable_optical_anchor()->set_replica_state(zb::rpc::REPLICA_READY);
    }
    return true;
}

bool MdsServiceImpl::SaveFileAnchorSet(uint64_t inode_id,
                                       const zb::rpc::FileAnchorSet& anchors,
                                       rocksdb::WriteBatch* batch) const {
    if (!batch || inode_id == 0) {
        return false;
    }
    zb::rpc::FileAnchorSet normalized = anchors;
    if (normalized.version() == 0) {
        normalized.set_version(1);
    }
    uint32_t mask = 0;
    if (!normalized.disk_anchor().node_id().empty() || !normalized.disk_anchor().disk_id().empty()) {
        normalized.mutable_disk_anchor()->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        normalized.mutable_disk_anchor()->set_replica_state(zb::rpc::REPLICA_READY);
        if (normalized.disk_anchor().object_id().empty()) {
            normalized.mutable_disk_anchor()->set_object_id(BuildStableObjectId(inode_id, 0));
        }
        mask |= kAnchorMaskDisk;
    }
    if (!normalized.optical_anchor().node_id().empty() || !normalized.optical_anchor().disk_id().empty()) {
        normalized.mutable_optical_anchor()->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        normalized.mutable_optical_anchor()->set_replica_state(zb::rpc::REPLICA_READY);
        if (normalized.optical_anchor().object_id().empty()) {
            normalized.mutable_optical_anchor()->set_object_id(BuildStableObjectId(inode_id, 0));
        }
        mask |= kAnchorMaskOptical;
    }
    normalized.set_anchor_mask(mask);
    if (normalized.primary_tier() != zb::rpc::STORAGE_TIER_DISK &&
        normalized.primary_tier() != zb::rpc::STORAGE_TIER_OPTICAL) {
        normalized.set_primary_tier((mask & kAnchorMaskDisk) ? zb::rpc::STORAGE_TIER_DISK
                                                              : zb::rpc::STORAGE_TIER_OPTICAL);
    }

    std::string payload;
    if (!normalized.SerializeToString(&payload)) {
        return false;
    }
    batch->Put(FileAnchorKey(inode_id), payload);
    return true;
}

zb::rpc::FileAnchorSet MdsServiceImpl::BuildFileAnchorSetFromSingle(const zb::rpc::ReplicaLocation& anchor) {
    zb::rpc::FileAnchorSet anchors;
    anchors.set_version(1);
    const bool optical = anchor.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL;
    if (optical) {
        anchors.set_anchor_mask(kAnchorMaskOptical);
        anchors.set_primary_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        *anchors.mutable_optical_anchor() = ToAnchorLite(anchor);
    } else {
        anchors.set_anchor_mask(kAnchorMaskDisk);
        anchors.set_primary_tier(zb::rpc::STORAGE_TIER_DISK);
        *anchors.mutable_disk_anchor() = ToAnchorLite(anchor);
    }
    return anchors;
}

bool MdsServiceImpl::SelectPrimaryAnchor(const zb::rpc::FileAnchorSet& anchors,
                                         zb::rpc::ReplicaLocation* anchor) {
    if (!anchor) {
        return false;
    }
    const uint32_t mask = anchors.anchor_mask();
    if (anchors.primary_tier() == zb::rpc::STORAGE_TIER_DISK && (mask & kAnchorMaskDisk) != 0U) {
        *anchor = ToReplicaLocation(anchors.disk_anchor());
        return !anchor->node_id().empty() && !anchor->disk_id().empty();
    }
    if (anchors.primary_tier() == zb::rpc::STORAGE_TIER_OPTICAL && (mask & kAnchorMaskOptical) != 0U) {
        *anchor = ToReplicaLocation(anchors.optical_anchor());
        return !anchor->node_id().empty() && !anchor->disk_id().empty();
    }
    if ((mask & kAnchorMaskDisk) != 0U) {
        *anchor = ToReplicaLocation(anchors.disk_anchor());
        return !anchor->node_id().empty() && !anchor->disk_id().empty();
    }
    if ((mask & kAnchorMaskOptical) != 0U) {
        *anchor = ToReplicaLocation(anchors.optical_anchor());
        return !anchor->node_id().empty() && !anchor->disk_id().empty();
    }
    return false;
}

bool MdsServiceImpl::SelectDiskAnchor(const zb::rpc::FileAnchorSet& anchors,
                                      zb::rpc::ReplicaLocation* anchor) {
    if (!anchor) {
        return false;
    }
    if ((anchors.anchor_mask() & kAnchorMaskDisk) == 0U) {
        return false;
    }
    *anchor = ToReplicaLocation(anchors.disk_anchor());
    return !anchor->node_id().empty() && !anchor->disk_id().empty();
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
    zb::rpc::FileAnchorSet anchors;
    if (!LoadFileAnchorSet(inode_id, &anchors, error)) {
        return false;
    }
    if (!SelectPrimaryAnchor(anchors, anchor)) {
        if (error) {
            *error = "file anchor set has no usable anchor";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::SaveFileAnchor(uint64_t inode_id,
                                    const zb::rpc::ReplicaLocation& anchor,
                                    rocksdb::WriteBatch* batch) const {
    if (!batch || inode_id == 0) {
        return false;
    }
    zb::rpc::FileAnchorSet anchors = BuildFileAnchorSetFromSingle(anchor);
    return SaveFileAnchorSet(inode_id, anchors, batch);
}

std::string MdsServiceImpl::BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

void MdsServiceImpl::StripReplicaAddresses(zb::rpc::ReplicaLocation* replica) {
    if (!replica) {
        return;
    }
    replica->clear_node_address();
    replica->clear_primary_address();
    replica->clear_secondary_address();
}

bool MdsServiceImpl::ResolveNodeAddress(const std::string& node_id, std::string* address, std::string* error) const {
    if (node_id.empty() || !address) {
        if (error) {
            *error = "node_id is empty";
        }
        return false;
    }
    if (!allocator_ || !allocator_->ResolveNodeAddress(node_id, address) || address->empty()) {
        if (error) {
            *error = "failed to resolve node address for node_id=" + node_id;
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::DeleteFileMetaOnAnchor(const zb::rpc::ReplicaLocation& anchor,
                                            uint64_t inode_id,
                                            bool purge_objects,
                                            std::string* error) {
    if (inode_id == 0) {
        if (error) {
            *error = "inode_id is zero";
        }
        return false;
    }
    if (anchor.node_id().empty()) {
        if (error) {
            *error = "anchor node_id is empty";
        }
        return false;
    }
    if (purge_objects && anchor.disk_id().empty()) {
        if (error) {
            *error = "anchor disk_id is empty";
        }
        return false;
    }
    std::string address;
    if (!ResolveNodeAddress(anchor.node_id(), &address, error)) {
        return false;
    }
    brpc::Channel* channel = GetDataChannel(address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::DeleteFileMetaRequest req;
    req.set_inode_id(inode_id);
    req.set_disk_id(anchor.disk_id());
    req.set_purge_objects(purge_objects);
    zb::rpc::DeleteFileMetaReply resp;
    stub.DeleteFileMeta(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() == zb::rpc::STATUS_OK || resp.status().code() == zb::rpc::STATUS_NOT_FOUND) {
        return true;
    }
    if (error) {
        *error = resp.status().message();
    }
    return false;
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
