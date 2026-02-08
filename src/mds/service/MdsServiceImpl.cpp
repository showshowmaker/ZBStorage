#include "MdsServiceImpl.h"

#include <brpc/controller.h>

#include <cmath>
#include <random>

namespace zb::mds {

MdsServiceImpl::MdsServiceImpl(RocksMetaStore* store, ChunkAllocator* allocator, uint64_t default_chunk_size)
    : store_(store), allocator_(allocator), default_chunk_size_(default_chunk_size) {}

void MdsServiceImpl::CreateFile(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::CreateFileRequest* request,
                                zb::rpc::CreateFileReply* response,
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
    if (store_->Exists(PathKey(request->path()), &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "path already exists");
        return;
    }
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t inode_id = AllocateInodeId(&error);
    if (inode_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t chunk_size = request->chunk_size() ? request->chunk_size() : default_chunk_size_;
    uint32_t replica = request->replica() ? request->replica() : 1;
    uint64_t size = request->size();
    uint32_t chunk_count = static_cast<uint32_t>((size + chunk_size - 1) / chunk_size);

    zb::rpc::FileMeta meta;
    meta.set_path(request->path());
    meta.set_inode_id(inode_id);
    meta.set_size(size);
    meta.set_chunk_size(chunk_size);

    rocksdb::WriteBatch batch;
    batch.Put(PathKey(request->path()), MetaCodec::EncodeUInt64(inode_id));

    for (uint32_t index = 0; index < chunk_count; ++index) {
        std::string chunk_id = GenerateChunkId();
        zb::rpc::ChunkMeta* chunk_meta = meta.add_chunks();
        chunk_meta->set_index(index);

        std::vector<zb::rpc::ReplicaLocation> replicas;
        if (!allocator_->AllocateChunk(replica, chunk_id, &replicas)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "Failed to allocate replicas");
            return;
        }
        for (const auto& replica_info : replicas) {
            *chunk_meta->add_replicas() = replica_info;
        }

        batch.Put(ChunkKey(inode_id, index), MetaCodec::EncodeChunkMeta(*chunk_meta));
    }

    batch.Put(InodeKey(inode_id), MetaCodec::EncodeFileMeta(meta));

    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    *response->mutable_meta() = meta;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetFileMeta(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::GetFileMetaRequest* request,
                                 zb::rpc::GetFileMetaReply* response,
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
    std::string inode_data;
    if (!store_->Get(PathKey(request->path()), &inode_data, &error)) {
        if (!error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        }
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }

    std::string meta_data;
    if (!store_->Get(InodeKey(inode_id), &meta_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::FileMeta meta;
    if (!MetaCodec::DecodeFileMeta(meta_data, &meta)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid file meta data");
        return;
    }

    *response->mutable_meta() = meta;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ReportNodeStatus(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::ReportNodeStatusRequest* request,
                                      zb::rpc::ReportNodeStatusReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
    (void)request;
}

uint64_t MdsServiceImpl::AllocateInodeId(std::string* error) {
    std::string value;
    uint64_t next_id = 1;
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
