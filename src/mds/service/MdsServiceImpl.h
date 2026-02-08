#pragma once

#include <string>

#include "../allocator/ChunkAllocator.h"
#include "../storage/RocksMetaStore.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "mds.pb.h"

namespace zb::mds {

class MdsServiceImpl : public zb::rpc::MdsService {
public:
    MdsServiceImpl(RocksMetaStore* store, ChunkAllocator* allocator, uint64_t default_chunk_size);

    void Lookup(google::protobuf::RpcController* cntl_base,
                const zb::rpc::LookupRequest* request,
                zb::rpc::LookupReply* response,
                google::protobuf::Closure* done) override;

    void Getattr(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::GetattrRequest* request,
                 zb::rpc::GetattrReply* response,
                 google::protobuf::Closure* done) override;

    void Open(google::protobuf::RpcController* cntl_base,
              const zb::rpc::OpenRequest* request,
              zb::rpc::OpenReply* response,
              google::protobuf::Closure* done) override;

    void Close(google::protobuf::RpcController* cntl_base,
               const zb::rpc::CloseRequest* request,
               zb::rpc::CloseReply* response,
               google::protobuf::Closure* done) override;

    void Create(google::protobuf::RpcController* cntl_base,
                const zb::rpc::CreateRequest* request,
                zb::rpc::CreateReply* response,
                google::protobuf::Closure* done) override;

    void Mkdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::MkdirRequest* request,
               zb::rpc::MkdirReply* response,
               google::protobuf::Closure* done) override;

    void Readdir(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::ReaddirRequest* request,
                 zb::rpc::ReaddirReply* response,
                 google::protobuf::Closure* done) override;

    void Rename(google::protobuf::RpcController* cntl_base,
                const zb::rpc::RenameRequest* request,
                zb::rpc::RenameReply* response,
                google::protobuf::Closure* done) override;

    void Unlink(google::protobuf::RpcController* cntl_base,
                const zb::rpc::UnlinkRequest* request,
                zb::rpc::UnlinkReply* response,
                google::protobuf::Closure* done) override;

    void Rmdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::RmdirRequest* request,
               zb::rpc::RmdirReply* response,
               google::protobuf::Closure* done) override;

    void AllocateWrite(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::AllocateWriteRequest* request,
                       zb::rpc::AllocateWriteReply* response,
                       google::protobuf::Closure* done) override;

    void GetLayout(google::protobuf::RpcController* cntl_base,
                   const zb::rpc::GetLayoutRequest* request,
                   zb::rpc::GetLayoutReply* response,
                   google::protobuf::Closure* done) override;

    void CommitWrite(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::CommitWriteRequest* request,
                     zb::rpc::CommitWriteReply* response,
                     google::protobuf::Closure* done) override;

    void ReportNodeStatus(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReportNodeStatusRequest* request,
                          zb::rpc::ReportNodeStatusReply* response,
                          google::protobuf::Closure* done) override;

private:
    bool EnsureRoot(std::string* error);
    bool ResolvePath(const std::string& path, uint64_t* inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool ResolveParent(const std::string& path, uint64_t* parent_inode, std::string* name, std::string* error);
    bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error);
    bool PutDentry(uint64_t parent_inode, const std::string& name, uint64_t inode_id, std::string* error);
    bool DeleteDentry(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DentryExists(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DeleteInodeData(uint64_t inode_id, std::string* error);

    uint64_t AllocateInodeId(std::string* error);
    uint64_t AllocateHandleId(std::string* error);
    static std::string GenerateChunkId();
    static uint64_t NowSeconds();
    static void FillStatus(zb::rpc::MdsStatus* status, zb::rpc::MdsStatusCode code, const std::string& message);

    RocksMetaStore* store_{};
    ChunkAllocator* allocator_{};
    uint64_t default_chunk_size_{0};
};

} // namespace zb::mds
