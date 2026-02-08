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

    void CreateFile(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::CreateFileRequest* request,
                    zb::rpc::CreateFileReply* response,
                    google::protobuf::Closure* done) override;

    void GetFileMeta(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::GetFileMetaRequest* request,
                     zb::rpc::GetFileMetaReply* response,
                     google::protobuf::Closure* done) override;

    void ReportNodeStatus(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReportNodeStatusRequest* request,
                          zb::rpc::ReportNodeStatusReply* response,
                          google::protobuf::Closure* done) override;

private:
    uint64_t AllocateInodeId(std::string* error);
    static std::string GenerateChunkId();
    static void FillStatus(zb::rpc::MdsStatus* status, zb::rpc::MdsStatusCode code, const std::string& message);

    RocksMetaStore* store_{};
    ChunkAllocator* allocator_{};
    uint64_t default_chunk_size_{0};
};

} // namespace zb::mds
