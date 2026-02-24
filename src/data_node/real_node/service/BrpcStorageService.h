#pragma once

#include "StorageServiceImpl.h"
#include "real_node.pb.h"

namespace zb::real_node {

class BrpcStorageService : public zb::rpc::RealNodeService {
public:
    explicit BrpcStorageService(StorageServiceImpl* service);

    void WriteChunk(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::WriteChunkRequest* request,
                    zb::rpc::WriteChunkReply* response,
                    google::protobuf::Closure* done) override;

    void ReadChunk(google::protobuf::RpcController* cntl_base,
                   const zb::rpc::ReadChunkRequest* request,
                   zb::rpc::ReadChunkReply* response,
                   google::protobuf::Closure* done) override;

    void DeleteChunk(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::DeleteChunkRequest* request,
                     zb::rpc::DeleteChunkReply* response,
                     google::protobuf::Closure* done) override;

    void GetDiskReport(google::protobuf::RpcController* cntl_base,
                       const google::protobuf::Empty* request,
                       zb::rpc::DiskReportReply* response,
                       google::protobuf::Closure* done) override;

private:
    StorageServiceImpl* service_{};
};

} // namespace zb::real_node
