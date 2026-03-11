#pragma once

#include "VirtualStorageServiceImpl.h"
#include "real_node.pb.h"

namespace zb::virtual_node {

class BrpcVirtualStorageService : public zb::rpc::RealNodeService {
public:
    explicit BrpcVirtualStorageService(VirtualStorageServiceImpl* service);

    void WriteObject(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::WriteObjectRequest* request,
                     zb::rpc::WriteObjectReply* response,
                     google::protobuf::Closure* done) override;

    void ReadObject(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::ReadObjectRequest* request,
                    zb::rpc::ReadObjectReply* response,
                    google::protobuf::Closure* done) override;

    void DeleteObject(google::protobuf::RpcController* cntl_base,
                      const zb::rpc::DeleteObjectRequest* request,
                      zb::rpc::DeleteObjectReply* response,
                      google::protobuf::Closure* done) override;

    void ReadArchivedFile(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReadArchivedFileRequest* request,
                          zb::rpc::ReadArchivedFileReply* response,
                          google::protobuf::Closure* done) override;

    void UpdateArchiveState(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::UpdateArchiveStateRequest* request,
                            zb::rpc::UpdateArchiveStateReply* response,
                            google::protobuf::Closure* done) override;

    void GetDiskReport(google::protobuf::RpcController* cntl_base,
                       const google::protobuf::Empty* request,
                       zb::rpc::DiskReportReply* response,
                       google::protobuf::Closure* done) override;

private:
    VirtualStorageServiceImpl* service_{};
};

} // namespace zb::virtual_node
