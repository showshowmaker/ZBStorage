#include "BrpcVirtualStorageService.h"

#include <brpc/controller.h>

#include "../../../msg/status.h"

namespace zb::virtual_node {

namespace {

zb::rpc::StatusCode ToProtoStatusCode(zb::msg::StatusCode code) {
    switch (code) {
        case zb::msg::StatusCode::kOk:
            return zb::rpc::STATUS_OK;
        case zb::msg::StatusCode::kInvalidArgument:
            return zb::rpc::STATUS_INVALID_ARGUMENT;
        case zb::msg::StatusCode::kNotFound:
            return zb::rpc::STATUS_NOT_FOUND;
        case zb::msg::StatusCode::kIoError:
            return zb::rpc::STATUS_IO_ERROR;
        case zb::msg::StatusCode::kInternalError:
        default:
            return zb::rpc::STATUS_INTERNAL_ERROR;
    }
}

void FillStatus(const zb::msg::Status& status, zb::rpc::Status* out) {
    if (!out) {
        return;
    }
    out->set_code(ToProtoStatusCode(status.code));
    out->set_message(status.message);
}

} // namespace

BrpcVirtualStorageService::BrpcVirtualStorageService(VirtualStorageServiceImpl* service) : service_(service) {}

void BrpcVirtualStorageService::WriteChunk(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::WriteChunkRequest* request,
                                           zb::rpc::WriteChunkReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::WriteChunkRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.chunk_id = request->chunk_id();
    internal_req.offset = request->offset();
    internal_req.data = request->data();
    internal_req.is_replication = request->is_replication();
    internal_req.epoch = request->epoch();

    zb::msg::WriteChunkReply internal_reply = service_->WriteChunk(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
}

void BrpcVirtualStorageService::ReadChunk(google::protobuf::RpcController* cntl_base,
                                          const zb::rpc::ReadChunkRequest* request,
                                          zb::rpc::ReadChunkReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::ReadChunkRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.chunk_id = request->chunk_id();
    internal_req.offset = request->offset();
    internal_req.size = request->size();

    zb::msg::ReadChunkReply internal_reply = service_->ReadChunk(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
    response->set_data(internal_reply.data);
}

void BrpcVirtualStorageService::DeleteChunk(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::DeleteChunkRequest* request,
                                            zb::rpc::DeleteChunkReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::DeleteChunkRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.chunk_id = request->chunk_id();
    zb::msg::DeleteChunkReply internal_reply = service_->DeleteChunk(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
}

void BrpcVirtualStorageService::GetDiskReport(google::protobuf::RpcController* cntl_base,
                                              const google::protobuf::Empty* request,
                                              zb::rpc::DiskReportReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!service_ || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::DiskReportReply internal_reply = service_->GetDiskReport();
    FillStatus(internal_reply.status, response->mutable_status());
    for (const auto& report : internal_reply.reports) {
        zb::rpc::DiskReport* item = response->add_reports();
        item->set_id(report.id);
        item->set_mount_point(report.mount_point);
        item->set_capacity_bytes(report.capacity_bytes);
        item->set_free_bytes(report.free_bytes);
        item->set_is_healthy(report.is_healthy);
    }
}

} // namespace zb::virtual_node
