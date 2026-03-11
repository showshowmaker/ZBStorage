#include "BrpcStorageService.h"

#include <brpc/controller.h>

#include "../../common/ObjectStore.h"
#include "../../../msg/status.h"

namespace zb::real_node {

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

BrpcStorageService::BrpcStorageService(StorageServiceImpl* service) : service_(service) {}

void BrpcStorageService::WriteObject(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::WriteObjectRequest* request,
                                     zb::rpc::WriteObjectReply* response,
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

    zb::data_node::ObjectWriteRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    object_req.offset = request->offset();
    object_req.data = request->data();
    object_req.epoch = request->epoch();
    object_req.is_replication = request->is_replication();

    const zb::msg::Status st = service_->PutObject(object_req);
    FillStatus(st, response->mutable_status());
    if (st.ok()) {
        response->set_bytes(static_cast<uint64_t>(request->data().size()));
    }
}

void BrpcStorageService::ReadObject(google::protobuf::RpcController* cntl_base,
                                    const zb::rpc::ReadObjectRequest* request,
                                    zb::rpc::ReadObjectReply* response,
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

    zb::data_node::ObjectReadRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    object_req.offset = request->offset();
    object_req.size = request->size();

    const zb::data_node::ObjectReadResult result = service_->GetObject(object_req);
    FillStatus(result.status, response->mutable_status());
    if (result.status.ok()) {
        response->set_bytes(static_cast<uint64_t>(result.data.size()));
        response->set_data(result.data);
    }
}

void BrpcStorageService::DeleteObject(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::DeleteObjectRequest* request,
                                      zb::rpc::DeleteObjectReply* response,
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

    zb::data_node::ObjectDeleteRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    const zb::msg::Status st = service_->DeleteObject(object_req);
    FillStatus(st, response->mutable_status());
}

void BrpcStorageService::ReadArchivedFile(google::protobuf::RpcController* cntl_base,
                                          const zb::rpc::ReadArchivedFileRequest* request,
                                          zb::rpc::ReadArchivedFileReply* response,
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

    zb::msg::ReadArchivedFileRequest internal_req;
    internal_req.disc_id = request->disc_id();
    internal_req.inode_id = request->inode_id();
    internal_req.file_id = request->file_id();
    internal_req.offset = request->offset();
    internal_req.size = request->size();

    const zb::msg::ReadArchivedFileReply internal_reply = service_->ReadArchivedFile(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
    response->set_data(internal_reply.data);
}

void BrpcStorageService::UpdateArchiveState(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::UpdateArchiveStateRequest* request,
                                            zb::rpc::UpdateArchiveStateReply* response,
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

    const std::string object_id = request->object_id();
    const zb::msg::Status internal_status =
        service_->UpdateArchiveState(request->disk_id(), object_id, request->archive_state(), request->version());
    FillStatus(internal_status, response->mutable_status());
}

void BrpcStorageService::GetDiskReport(google::protobuf::RpcController* cntl_base,
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

} // namespace zb::real_node
