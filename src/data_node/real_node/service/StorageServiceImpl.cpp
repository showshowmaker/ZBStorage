#include "StorageServiceImpl.h"

namespace zb::real_node {

StorageServiceImpl::StorageServiceImpl(DiskManager* disk_manager,
                                       LocalPathResolver* path_resolver,
                                       IOExecutor* io_executor)
    : disk_manager_(disk_manager),
      path_resolver_(path_resolver),
      io_executor_(io_executor) {}

zb::msg::WriteChunkReply StorageServiceImpl::WriteChunk(const zb::msg::WriteChunkRequest& request) {
    zb::msg::WriteChunkReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, request.chunk_id, true);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }

    uint64_t bytes_written = 0;
    reply.status = io_executor_->Write(path, request.offset, request.data, &bytes_written);
    reply.bytes = bytes_written;
    return reply;
}

zb::msg::ReadChunkReply StorageServiceImpl::ReadChunk(const zb::msg::ReadChunkRequest& request) {
    zb::msg::ReadChunkReply reply;
    if (!disk_manager_ || !path_resolver_ || !io_executor_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    if (request.disk_id.empty() || request.chunk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or chunk_id is empty");
        return reply;
    }

    std::string mount_point = disk_manager_->GetMountPoint(request.disk_id);
    if (mount_point.empty()) {
        reply.status = zb::msg::Status::NotFound("Disk not found or unhealthy: " + request.disk_id);
        return reply;
    }

    std::string path = path_resolver_->Resolve(mount_point, request.chunk_id, false);
    if (path.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("Failed to resolve path");
        return reply;
    }

    uint64_t bytes_read = 0;
    reply.status = io_executor_->Read(path, request.offset, request.size, &reply.data, &bytes_read);
    reply.bytes = bytes_read;
    return reply;
}

zb::msg::DiskReportReply StorageServiceImpl::GetDiskReport() const {
    zb::msg::DiskReportReply reply;
    if (!disk_manager_) {
        reply.status = zb::msg::Status::InternalError("Service not initialized");
        return reply;
    }
    reply.reports = disk_manager_->GetReport();
    reply.status = zb::msg::Status::Ok();
    return reply;
}

} // namespace zb::real_node
