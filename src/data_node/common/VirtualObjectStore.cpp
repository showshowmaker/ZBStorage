#include "ObjectStore.h"

#include "../virtual_node/service/VirtualStorageServiceImpl.h"
#include "../../msg/storage_node_messages.h"

namespace zb::data_node {

namespace {

zb::msg::WriteObjectRequest ToWriteObjectRequest(const ObjectWriteRequest& request) {
    zb::msg::WriteObjectRequest out;
    out.disk_id = request.disk_id;
    out.SetArchiveObjectId(request.object_id);
    out.offset = request.offset;
    out.data.assign(request.data.data(), request.data.size());
    out.epoch = request.epoch;
    out.is_replication = request.is_replication;
    return out;
}

zb::msg::ReadObjectRequest ToReadObjectRequest(const ObjectReadRequest& request) {
    zb::msg::ReadObjectRequest out;
    out.disk_id = request.disk_id;
    out.SetArchiveObjectId(request.object_id);
    out.offset = request.offset;
    out.size = request.size;
    return out;
}

zb::msg::DeleteObjectRequest ToDeleteObjectRequest(const ObjectDeleteRequest& request) {
    zb::msg::DeleteObjectRequest out;
    out.disk_id = request.disk_id;
    out.SetArchiveObjectId(request.object_id);
    return out;
}

} // namespace

VirtualObjectStore::VirtualObjectStore(zb::virtual_node::VirtualStorageServiceImpl* storage)
    : storage_(storage) {}

zb::msg::Status VirtualObjectStore::PutObject(const ObjectWriteRequest& request) {
    if (!storage_) {
        return zb::msg::Status::InternalError("virtual object store is not initialized");
    }
    const zb::msg::WriteObjectReply reply = storage_->WriteObject(ToWriteObjectRequest(request));
    return reply.status;
}

ObjectReadResult VirtualObjectStore::GetObject(const ObjectReadRequest& request) {
    ObjectReadResult result;
    if (!storage_) {
        result.status = zb::msg::Status::InternalError("virtual object store is not initialized");
        return result;
    }
    const zb::msg::ReadObjectReply reply = storage_->ReadObject(ToReadObjectRequest(request));
    result.status = reply.status;
    if (reply.status.ok()) {
        result.data = reply.data;
    }
    return result;
}

zb::msg::Status VirtualObjectStore::DeleteObject(const ObjectDeleteRequest& request) {
    if (!storage_) {
        return zb::msg::Status::InternalError("virtual object store is not initialized");
    }
    const zb::msg::DeleteObjectReply reply = storage_->DeleteObject(ToDeleteObjectRequest(request));
    return reply.status;
}

} // namespace zb::data_node
