#include "ObjectStore.h"

#include "../real_node/service/StorageServiceImpl.h"
#include "../../msg/storage_node_messages.h"

namespace zb::data_node {

namespace {

zb::msg::WriteChunkRequest ToWriteChunkRequest(const ObjectWriteRequest& request) {
    zb::msg::WriteChunkRequest out;
    out.disk_id = request.disk_id;
    out.chunk_id = request.object_id;
    out.offset = request.offset;
    out.data.assign(request.data.data(), request.data.size());
    out.epoch = request.epoch;
    return out;
}

zb::msg::ReadChunkRequest ToReadChunkRequest(const ObjectReadRequest& request) {
    zb::msg::ReadChunkRequest out;
    out.disk_id = request.disk_id;
    out.chunk_id = request.object_id;
    out.offset = request.offset;
    out.size = request.size;
    return out;
}

zb::msg::DeleteChunkRequest ToDeleteChunkRequest(const ObjectDeleteRequest& request) {
    zb::msg::DeleteChunkRequest out;
    out.disk_id = request.disk_id;
    out.chunk_id = request.object_id;
    return out;
}

} // namespace

RealChunkObjectStore::RealChunkObjectStore(zb::real_node::StorageServiceImpl* storage) : storage_(storage) {}

zb::msg::Status RealChunkObjectStore::PutObject(const ObjectWriteRequest& request) {
    if (!storage_) {
        return zb::msg::Status::InternalError("real object store is not initialized");
    }
    const zb::msg::WriteChunkReply reply = storage_->WriteChunk(ToWriteChunkRequest(request));
    return reply.status;
}

ObjectReadResult RealChunkObjectStore::GetObject(const ObjectReadRequest& request) {
    ObjectReadResult result;
    if (!storage_) {
        result.status = zb::msg::Status::InternalError("real object store is not initialized");
        return result;
    }
    const zb::msg::ReadChunkReply reply = storage_->ReadChunk(ToReadChunkRequest(request));
    result.status = reply.status;
    if (reply.status.ok()) {
        result.data = reply.data;
    }
    return result;
}

zb::msg::Status RealChunkObjectStore::DeleteObject(const ObjectDeleteRequest& request) {
    if (!storage_) {
        return zb::msg::Status::InternalError("real object store is not initialized");
    }
    const zb::msg::DeleteChunkReply reply = storage_->DeleteChunk(ToDeleteChunkRequest(request));
    return reply.status;
}

} // namespace zb::data_node
