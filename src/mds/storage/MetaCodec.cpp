#include "MetaCodec.h"

#include <cstring>

namespace zb::mds {

std::string MetaCodec::EncodeUInt64(uint64_t value) {
    std::string out(sizeof(uint64_t), '\0');
    std::memcpy(&out[0], &value, sizeof(uint64_t));
    return out;
}

bool MetaCodec::DecodeUInt64(const std::string& data, uint64_t* value) {
    if (!value || data.size() != sizeof(uint64_t)) {
        return false;
    }
    std::memcpy(value, data.data(), sizeof(uint64_t));
    return true;
}

std::string MetaCodec::EncodeInodeAttr(const zb::rpc::InodeAttr& attr) {
    std::string out;
    attr.SerializeToString(&out);
    return out;
}

bool MetaCodec::DecodeInodeAttr(const std::string& data, zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return false;
    }
    return attr->ParseFromString(data);
}

std::string MetaCodec::EncodeChunkMeta(const zb::rpc::ChunkMeta& meta) {
    std::string out;
    meta.SerializeToString(&out);
    return out;
}

bool MetaCodec::DecodeChunkMeta(const std::string& data, zb::rpc::ChunkMeta* meta) {
    if (!meta) {
        return false;
    }
    return meta->ParseFromString(data);
}

} // namespace zb::mds
