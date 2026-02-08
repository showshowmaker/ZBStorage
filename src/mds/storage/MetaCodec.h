#pragma once

#include <cstdint>
#include <string>

#include "mds.pb.h"

namespace zb::mds {

class MetaCodec {
public:
    static std::string EncodeUInt64(uint64_t value);
    static bool DecodeUInt64(const std::string& data, uint64_t* value);

    static std::string EncodeFileMeta(const zb::rpc::FileMeta& meta);
    static bool DecodeFileMeta(const std::string& data, zb::rpc::FileMeta* meta);

    static std::string EncodeChunkMeta(const zb::rpc::ChunkMeta& meta);
    static bool DecodeChunkMeta(const std::string& data, zb::rpc::ChunkMeta* meta);
};

} // namespace zb::mds
