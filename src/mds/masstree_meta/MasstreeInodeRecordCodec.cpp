#include "MasstreeInodeRecordCodec.h"

namespace zb::mds {

namespace {

constexpr uint8_t kMasstreeInodeRecordVersion = 1;
constexpr uint8_t kHasOpticalImageFlag = 0x1U;

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

void AppendLe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

uint32_t DecodeLe32(const char* data) {
    uint32_t value = 0;
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

} // namespace

bool MasstreeInodeRecordCodec::Encode(const MasstreeInodeRecord& record,
                                      std::string* data,
                                      std::string* error) {
    if (!data) {
        if (error) {
            *error = "masstree inode record encode output is null";
        }
        return false;
    }
    std::string attr_payload;
    if (!record.attr.SerializeToString(&attr_payload)) {
        if (error) {
            *error = "failed to serialize masstree inode attr";
        }
        return false;
    }

    data->clear();
    data->reserve(1U + 1U + sizeof(uint32_t) + attr_payload.size() +
                  (record.has_optical_image ? sizeof(uint64_t) : 0U));
    data->push_back(static_cast<char>(kMasstreeInodeRecordVersion));
    data->push_back(static_cast<char>(record.has_optical_image ? kHasOpticalImageFlag : 0U));
    AppendLe32(data, static_cast<uint32_t>(attr_payload.size()));
    data->append(attr_payload);
    if (record.has_optical_image) {
        AppendLe64(data, record.optical_image_global_id);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeInodeRecordCodec::Decode(const std::string& data,
                                      MasstreeInodeRecord* record,
                                      std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree inode record decode output is null";
        }
        return false;
    }
    if (data.size() < 1U + 1U + sizeof(uint32_t)) {
        if (error) {
            *error = "masstree inode record payload is too short";
        }
        return false;
    }
    const uint8_t version = static_cast<uint8_t>(data[0]);
    if (version != kMasstreeInodeRecordVersion) {
        if (error) {
            *error = "unsupported masstree inode record version";
        }
        return false;
    }
    const uint8_t flags = static_cast<uint8_t>(data[1]);
    const uint32_t attr_len = DecodeLe32(data.data() + 2U);
    const size_t payload_offset = 1U + 1U + sizeof(uint32_t);
    if (data.size() < payload_offset + attr_len) {
        if (error) {
            *error = "masstree inode attr payload is truncated";
        }
        return false;
    }

    MasstreeInodeRecord decoded;
    if (!decoded.attr.ParseFromArray(data.data() + payload_offset, static_cast<int>(attr_len))) {
        if (error) {
            *error = "failed to parse masstree inode attr payload";
        }
        return false;
    }
    decoded.has_optical_image = (flags & kHasOpticalImageFlag) != 0U;
    if (decoded.has_optical_image) {
        if (data.size() < payload_offset + attr_len + sizeof(uint64_t)) {
            if (error) {
                *error = "masstree inode optical image payload is truncated";
            }
            return false;
        }
        decoded.optical_image_global_id = DecodeLe64(data.data() + payload_offset + attr_len);
    }

    *record = std::move(decoded);
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
