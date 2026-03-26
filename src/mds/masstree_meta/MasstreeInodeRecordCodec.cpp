#include "MasstreeInodeRecordCodec.h"

namespace zb::mds {

namespace {

constexpr uint8_t kMasstreeInodeRecordVersion = 1;
constexpr uint8_t kHasOpticalImageFlag = 0x1U;
constexpr uint64_t kInodeIdFieldNumber = 1U;
constexpr uint8_t kWireTypeVarint = 0U;

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

void AppendVarint(std::string* out, uint64_t value) {
    do {
        uint8_t byte = static_cast<uint8_t>(value & 0x7FU);
        value >>= 7U;
        if (value != 0) {
            byte |= 0x80U;
        }
        out->push_back(static_cast<char>(byte));
    } while (value != 0);
}

bool ReadVarint(const std::string& data,
                size_t* cursor,
                uint64_t* value,
                size_t* begin,
                size_t* end) {
    if (!cursor || !value || *cursor >= data.size()) {
        return false;
    }
    const size_t start = *cursor;
    uint64_t parsed = 0;
    uint32_t shift = 0;
    while (*cursor < data.size() && shift < 64U) {
        const uint8_t byte = static_cast<uint8_t>(data[*cursor]);
        ++(*cursor);
        parsed |= static_cast<uint64_t>(byte & 0x7FU) << shift;
        if ((byte & 0x80U) == 0) {
            *value = parsed;
            if (begin) {
                *begin = start;
            }
            if (end) {
                *end = *cursor;
            }
            return true;
        }
        shift += 7U;
    }
    return false;
}

bool PatchAttrPayloadInodeId(const char* data,
                             size_t len,
                             uint64_t inode_id,
                             std::string* patched,
                             std::string* error) {
    if (!data || !patched) {
        if (error) {
            *error = "invalid masstree inode attr patch args";
        }
        return false;
    }
    const std::string payload(data, len);
    patched->clear();
    patched->reserve(payload.size() + 8U);
    size_t cursor = 0;
    bool inode_id_replaced = false;
    while (cursor < payload.size()) {
        size_t key_begin = 0;
        size_t key_end = 0;
        uint64_t key = 0;
        if (!ReadVarint(payload, &cursor, &key, &key_begin, &key_end)) {
            if (error) {
                *error = "failed to parse masstree inode attr key";
            }
            return false;
        }
        const uint64_t field_number = key >> 3U;
        const uint8_t wire_type = static_cast<uint8_t>(key & 0x7U);
        patched->append(payload, key_begin, key_end - key_begin);
        if (field_number == kInodeIdFieldNumber && wire_type == kWireTypeVarint && !inode_id_replaced) {
            uint64_t ignored = 0;
            if (!ReadVarint(payload, &cursor, &ignored, nullptr, nullptr)) {
                if (error) {
                    *error = "failed to parse masstree inode_id field";
                }
                return false;
            }
            AppendVarint(patched, inode_id);
            inode_id_replaced = true;
            continue;
        }
        switch (wire_type) {
        case 0: {
            uint64_t ignored = 0;
            size_t value_begin = 0;
            size_t value_end = 0;
            if (!ReadVarint(payload, &cursor, &ignored, &value_begin, &value_end)) {
                if (error) {
                    *error = "failed to parse masstree inode attr varint field";
                }
                return false;
            }
            patched->append(payload, value_begin, value_end - value_begin);
            break;
        }
        case 1:
            if (cursor + sizeof(uint64_t) > payload.size()) {
                if (error) {
                    *error = "masstree inode attr fixed64 field is truncated";
                }
                return false;
            }
            patched->append(payload, cursor, sizeof(uint64_t));
            cursor += sizeof(uint64_t);
            break;
        case 2: {
            uint64_t field_len = 0;
            size_t len_begin = 0;
            size_t len_end = 0;
            if (!ReadVarint(payload, &cursor, &field_len, &len_begin, &len_end) ||
                cursor + field_len > payload.size()) {
                if (error) {
                    *error = "masstree inode attr length-delimited field is truncated";
                }
                return false;
            }
            patched->append(payload, len_begin, len_end - len_begin);
            patched->append(payload, cursor, static_cast<size_t>(field_len));
            cursor += static_cast<size_t>(field_len);
            break;
        }
        case 5:
            if (cursor + sizeof(uint32_t) > payload.size()) {
                if (error) {
                    *error = "masstree inode attr fixed32 field is truncated";
                }
                return false;
            }
            patched->append(payload, cursor, sizeof(uint32_t));
            cursor += sizeof(uint32_t);
            break;
        default:
            if (error) {
                *error = "unsupported masstree inode attr wire type";
            }
            return false;
        }
    }
    if (!inode_id_replaced) {
        if (error) {
            *error = "masstree inode attr missing inode_id field";
        }
        return false;
    }
    return true;
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

bool MasstreeInodeRecordCodec::HasOpticalImage(const std::string& data,
                                               bool* has_optical_image,
                                               std::string* error) {
    if (!has_optical_image) {
        if (error) {
            *error = "masstree inode record optical flag output is null";
        }
        return false;
    }
    if (data.size() < 2U) {
        if (error) {
            *error = "masstree inode record payload is too short";
        }
        return false;
    }
    if (static_cast<uint8_t>(data[0]) != kMasstreeInodeRecordVersion) {
        if (error) {
            *error = "unsupported masstree inode record version";
        }
        return false;
    }
    *has_optical_image = (static_cast<uint8_t>(data[1]) & kHasOpticalImageFlag) != 0U;
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeInodeRecordCodec::RebaseEncoded(const std::string& data,
                                             uint64_t inode_id,
                                             bool has_optical_image,
                                             uint64_t optical_image_global_id,
                                             std::string* rebased,
                                             std::string* error) {
    if (!rebased) {
        if (error) {
            *error = "masstree inode record rebased output is null";
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
    const bool source_has_optical_image = (flags & kHasOpticalImageFlag) != 0U;
    if (source_has_optical_image && data.size() < payload_offset + attr_len + sizeof(uint64_t)) {
        if (error) {
            *error = "masstree inode optical image payload is truncated";
        }
        return false;
    }
    if (source_has_optical_image != has_optical_image) {
        if (error) {
            *error = "masstree inode record optical flag mismatch";
        }
        return false;
    }
    std::string patched_attr_payload;
    if (!PatchAttrPayloadInodeId(data.data() + payload_offset,
                                 static_cast<size_t>(attr_len),
                                 inode_id,
                                 &patched_attr_payload,
                                 error)) {
        return false;
    }

    rebased->clear();
    rebased->reserve(1U + 1U + sizeof(uint32_t) + patched_attr_payload.size() +
                     (has_optical_image ? sizeof(uint64_t) : 0U));
    rebased->push_back(static_cast<char>(version));
    rebased->push_back(static_cast<char>(flags));
    AppendLe32(rebased, static_cast<uint32_t>(patched_attr_payload.size()));
    rebased->append(patched_attr_payload);
    if (has_optical_image) {
        AppendLe64(rebased, optical_image_global_id);
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
