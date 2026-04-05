#include "MetaCodec.h"

#include <cstring>

namespace zb::mds {

namespace {

constexpr uint32_t kEnvelopeMagic = 0x5A424D44U; // ZBMD
constexpr uint32_t kEnvelopeVersionV1 = 1;
constexpr uint32_t kEnvelopeVersionV2 = 2;
constexpr uint32_t kEnvelopeVersionCurrent = kEnvelopeVersionV2;

constexpr size_t kU32Size = sizeof(uint32_t);
constexpr size_t kU64Size = sizeof(uint64_t);

uint32_t Crc32(const char* data, size_t len) {
    static uint32_t table[256];
    static bool inited = false;
    if (!inited) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1U) ? (0xEDB88320U ^ (c >> 1U)) : (c >> 1U);
            }
            table[i] = c;
        }
        inited = true;
    }
    uint32_t crc = 0xFFFFFFFFU;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFFU] ^ (crc >> 8U);
    }
    return crc ^ 0xFFFFFFFFU;
}

void AppendU32(std::string* out, uint32_t value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendU64(std::string* out, uint64_t value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendBool(std::string* out, bool value) {
    const uint8_t v = value ? 1 : 0;
    out->push_back(static_cast<char>(v));
}

void AppendString(std::string* out, const std::string& value) {
    AppendU32(out, static_cast<uint32_t>(value.size()));
    out->append(value);
}

bool ReadU32(const std::string& in, size_t* cursor, uint32_t* value) {
    if (!cursor || !value || *cursor + kU32Size > in.size()) {
        return false;
    }
    std::memcpy(value, in.data() + *cursor, kU32Size);
    *cursor += kU32Size;
    return true;
}

bool ReadU64(const std::string& in, size_t* cursor, uint64_t* value) {
    if (!cursor || !value || *cursor + kU64Size > in.size()) {
        return false;
    }
    std::memcpy(value, in.data() + *cursor, kU64Size);
    *cursor += kU64Size;
    return true;
}

bool ReadBool(const std::string& in, size_t* cursor, bool* value) {
    if (!cursor || !value || *cursor >= in.size()) {
        return false;
    }
    const uint8_t v = static_cast<uint8_t>(in[*cursor]);
    if (v > 1U) {
        return false;
    }
    *value = (v == 1U);
    *cursor += 1;
    return true;
}

bool ReadString(const std::string& in, size_t* cursor, std::string* value) {
    if (!cursor || !value) {
        return false;
    }
    uint32_t len = 0;
    if (!ReadU32(in, cursor, &len)) {
        return false;
    }
    if (*cursor + len > in.size()) {
        return false;
    }
    *value = in.substr(*cursor, len);
    *cursor += len;
    return true;
}

std::string EncodeEnvelope(const std::string& payload, uint32_t version = kEnvelopeVersionCurrent) {
    std::string out;
    out.reserve(sizeof(uint32_t) * 4 + payload.size());
    AppendU32(&out, kEnvelopeMagic);
    AppendU32(&out, version);
    AppendU32(&out, static_cast<uint32_t>(payload.size()));
    out.append(payload);
    AppendU32(&out, Crc32(payload.data(), payload.size()));
    return out;
}

bool DecodeEnvelope(const std::string& data, std::string* payload, uint32_t* version_out = nullptr) {
    if (!payload) {
        return false;
    }
    size_t cursor = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t len = 0;
    if (!ReadU32(data, &cursor, &magic) || !ReadU32(data, &cursor, &version) ||
        !ReadU32(data, &cursor, &len)) {
        return false;
    }
    if (magic != kEnvelopeMagic || version < kEnvelopeVersionV1 || version > kEnvelopeVersionCurrent) {
        return false;
    }
    if (cursor + len + kU32Size != data.size()) {
        return false;
    }
    *payload = data.substr(cursor, len);
    cursor += len;
    uint32_t crc = 0;
    if (!ReadU32(data, &cursor, &crc)) {
        return false;
    }
    if (crc != Crc32(payload->data(), payload->size())) {
        return false;
    }
    if (version_out) {
        *version_out = version;
    }
    return true;
}

void EncodePgMemberPayload(const PgViewMemberRecord& member, std::string* payload) {
    AppendString(payload, member.node_id);
    AppendString(payload, member.node_address);
    AppendString(payload, member.disk_id);
    AppendString(payload, member.group_id);
    AppendU64(payload, member.epoch);
    AppendString(payload, member.primary_node_id);
    AppendString(payload, member.primary_address);
    AppendString(payload, member.secondary_node_id);
    AppendString(payload, member.secondary_address);
    AppendBool(payload, member.sync_ready);
}

bool DecodePgMemberPayload(const std::string& payload, size_t* cursor, PgViewMemberRecord* member) {
    return ReadString(payload, cursor, &member->node_id) &&
           ReadString(payload, cursor, &member->node_address) &&
           ReadString(payload, cursor, &member->disk_id) &&
           ReadString(payload, cursor, &member->group_id) &&
           ReadU64(payload, cursor, &member->epoch) &&
           ReadString(payload, cursor, &member->primary_node_id) &&
           ReadString(payload, cursor, &member->primary_address) &&
           ReadString(payload, cursor, &member->secondary_node_id) &&
           ReadString(payload, cursor, &member->secondary_address) &&
           ReadBool(payload, cursor, &member->sync_ready);
}

} // namespace

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

bool MetaCodec::EncodeUnifiedInodeRecord(const UnifiedInodeRecord& record, std::string* out, std::string* error) {
    return zb::mds::EncodeUnifiedInodeRecord(record, out, error);
}

bool MetaCodec::DecodeUnifiedInodeRecord(const std::string& data,
                                         UnifiedInodeRecord* record,
                                         std::string* error) {
    return zb::mds::DecodeUnifiedInodeRecord(data, record, error);
}

bool MetaCodec::DecodeUnifiedInodeAttr(const std::string& data,
                                       zb::rpc::InodeAttr* attr,
                                       std::string* error) {
    if (!attr) {
        if (error) {
            *error = "inode attr output is null";
        }
        return false;
    }
    UnifiedInodeRecord record;
    if (!zb::mds::DecodeUnifiedInodeRecord(data, &record, error)) {
        return false;
    }
    UnifiedInodeRecordToAttr(record, attr);
    if (error) {
        error->clear();
    }
    return true;
}

std::string MetaCodec::EncodePgView(const PgViewRecord& view) {
    std::string payload;
    payload.reserve(256);
    AppendU64(&payload, view.epoch);
    AppendU32(&payload, view.pg_id);
    AppendU32(&payload, static_cast<uint32_t>(view.members.size()));
    for (const auto& member : view.members) {
        EncodePgMemberPayload(member, &payload);
    }
    return EncodeEnvelope(payload);
}

bool MetaCodec::DecodePgView(const std::string& data, PgViewRecord* view) {
    if (!view) {
        return false;
    }
    std::string payload;
    if (!DecodeEnvelope(data, &payload)) {
        return false;
    }
    size_t cursor = 0;
    if (!ReadU64(payload, &cursor, &view->epoch) || !ReadU32(payload, &cursor, &view->pg_id)) {
        return false;
    }
    uint32_t member_count = 0;
    if (!ReadU32(payload, &cursor, &member_count)) {
        return false;
    }
    view->members.clear();
    view->members.reserve(member_count);
    for (uint32_t i = 0; i < member_count; ++i) {
        PgViewMemberRecord member;
        if (!DecodePgMemberPayload(payload, &cursor, &member)) {
            return false;
        }
        view->members.push_back(std::move(member));
    }
    return cursor == payload.size();
}

} // namespace zb::mds
