#include "MasstreePageLayout.h"

namespace zb::mds {

namespace {

void AppendLe16(std::string* out, uint16_t value) {
    out->push_back(static_cast<char>(value & 0xFFU));
    out->push_back(static_cast<char>((value >> 8U) & 0xFFU));
}

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

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8U);
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

bool ConsumeString(const std::string& payload, size_t* cursor, size_t len, std::string* out) {
    if (!cursor || !out || *cursor + len > payload.size()) {
        return false;
    }
    out->assign(payload.data() + *cursor, len);
    *cursor += len;
    return true;
}

} // namespace

size_t EncodedMasstreeDentryPageEntrySize(const MasstreeDentryPageEntry& entry) {
    return sizeof(uint64_t) + sizeof(uint16_t) + entry.name.size() + sizeof(uint64_t) + sizeof(uint8_t);
}

size_t EncodedMasstreeInodePageEntrySize(const MasstreeInodePageEntry& entry) {
    return sizeof(uint64_t) + sizeof(uint32_t) + entry.payload.size();
}

bool EncodeMasstreeInodePage(const std::vector<MasstreeInodePageEntry>& entries,
                             std::string* payload,
                             std::string* error) {
    if (!payload || entries.empty()) {
        if (error) {
            *error = "invalid masstree inode page encode args";
        }
        return false;
    }
    payload->clear();
    payload->reserve(sizeof(uint32_t) + entries.size() * 32U);
    AppendLe32(payload, static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        AppendLe64(payload, entry.inode_id);
        AppendLe32(payload, static_cast<uint32_t>(entry.payload.size()));
        payload->append(entry.payload);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool DecodeMasstreeInodePage(const std::string& payload,
                             MasstreeInodePage* page,
                             std::string* error) {
    if (!page || payload.size() < sizeof(uint32_t)) {
        if (error) {
            *error = "invalid masstree inode page payload";
        }
        return false;
    }
    page->entries.clear();
    size_t cursor = 0;
    const uint32_t count = DecodeLe32(payload.data());
    cursor += sizeof(uint32_t);
    page->entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor + sizeof(uint64_t) + sizeof(uint32_t) > payload.size()) {
            if (error) {
                *error = "corrupted masstree inode page header";
            }
            return false;
        }
        MasstreeInodePageEntry entry;
        entry.inode_id = DecodeLe64(payload.data() + cursor);
        cursor += sizeof(uint64_t);
        const uint32_t record_len = DecodeLe32(payload.data() + cursor);
        cursor += sizeof(uint32_t);
        if (!ConsumeString(payload, &cursor, record_len, &entry.payload)) {
            if (error) {
                *error = "corrupted masstree inode page payload";
            }
            return false;
        }
        page->entries.push_back(std::move(entry));
    }
    if (cursor != payload.size()) {
        if (error) {
            *error = "unexpected trailing bytes in masstree inode page";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool EncodeMasstreeInodeSparseEntry(const MasstreeInodeSparseEntry& entry,
                                    std::string* payload,
                                    std::string* error) {
    if (!payload) {
        if (error) {
            *error = "invalid masstree inode sparse entry args";
        }
        return false;
    }
    payload->clear();
    payload->reserve(sizeof(uint64_t) * 2U);
    AppendLe64(payload, entry.page_offset);
    AppendLe64(payload, entry.max_inode_id);
    if (error) {
        error->clear();
    }
    return true;
}

bool DecodeMasstreeInodeSparseEntry(const std::string& payload,
                                    MasstreeInodeSparseEntry* entry,
                                    std::string* error) {
    if (!entry || payload.size() != sizeof(uint64_t) * 2U) {
        if (error) {
            *error = "invalid masstree inode sparse payload";
        }
        return false;
    }
    entry->page_offset = DecodeLe64(payload.data());
    entry->max_inode_id = DecodeLe64(payload.data() + sizeof(uint64_t));
    if (error) {
        error->clear();
    }
    return true;
}

bool EncodeMasstreeDentryPage(const std::vector<MasstreeDentryPageEntry>& entries,
                              std::string* payload,
                              std::string* error) {
    if (!payload || entries.empty()) {
        if (error) {
            *error = "invalid masstree dentry page encode args";
        }
        return false;
    }
    payload->clear();
    payload->reserve(sizeof(uint32_t) + entries.size() * 32U);
    AppendLe32(payload, static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        if (entry.name.size() > UINT16_MAX) {
            if (error) {
                *error = "masstree dentry page entry name is too long";
            }
            return false;
        }
        AppendLe64(payload, entry.parent_inode);
        AppendLe16(payload, static_cast<uint16_t>(entry.name.size()));
        payload->append(entry.name);
        AppendLe64(payload, entry.child_inode);
        payload->push_back(static_cast<char>(entry.type));
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool DecodeMasstreeDentryPage(const std::string& payload,
                              MasstreeDentryPage* page,
                              std::string* error) {
    if (!page || payload.size() < sizeof(uint32_t)) {
        if (error) {
            *error = "invalid masstree dentry page payload";
        }
        return false;
    }
    page->entries.clear();
    size_t cursor = 0;
    const uint32_t count = DecodeLe32(payload.data());
    cursor += sizeof(uint32_t);
    page->entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor + sizeof(uint64_t) + sizeof(uint16_t) > payload.size()) {
            if (error) {
                *error = "corrupted masstree dentry page header";
            }
            return false;
        }
        MasstreeDentryPageEntry entry;
        entry.parent_inode = DecodeLe64(payload.data() + cursor);
        cursor += sizeof(uint64_t);
        const uint16_t name_len = DecodeLe16(payload.data() + cursor);
        cursor += sizeof(uint16_t);
        if (!ConsumeString(payload, &cursor, name_len, &entry.name)) {
            if (error) {
                *error = "corrupted masstree dentry page name";
            }
            return false;
        }
        if (cursor + sizeof(uint64_t) + sizeof(uint8_t) > payload.size()) {
            if (error) {
                *error = "corrupted masstree dentry page tail";
            }
            return false;
        }
        entry.child_inode = DecodeLe64(payload.data() + cursor);
        cursor += sizeof(uint64_t);
        entry.type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(payload[cursor]));
        cursor += sizeof(uint8_t);
        page->entries.push_back(std::move(entry));
    }
    if (cursor != payload.size()) {
        if (error) {
            *error = "unexpected trailing bytes in masstree dentry page";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool EncodeMasstreeDentrySparseEntry(const MasstreeDentrySparseEntry& entry,
                                     std::string* payload,
                                     std::string* error) {
    if (!payload || entry.max_name.size() > UINT16_MAX) {
        if (error) {
            *error = "invalid masstree dentry sparse entry";
        }
        return false;
    }
    payload->clear();
    payload->reserve(sizeof(uint64_t) * 2U + sizeof(uint16_t) + entry.max_name.size());
    AppendLe64(payload, entry.page_offset);
    AppendLe64(payload, entry.max_parent_inode);
    AppendLe16(payload, static_cast<uint16_t>(entry.max_name.size()));
    payload->append(entry.max_name);
    if (error) {
        error->clear();
    }
    return true;
}

bool DecodeMasstreeDentrySparseEntry(const std::string& payload,
                                     MasstreeDentrySparseEntry* entry,
                                     std::string* error) {
    if (!entry || payload.size() < (sizeof(uint64_t) * 2U + sizeof(uint16_t))) {
        if (error) {
            *error = "invalid masstree dentry sparse payload";
        }
        return false;
    }
    size_t cursor = 0;
    entry->page_offset = DecodeLe64(payload.data() + cursor);
    cursor += sizeof(uint64_t);
    entry->max_parent_inode = DecodeLe64(payload.data() + cursor);
    cursor += sizeof(uint64_t);
    const uint16_t name_len = DecodeLe16(payload.data() + cursor);
    cursor += sizeof(uint16_t);
    if (!ConsumeString(payload, &cursor, name_len, &entry->max_name)) {
        if (error) {
            *error = "corrupted masstree dentry sparse name";
        }
        return false;
    }
    if (cursor != payload.size()) {
        if (error) {
            *error = "unexpected trailing bytes in masstree dentry sparse entry";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeDentryKeyLess(uint64_t lhs_parent,
                           const std::string& lhs_name,
                           uint64_t rhs_parent,
                           const std::string& rhs_name) {
    if (lhs_parent != rhs_parent) {
        return lhs_parent < rhs_parent;
    }
    return lhs_name < rhs_name;
}

} // namespace zb::mds
