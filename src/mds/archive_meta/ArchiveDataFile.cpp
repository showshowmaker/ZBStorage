#include "ArchiveDataFile.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string_view>

#include "../storage/MetaCodec.h"

namespace zb::mds {

namespace {

constexpr char kArchiveSegmentMagic[8] = {'A', 'R', 'C', 'D', 'A', 'T', '1', '\0'};

struct InodeRecordView {
    uint64_t inode_id{0};
    const char* payload{nullptr};
    uint32_t payload_len{0};
};

struct DentryRecordView {
    uint64_t parent_inode{0};
    std::string_view name;
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

bool ReadExact(std::ifstream* in, char* buf, size_t len) {
    return in && buf && in->read(buf, static_cast<std::streamsize>(len)).good();
}

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8);
}

uint32_t DecodeLe32(const char* data) {
    return static_cast<uint32_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[3])) << 24);
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8);
    }
    return value;
}

uint64_t DecodeBe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value = (value << 8) | static_cast<uint64_t>(static_cast<unsigned char>(data[i]));
    }
    return value;
}

std::string EncodeBe64(uint64_t value) {
    std::string out(sizeof(uint64_t), '\0');
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out[sizeof(uint64_t) - 1 - i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    return out;
}

std::string EncodeDentryKey(uint64_t parent_inode, const std::string& name) {
    std::string key = EncodeBe64(parent_inode);
    key.append(name);
    return key;
}

bool ReadString(std::ifstream* in, size_t len, std::string* out) {
    if (!out) {
        return false;
    }
    out->assign(len, '\0');
    if (len == 0) {
        return true;
    }
    return ReadExact(in, &(*out)[0], len);
}

bool ReadPageHeader(const std::string& page, uint32_t* record_count, uint32_t* data_begin) {
    if (!record_count || !data_begin || page.size() < kArchivePageHeaderSize) {
        return false;
    }
    *record_count = DecodeLe32(page.data());
    *data_begin = DecodeLe32(page.data() + sizeof(uint32_t));
    const uint64_t offset_table_end =
        static_cast<uint64_t>(kArchivePageHeaderSize) + static_cast<uint64_t>(*record_count) * sizeof(uint32_t);
    return *data_begin >= offset_table_end && *data_begin <= page.size();
}

bool ReadRecordOffset(const std::string& page, uint32_t record_index, uint32_t record_count, uint32_t* offset) {
    if (!offset) {
        return false;
    }
    if (page.size() < kArchivePageHeaderSize + static_cast<size_t>(record_count) * sizeof(uint32_t)) {
        return false;
    }
    const size_t pos = kArchivePageHeaderSize + static_cast<size_t>(record_index) * sizeof(uint32_t);
    *offset = DecodeLe32(page.data() + pos);
    return *offset < page.size();
}

bool ParseInodeRecord(const std::string& page, uint32_t record_index, InodeRecordView* view) {
    if (!view) {
        return false;
    }
    uint32_t record_count = 0;
    uint32_t data_begin = 0;
    if (!ReadPageHeader(page, &record_count, &data_begin) || record_index >= record_count) {
        return false;
    }
    uint32_t offset = 0;
    if (!ReadRecordOffset(page, record_index, record_count, &offset) || offset < data_begin) {
        return false;
    }
    const size_t min_len = sizeof(uint64_t) + sizeof(uint32_t);
    if (offset + min_len > page.size()) {
        return false;
    }
    const char* data = page.data() + offset;
    const uint32_t payload_len = DecodeLe32(data + sizeof(uint64_t));
    if (offset + min_len + payload_len > page.size()) {
        return false;
    }
    view->inode_id = DecodeBe64(data);
    view->payload_len = payload_len;
    view->payload = data + min_len;
    return true;
}

bool ParseDentryRecord(const std::string& page, uint32_t record_index, DentryRecordView* view) {
    if (!view) {
        return false;
    }
    uint32_t record_count = 0;
    uint32_t data_begin = 0;
    if (!ReadPageHeader(page, &record_count, &data_begin) || record_index >= record_count) {
        return false;
    }
    uint32_t offset = 0;
    if (!ReadRecordOffset(page, record_index, record_count, &offset) || offset < data_begin) {
        return false;
    }

    const size_t prefix_len = sizeof(uint64_t) + sizeof(uint16_t);
    const size_t suffix_len = sizeof(uint64_t) + sizeof(uint8_t);
    if (offset + prefix_len + suffix_len > page.size()) {
        return false;
    }
    const char* data = page.data() + offset;
    const uint16_t name_len = DecodeLe16(data + sizeof(uint64_t));
    if (offset + prefix_len + name_len + suffix_len > page.size()) {
        return false;
    }
    const char* name_data = data + prefix_len;
    const char* child_data = name_data + name_len;
    view->parent_inode = DecodeBe64(data);
    view->name = std::string_view(name_data, name_len);
    view->child_inode = DecodeBe64(child_data);
    view->type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(child_data[sizeof(uint64_t)]));
    return true;
}

int CompareDentryView(const DentryRecordView& lhs, uint64_t parent_inode, const std::string& name) {
    if (lhs.parent_inode < parent_inode) {
        return -1;
    }
    if (lhs.parent_inode > parent_inode) {
        return 1;
    }
    if (lhs.name < name) {
        return -1;
    }
    if (lhs.name > name) {
        return 1;
    }
    return 0;
}

uint32_t LowerBoundInode(const std::string& page, uint64_t inode_id, bool* exact_match) {
    uint32_t record_count = 0;
    uint32_t data_begin = 0;
    if (!ReadPageHeader(page, &record_count, &data_begin)) {
        if (exact_match) {
            *exact_match = false;
        }
        return 0;
    }
    uint32_t low = 0;
    uint32_t high = record_count;
    while (low < high) {
        const uint32_t mid = low + (high - low) / 2;
        InodeRecordView view;
        if (!ParseInodeRecord(page, mid, &view)) {
            if (exact_match) {
                *exact_match = false;
            }
            return record_count;
        }
        if (view.inode_id < inode_id) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if (exact_match) {
        *exact_match = false;
        if (low < record_count) {
            InodeRecordView view;
            if (ParseInodeRecord(page, low, &view) && view.inode_id == inode_id) {
                *exact_match = true;
            }
        }
    }
    return low;
}

uint32_t LowerBoundDentry(const std::string& page,
                          uint64_t parent_inode,
                          const std::string& name,
                          bool* exact_match) {
    uint32_t record_count = 0;
    uint32_t data_begin = 0;
    if (!ReadPageHeader(page, &record_count, &data_begin)) {
        if (exact_match) {
            *exact_match = false;
        }
        return 0;
    }
    uint32_t low = 0;
    uint32_t high = record_count;
    while (low < high) {
        const uint32_t mid = low + (high - low) / 2;
        DentryRecordView view;
        if (!ParseDentryRecord(page, mid, &view)) {
            if (exact_match) {
                *exact_match = false;
            }
            return record_count;
        }
        if (CompareDentryView(view, parent_inode, name) < 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if (exact_match) {
        *exact_match = false;
        if (low < record_count) {
            DentryRecordView view;
            if (ParseDentryRecord(page, low, &view) && CompareDentryView(view, parent_inode, name) == 0) {
                *exact_match = true;
            }
        }
    }
    return low;
}

} // namespace

bool ArchiveDataFile::Open(ArchiveTableKind kind,
                           const std::string& data_path,
                           const std::string& index_path,
                           uint32_t page_size_bytes,
                           std::string* error) {
    kind_ = kind;
    data_path_ = data_path;
    page_size_bytes_ = page_size_bytes;
    page_count_ = 0;
    sparse_index_.reset();
    mode_ = LayoutMode::kLegacySequential;

    if (kind_ == ArchiveTableKind::kUnknown || data_path_.empty()) {
        if (error) {
            *error = "invalid archive data file open args";
        }
        return false;
    }

    if (page_size_bytes_ == 0) {
        if (error) {
            error->clear();
        }
        return true;
    }
    if (index_path.empty() || !std::filesystem::exists(index_path)) {
        if (error) {
            *error = "missing archive sparse index: " + index_path;
        }
        return false;
    }
    return OpenPaged(index_path, error);
}

bool ArchiveDataFile::OpenPaged(const std::string& index_path, std::string* error) {
    if (!ValidateSegmentHeader(error)) {
        return false;
    }

    auto sparse_index = std::make_shared<ArchiveSparseIndex>();
    if (!sparse_index->Load(index_path, kind_, error)) {
        return false;
    }
    sparse_index_ = std::move(sparse_index);
    mode_ = LayoutMode::kPaged;
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::ValidateSegmentHeader(std::string* error) {
    std::ifstream input(data_path_, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive segment file: " + data_path_;
        }
        return false;
    }

    char magic[sizeof(kArchiveSegmentMagic)] = {};
    char raw_kind = 0;
    char raw_version = 0;
    char reserved[2] = {};
    char raw_page_size[sizeof(uint32_t)] = {};
    char raw_page_count[sizeof(uint64_t)] = {};
    if (!ReadExact(&input, magic, sizeof(magic)) ||
        !ReadExact(&input, &raw_kind, sizeof(raw_kind)) ||
        !ReadExact(&input, &raw_version, sizeof(raw_version)) ||
        !ReadExact(&input, reserved, sizeof(reserved)) ||
        !ReadExact(&input, raw_page_size, sizeof(raw_page_size)) ||
        !ReadExact(&input, raw_page_count, sizeof(raw_page_count))) {
        if (error) {
            *error = "corrupted archive segment header: " + data_path_;
        }
        return false;
    }
    if (std::string(magic, sizeof(magic)) != std::string(kArchiveSegmentMagic, sizeof(kArchiveSegmentMagic))) {
        if (error) {
            *error = "invalid archive segment header: " + data_path_;
        }
        return false;
    }
    if (static_cast<ArchiveTableKind>(raw_kind) != kind_) {
        if (error) {
            *error = "archive segment kind mismatch: " + data_path_;
        }
        return false;
    }
    if (static_cast<unsigned char>(raw_version) != kArchiveFormatVersion) {
        if (error) {
            *error = "unsupported archive segment version: " + data_path_;
        }
        return false;
    }

    const uint32_t actual_page_size = DecodeLe32(raw_page_size);
    const uint64_t actual_page_count = DecodeLe64(raw_page_count);
    if (actual_page_size == 0) {
        if (error) {
            *error = "invalid archive segment geometry: " + data_path_;
        }
        return false;
    }
    if (page_size_bytes_ != 0 && page_size_bytes_ != actual_page_size) {
        if (error) {
            *error = "archive segment page_size_bytes mismatch: " + data_path_;
        }
        return false;
    }
    page_size_bytes_ = actual_page_size;
    page_count_ = actual_page_count;
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::ReadPage(uint64_t page_id, std::string* page, std::string* error) const {
    if (!page || mode_ != LayoutMode::kPaged || page_id >= page_count_) {
        if (error) {
            *error = "invalid archive page read args";
        }
        return false;
    }
    std::ifstream input(data_path_, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive segment file: " + data_path_;
        }
        return false;
    }
    const uint64_t offset =
        static_cast<uint64_t>(kArchiveSegmentFileHeaderSize) + page_id * static_cast<uint64_t>(page_size_bytes_);
    input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!input.good()) {
        if (error) {
            *error = "failed to seek archive segment page: " + data_path_;
        }
        return false;
    }
    page->assign(page_size_bytes_, '\0');
    if (!ReadExact(&input, &(*page)[0], page->size())) {
        if (error) {
            *error = "failed to read archive segment page: " + data_path_;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::FindInode(uint64_t inode_id,
                                zb::rpc::InodeAttr* attr,
                                std::string* error) const {
    if (!attr || inode_id == 0 || kind_ != ArchiveTableKind::kInode) {
        if (error) {
            *error = "invalid inode lookup args";
        }
        return false;
    }
    if (mode_ == LayoutMode::kPaged) {
        return FindInodePaged(inode_id, attr, error);
    }
    return FindInodeLegacy(inode_id, attr, error);
}

bool ArchiveDataFile::FindDentry(uint64_t parent_inode,
                                 const std::string& name,
                                 uint64_t* child_inode,
                                 zb::rpc::InodeType* type,
                                 std::string* error) const {
    if (!child_inode || parent_inode == 0 || name.empty() || kind_ != ArchiveTableKind::kDentry) {
        if (error) {
            *error = "invalid dentry lookup args";
        }
        return false;
    }
    if (mode_ == LayoutMode::kPaged) {
        return FindDentryPaged(parent_inode, name, child_inode, type, error);
    }
    return FindDentryLegacy(parent_inode, name, child_inode, type, error);
}

bool ArchiveDataFile::ListDentries(uint64_t parent_inode,
                                   const std::string& start_after,
                                   uint32_t limit,
                                   std::vector<zb::rpc::Dentry>* entries,
                                   bool* has_more,
                                   std::string* next_token,
                                   std::string* error) const {
    if (!entries || parent_inode == 0 || kind_ != ArchiveTableKind::kDentry) {
        if (error) {
            *error = "invalid dentry list args";
        }
        return false;
    }
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }
    if (mode_ == LayoutMode::kPaged) {
        return ListDentriesPaged(parent_inode, start_after, limit, entries, has_more, next_token, error);
    }
    return ListDentriesLegacy(parent_inode, start_after, limit, entries, has_more, next_token, error);
}

bool ArchiveDataFile::FindInodeLegacy(uint64_t inode_id,
                                      zb::rpc::InodeAttr* attr,
                                      std::string* error) const {
    std::ifstream input(data_path_, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive inode data file: " + data_path_;
        }
        return false;
    }

    while (true) {
        char inode_buf[sizeof(uint64_t)] = {};
        char len_buf[sizeof(uint32_t)] = {};
        if (!ReadExact(&input, inode_buf, sizeof(inode_buf))) {
            break;
        }
        if (!ReadExact(&input, len_buf, sizeof(len_buf))) {
            if (error) {
                *error = "corrupted archive inode data file: " + data_path_;
            }
            return false;
        }
        const uint64_t current_inode = DecodeLe64(inode_buf);
        const uint32_t value_len = DecodeLe32(len_buf);
        std::string payload;
        if (!ReadString(&input, value_len, &payload)) {
            if (error) {
                *error = "corrupted archive inode payload: " + data_path_;
            }
            return false;
        }
        if (current_inode == inode_id) {
            std::string decode_error;
            if (!MetaCodec::DecodeUnifiedInodeAttr(payload, attr, &decode_error)) {
                if (error) {
                    *error = decode_error.empty()
                                 ? "invalid archive inode payload: " + data_path_
                                 : decode_error + ": " + data_path_;
                }
                return false;
            }
            if (error) {
                error->clear();
            }
            return true;
        }
        if (current_inode > inode_id) {
            break;
        }
    }

    if (error) {
        error->clear();
    }
    return false;
}

bool ArchiveDataFile::FindDentryLegacy(uint64_t parent_inode,
                                       const std::string& name,
                                       uint64_t* child_inode,
                                       zb::rpc::InodeType* type,
                                       std::string* error) const {
    std::ifstream input(data_path_, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive dentry data file: " + data_path_;
        }
        return false;
    }

    while (true) {
        char parent_buf[sizeof(uint64_t)] = {};
        char len_buf[sizeof(uint16_t)] = {};
        char child_buf[sizeof(uint64_t)] = {};
        if (!ReadExact(&input, parent_buf, sizeof(parent_buf))) {
            break;
        }
        if (!ReadExact(&input, len_buf, sizeof(len_buf))) {
            if (error) {
                *error = "corrupted archive dentry data file: " + data_path_;
            }
            return false;
        }
        const uint64_t current_parent = DecodeLe64(parent_buf);
        const uint16_t name_len = DecodeLe16(len_buf);
        std::string current_name;
        if (!ReadString(&input, name_len, &current_name) ||
            !ReadExact(&input, child_buf, sizeof(child_buf))) {
            if (error) {
                *error = "corrupted archive dentry payload: " + data_path_;
            }
            return false;
        }
        char raw_type = 0;
        if (!ReadExact(&input, &raw_type, sizeof(raw_type))) {
            if (error) {
                *error = "corrupted archive dentry type payload: " + data_path_;
            }
            return false;
        }
        if (current_parent == parent_inode && current_name == name) {
            *child_inode = DecodeLe64(child_buf);
            if (type) {
                *type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(raw_type));
            }
            if (error) {
                error->clear();
            }
            return true;
        }
        if (current_parent > parent_inode || (current_parent == parent_inode && current_name > name)) {
            break;
        }
    }

    if (error) {
        error->clear();
    }
    return false;
}

bool ArchiveDataFile::ListDentriesLegacy(uint64_t parent_inode,
                                         const std::string& start_after,
                                         uint32_t limit,
                                         std::vector<zb::rpc::Dentry>* entries,
                                         bool* has_more,
                                         std::string* next_token,
                                         std::string* error) const {
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    std::ifstream input(data_path_, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive dentry data file: " + data_path_;
        }
        return false;
    }

    bool started = false;
    while (true) {
        char parent_buf[sizeof(uint64_t)] = {};
        char len_buf[sizeof(uint16_t)] = {};
        char child_buf[sizeof(uint64_t)] = {};
        if (!ReadExact(&input, parent_buf, sizeof(parent_buf))) {
            break;
        }
        if (!ReadExact(&input, len_buf, sizeof(len_buf))) {
            if (error) {
                *error = "corrupted archive dentry data file: " + data_path_;
            }
            return false;
        }
        const uint64_t current_parent = DecodeLe64(parent_buf);
        const uint16_t name_len = DecodeLe16(len_buf);
        std::string current_name;
        if (!ReadString(&input, name_len, &current_name) ||
            !ReadExact(&input, child_buf, sizeof(child_buf))) {
            if (error) {
                *error = "corrupted archive dentry payload: " + data_path_;
            }
            return false;
        }
        char raw_type = 0;
        if (!ReadExact(&input, &raw_type, sizeof(raw_type))) {
            if (error) {
                *error = "corrupted archive dentry type payload: " + data_path_;
            }
            return false;
        }

        if (current_parent < parent_inode || (current_parent == parent_inode && current_name <= start_after)) {
            continue;
        }
        if (current_parent > parent_inode) {
            if (started) {
                break;
            }
            continue;
        }

        started = true;
        zb::rpc::Dentry entry;
        entry.set_name(current_name);
        entry.set_inode_id(DecodeLe64(child_buf));
        entry.set_type(static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(raw_type)));
        entries->push_back(std::move(entry));
        if (next_token) {
            *next_token = current_name;
        }
        if (limit > 0 && entries->size() >= limit) {
            while (true) {
                if (!ReadExact(&input, parent_buf, sizeof(parent_buf))) {
                    if (error) {
                        error->clear();
                    }
                    return true;
                }
                if (!ReadExact(&input, len_buf, sizeof(len_buf))) {
                    if (error) {
                        *error = "corrupted archive dentry data file: " + data_path_;
                    }
                    return false;
                }
                const uint64_t next_parent = DecodeLe64(parent_buf);
                const uint16_t next_name_len = DecodeLe16(len_buf);
                std::string next_name;
                if (!ReadString(&input, next_name_len, &next_name) ||
                    !ReadExact(&input, child_buf, sizeof(child_buf))) {
                    if (error) {
                        *error = "corrupted archive dentry payload: " + data_path_;
                    }
                    return false;
                }
                char next_raw_type = 0;
                if (!ReadExact(&input, &next_raw_type, sizeof(next_raw_type))) {
                    if (error) {
                        *error = "corrupted archive dentry type payload: " + data_path_;
                    }
                    return false;
                }
                if (next_parent != parent_inode) {
                    if (error) {
                        error->clear();
                    }
                    return true;
                }
                if (has_more) {
                    *has_more = true;
                }
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
    }

    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::FindInodePaged(uint64_t inode_id,
                                     zb::rpc::InodeAttr* attr,
                                     std::string* error) const {
    if (page_count_ == 0) {
        if (error) {
            error->clear();
        }
        return false;
    }
    uint64_t page_id = 0;
    if (!sparse_index_ || !sparse_index_->LookupPage(EncodeBe64(inode_id), &page_id, error)) {
        return false;
    }
    std::string page;
    if (!ReadPage(page_id, &page, error)) {
        return false;
    }
    bool exact_match = false;
    const uint32_t pos = LowerBoundInode(page, inode_id, &exact_match);
    if (!exact_match) {
        if (error) {
            error->clear();
        }
        return false;
    }
    InodeRecordView view;
    if (!ParseInodeRecord(page, pos, &view)) {
        if (error) {
            *error = "corrupted archive inode page: " + data_path_;
        }
        return false;
    }
    const std::string payload(view.payload, view.payload_len);
    std::string decode_error;
    if (!MetaCodec::DecodeUnifiedInodeAttr(payload, attr, &decode_error)) {
        if (error) {
            *error = decode_error.empty()
                         ? "invalid archive inode payload: " + data_path_
                         : decode_error + ": " + data_path_;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::FindDentryPaged(uint64_t parent_inode,
                                      const std::string& name,
                                      uint64_t* child_inode,
                                      zb::rpc::InodeType* type,
                                      std::string* error) const {
    if (page_count_ == 0) {
        if (error) {
            error->clear();
        }
        return false;
    }
    uint64_t page_id = 0;
    if (!sparse_index_ || !sparse_index_->LookupPage(EncodeDentryKey(parent_inode, name), &page_id, error)) {
        return false;
    }
    std::string page;
    if (!ReadPage(page_id, &page, error)) {
        return false;
    }
    bool exact_match = false;
    const uint32_t pos = LowerBoundDentry(page, parent_inode, name, &exact_match);
    if (!exact_match) {
        if (error) {
            error->clear();
        }
        return false;
    }
    DentryRecordView view;
    if (!ParseDentryRecord(page, pos, &view)) {
        if (error) {
            *error = "corrupted archive dentry page: " + data_path_;
        }
        return false;
    }
    *child_inode = view.child_inode;
    if (type) {
        *type = view.type;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveDataFile::ListDentriesPaged(uint64_t parent_inode,
                                        const std::string& start_after,
                                        uint32_t limit,
                                        std::vector<zb::rpc::Dentry>* entries,
                                        bool* has_more,
                                        std::string* next_token,
                                        std::string* error) const {
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }
    if (page_count_ == 0) {
        if (error) {
            error->clear();
        }
        return true;
    }

    uint64_t page_id = 0;
    if (!sparse_index_) {
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!sparse_index_->LookupPage(EncodeDentryKey(parent_inode, start_after), &page_id, error)) {
        if (error && !error->empty()) {
            return false;
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    bool first_page = true;
    for (uint64_t current_page = page_id; current_page < page_count_; ++current_page) {
        std::string page;
        if (!ReadPage(current_page, &page, error)) {
            return false;
        }
        uint32_t record_count = 0;
        uint32_t data_begin = 0;
        if (!ReadPageHeader(page, &record_count, &data_begin)) {
            if (error) {
                *error = "corrupted archive dentry page header: " + data_path_;
            }
            return false;
        }
        uint32_t pos = 0;
        if (first_page) {
            bool exact_match = false;
            pos = LowerBoundDentry(page, parent_inode, start_after, &exact_match);
            while (pos < record_count) {
                DentryRecordView view;
                if (!ParseDentryRecord(page, pos, &view)) {
                    if (error) {
                        *error = "corrupted archive dentry page: " + data_path_;
                    }
                    return false;
                }
                if (view.parent_inode != parent_inode || view.name > start_after) {
                    break;
                }
                ++pos;
            }
            first_page = false;
        }

        for (uint32_t i = pos; i < record_count; ++i) {
            DentryRecordView view;
            if (!ParseDentryRecord(page, i, &view)) {
                if (error) {
                    *error = "corrupted archive dentry page: " + data_path_;
                }
                return false;
            }
            if (view.parent_inode < parent_inode) {
                continue;
            }
            if (view.parent_inode > parent_inode) {
                if (error) {
                    error->clear();
                }
                return true;
            }
            zb::rpc::Dentry entry;
            entry.set_name(std::string(view.name));
            entry.set_inode_id(view.child_inode);
            entry.set_type(view.type);
            entries->push_back(std::move(entry));
            if (next_token) {
                *next_token = std::string(view.name);
            }
            if (limit > 0 && entries->size() >= limit) {
                for (uint32_t j = i + 1; j < record_count; ++j) {
                    DentryRecordView remaining;
                    if (!ParseDentryRecord(page, j, &remaining)) {
                        if (error) {
                            *error = "corrupted archive dentry page: " + data_path_;
                        }
                        return false;
                    }
                    if (remaining.parent_inode == parent_inode) {
                        if (has_more) {
                            *has_more = true;
                        }
                        if (error) {
                            error->clear();
                        }
                        return true;
                    }
                    if (remaining.parent_inode > parent_inode) {
                        if (error) {
                            error->clear();
                        }
                        return true;
                    }
                }
                for (uint64_t lookahead_page = current_page + 1; lookahead_page < page_count_; ++lookahead_page) {
                    std::string next_page;
                    if (!ReadPage(lookahead_page, &next_page, error)) {
                        return false;
                    }
                    uint32_t next_count = 0;
                    uint32_t next_begin = 0;
                    if (!ReadPageHeader(next_page, &next_count, &next_begin)) {
                        if (error) {
                            *error = "corrupted archive dentry page header: " + data_path_;
                        }
                        return false;
                    }
                    for (uint32_t k = 0; k < next_count; ++k) {
                        DentryRecordView remaining;
                        if (!ParseDentryRecord(next_page, k, &remaining)) {
                            if (error) {
                                *error = "corrupted archive dentry page: " + data_path_;
                            }
                            return false;
                        }
                        if (remaining.parent_inode < parent_inode) {
                            continue;
                        }
                        if (remaining.parent_inode == parent_inode) {
                            if (has_more) {
                                *has_more = true;
                            }
                        }
                        if (error) {
                            error->clear();
                        }
                        return true;
                    }
                }
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
    }

    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
