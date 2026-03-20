#include "ArchiveSparseIndex.h"

#include <algorithm>
#include <fstream>
#include <functional>
#include <thread>

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
#include "masstree_wrapper.h"
#endif

namespace zb::mds {

namespace {

constexpr char kArchiveIndexMagic[8] = {'A', 'R', 'C', 'I', 'D', 'X', '1', '\0'};

bool ReadExact(std::ifstream* in, char* buf, size_t len) {
    return in && buf && in->read(buf, static_cast<std::streamsize>(len)).good();
}

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8);
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8);
    }
    return value;
}

bool ReadLe16(std::ifstream* in, uint16_t* value) {
    char buf[sizeof(uint16_t)] = {};
    if (!value || !ReadExact(in, buf, sizeof(buf))) {
        return false;
    }
    *value = DecodeLe16(buf);
    return true;
}

bool ReadLe64(std::ifstream* in, uint64_t* value) {
    char buf[sizeof(uint64_t)] = {};
    if (!value || !ReadExact(in, buf, sizeof(buf))) {
        return false;
    }
    *value = DecodeLe64(buf);
    return true;
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

} // namespace

ArchiveSparseIndex::ArchiveSparseIndex() = default;

ArchiveSparseIndex::~ArchiveSparseIndex() = default;

bool ArchiveSparseIndex::Load(const std::string& index_path,
                              ArchiveTableKind expected_kind,
                              std::string* error) {
    entries_.clear();
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    tree_.reset();
#endif

    std::ifstream input(index_path, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open archive sparse index: " + index_path;
        }
        return false;
    }

    char magic[sizeof(kArchiveIndexMagic)] = {};
    if (!ReadExact(&input, magic, sizeof(magic)) ||
        std::string(magic, sizeof(magic)) != std::string(kArchiveIndexMagic, sizeof(kArchiveIndexMagic))) {
        if (error) {
            *error = "invalid archive sparse index header: " + index_path;
        }
        return false;
    }

    char raw_kind = 0;
    char raw_version = 0;
    char reserved[2] = {};
    char raw_page_count[sizeof(uint64_t)] = {};
    if (!ReadExact(&input, &raw_kind, sizeof(raw_kind)) ||
        !ReadExact(&input, &raw_version, sizeof(raw_version)) ||
        !ReadExact(&input, reserved, sizeof(reserved)) ||
        !ReadExact(&input, raw_page_count, sizeof(raw_page_count))) {
        if (error) {
            *error = "corrupted archive sparse index header: " + index_path;
        }
        return false;
    }
    if (static_cast<ArchiveTableKind>(raw_kind) != expected_kind) {
        if (error) {
            *error = "archive sparse index kind mismatch: " + index_path;
        }
        return false;
    }
    if (static_cast<unsigned char>(raw_version) != kArchiveFormatVersion) {
        if (error) {
            *error = "unsupported archive sparse index version: " + index_path;
        }
        return false;
    }

    const uint64_t entry_count = DecodeLe64(raw_page_count);
    entries_.reserve(static_cast<size_t>(entry_count));
    for (uint64_t i = 0; i < entry_count; ++i) {
        uint64_t page_id = 0;
        uint16_t key_len = 0;
        std::string max_key;
        if (!ReadLe64(&input, &page_id) ||
            !ReadLe16(&input, &key_len) ||
            !ReadString(&input, key_len, &max_key)) {
            if (error) {
                *error = "corrupted archive sparse index body: " + index_path;
            }
            return false;
        }
        entries_.push_back({std::move(max_key), page_id});
    }

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    tree_ = std::make_unique<MasstreeWrapper>();
    for (const auto& entry : entries_) {
        tree_->insert(entry.max_key, entry.page_id);
    }
#endif

    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveSparseIndex::LookupPage(const std::string& key,
                                    uint64_t* page_id,
                                    std::string* error) const {
    if (!page_id) {
        if (error) {
            *error = "page_id output is null";
        }
        return false;
    }

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (tree_) {
        EnsureThreadInit();
        std::vector<std::pair<std::string, uint64_t>> hits;
        tree_->scan(key, 1, hits);
        if (hits.empty()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        *page_id = hits.front().second;
        if (error) {
            error->clear();
        }
        return true;
    }
#endif

    const auto it = std::lower_bound(entries_.begin(), entries_.end(), key, [](const Entry& lhs, const std::string& rhs) {
        return lhs.max_key < rhs;
    });
    if (it == entries_.end()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    *page_id = it->page_id;
    if (error) {
        error->clear();
    }
    return true;
}

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
void ArchiveSparseIndex::EnsureThreadInit() const {
    if (MasstreeWrapper::ti != nullptr) {
        return;
    }
    const uint64_t tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    MasstreeWrapper::thread_init(static_cast<int>(tid & 0x7fffffff));
}
#endif

} // namespace zb::mds
