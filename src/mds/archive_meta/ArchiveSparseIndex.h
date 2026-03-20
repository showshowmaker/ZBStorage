#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ArchiveFormat.h"

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
class MasstreeWrapper;
#endif

namespace zb::mds {

class ArchiveSparseIndex {
public:
    ArchiveSparseIndex();
    ~ArchiveSparseIndex();

    struct Entry {
        std::string max_key;
        uint64_t page_id{0};
    };

    bool Load(const std::string& index_path,
              ArchiveTableKind expected_kind,
              std::string* error);

    bool LookupPage(const std::string& key,
                    uint64_t* page_id,
                    std::string* error) const;

    size_t size() const {
        return entries_.size();
    }

private:
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    void EnsureThreadInit() const;
#endif

    std::vector<Entry> entries_;

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    std::unique_ptr<MasstreeWrapper> tree_;
#endif
};

} // namespace zb::mds
