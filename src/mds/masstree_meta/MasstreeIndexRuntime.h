#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "MasstreePageLayout.h"

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
class MasstreeWrapper;
#endif

namespace zb::mds {

class MasstreeIndexRuntime {
public:
    MasstreeIndexRuntime();
    ~MasstreeIndexRuntime();

    bool Init(std::string* error);

    bool PutInodePageBoundary(const std::string& namespace_id,
                              uint64_t max_inode_id,
                              uint64_t page_offset,
                              std::string* error);
    bool FindInodePageBoundary(const std::string& namespace_id,
                               uint64_t inode_id,
                               MasstreeInodeSparseEntry* entry,
                               std::string* error) const;

    bool PutDentryPageBoundary(const std::string& namespace_id,
                               uint64_t parent_inode,
                               const std::string& max_name,
                               uint64_t page_offset,
                               std::string* error);
    bool FindDentryPageBoundary(const std::string& namespace_id,
                                uint64_t parent_inode,
                                const std::string& name,
                                MasstreeDentrySparseEntry* entry,
                                std::string* error) const;

private:
    bool EnsureThreadInitialized(std::string* error) const;

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    std::unique_ptr<MasstreeWrapper> inode_tree_;
    std::unique_ptr<MasstreeWrapper> dentry_tree_;
#endif
};

} // namespace zb::mds
