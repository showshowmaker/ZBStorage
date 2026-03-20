#pragma once

#include <cstdint>
#include <string>

#include "MasstreePageLayout.h"

namespace zb::mds {

class MasstreePageReader {
public:
    static bool LoadInodePage(const std::string& path,
                              uint64_t page_offset,
                              MasstreeInodePage* page,
                              std::string* error);
    static bool FindInodeInPage(const MasstreeInodePage& page,
                                uint64_t inode_id,
                                MasstreeInodePageEntry* entry,
                                std::string* error);
    static size_t LowerBoundInodeInPage(const MasstreeInodePage& page, uint64_t inode_id);
    static bool LoadDentryPage(const std::string& path,
                               uint64_t page_offset,
                               MasstreeDentryPage* page,
                               std::string* error);
    static bool FindDentryInPage(const MasstreeDentryPage& page,
                                 uint64_t parent_inode,
                                 const std::string& name,
                                 MasstreeDentryPageEntry* entry,
                                 std::string* error);
    static size_t LowerBoundDentryInPage(const MasstreeDentryPage& page,
                                         uint64_t parent_inode,
                                         const std::string& name);
};

} // namespace zb::mds
