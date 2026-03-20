#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "MasstreeInodeRecordCodec.h"
#include "mds.pb.h"

namespace zb::mds {

constexpr uint32_t kMasstreeDefaultPageSizeBytes = 64U * 1024U;

struct MasstreeInodePageEntry {
    uint64_t inode_id{0};
    std::string payload;
};

struct MasstreeInodePage {
    uint64_t page_offset{0};
    uint64_t next_page_offset{0};
    std::vector<MasstreeInodePageEntry> entries;
};

struct MasstreeInodeSparseEntry {
    uint64_t page_offset{0};
    uint64_t max_inode_id{0};
};

struct MasstreeDentryPageEntry {
    uint64_t parent_inode{0};
    std::string name;
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

struct MasstreeDentryPage {
    uint64_t page_offset{0};
    uint64_t next_page_offset{0};
    std::vector<MasstreeDentryPageEntry> entries;
};

struct MasstreeDentrySparseEntry {
    uint64_t page_offset{0};
    uint64_t max_parent_inode{0};
    std::string max_name;
};

size_t EncodedMasstreeDentryPageEntrySize(const MasstreeDentryPageEntry& entry);
size_t EncodedMasstreeInodePageEntrySize(const MasstreeInodePageEntry& entry);

bool EncodeMasstreeInodePage(const std::vector<MasstreeInodePageEntry>& entries,
                             std::string* payload,
                             std::string* error);
bool DecodeMasstreeInodePage(const std::string& payload,
                             MasstreeInodePage* page,
                             std::string* error);
bool EncodeMasstreeInodeSparseEntry(const MasstreeInodeSparseEntry& entry,
                                    std::string* payload,
                                    std::string* error);
bool DecodeMasstreeInodeSparseEntry(const std::string& payload,
                                    MasstreeInodeSparseEntry* entry,
                                    std::string* error);

bool EncodeMasstreeDentryPage(const std::vector<MasstreeDentryPageEntry>& entries,
                              std::string* payload,
                              std::string* error);
bool DecodeMasstreeDentryPage(const std::string& payload,
                              MasstreeDentryPage* page,
                              std::string* error);

bool EncodeMasstreeDentrySparseEntry(const MasstreeDentrySparseEntry& entry,
                                     std::string* payload,
                                     std::string* error);
bool DecodeMasstreeDentrySparseEntry(const std::string& payload,
                                     MasstreeDentrySparseEntry* entry,
                                     std::string* error);

bool MasstreeDentryKeyLess(uint64_t lhs_parent,
                           const std::string& lhs_name,
                           uint64_t rhs_parent,
                           const std::string& rhs_name);

} // namespace zb::mds
