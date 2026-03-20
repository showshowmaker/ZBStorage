#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include <vector>

#include "ArchiveFormat.h"
#include "ArchiveSparseIndex.h"
#include "mds.pb.h"

namespace zb::mds {

class ArchiveDataFile {
public:
    ArchiveDataFile() = default;

    bool Open(ArchiveTableKind kind,
              const std::string& data_path,
              const std::string& index_path,
              uint32_t page_size_bytes,
              std::string* error);

    bool FindInode(uint64_t inode_id,
                   zb::rpc::InodeAttr* attr,
                   std::string* error) const;

    bool FindDentry(uint64_t parent_inode,
                    const std::string& name,
                    uint64_t* child_inode,
                    zb::rpc::InodeType* type,
                    std::string* error) const;

    bool ListDentries(uint64_t parent_inode,
                      const std::string& start_after,
                      uint32_t limit,
                      std::vector<zb::rpc::Dentry>* entries,
                      bool* has_more,
                      std::string* next_token,
                      std::string* error) const;

private:
    enum class LayoutMode : uint8_t {
        kLegacySequential = 1,
        kPaged = 2,
    };

    bool OpenPaged(const std::string& index_path, std::string* error);
    bool ValidateSegmentHeader(std::string* error);
    bool ReadPage(uint64_t page_id, std::string* page, std::string* error) const;

    bool FindInodeLegacy(uint64_t inode_id,
                         zb::rpc::InodeAttr* attr,
                         std::string* error) const;
    bool FindDentryLegacy(uint64_t parent_inode,
                          const std::string& name,
                          uint64_t* child_inode,
                          zb::rpc::InodeType* type,
                          std::string* error) const;
    bool ListDentriesLegacy(uint64_t parent_inode,
                            const std::string& start_after,
                            uint32_t limit,
                            std::vector<zb::rpc::Dentry>* entries,
                            bool* has_more,
                            std::string* next_token,
                            std::string* error) const;

    bool FindInodePaged(uint64_t inode_id,
                        zb::rpc::InodeAttr* attr,
                        std::string* error) const;
    bool FindDentryPaged(uint64_t parent_inode,
                         const std::string& name,
                         uint64_t* child_inode,
                         zb::rpc::InodeType* type,
                         std::string* error) const;
    bool ListDentriesPaged(uint64_t parent_inode,
                           const std::string& start_after,
                           uint32_t limit,
                           std::vector<zb::rpc::Dentry>* entries,
                           bool* has_more,
                           std::string* next_token,
                           std::string* error) const;

    ArchiveTableKind kind_{ArchiveTableKind::kUnknown};
    LayoutMode mode_{LayoutMode::kLegacySequential};
    std::string data_path_;
    uint32_t page_size_bytes_{0};
    uint64_t page_count_{0};
    std::shared_ptr<const ArchiveSparseIndex> sparse_index_;
};

} // namespace zb::mds
