#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "MasstreeValueCodec.h"

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
class MasstreeWrapper;
#endif

namespace zb::mds {

class MasstreeIndexRuntime {
public:
    struct DentryScanEntry {
        std::string name;
        MasstreePackedDentryValue value;
    };

    MasstreeIndexRuntime();
    ~MasstreeIndexRuntime();

    bool Init(std::string* error);

    bool PutInodeOffset(const std::string& namespace_id,
                        uint64_t inode_id,
                        uint64_t offset,
                        std::string* error);
    bool GetInodeOffset(const std::string& namespace_id,
                        uint64_t inode_id,
                        uint64_t* offset,
                        std::string* error) const;

    bool PutDentryValue(const std::string& namespace_id,
                        uint64_t parent_inode,
                        const std::string& name,
                        uint64_t child_inode,
                        zb::rpc::InodeType type,
                        std::string* error);
    bool GetDentryValue(const std::string& namespace_id,
                        uint64_t parent_inode,
                        const std::string& name,
                        MasstreePackedDentryValue* value,
                        std::string* error) const;
    bool ScanDentryValues(const std::string& namespace_id,
                          uint64_t parent_inode,
                          const std::string& start_after,
                          uint32_t limit,
                          std::vector<DentryScanEntry>* entries,
                          bool* has_more,
                          std::string* next_name,
                          std::string* error) const;

private:
    bool EnsureThreadInitialized(std::string* error) const;

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    std::unique_ptr<MasstreeWrapper> inode_tree_;
    std::unique_ptr<MasstreeWrapper> dentry_tree_;
#endif
};

} // namespace zb::mds
