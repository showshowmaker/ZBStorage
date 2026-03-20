#pragma once

#include <cstdint>
#include <string>

#include "ArchiveGenerationPublisher.h"
#include "ArchiveNamespaceCatalog.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {

class ArchiveImportService {
public:
    struct Request {
        std::string archive_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        uint32_t page_size_bytes{0};
        bool publish_route{true};
    };

    struct Result {
        std::string manifest_path;
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t inode_min{0};
        uint64_t inode_max{0};
    };

    ArchiveImportService(RocksMetaStore* source_store,
                         ArchiveNamespaceCatalog* catalog);

    bool ImportPathPrefix(const Request& request,
                          Result* result,
                          std::string* error) const;

private:
    RocksMetaStore* source_store_{};
    ArchiveNamespaceCatalog* catalog_{};
};

} // namespace zb::mds
