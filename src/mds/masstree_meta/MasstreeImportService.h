#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "MasstreeBulkImporter.h"
#include "MasstreeBulkMetaGenerator.h"
#include "MasstreeGenerationPublisher.h"
#include "MasstreeNamespaceCatalog.h"
#include "MasstreeStatsStore.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {

class MasstreeImportService {
public:
    struct Request {
        std::string masstree_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        uint64_t inode_start{0};
        uint64_t file_count{100000000000ULL};
        uint32_t max_files_per_leaf_dir{2048};
        uint32_t max_subdirs_per_dir{256};
        uint32_t verify_inode_samples{16};
        uint32_t verify_dentry_samples{16};
        bool publish_route{true};
    };

    struct Result {
        std::string manifest_path;
        std::string staging_dir;
        uint64_t root_inode_id{0};
        uint64_t inode_min{0};
        uint64_t inode_max{0};
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t inode_blob_bytes{0};
        uint64_t avg_file_size_bytes{0};
        std::string total_file_bytes;
    };

    MasstreeImportService(RocksMetaStore* meta_store,
                          MasstreeNamespaceCatalog* catalog);

    bool ImportNamespace(const Request& request,
                         Result* result,
                         std::string* error) const;

private:
    bool ReserveInodeRange(uint64_t inode_count,
                           uint64_t* inode_start,
                           std::string* error) const;
    bool NormalizePathPrefix(const std::string& path_prefix,
                             std::string* normalized,
                             std::string* error) const;

    RocksMetaStore* meta_store_{};
    MasstreeNamespaceCatalog* catalog_{};
    mutable std::mutex import_mu_;
};

} // namespace zb::mds
