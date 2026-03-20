#pragma once

#include <cstdint>
#include <string>

#include "MasstreeOpticalProfile.h"

namespace zb::mds {

class MasstreeIndexRuntime;

class MasstreeBulkImporter {
public:
    struct Request {
        std::string manifest_path;
        uint32_t verify_inode_samples{16};
        uint32_t verify_dentry_samples{16};
        MasstreeOpticalClusterCursor start_cursor;
    };

    struct Result {
        uint64_t inode_imported{0};
        uint64_t dentry_imported{0};
        uint64_t inode_page_count{0};
        uint64_t dentry_page_count{0};
        uint64_t inode_pages_bytes{0};
        uint64_t verified_inode_samples{0};
        uint64_t verified_dentry_samples{0};
        uint64_t file_count{0};
        std::string total_file_bytes;
        uint64_t avg_file_size_bytes{0};
        uint64_t start_global_image_id{0};
        uint64_t end_global_image_id{0};
        MasstreeOpticalClusterCursor start_cursor;
        MasstreeOpticalClusterCursor end_cursor;
    };

    bool Import(const Request& request,
                MasstreeIndexRuntime* runtime,
                Result* result,
                std::string* error) const;
};

} // namespace zb::mds
