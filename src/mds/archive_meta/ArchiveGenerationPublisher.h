#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ArchiveManifest.h"
#include "ArchiveNamespaceCatalog.h"

namespace zb::mds {

class ArchiveGenerationPublisher {
public:
    struct PublishRequest {
        std::string archive_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        std::string manifest_tmp_path;
        uint64_t inode_min{0};
        uint64_t inode_max{0};
        uint64_t inode_count{0};
        std::vector<uint64_t> inode_bucket_ids;
        bool publish_route{true};
    };

    explicit ArchiveGenerationPublisher(ArchiveNamespaceCatalog* catalog);

    bool PrepareStagingDir(const std::string& archive_root,
                           const std::string& namespace_id,
                           const std::string& generation_id,
                           std::string* staging_dir,
                           std::string* error) const;

    bool Publish(const PublishRequest& request,
                 std::string* final_manifest_path,
                 std::string* error) const;

    bool CleanupStagingDir(const std::string& staging_dir, std::string* error) const;
    bool CleanupNamespaceStaging(const std::string& archive_root,
                                 const std::string& namespace_id,
                                 std::vector<std::string>* removed_paths,
                                 std::string* error) const;
    bool ListPublishedGenerations(const std::string& archive_root,
                                  const std::string& namespace_id,
                                  std::vector<std::string>* generation_ids,
                                  std::string* error) const;
    bool RecoverCurrentRouteFromLatest(const std::string& archive_root,
                                       const std::string& namespace_id,
                                       ArchiveNamespaceRoute* route,
                                       std::string* error) const;
    bool PruneOldGenerations(const std::string& archive_root,
                             const std::string& namespace_id,
                             size_t keep_last,
                             const std::string& keep_generation_id,
                             std::vector<std::string>* removed_generation_ids,
                             std::string* error) const;

private:
    std::string FinalGenerationDir(const std::string& archive_root,
                                   const std::string& namespace_id,
                                   const std::string& generation_id) const;
    std::string GenerationsRoot(const std::string& archive_root,
                                const std::string& namespace_id) const;
    bool LoadRouteFromManifest(const std::string& manifest_path,
                               ArchiveNamespaceRoute* route,
                               std::string* error) const;

    ArchiveNamespaceCatalog* catalog_{};
};

} // namespace zb::mds
