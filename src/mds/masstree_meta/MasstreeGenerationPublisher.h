#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "MasstreeManifest.h"
#include "MasstreeNamespaceCatalog.h"

namespace zb::mds {

class MasstreeGenerationPublisher {
public:
    struct PublishRequest {
        std::string masstree_root;
        std::string namespace_id;
        std::string generation_id;
        std::string manifest_path;
        bool publish_route{true};
    };

    explicit MasstreeGenerationPublisher(MasstreeNamespaceCatalog* catalog);

    bool Publish(const PublishRequest& request,
                 std::string* final_manifest_path,
                 std::string* error) const;
    bool CleanupStagingDir(const std::string& staging_dir, std::string* error) const;
    bool ListPublishedGenerations(const std::string& masstree_root,
                                  const std::string& namespace_id,
                                  std::vector<std::string>* generation_ids,
                                  std::string* error) const;
    bool RecoverCurrentRouteFromLatest(const std::string& masstree_root,
                                       const std::string& namespace_id,
                                       MasstreeNamespaceRoute* route,
                                       std::string* error) const;

private:
    std::string FinalGenerationDir(const std::string& masstree_root,
                                   const std::string& namespace_id,
                                   const std::string& generation_id) const;
    std::string GenerationsRoot(const std::string& masstree_root,
                                const std::string& namespace_id) const;
    bool RewriteManifestPaths(const std::string& final_dir,
                              MasstreeNamespaceManifest* manifest,
                              std::string* error) const;
    bool BuildRouteFromManifest(const MasstreeNamespaceManifest& manifest,
                                MasstreeNamespaceRoute* route,
                                std::vector<uint64_t>* bucket_ids,
                                std::string* error) const;

    MasstreeNamespaceCatalog* catalog_{};
};

} // namespace zb::mds
