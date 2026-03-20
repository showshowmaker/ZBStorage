#pragma once

#include <string>
#include <vector>

#include "../storage/RocksMetaStore.h"

namespace zb::mds {

struct MasstreeNamespaceRoute {
    std::string namespace_id;
    std::string path_prefix;
    std::string generation_id;
    std::string manifest_path;
    uint64_t root_inode_id{0};
    uint64_t inode_min{0};
    uint64_t inode_max{0};
    uint64_t inode_count{0};
    uint64_t dentry_count{0};
};

class MasstreeNamespaceCatalog {
public:
    explicit MasstreeNamespaceCatalog(RocksMetaStore* store);

    bool PutRoute(const MasstreeNamespaceRoute& route, std::string* error);
    bool PutRoute(const MasstreeNamespaceRoute& route,
                  const std::vector<uint64_t>& inode_bucket_ids,
                  std::string* error);
    bool LookupByPath(const std::string& path,
                      MasstreeNamespaceRoute* route,
                      std::string* error) const;
    bool LookupByInode(uint64_t inode_id,
                       std::vector<MasstreeNamespaceRoute>* routes,
                       std::string* error) const;
    bool ListRoutes(std::vector<MasstreeNamespaceRoute>* routes, std::string* error) const;
    bool SetCurrentRoute(const MasstreeNamespaceRoute& route, std::string* error);
    bool LookupCurrentRoute(const std::string& path_prefix,
                            MasstreeNamespaceRoute* route,
                            std::string* error) const;
    bool DeleteRoute(const std::string& path_prefix, std::string* error);

private:
    RocksMetaStore* store_{};
};

} // namespace zb::mds
