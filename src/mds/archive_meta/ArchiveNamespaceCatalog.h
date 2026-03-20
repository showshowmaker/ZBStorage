#pragma once

#include <string>
#include <vector>

#include "../storage/RocksMetaStore.h"

namespace zb::mds {

struct ArchiveNamespaceRoute {
    std::string namespace_id;
    std::string path_prefix;
    std::string generation_id;
    std::string manifest_path;
    uint64_t inode_min{0};
    uint64_t inode_max{0};
    uint64_t inode_count{0};
};

class ArchiveNamespaceCatalog {
public:
    explicit ArchiveNamespaceCatalog(RocksMetaStore* store);

    bool PutRoute(const ArchiveNamespaceRoute& route, std::string* error);
    bool PutRoute(const ArchiveNamespaceRoute& route,
                  const std::vector<uint64_t>& inode_bucket_ids,
                  std::string* error);
    bool LookupByPath(const std::string& path,
                      ArchiveNamespaceRoute* route,
                      std::string* error) const;
    bool LookupByInode(uint64_t inode_id,
                       std::vector<ArchiveNamespaceRoute>* routes,
                       std::string* error) const;
    bool ListRoutes(std::vector<ArchiveNamespaceRoute>* routes, std::string* error) const;
    bool SetCurrentRoute(const ArchiveNamespaceRoute& route, std::string* error);
    bool LookupCurrentRoute(const std::string& path_prefix,
                            ArchiveNamespaceRoute* route,
                            std::string* error) const;
    bool DeleteRoute(const std::string& path_prefix, std::string* error);

private:
    RocksMetaStore* store_{};
};

} // namespace zb::mds
