#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "MasstreeIndexRuntime.h"
#include "MasstreeInodeRecordCodec.h"
#include "MasstreeManifest.h"
#include "MasstreeNamespaceCatalog.h"
#include "MasstreePageLayout.h"
#include "mds.pb.h"

namespace zb::mds {

class MasstreeMetaStore {
public:
    MasstreeMetaStore() = default;

    bool ResolvePath(const MasstreeNamespaceRoute& route,
                     const std::string& path,
                     uint64_t* inode_id,
                     zb::rpc::InodeAttr* attr,
                     std::string* error) const;
    bool GetInode(const MasstreeNamespaceRoute& route,
                  uint64_t inode_id,
                  zb::rpc::InodeAttr* attr,
                  std::string* error) const;
    bool GetOpticalFileLocation(const MasstreeNamespaceRoute& route,
                                uint64_t inode_id,
                                zb::rpc::OpticalFileLocation* location,
                                std::string* error) const;
    bool Readdir(const MasstreeNamespaceRoute& route,
                 const std::string& path,
                 const std::string& start_after,
                 uint32_t limit,
                 std::vector<zb::rpc::Dentry>* entries,
                 bool* has_more,
                 std::string* next_token,
                 std::string* error) const;

private:
    struct LoadedGeneration {
        std::shared_ptr<const MasstreeNamespaceManifest> manifest;
        std::shared_ptr<MasstreeIndexRuntime> runtime;
    };

    bool EnsureGenerationLoaded(const MasstreeNamespaceRoute& route,
                                std::shared_ptr<LoadedGeneration>* generation,
                                std::string* error) const;
    bool LoadGenerationData(const MasstreeNamespaceManifest& manifest,
                            MasstreeIndexRuntime* runtime,
                            std::string* error) const;
    bool ReadInodeAttr(const LoadedGeneration& generation,
                       uint64_t inode_id,
                       zb::rpc::InodeAttr* attr,
                       std::string* error) const;
    bool ReadUnifiedInode(const LoadedGeneration& generation,
                          uint64_t inode_id,
                          UnifiedInodeRecord* inode,
                          std::string* error) const;
    bool ReadInodeRecord(const LoadedGeneration& generation,
                         uint64_t inode_id,
                         MasstreeInodeRecord* record,
                         std::string* error) const;
    bool ReadInodeRecordFromSparsePages(const LoadedGeneration& generation,
                                        uint64_t inode_id,
                                        MasstreeInodeRecord* record,
                                        std::string* error) const;
    bool FindDentry(const LoadedGeneration& generation,
                    uint64_t parent_inode,
                    const std::string& name,
                    uint64_t* child_inode,
                    zb::rpc::InodeType* type,
                    std::string* error) const;
    bool FindDentryInSparsePages(const LoadedGeneration& generation,
                                 uint64_t parent_inode,
                                 const std::string& name,
                                 uint64_t* child_inode,
                                 zb::rpc::InodeType* type,
                                 std::string* error) const;
    bool StripRoutePrefix(const std::string& route_prefix,
                          const std::string& path,
                          std::string* relative_path,
                          std::string* error) const;
    static std::string ResolveManifestPath(const MasstreeNamespaceRoute& route);

    mutable std::mutex generation_mu_;
    mutable std::unordered_map<std::string, std::shared_ptr<LoadedGeneration>> generation_cache_;
};

} // namespace zb::mds
