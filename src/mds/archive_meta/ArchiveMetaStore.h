#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ArchiveBloomFilter.h"
#include "ArchiveDataFile.h"
#include "ArchiveManifest.h"
#include "ArchiveNamespaceCatalog.h"
#include "mds.pb.h"

namespace zb::mds {

class ArchiveMetaStore {
public:
    struct Options {
        size_t max_cached_generations{16};
        uint64_t max_cached_bytes{512ULL * 1024ULL * 1024ULL};
    };

    ArchiveMetaStore() = default;

    bool Init(const std::string& archive_root, const Options& options, std::string* error);

    bool ResolvePath(const ArchiveNamespaceRoute& route,
                     const std::string& path,
                     uint64_t* inode_id,
                     zb::rpc::InodeAttr* attr,
                     std::string* error) const;
    bool GetInode(const ArchiveNamespaceRoute& route,
                  uint64_t inode_id,
                  zb::rpc::InodeAttr* attr,
                  std::string* error) const;
    bool MayContainInode(const ArchiveNamespaceRoute& route,
                         uint64_t inode_id,
                         bool* may_contain,
                         std::string* error) const;

    bool Readdir(const ArchiveNamespaceRoute& route,
                 const std::string& path,
                 const std::string& start_after,
                 uint32_t limit,
                 std::vector<zb::rpc::Dentry>* entries,
                 bool* has_more,
                 std::string* next_token,
                 std::string* error) const;

private:
    struct LoadedGeneration {
        std::shared_ptr<const ArchiveManifest> manifest;
        std::shared_ptr<ArchiveDataFile> inode_data;
        std::shared_ptr<ArchiveDataFile> dentry_data;
        std::shared_ptr<ArchiveBloomFilter> inode_bloom;
        uint64_t estimated_bytes{0};
        uint64_t hit_count{0};
        uint64_t last_access_time_ms{0};
    };

    bool EnsureGenerationLoaded(const ArchiveNamespaceRoute& route,
                                std::shared_ptr<LoadedGeneration>* generation,
                                std::string* error) const;
    void TouchGenerationLocked(const std::string& manifest_path, const std::shared_ptr<LoadedGeneration>& generation) const;
    void MaybeEvictLocked() const;
    uint64_t EstimateGenerationBytes(const ArchiveManifest& manifest) const;
    bool FindDentry(const LoadedGeneration& generation,
                    uint64_t parent_inode,
                    const std::string& name,
                    uint64_t* child_inode,
                    zb::rpc::InodeType* type,
                    std::string* error) const;
    bool ListDentries(const LoadedGeneration& generation,
                      uint64_t parent_inode,
                      const std::string& start_after,
                      uint32_t limit,
                      std::vector<zb::rpc::Dentry>* entries,
                      bool* has_more,
                      std::string* next_token,
                      std::string* error) const;
    bool StripRoutePrefix(const std::string& route_prefix,
                          const std::string& path,
                          std::string* relative_path,
                          std::string* error) const;
    std::string ResolveArchivePath(const ArchiveManifest& manifest, const std::string& relative_or_absolute) const;
    std::string DefaultIndexPath(const std::string& data_path) const;
    std::string ResolveManifestPath(const ArchiveNamespaceRoute& route) const;
    static uint64_t NowMilliseconds();

    std::string archive_root_;
    Options options_;
    mutable std::mutex generation_mu_;
    mutable std::unordered_map<std::string, std::shared_ptr<LoadedGeneration>> generation_cache_;
};

} // namespace zb::mds
