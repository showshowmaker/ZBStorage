#include "ArchiveMetaStore.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <string_view>

#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(path);
    while (std::getline(stream, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool NormalizePath(std::string path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    std::string out;
    out.reserve(path.size() + 1);
    bool prev_slash = false;
    for (char ch : path) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
            out.push_back(ch);
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    if (out.empty()) {
        out = "/";
    }
    *normalized = std::move(out);
    return true;
}

uint64_t TryFileSize(const std::string& path) {
    if (path.empty()) {
        return 0;
    }
    std::error_code ec;
    const uint64_t size = std::filesystem::file_size(path, ec);
    return ec ? 0 : size;
}

constexpr const char* kArchiveReaddirTokenPrefix = "zb-archive-readdir-v1:";

char EncodeHexNibble(unsigned char value) {
    return static_cast<char>(value < 10 ? ('0' + value) : ('a' + (value - 10)));
}

bool DecodeHexNibble(char ch, unsigned char* value) {
    if (!value) {
        return false;
    }
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (uch >= '0' && uch <= '9') {
        *value = static_cast<unsigned char>(uch - '0');
        return true;
    }
    if (uch >= 'a' && uch <= 'f') {
        *value = static_cast<unsigned char>(uch - 'a' + 10);
        return true;
    }
    if (uch >= 'A' && uch <= 'F') {
        *value = static_cast<unsigned char>(uch - 'A' + 10);
        return true;
    }
    return false;
}

std::string HexEncode(std::string_view input) {
    std::string output;
    output.reserve(input.size() * 2);
    for (const unsigned char ch : input) {
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch >> 4U)));
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch & 0x0FU)));
    }
    return output;
}

bool HexDecode(std::string_view input, std::string* output) {
    if (!output || (input.size() % 2U) != 0U) {
        return false;
    }
    output->clear();
    output->reserve(input.size() / 2U);
    for (size_t i = 0; i < input.size(); i += 2U) {
        unsigned char hi = 0;
        unsigned char lo = 0;
        if (!DecodeHexNibble(input[i], &hi) || !DecodeHexNibble(input[i + 1U], &lo)) {
            return false;
        }
        output->push_back(static_cast<char>((hi << 4U) | lo));
    }
    return true;
}

std::string EncodeArchiveReaddirToken(const std::string& generation_id, const std::string& last_name) {
    std::string payload = generation_id;
    payload.push_back('\0');
    payload.append(last_name);
    return std::string(kArchiveReaddirTokenPrefix) + HexEncode(payload);
}

bool DecodeArchiveReaddirToken(const std::string& token,
                               std::string* generation_id,
                               std::string* last_name) {
    if (!generation_id || !last_name ||
        token.rfind(kArchiveReaddirTokenPrefix, 0) != 0) {
        return false;
    }
    std::string payload;
    if (!HexDecode(token.substr(std::char_traits<char>::length(kArchiveReaddirTokenPrefix)), &payload)) {
        return false;
    }
    const size_t separator = payload.find('\0');
    if (separator == std::string::npos) {
        return false;
    }
    *generation_id = payload.substr(0, separator);
    *last_name = payload.substr(separator + 1U);
    return true;
}

} // namespace

bool ArchiveMetaStore::Init(const std::string& archive_root, const Options& options, std::string* error) {
    archive_root_ = archive_root;
    options_ = options;
    if (options_.max_cached_generations == 0) {
        options_.max_cached_generations = 1;
    }
    if (options_.max_cached_bytes == 0) {
        options_.max_cached_bytes = 64ULL * 1024ULL * 1024ULL;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveMetaStore::ResolvePath(const ArchiveNamespaceRoute& route,
                                   const std::string& path,
                                   uint64_t* inode_id,
                                   zb::rpc::InodeAttr* attr,
                                   std::string* error) const {
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    std::string relative_path;
    if (!StripRoutePrefix(route.path_prefix, path, &relative_path, error)) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(relative_path);
    uint64_t current = generation->manifest->root_inode_id;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return generation->inode_data->FindInode(current, attr, error);
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    for (const auto& name : parts) {
        uint64_t next = 0;
        if (!FindDentry(*generation, current, name, &next, nullptr, error)) {
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return generation->inode_data->FindInode(current, attr, error);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveMetaStore::GetInode(const ArchiveNamespaceRoute& route,
                                uint64_t inode_id,
                                zb::rpc::InodeAttr* attr,
                                std::string* error) const {
    if (!attr || inode_id == 0) {
        if (error) {
            *error = "invalid archive inode lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    if (generation->inode_bloom && !generation->inode_bloom->MayContainUInt64(inode_id)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    return generation->inode_data->FindInode(inode_id, attr, error);
}

bool ArchiveMetaStore::MayContainInode(const ArchiveNamespaceRoute& route,
                                       uint64_t inode_id,
                                       bool* may_contain,
                                       std::string* error) const {
    if (!may_contain || inode_id == 0) {
        if (error) {
            *error = "invalid archive bloom lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    if (!generation->inode_bloom) {
        *may_contain = true;
    } else {
        *may_contain = generation->inode_bloom->MayContainUInt64(inode_id);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveMetaStore::Readdir(const ArchiveNamespaceRoute& route,
                               const std::string& path,
                               const std::string& start_after,
                               uint32_t limit,
                               std::vector<zb::rpc::Dentry>* entries,
                               bool* has_more,
                               std::string* next_token,
                               std::string* error) const {
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    uint64_t inode_id = 0;
    zb::rpc::InodeAttr attr;
    if (!ResolvePath(route, path, &inode_id, &attr, error)) {
        return false;
    }
    if (attr.type() != zb::rpc::INODE_DIR) {
        if (error) {
            *error = "not a directory";
        }
        return false;
    }
    std::string decoded_start_after;
    if (start_after.empty()) {
        decoded_start_after.clear();
    } else {
        std::string token_generation_id;
        if (start_after.rfind(kArchiveReaddirTokenPrefix, 0) == 0) {
            if (!DecodeArchiveReaddirToken(start_after, &token_generation_id, &decoded_start_after)) {
                if (error) {
                    *error = "invalid archive readdir token";
                }
                return false;
            }
            if (token_generation_id != generation->manifest->generation_id) {
                if (error) {
                    *error = "archive readdir token generation mismatch";
                }
                return false;
            }
        } else {
            decoded_start_after = start_after;
        }
    }

    std::string raw_next_token;
    const bool ok = ListDentries(*generation,
                                 inode_id,
                                 decoded_start_after,
                                 limit,
                                 entries,
                                 has_more,
                                 &raw_next_token,
                                 error);
    if (!ok) {
        return false;
    }
    if (next_token) {
        next_token->clear();
        if (has_more && *has_more && !raw_next_token.empty()) {
            *next_token = EncodeArchiveReaddirToken(generation->manifest->generation_id, raw_next_token);
        }
    }
    return true;
}

bool ArchiveMetaStore::EnsureGenerationLoaded(const ArchiveNamespaceRoute& route,
                                              std::shared_ptr<LoadedGeneration>* generation,
                                              std::string* error) const {
    if (!generation) {
        if (error) {
            *error = "generation output is null";
        }
        return false;
    }
    const std::string manifest_path = ResolveManifestPath(route);
    if (manifest_path.empty()) {
        if (error) {
            *error = "archive manifest path is empty";
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        auto it = generation_cache_.find(manifest_path);
        if (it != generation_cache_.end()) {
            TouchGenerationLocked(manifest_path, it->second);
            *generation = it->second;
            if (error) {
                error->clear();
            }
            return true;
        }
    }

    ArchiveManifest parsed;
    if (!ArchiveManifest::LoadFromFile(manifest_path, &parsed, error)) {
        return false;
    }

    auto loaded = std::make_shared<LoadedGeneration>();
    loaded->manifest = std::make_shared<ArchiveManifest>(std::move(parsed));
    loaded->inode_data = std::make_shared<ArchiveDataFile>();
    loaded->dentry_data = std::make_shared<ArchiveDataFile>();
    loaded->inode_bloom = std::make_shared<ArchiveBloomFilter>();

    const std::string inode_path = ResolveArchivePath(*loaded->manifest, loaded->manifest->inode_root);
    const std::string inode_index_path = loaded->manifest->inode_index_root.empty()
                                             ? DefaultIndexPath(inode_path)
                                             : ResolveArchivePath(*loaded->manifest, loaded->manifest->inode_index_root);
    const std::string inode_bloom_path = loaded->manifest->inode_bloom_root.empty()
                                             ? std::string()
                                             : ResolveArchivePath(*loaded->manifest, loaded->manifest->inode_bloom_root);
    const std::string dentry_path = ResolveArchivePath(*loaded->manifest, loaded->manifest->dentry_root);
    const std::string dentry_index_path = loaded->manifest->dentry_index_root.empty()
                                              ? DefaultIndexPath(dentry_path)
                                              : ResolveArchivePath(*loaded->manifest, loaded->manifest->dentry_index_root);

    if (!loaded->inode_data->Open(ArchiveTableKind::kInode,
                                  inode_path,
                                  inode_index_path,
                                  static_cast<uint32_t>(loaded->manifest->page_size_bytes),
                                  error) ||
        !loaded->dentry_data->Open(ArchiveTableKind::kDentry,
                                   dentry_path,
                                   dentry_index_path,
                                   static_cast<uint32_t>(loaded->manifest->page_size_bytes),
                                   error)) {
        return false;
    }
    if (!inode_bloom_path.empty() && !loaded->inode_bloom->LoadFromFile(inode_bloom_path, error)) {
        return false;
    }

    loaded->estimated_bytes = EstimateGenerationBytes(*loaded->manifest);
    loaded->hit_count = 0;
    loaded->last_access_time_ms = 0;

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        generation_cache_[manifest_path] = loaded;
        TouchGenerationLocked(manifest_path, loaded);
        MaybeEvictLocked();
    }
    *generation = std::move(loaded);
    if (error) {
        error->clear();
    }
    return true;
}

void ArchiveMetaStore::TouchGenerationLocked(const std::string& manifest_path,
                                             const std::shared_ptr<LoadedGeneration>& generation) const {
    (void)manifest_path;
    if (!generation) {
        return;
    }
    ++generation->hit_count;
    generation->last_access_time_ms = NowMilliseconds();
}

void ArchiveMetaStore::MaybeEvictLocked() const {
    auto current_bytes = [&]() -> uint64_t {
        uint64_t total = 0;
        for (const auto& item : generation_cache_) {
            total += item.second ? item.second->estimated_bytes : 0;
        }
        return total;
    };

    uint64_t cache_bytes = current_bytes();
    while (generation_cache_.size() > options_.max_cached_generations || cache_bytes > options_.max_cached_bytes) {
        auto evict_it = generation_cache_.end();
        for (auto it = generation_cache_.begin(); it != generation_cache_.end(); ++it) {
            if (!it->second || it->second.use_count() > 1) {
                continue;
            }
            if (evict_it == generation_cache_.end() ||
                it->second->last_access_time_ms < evict_it->second->last_access_time_ms) {
                evict_it = it;
            }
        }
        if (evict_it == generation_cache_.end()) {
            break;
        }
        cache_bytes -= evict_it->second ? evict_it->second->estimated_bytes : 0;
        generation_cache_.erase(evict_it);
    }
}

uint64_t ArchiveMetaStore::EstimateGenerationBytes(const ArchiveManifest& manifest) const {
    const std::string inode_path = ResolveArchivePath(manifest, manifest.inode_root);
    const std::string inode_index_path = manifest.inode_index_root.empty()
                                             ? DefaultIndexPath(inode_path)
                                             : ResolveArchivePath(manifest, manifest.inode_index_root);
    const std::string inode_bloom_path = manifest.inode_bloom_root.empty()
                                             ? std::string()
                                             : ResolveArchivePath(manifest, manifest.inode_bloom_root);
    const std::string dentry_path = ResolveArchivePath(manifest, manifest.dentry_root);
    const std::string dentry_index_path = manifest.dentry_index_root.empty()
                                              ? DefaultIndexPath(dentry_path)
                                              : ResolveArchivePath(manifest, manifest.dentry_index_root);
    return TryFileSize(manifest.manifest_path) +
           TryFileSize(inode_path) +
           TryFileSize(inode_index_path) +
           TryFileSize(inode_bloom_path) +
           TryFileSize(dentry_path) +
           TryFileSize(dentry_index_path);
}

bool ArchiveMetaStore::FindDentry(const LoadedGeneration& generation,
                                  uint64_t parent_inode,
                                  const std::string& name,
                                  uint64_t* child_inode,
                                  zb::rpc::InodeType* type,
                                  std::string* error) const {
    return generation.dentry_data->FindDentry(parent_inode, name, child_inode, type, error);
}

bool ArchiveMetaStore::ListDentries(const LoadedGeneration& generation,
                                    uint64_t parent_inode,
                                    const std::string& start_after,
                                    uint32_t limit,
                                    std::vector<zb::rpc::Dentry>* entries,
                                    bool* has_more,
                                    std::string* next_token,
                                    std::string* error) const {
    return generation.dentry_data->ListDentries(parent_inode, start_after, limit, entries, has_more, next_token, error);
}

bool ArchiveMetaStore::StripRoutePrefix(const std::string& route_prefix,
                                        const std::string& path,
                                        std::string* relative_path,
                                        std::string* error) const {
    if (!relative_path) {
        if (error) {
            *error = "relative_path output is null";
        }
        return false;
    }
    std::string normalized_route;
    std::string normalized_path;
    if (!NormalizePath(route_prefix, &normalized_route) || !NormalizePath(path, &normalized_path)) {
        if (error) {
            *error = "invalid archive route path";
        }
        return false;
    }
    if (normalized_path == normalized_route) {
        *relative_path = "/";
        if (error) {
            error->clear();
        }
        return true;
    }
    const std::string prefix = normalized_route + "/";
    if (normalized_path.rfind(prefix, 0) != 0) {
        if (error) {
            *error = "path does not belong to archive route";
        }
        return false;
    }
    *relative_path = normalized_path.substr(normalized_route.size());
    if (relative_path->empty()) {
        *relative_path = "/";
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string ArchiveMetaStore::ResolveArchivePath(const ArchiveManifest& manifest,
                                                 const std::string& relative_or_absolute) const {
    if (relative_or_absolute.empty()) {
        return std::string();
    }
    std::filesystem::path path(relative_or_absolute);
    if (path.is_absolute()) {
        return path.string();
    }
    return (std::filesystem::path(manifest.manifest_path).parent_path() / path).string();
}

std::string ArchiveMetaStore::DefaultIndexPath(const std::string& data_path) const {
    if (data_path.empty()) {
        return std::string();
    }
    return data_path + ".idx";
}

std::string ArchiveMetaStore::ResolveManifestPath(const ArchiveNamespaceRoute& route) const {
    if (!route.manifest_path.empty()) {
        return route.manifest_path;
    }
    if (archive_root_.empty() || route.namespace_id.empty() || route.generation_id.empty()) {
        return std::string();
    }
    return (std::filesystem::path(archive_root_) / "namespaces" / route.namespace_id /
            "generations" / route.generation_id / "manifest.txt")
        .string();
}

uint64_t ArchiveMetaStore::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::mds
