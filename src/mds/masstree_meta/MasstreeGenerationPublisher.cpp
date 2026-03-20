#include "MasstreeGenerationPublisher.h"

#include <algorithm>
#include <filesystem>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#endif

#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

#ifdef _WIN32
std::string FormatWinError(const std::string& prefix, DWORD code) {
    LPSTR buffer = nullptr;
    const DWORD flags = FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    const DWORD length = FormatMessageA(flags,
                                        nullptr,
                                        code,
                                        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                        reinterpret_cast<LPSTR>(&buffer),
                                        0,
                                        nullptr);
    std::string message = prefix + " (code=" + std::to_string(code) + ")";
    if (length != 0 && buffer) {
        std::string text(buffer, length);
        while (!text.empty() && (text.back() == '\r' || text.back() == '\n')) {
            text.pop_back();
        }
        message += ": " + text;
    }
    if (buffer) {
        LocalFree(buffer);
    }
    return message;
}

bool SyncRegularFile(const std::filesystem::path& path, std::string* error) {
    HANDLE handle = CreateFileW(path.wstring().c_str(),
                                GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr,
                                OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL,
                                nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        if (error) {
            *error = FormatWinError("failed to open file for sync: " + path.string(), GetLastError());
        }
        return false;
    }
    const BOOL ok = FlushFileBuffers(handle);
    const DWORD flush_error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!ok) {
        if (error) {
            *error = FormatWinError("failed to sync file: " + path.string(), flush_error);
        }
        return false;
    }
    return true;
}

bool SyncDirectory(const std::filesystem::path& path, std::string* error) {
    HANDLE handle = CreateFileW(path.wstring().c_str(),
                                FILE_LIST_DIRECTORY,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr,
                                OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS,
                                nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        if (error) {
            *error = FormatWinError("failed to open directory for sync: " + path.string(), GetLastError());
        }
        return false;
    }
    const BOOL ok = FlushFileBuffers(handle);
    const DWORD flush_error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!ok && (flush_error == ERROR_ACCESS_DENIED || flush_error == ERROR_INVALID_FUNCTION)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!ok) {
        if (error) {
            *error = FormatWinError("failed to sync directory: " + path.string(), flush_error);
        }
        return false;
    }
    return true;
}
#else
std::string FormatErrno(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

bool SyncRegularFile(const std::filesystem::path& path, std::string* error) {
    const int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (error) {
            *error = FormatErrno("failed to open file for sync: " + path.string());
        }
        return false;
    }
    const int rc = fsync(fd);
    const int saved_errno = (rc == 0) ? 0 : errno;
    close(fd);
    if (rc != 0) {
        errno = saved_errno;
        if (error) {
            *error = FormatErrno("failed to sync file: " + path.string());
        }
        return false;
    }
    return true;
}

bool SyncDirectory(const std::filesystem::path& path, std::string* error) {
    const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        if (error) {
            *error = FormatErrno("failed to open directory for sync: " + path.string());
        }
        return false;
    }
    const int rc = fsync(fd);
    const int saved_errno = (rc == 0) ? 0 : errno;
    close(fd);
    if (rc != 0) {
        errno = saved_errno;
        if (error) {
            *error = FormatErrno("failed to sync directory: " + path.string());
        }
        return false;
    }
    return true;
}
#endif

bool SyncParentDirectory(const std::filesystem::path& path, std::string* error) {
    const std::filesystem::path parent = path.parent_path();
    if (parent.empty()) {
        if (error) {
            error->clear();
        }
        return true;
    }
    return SyncDirectory(parent, error);
}

bool RemoveTreeAndSyncParent(const std::filesystem::path& path, std::string* error) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
        if (error) {
            *error = "failed to cleanup masstree generation tree: " + ec.message();
        }
        return false;
    }
    return SyncParentDirectory(path, error);
}

bool IsStagingPath(const std::filesystem::path& path) {
    const std::string name = path.filename().string();
    return name.size() >= 8U && name.rfind(".staging") == name.size() - 8U;
}

std::string RewritePathToFinalDir(const std::filesystem::path& final_dir, const std::string& path_value) {
    if (path_value.empty()) {
        return std::string();
    }
    return (final_dir / std::filesystem::path(path_value).filename()).string();
}

} // namespace

MasstreeGenerationPublisher::MasstreeGenerationPublisher(MasstreeNamespaceCatalog* catalog)
    : catalog_(catalog) {
}

bool MasstreeGenerationPublisher::Publish(const PublishRequest& request,
                                          std::string* final_manifest_path,
                                          std::string* error) const {
    if (request.masstree_root.empty() || request.namespace_id.empty() ||
        request.generation_id.empty() || request.manifest_path.empty()) {
        if (error) {
            *error = "invalid masstree generation publish request";
        }
        return false;
    }

    const std::filesystem::path staging_manifest(request.manifest_path);
    const std::filesystem::path staging_dir = staging_manifest.parent_path();
    const std::filesystem::path final_dir =
        FinalGenerationDir(request.masstree_root, request.namespace_id, request.generation_id);
    const std::filesystem::path final_manifest = final_dir / "manifest.txt";
    bool generation_moved = false;

    if (!std::filesystem::exists(staging_manifest)) {
        if (error) {
            *error = "masstree manifest is missing: " + staging_manifest.string();
        }
        return false;
    }
    if (!std::filesystem::exists(staging_dir) || !IsStagingPath(staging_dir)) {
        if (error) {
            *error = "masstree staging dir is invalid: " + staging_dir.string();
        }
        return false;
    }
    if (std::filesystem::exists(final_dir)) {
        if (error) {
            *error = "masstree generation already exists: " + final_dir.string();
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(final_dir.parent_path(), ec);
    if (ec) {
        if (error) {
            *error = "failed to create masstree generation parent dir: " + ec.message();
        }
        return false;
    }

    std::filesystem::rename(staging_dir, final_dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to publish masstree generation dir: " + ec.message();
        }
        return false;
    }
    generation_moved = true;
    if (!SyncDirectory(final_dir.parent_path(), error)) {
        std::string cleanup_error;
        if (!RemoveTreeAndSyncParent(final_dir, &cleanup_error) && error) {
            *error += "; cleanup error: " + cleanup_error;
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    if (!MasstreeNamespaceManifest::LoadFromFile(final_manifest.string(), &manifest, error)) {
        std::string cleanup_error;
        if (!RemoveTreeAndSyncParent(final_dir, &cleanup_error) && error) {
            *error += "; cleanup error: " + cleanup_error;
        }
        return false;
    }
    manifest.manifest_path = final_manifest.string();
    if (!RewriteManifestPaths(final_dir.string(), &manifest, error) ||
        !manifest.SaveToFile(final_manifest.string(), error) ||
        !SyncRegularFile(final_manifest, error) ||
        !SyncParentDirectory(final_manifest, error)) {
        std::string cleanup_error;
        if (!RemoveTreeAndSyncParent(final_dir, &cleanup_error) && error) {
            *error += "; cleanup error: " + cleanup_error;
        }
        return false;
    }

    if (request.publish_route) {
        if (!catalog_) {
            if (error) {
                *error = "masstree namespace catalog is unavailable";
            }
            if (generation_moved) {
                std::string cleanup_error;
                if (!RemoveTreeAndSyncParent(final_dir, &cleanup_error) && error) {
                    *error += "; cleanup error: " + cleanup_error;
                }
            }
            return false;
        }
        MasstreeNamespaceRoute route;
        std::vector<uint64_t> bucket_ids;
        if (!BuildRouteFromManifest(manifest, &route, &bucket_ids, error) ||
            !catalog_->PutRoute(route, bucket_ids, error)) {
            if (generation_moved) {
                std::string cleanup_error;
                if (!RemoveTreeAndSyncParent(final_dir, &cleanup_error) && error) {
                    *error += "; cleanup error: " + cleanup_error;
                }
            }
            return false;
        }
    }

    if (final_manifest_path) {
        *final_manifest_path = final_manifest.string();
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeGenerationPublisher::CleanupStagingDir(const std::string& staging_dir, std::string* error) const {
    if (staging_dir.empty()) {
        if (error) {
            error->clear();
        }
        return true;
    }
    std::error_code ec;
    std::filesystem::remove_all(staging_dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to cleanup masstree staging dir: " + ec.message();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeGenerationPublisher::ListPublishedGenerations(const std::string& masstree_root,
                                                           const std::string& namespace_id,
                                                           std::vector<std::string>* generation_ids,
                                                           std::string* error) const {
    if (!generation_ids || masstree_root.empty() || namespace_id.empty()) {
        if (error) {
            *error = "invalid masstree generation listing args";
        }
        return false;
    }
    generation_ids->clear();
    const std::filesystem::path root = GenerationsRoot(masstree_root, namespace_id);
    std::error_code ec;
    if (!std::filesystem::exists(root, ec)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    for (const auto& entry : std::filesystem::directory_iterator(root, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to iterate masstree generation dir: " + ec.message();
            }
            return false;
        }
        if (!entry.is_directory(ec) || ec) {
            ec.clear();
            continue;
        }
        generation_ids->push_back(entry.path().filename().string());
    }
    std::sort(generation_ids->begin(), generation_ids->end());
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeGenerationPublisher::RecoverCurrentRouteFromLatest(const std::string& masstree_root,
                                                                const std::string& namespace_id,
                                                                MasstreeNamespaceRoute* route,
                                                                std::string* error) const {
    if (!catalog_ || !route) {
        if (error) {
            *error = "masstree current recovery args are invalid";
        }
        return false;
    }
    std::vector<std::string> generations;
    if (!ListPublishedGenerations(masstree_root, namespace_id, &generations, error)) {
        return false;
    }
    if (generations.empty()) {
        if (error) {
            *error = "no published masstree generations found";
        }
        return false;
    }
    const std::filesystem::path manifest_path =
        std::filesystem::path(GenerationsRoot(masstree_root, namespace_id)) /
        generations.back() / "manifest.txt";
    MasstreeNamespaceManifest manifest;
    if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path.string(), &manifest, error)) {
        return false;
    }
    std::vector<uint64_t> bucket_ids;
    MasstreeNamespaceRoute recovered;
    if (!BuildRouteFromManifest(manifest, &recovered, &bucket_ids, error) ||
        !catalog_->PutRoute(recovered, bucket_ids, error) ||
        !catalog_->SetCurrentRoute(recovered, error)) {
        return false;
    }
    *route = std::move(recovered);
    if (error) {
        error->clear();
    }
    return true;
}

std::string MasstreeGenerationPublisher::FinalGenerationDir(const std::string& masstree_root,
                                                            const std::string& namespace_id,
                                                            const std::string& generation_id) const {
    return (std::filesystem::path(masstree_root) / "namespaces" / namespace_id / "generations" / generation_id)
        .string();
}

std::string MasstreeGenerationPublisher::GenerationsRoot(const std::string& masstree_root,
                                                         const std::string& namespace_id) const {
    return (std::filesystem::path(masstree_root) / "namespaces" / namespace_id / "generations").string();
}

bool MasstreeGenerationPublisher::RewriteManifestPaths(const std::string& final_dir,
                                                       MasstreeNamespaceManifest* manifest,
                                                       std::string* error) const {
    if (!manifest || final_dir.empty()) {
        if (error) {
            *error = "invalid masstree manifest rewrite args";
        }
        return false;
    }
    const std::filesystem::path final_path(final_dir);
    manifest->manifest_path = (final_path / "manifest.txt").string();
    manifest->inode_records_path = RewritePathToFinalDir(final_path, manifest->inode_records_path);
    manifest->dentry_records_path = RewritePathToFinalDir(final_path, manifest->dentry_records_path);
    manifest->inode_pages_path = RewritePathToFinalDir(final_path, manifest->inode_pages_path);
    manifest->inode_sparse_index_path = RewritePathToFinalDir(final_path, manifest->inode_sparse_index_path);
    manifest->dentry_pages_path = RewritePathToFinalDir(final_path, manifest->dentry_pages_path);
    manifest->dentry_sparse_index_path = RewritePathToFinalDir(final_path, manifest->dentry_sparse_index_path);
    manifest->verify_manifest_path = RewritePathToFinalDir(final_path, manifest->verify_manifest_path);
    manifest->cluster_stats_path = RewritePathToFinalDir(final_path, manifest->cluster_stats_path);
    manifest->allocation_summary_path = RewritePathToFinalDir(final_path, manifest->allocation_summary_path);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeGenerationPublisher::BuildRouteFromManifest(const MasstreeNamespaceManifest& manifest,
                                                         MasstreeNamespaceRoute* route,
                                                         std::vector<uint64_t>* bucket_ids,
                                                         std::string* error) const {
    if (!route || !bucket_ids) {
        if (error) {
            *error = "invalid masstree route build args";
        }
        return false;
    }
    route->namespace_id = manifest.namespace_id;
    route->path_prefix = manifest.path_prefix;
    route->generation_id = manifest.generation_id;
    route->manifest_path = manifest.manifest_path;
    route->root_inode_id = manifest.root_inode_id;
    route->inode_min = manifest.inode_min;
    route->inode_max = manifest.inode_max;
    route->inode_count = manifest.inode_count;
    route->dentry_count = manifest.dentry_count;

    bucket_ids->clear();
    if (manifest.inode_min != 0 && manifest.inode_max >= manifest.inode_min) {
        const uint64_t first_bucket = MasstreeInodeBucketId(manifest.inode_min);
        const uint64_t last_bucket = MasstreeInodeBucketId(manifest.inode_max);
        bucket_ids->reserve(static_cast<size_t>(last_bucket - first_bucket + 1U));
        for (uint64_t bucket_id = first_bucket; bucket_id <= last_bucket; ++bucket_id) {
            bucket_ids->push_back(bucket_id);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
