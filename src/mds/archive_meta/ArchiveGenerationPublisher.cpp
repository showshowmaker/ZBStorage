#include "ArchiveGenerationPublisher.h"

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
    const std::wstring wide_path = path.wstring();
    HANDLE handle = CreateFileW(wide_path.c_str(),
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
    const std::wstring wide_path = path.wstring();
    HANDLE handle = CreateFileW(wide_path.c_str(),
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
            *error = "failed to cleanup published archive generation: " + ec.message();
        }
        return false;
    }
    return SyncParentDirectory(path, error);
}

bool IsStagingPath(const std::filesystem::path& path) {
    const std::string name = path.filename().string();
    return name.size() >= 8 && name.rfind(".staging") == name.size() - 8;
}

} // namespace

ArchiveGenerationPublisher::ArchiveGenerationPublisher(ArchiveNamespaceCatalog* catalog)
    : catalog_(catalog) {
}

bool ArchiveGenerationPublisher::PrepareStagingDir(const std::string& archive_root,
                                                   const std::string& namespace_id,
                                                   const std::string& generation_id,
                                                   std::string* staging_dir,
                                                   std::string* error) const {
    if (!staging_dir || archive_root.empty() || namespace_id.empty() || generation_id.empty()) {
        if (error) {
            *error = "invalid archive generation staging args";
        }
        return false;
    }
    const std::filesystem::path staging =
        std::filesystem::path(archive_root) / "staging" / namespace_id / (generation_id + ".staging");
    std::error_code ec;
    std::filesystem::remove_all(staging, ec);
    ec.clear();
    std::filesystem::create_directories(staging, ec);
    if (ec) {
        if (error) {
            *error = "failed to create archive staging dir: " + ec.message();
        }
        return false;
    }
    *staging_dir = staging.string();
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveGenerationPublisher::Publish(const PublishRequest& request,
                                         std::string* final_manifest_path,
                                         std::string* error) const {
    if (request.archive_root.empty() || request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.manifest_tmp_path.empty()) {
        if (error) {
            *error = "invalid archive generation publish request";
        }
        return false;
    }

    const std::filesystem::path manifest_tmp(request.manifest_tmp_path);
    const std::filesystem::path staging_dir = manifest_tmp.parent_path();
    const std::filesystem::path manifest_path = staging_dir / "manifest.txt";
    const std::filesystem::path final_dir =
        FinalGenerationDir(request.archive_root, request.namespace_id, request.generation_id);
    bool generation_moved = false;

    if (!std::filesystem::exists(manifest_tmp)) {
        if (error) {
            *error = "archive manifest.tmp is missing: " + manifest_tmp.string();
        }
        return false;
    }
    if (std::filesystem::exists(final_dir)) {
        if (error) {
            *error = "archive generation already exists: " + final_dir.string();
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::rename(manifest_tmp, manifest_path, ec);
    if (ec) {
        if (error) {
            *error = "failed to finalize archive manifest: " + ec.message();
        }
        return false;
    }
    if (!SyncParentDirectory(manifest_path, error)) {
        return false;
    }

    std::filesystem::create_directories(final_dir.parent_path(), ec);
    if (ec) {
        if (error) {
            *error = "failed to create archive generation parent dir: " + ec.message();
        }
        return false;
    }

    std::filesystem::rename(staging_dir, final_dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to publish archive generation dir: " + ec.message();
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

    const std::filesystem::path final_manifest = final_dir / "manifest.txt";
    if (request.publish_route) {
        if (!catalog_) {
            if (error) {
                *error = "archive namespace catalog is unavailable";
            }
            return false;
        }
        ArchiveNamespaceRoute route;
        route.namespace_id = request.namespace_id;
        route.path_prefix = request.path_prefix;
        route.generation_id = request.generation_id;
        route.manifest_path = final_manifest.string();
        route.inode_min = request.inode_min;
        route.inode_max = request.inode_max;
        route.inode_count = request.inode_count;
        if (!catalog_->PutRoute(route, request.inode_bucket_ids, error)) {
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

bool ArchiveGenerationPublisher::CleanupStagingDir(const std::string& staging_dir, std::string* error) const {
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
            *error = "failed to cleanup archive staging dir: " + ec.message();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveGenerationPublisher::CleanupNamespaceStaging(const std::string& archive_root,
                                                         const std::string& namespace_id,
                                                         std::vector<std::string>* removed_paths,
                                                         std::string* error) const {
    if (removed_paths) {
        removed_paths->clear();
    }
    if (archive_root.empty() || namespace_id.empty()) {
        if (error) {
            *error = "invalid archive staging cleanup args";
        }
        return false;
    }
    const std::filesystem::path staging_root = std::filesystem::path(archive_root) / "staging" / namespace_id;
    std::error_code ec;
    if (!std::filesystem::exists(staging_root, ec)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    for (const auto& entry : std::filesystem::directory_iterator(staging_root, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to iterate staging dir: " + ec.message();
            }
            return false;
        }
        if (!entry.is_directory(ec) || ec || !IsStagingPath(entry.path())) {
            ec.clear();
            continue;
        }
        if (!RemoveTreeAndSyncParent(entry.path(), error)) {
            return false;
        }
        if (removed_paths) {
            removed_paths->push_back(entry.path().string());
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveGenerationPublisher::ListPublishedGenerations(const std::string& archive_root,
                                                          const std::string& namespace_id,
                                                          std::vector<std::string>* generation_ids,
                                                          std::string* error) const {
    if (!generation_ids || archive_root.empty() || namespace_id.empty()) {
        if (error) {
            *error = "invalid archive generation listing args";
        }
        return false;
    }
    generation_ids->clear();
    const std::filesystem::path root = GenerationsRoot(archive_root, namespace_id);
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
                *error = "failed to iterate generation dir: " + ec.message();
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

bool ArchiveGenerationPublisher::RecoverCurrentRouteFromLatest(const std::string& archive_root,
                                                               const std::string& namespace_id,
                                                               ArchiveNamespaceRoute* route,
                                                               std::string* error) const {
    if (!catalog_ || !route) {
        if (error) {
            *error = "archive current recovery args are invalid";
        }
        return false;
    }
    std::vector<std::string> generations;
    if (!ListPublishedGenerations(archive_root, namespace_id, &generations, error)) {
        return false;
    }
    if (generations.empty()) {
        if (error) {
            *error = "no published generations found";
        }
        return false;
    }
    const std::string latest_generation = generations.back();
    ArchiveNamespaceRoute recovered;
    const std::filesystem::path manifest_path =
        std::filesystem::path(GenerationsRoot(archive_root, namespace_id)) / latest_generation / "manifest.txt";
    if (!LoadRouteFromManifest(manifest_path.string(), &recovered, error)) {
        return false;
    }
    if (!catalog_->PutRoute(recovered, error) || !catalog_->SetCurrentRoute(recovered, error)) {
        return false;
    }
    *route = std::move(recovered);
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveGenerationPublisher::PruneOldGenerations(const std::string& archive_root,
                                                     const std::string& namespace_id,
                                                     size_t keep_last,
                                                     const std::string& keep_generation_id,
                                                     std::vector<std::string>* removed_generation_ids,
                                                     std::string* error) const {
    if (!removed_generation_ids) {
        if (error) {
            *error = "removed_generation_ids output is null";
        }
        return false;
    }
    removed_generation_ids->clear();
    if (archive_root.empty() || namespace_id.empty()) {
        if (error) {
            *error = "invalid archive generation prune args";
        }
        return false;
    }
    std::vector<std::string> generations;
    if (!ListPublishedGenerations(archive_root, namespace_id, &generations, error)) {
        return false;
    }
    if (keep_last == 0) {
        keep_last = 1;
    }
    if (generations.size() <= keep_last) {
        if (error) {
            error->clear();
        }
        return true;
    }
    const size_t keep_begin = generations.size() - keep_last;
    for (size_t i = 0; i < keep_begin; ++i) {
        const std::string& generation_id = generations[i];
        if (!keep_generation_id.empty() && generation_id == keep_generation_id) {
            continue;
        }
        const std::filesystem::path path = FinalGenerationDir(archive_root, namespace_id, generation_id);
        if (!RemoveTreeAndSyncParent(path, error)) {
            return false;
        }
        removed_generation_ids->push_back(generation_id);
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string ArchiveGenerationPublisher::FinalGenerationDir(const std::string& archive_root,
                                                           const std::string& namespace_id,
                                                           const std::string& generation_id) const {
    return (std::filesystem::path(archive_root) / "namespaces" / namespace_id / "generations" / generation_id).string();
}

std::string ArchiveGenerationPublisher::GenerationsRoot(const std::string& archive_root,
                                                        const std::string& namespace_id) const {
    return (std::filesystem::path(archive_root) / "namespaces" / namespace_id / "generations").string();
}

bool ArchiveGenerationPublisher::LoadRouteFromManifest(const std::string& manifest_path,
                                                       ArchiveNamespaceRoute* route,
                                                       std::string* error) const {
    if (!route || manifest_path.empty()) {
        if (error) {
            *error = "invalid archive manifest route args";
        }
        return false;
    }
    ArchiveManifest manifest;
    if (!ArchiveManifest::LoadFromFile(manifest_path, &manifest, error)) {
        return false;
    }
    route->namespace_id = manifest.namespace_id;
    route->path_prefix = manifest.path_prefix;
    route->generation_id = manifest.generation_id;
    route->manifest_path = manifest.manifest_path;
    route->inode_min = manifest.inode_min;
    route->inode_max = manifest.inode_max;
    route->inode_count = manifest.inode_count;
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
