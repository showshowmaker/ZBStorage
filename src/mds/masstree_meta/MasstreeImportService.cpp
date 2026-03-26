#include "MasstreeImportService.h"

#include <algorithm>
#include <filesystem>

#include "MasstreePageLayout.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

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

} // namespace

MasstreeImportService::MasstreeImportService(RocksMetaStore* meta_store,
                                             MasstreeNamespaceCatalog* catalog)
    : meta_store_(meta_store),
      catalog_(catalog) {
}

bool MasstreeImportService::ImportNamespace(const Request& request,
                                            Result* result,
                                            std::string* error) const {
    std::lock_guard<std::mutex> lock(import_mu_);
    if (result) {
        *result = Result();
    }
    if (!meta_store_ || !catalog_ || request.masstree_root.empty() ||
        request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.file_count == 0 ||
        request.max_files_per_leaf_dir == 0 || request.max_subdirs_per_dir == 0) {
        if (error) {
            *error = "invalid masstree import request";
        }
        return false;
    }

    Request normalized = request;
    if (!NormalizePathPrefix(request.path_prefix, &normalized.path_prefix, error)) {
        return false;
    }
    if (normalized.page_size_bytes == 0) {
        normalized.page_size_bytes = kMasstreeDefaultPageSizeBytes;
    }
    if (normalized.page_size_bytes < 4096U) {
        if (error) {
            *error = "masstree page_size_bytes must be >= 4096";
        }
        return false;
    }

    const uint64_t leaf_dir_count =
        (normalized.file_count + normalized.max_files_per_leaf_dir - 1ULL) / normalized.max_files_per_leaf_dir;
    const uint64_t level1_dir_count =
        std::max<uint64_t>(1, (leaf_dir_count + normalized.max_subdirs_per_dir - 1ULL) / normalized.max_subdirs_per_dir);
    const uint64_t required_inode_count = 1ULL + level1_dir_count + leaf_dir_count + normalized.file_count;
    if (required_inode_count == 0) {
        if (error) {
            *error = "invalid masstree inode reservation size";
        }
        return false;
    }

    if (normalized.inode_start == 0) {
        if (!ReserveInodeRange(required_inode_count, &normalized.inode_start, error)) {
            return false;
        }
    }

    MasstreeStatsStore stats_store(meta_store_);
    MasstreeClusterStatsRecord cluster_before;
    if (!stats_store.LoadClusterStats(&cluster_before, error)) {
        return false;
    }

    MasstreeBulkMetaGenerator generator;
    MasstreeBulkMetaGenerator::Request generate_request;
    generate_request.output_root = (std::filesystem::path(normalized.masstree_root) / "staging").string();
    generate_request.namespace_id = normalized.namespace_id;
    generate_request.generation_id = normalized.generation_id;
    generate_request.path_prefix = normalized.path_prefix;
    generate_request.inode_start = normalized.inode_start;
    generate_request.file_count = normalized.file_count;
    generate_request.page_size_bytes = normalized.page_size_bytes;
    generate_request.max_files_per_leaf_dir = normalized.max_files_per_leaf_dir;
    generate_request.max_subdirs_per_dir = normalized.max_subdirs_per_dir;

    MasstreeBulkMetaGenerator::Result generate_result;
    if (!generator.Generate(generate_request, &generate_result, error)) {
        return false;
    }

    MasstreeGenerationPublisher publisher(catalog_);
    auto cleanup = [&]() {
        std::string cleanup_error;
        publisher.CleanupStagingDir(generate_result.staging_dir, &cleanup_error);
    };

    MasstreeBulkImporter importer;
    MasstreeBulkImporter::Request import_request;
    import_request.manifest_path = generate_result.manifest_path;
    import_request.page_size_bytes = normalized.page_size_bytes;
    import_request.verify_inode_samples = normalized.verify_inode_samples;
    import_request.verify_dentry_samples = normalized.verify_dentry_samples;
    import_request.start_cursor = cluster_before.cursor;

    MasstreeBulkImporter::Result import_result;
    if (!importer.Import(import_request, nullptr, &import_result, error)) {
        cleanup();
        return false;
    }

    bool had_previous_namespace_stats = false;
    std::string previous_namespace_stats_raw;
    if (!stats_store.LoadNamespaceStatsRaw(normalized.namespace_id,
                                           &had_previous_namespace_stats,
                                           &previous_namespace_stats_raw,
                                           error)) {
        cleanup();
        return false;
    }

    MasstreeNamespaceStatsRecord previous_namespace_stats;
    const MasstreeNamespaceStatsRecord* previous_namespace_stats_ptr = nullptr;
    if (had_previous_namespace_stats &&
        !stats_store.LoadNamespaceStats(normalized.namespace_id, &previous_namespace_stats, error)) {
        cleanup();
        return false;
    }
    if (had_previous_namespace_stats) {
        previous_namespace_stats_ptr = &previous_namespace_stats;
    }

    MasstreeNamespaceStatsRecord namespace_stats;
    namespace_stats.namespace_id = normalized.namespace_id;
    namespace_stats.generation_id = normalized.generation_id;
    namespace_stats.file_count = import_result.file_count;
    namespace_stats.total_file_bytes = import_result.total_file_bytes;
    namespace_stats.total_metadata_bytes = std::to_string(import_result.inode_pages_bytes);
    namespace_stats.avg_file_size_bytes = import_result.avg_file_size_bytes;
    namespace_stats.start_global_image_id = import_result.start_global_image_id;
    namespace_stats.end_global_image_id = import_result.end_global_image_id;
    namespace_stats.start_cursor = import_result.start_cursor;
    namespace_stats.end_cursor = import_result.end_cursor;

    const MasstreeClusterStatsRecord cluster_after =
        stats_store.BuildReplacedClusterStats(cluster_before,
                                             previous_namespace_stats_ptr,
                                             namespace_stats,
                                             import_result.end_cursor);

    bool had_previous_cluster_stats = false;
    std::string previous_cluster_stats_raw;
    if (!stats_store.LoadClusterStatsRaw(&had_previous_cluster_stats, &previous_cluster_stats_raw, error)) {
        cleanup();
        return false;
    }

    rocksdb::WriteBatch stats_batch;
    if (!stats_store.PutClusterStats(cluster_after, &stats_batch, error) ||
        !stats_store.PutNamespaceStats(namespace_stats, &stats_batch, error) ||
        !meta_store_->WriteBatch(&stats_batch, error)) {
        cleanup();
        return false;
    }

    MasstreeGenerationPublisher::PublishRequest publish_request;
    publish_request.masstree_root = normalized.masstree_root;
    publish_request.namespace_id = normalized.namespace_id;
    publish_request.generation_id = normalized.generation_id;
    publish_request.manifest_path = generate_result.manifest_path;
    publish_request.publish_route = normalized.publish_route;

    std::string final_manifest_path;
    if (!publisher.Publish(publish_request, &final_manifest_path, error)) {
        rocksdb::WriteBatch rollback_batch;
        std::string rollback_error;
        if (stats_store.RestoreRawClusterStats(had_previous_cluster_stats,
                                               previous_cluster_stats_raw,
                                               &rollback_batch,
                                               &rollback_error) &&
            stats_store.RestoreRawNamespaceStats(normalized.namespace_id,
                                                had_previous_namespace_stats,
                                                previous_namespace_stats_raw,
                                                &rollback_batch,
                                                &rollback_error)) {
            meta_store_->WriteBatch(&rollback_batch, &rollback_error);
        }
        cleanup();
        return false;
    }

    if (result) {
        result->manifest_path = final_manifest_path;
        result->staging_dir = generate_result.staging_dir;
        result->root_inode_id = generate_result.root_inode_id;
        result->inode_min = generate_result.inode_min;
        result->inode_max = generate_result.inode_max;
        result->inode_count = generate_result.inode_count;
        result->dentry_count = generate_result.dentry_count;
        result->level1_dir_count = generate_result.level1_dir_count;
        result->leaf_dir_count = generate_result.leaf_dir_count;
        result->inode_pages_bytes = import_result.inode_pages_bytes;
        result->avg_file_size_bytes = import_result.avg_file_size_bytes;
        result->total_file_bytes = import_result.total_file_bytes;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeImportService::ReserveInodeRange(uint64_t inode_count,
                                              uint64_t* inode_start,
                                              std::string* error) const {
    if (!meta_store_ || !inode_start || inode_count == 0) {
        if (error) {
            *error = "invalid masstree inode range reservation args";
        }
        return false;
    }

    std::string value;
    uint64_t next_id = kRootInodeId + 1ULL;
    if (meta_store_->Get(NextInodeKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "invalid next inode value";
            }
            return false;
        }
    } else if (error && !error->empty()) {
        return false;
    }

    *inode_start = next_id;
    const uint64_t new_next_id = next_id + inode_count;
    if (!meta_store_->Put(NextInodeKey(), MetaCodec::EncodeUInt64(new_next_id), error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeImportService::NormalizePathPrefix(const std::string& path_prefix,
                                                std::string* normalized,
                                                std::string* error) const {
    if (!NormalizePath(path_prefix, normalized)) {
        if (error) {
            *error = "invalid masstree path_prefix";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
