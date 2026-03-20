#include "ArchiveNamespaceCatalog.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <sstream>
#include <vector>

#include <rocksdb/iterator.h>

#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

bool NormalizePathPrefix(const std::string& path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::string p = path;
    std::replace(p.begin(), p.end(), '\\', '/');
    if (p.empty() || p[0] != '/') {
        p.insert(p.begin(), '/');
    }
    std::string out;
    out.reserve(p.size() + 1);
    bool prev_slash = false;
    for (char ch : p) {
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
    if (out.empty()) {
        out = "/";
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    *normalized = std::move(out);
    return true;
}

std::vector<std::string> BuildPrefixCandidates(const std::string& normalized_path) {
    std::vector<std::string> out;
    if (normalized_path.empty() || normalized_path[0] != '/') {
        return out;
    }
    std::string current = normalized_path;
    while (true) {
        out.push_back(current);
        if (current == "/") {
            break;
        }
        const size_t slash = current.find_last_of('/');
        if (slash == std::string::npos || slash == 0) {
            current = "/";
        } else {
            current = current.substr(0, slash);
        }
    }
    return out;
}

std::string EncodeRoute(const ArchiveNamespaceRoute& route) {
    return route.namespace_id + "\t" + route.path_prefix + "\t" +
           route.generation_id + "\t" + route.manifest_path + "\t" +
           std::to_string(route.inode_min) + "\t" +
           std::to_string(route.inode_max) + "\t" +
           std::to_string(route.inode_count);
}

bool DecodeRoute(const std::string& payload, ArchiveNamespaceRoute* route) {
    if (!route) {
        return false;
    }
    std::istringstream stream(payload);
    std::string part;
    std::vector<std::string> fields;
    while (std::getline(stream, part, '\t')) {
        fields.push_back(part);
    }
    if (fields.size() != 4 && fields.size() != 7) {
        return false;
    }
    route->namespace_id = fields[0];
    route->path_prefix = fields[1];
    route->generation_id = fields[2];
    route->manifest_path = fields[3];
    route->inode_min = 0;
    route->inode_max = 0;
    route->inode_count = 0;
    if (fields.size() == 7) {
        try {
            route->inode_min = static_cast<uint64_t>(std::stoull(fields[4]));
            route->inode_max = static_cast<uint64_t>(std::stoull(fields[5]));
            route->inode_count = static_cast<uint64_t>(std::stoull(fields[6]));
        } catch (...) {
            return false;
        }
    }
    return !route->path_prefix.empty();
}

std::string EncodeCurrentRoute(const ArchiveNamespaceRoute& route) {
    return route.generation_id + "\t" + route.manifest_path;
}

bool DecodeCurrentRoute(const std::string& normalized_prefix,
                        const std::string& payload,
                        ArchiveNamespaceRoute* route) {
    if (!route) {
        return false;
    }
    std::istringstream stream(payload);
    std::vector<std::string> fields;
    std::string part;
    while (std::getline(stream, part, '\t')) {
        fields.push_back(part);
    }
    if (fields.size() != 2 || fields[0].empty()) {
        return false;
    }
    route->path_prefix = normalized_prefix;
    route->generation_id = fields[0];
    route->manifest_path = fields[1];
    return true;
}

bool ApplyCurrentRoute(RocksMetaStore* store,
                       ArchiveNamespaceRoute* route,
                       std::string* error) {
    if (!route || !store) {
        if (error) {
            *error = "archive namespace current route args are invalid";
        }
        return false;
    }
    std::string payload;
    std::string local_error;
    if (!store->Get(ArchiveNamespaceCurrentKey(route->path_prefix), &payload, &local_error)) {
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
        if (error) {
            error->clear();
        }
        return true;
    }
    ArchiveNamespaceRoute current;
    if (!DecodeCurrentRoute(route->path_prefix, payload, &current)) {
        if (error) {
            *error = "invalid archive namespace current payload";
        }
        return false;
    }
    route->generation_id = current.generation_id;
    route->manifest_path = current.manifest_path;
    if (error) {
        error->clear();
    }
    return true;
}

bool DeleteRouteSecondaryIndexes(RocksMetaStore* store,
                                 const std::string& normalized_prefix,
                                 rocksdb::WriteBatch* batch,
                                 std::string* error) {
    if (!store || !store->db() || !batch) {
        if (error) {
            *error = "archive namespace secondary index delete args are invalid";
        }
        return false;
    }
    std::unique_ptr<rocksdb::Iterator> it(store->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string reverse_prefix = ArchiveRouteBucketPrefix(normalized_prefix);
    for (it->Seek(reverse_prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(reverse_prefix, 0) != 0) {
            break;
        }
        const std::string bucket_suffix = key.substr(reverse_prefix.size());
        batch->Delete(key);
        batch->Delete("AIB/" + bucket_suffix + normalized_prefix);
    }
    if (!it->status().ok()) {
        if (error) {
            *error = it->status().ToString();
        }
        return false;
    }
    return true;
}

} // namespace

ArchiveNamespaceCatalog::ArchiveNamespaceCatalog(RocksMetaStore* store)
    : store_(store) {
}

bool ArchiveNamespaceCatalog::PutRoute(const ArchiveNamespaceRoute& route, std::string* error) {
    return PutRoute(route, std::vector<uint64_t>(), error);
}

bool ArchiveNamespaceCatalog::PutRoute(const ArchiveNamespaceRoute& route,
                                       const std::vector<uint64_t>& inode_bucket_ids,
                                       std::string* error) {
    if (!store_) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(route.path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid archive namespace path prefix";
        }
        return false;
    }
    ArchiveNamespaceRoute normalized = route;
    normalized.path_prefix = normalized_prefix;

    rocksdb::WriteBatch batch;
    if (!DeleteRouteSecondaryIndexes(store_, normalized_prefix, &batch, error)) {
        return false;
    }
    const std::string payload = EncodeRoute(normalized);
    const std::string current_payload = EncodeCurrentRoute(normalized);
    batch.Put(ArchiveNamespaceRouteKey(normalized_prefix), payload);
    batch.Put(ArchiveNamespaceCurrentKey(normalized_prefix), current_payload);
    for (uint64_t bucket_id : inode_bucket_ids) {
        batch.Put(ArchiveInodeBucketKey(bucket_id, normalized_prefix), payload);
        batch.Put(ArchiveRouteBucketKey(normalized_prefix, bucket_id), "");
    }
    return store_->WriteBatch(&batch, error);
}

bool ArchiveNamespaceCatalog::LookupByPath(const std::string& path,
                                           ArchiveNamespaceRoute* route,
                                           std::string* error) const {
    if (route) {
        *route = ArchiveNamespaceRoute();
    }
    if (!store_) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_path;
    if (!NormalizePathPrefix(path, &normalized_path)) {
        if (error) {
            *error = "invalid archive namespace lookup path";
        }
        return false;
    }
    for (const auto& prefix : BuildPrefixCandidates(normalized_path)) {
        std::string payload;
        std::string local_error;
        if (!store_->Get(ArchiveNamespaceRouteKey(prefix), &payload, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }
        ArchiveNamespaceRoute loaded;
        if (!DecodeRoute(payload, &loaded)) {
            if (error) {
                *error = "invalid archive namespace route payload";
            }
            return false;
        }
        if (!ApplyCurrentRoute(store_, &loaded, error)) {
            return false;
        }
        if (route) {
            *route = std::move(loaded);
        }
        if (error) {
            error->clear();
        }
        return true;
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool ArchiveNamespaceCatalog::ListRoutes(std::vector<ArchiveNamespaceRoute>* routes, std::string* error) const {
    if (!routes) {
        if (error) {
            *error = "routes output is null";
        }
        return false;
    }
    routes->clear();
    if (!store_ || !store_->db()) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }

    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string prefix = ArchiveNamespaceRoutePrefix();
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        ArchiveNamespaceRoute route;
        if (!DecodeRoute(it->value().ToString(), &route)) {
            if (error) {
                *error = "invalid archive namespace route payload";
            }
            return false;
        }
        if (!ApplyCurrentRoute(store_, &route, error)) {
            return false;
        }
        routes->push_back(std::move(route));
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveNamespaceCatalog::LookupByInode(uint64_t inode_id,
                                            std::vector<ArchiveNamespaceRoute>* routes,
                                            std::string* error) const {
    if (!routes) {
        if (error) {
            *error = "routes output is null";
        }
        return false;
    }
    routes->clear();
    if (!store_ || !store_->db()) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    const uint64_t bucket_id = ArchiveInodeBucketId(inode_id);
    const std::string prefix = ArchiveInodeBucketPrefix(bucket_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        ArchiveNamespaceRoute route;
        if (!DecodeRoute(it->value().ToString(), &route)) {
            if (error) {
                *error = "invalid archive namespace route payload";
            }
            return false;
        }
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        if (!ApplyCurrentRoute(store_, &route, error)) {
            return false;
        }
        routes->push_back(std::move(route));
    }
    if (!it->status().ok()) {
        if (error) {
            *error = it->status().ToString();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveNamespaceCatalog::DeleteRoute(const std::string& path_prefix, std::string* error) {
    if (!store_) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid archive namespace path prefix";
        }
        return false;
    }
    rocksdb::WriteBatch batch;
    if (!DeleteRouteSecondaryIndexes(store_, normalized_prefix, &batch, error)) {
        return false;
    }
    batch.Delete(ArchiveNamespaceRouteKey(normalized_prefix));
    batch.Delete(ArchiveNamespaceCurrentKey(normalized_prefix));
    return store_->WriteBatch(&batch, error);
}

bool ArchiveNamespaceCatalog::SetCurrentRoute(const ArchiveNamespaceRoute& route, std::string* error) {
    if (!store_) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(route.path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid archive namespace path prefix";
        }
        return false;
    }
    if (route.generation_id.empty()) {
        if (error) {
            *error = "archive namespace current generation_id is empty";
        }
        return false;
    }
    ArchiveNamespaceRoute current = route;
    current.path_prefix = normalized_prefix;
    return store_->Put(ArchiveNamespaceCurrentKey(normalized_prefix), EncodeCurrentRoute(current), error);
}

bool ArchiveNamespaceCatalog::LookupCurrentRoute(const std::string& path_prefix,
                                                 ArchiveNamespaceRoute* route,
                                                 std::string* error) const {
    if (route) {
        *route = ArchiveNamespaceRoute();
    }
    if (!store_) {
        if (error) {
            *error = "archive namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid archive namespace path prefix";
        }
        return false;
    }
    std::string payload;
    if (!store_->Get(ArchiveNamespaceCurrentKey(normalized_prefix), &payload, error)) {
        return false;
    }
    ArchiveNamespaceRoute current;
    if (!DecodeCurrentRoute(normalized_prefix, payload, &current)) {
        if (error) {
            *error = "invalid archive namespace current payload";
        }
        return false;
    }
    std::string route_payload;
    std::string route_error;
    if (store_->Get(ArchiveNamespaceRouteKey(normalized_prefix), &route_payload, &route_error)) {
        ArchiveNamespaceRoute metadata;
        if (!DecodeRoute(route_payload, &metadata)) {
            if (error) {
                *error = "invalid archive namespace route payload";
            }
            return false;
        }
        metadata.generation_id = current.generation_id;
        metadata.manifest_path = current.manifest_path;
        if (route) {
            *route = std::move(metadata);
        }
    } else {
        if (!route_error.empty()) {
            if (error) {
                *error = route_error;
            }
            return false;
        }
        if (route) {
            *route = std::move(current);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
