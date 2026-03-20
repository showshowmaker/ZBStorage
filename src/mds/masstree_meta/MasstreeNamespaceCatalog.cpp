#include "MasstreeNamespaceCatalog.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

#include <rocksdb/iterator.h>

#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

bool NormalizePathPrefix(const std::string& path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::string p = path;
    std::replace(p.begin(), p.end(), '\\', '/');
    if (p.empty() || p.front() != '/') {
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
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    if (out.empty()) {
        out = "/";
    }
    *normalized = std::move(out);
    return true;
}

std::vector<std::string> BuildPrefixCandidates(const std::string& normalized_path) {
    std::vector<std::string> out;
    if (normalized_path.empty() || normalized_path.front() != '/') {
        return out;
    }
    std::string current = normalized_path;
    while (true) {
        out.push_back(current);
        if (current == "/") {
            break;
        }
        const size_t slash = current.find_last_of('/');
        current = (slash == std::string::npos || slash == 0) ? "/" : current.substr(0, slash);
    }
    return out;
}

std::string EncodeRoute(const MasstreeNamespaceRoute& route) {
    return route.namespace_id + "\t" + route.path_prefix + "\t" +
           route.generation_id + "\t" + route.manifest_path + "\t" +
           std::to_string(route.root_inode_id) + "\t" +
           std::to_string(route.inode_min) + "\t" +
           std::to_string(route.inode_max) + "\t" +
           std::to_string(route.inode_count) + "\t" +
           std::to_string(route.dentry_count);
}

bool DecodeRoute(const std::string& payload, MasstreeNamespaceRoute* route) {
    if (!route) {
        return false;
    }
    std::istringstream stream(payload);
    std::string part;
    std::vector<std::string> fields;
    while (std::getline(stream, part, '\t')) {
        fields.push_back(part);
    }
    if (fields.size() != 9) {
        return false;
    }
    route->namespace_id = fields[0];
    route->path_prefix = fields[1];
    route->generation_id = fields[2];
    route->manifest_path = fields[3];
    try {
        route->root_inode_id = static_cast<uint64_t>(std::stoull(fields[4]));
        route->inode_min = static_cast<uint64_t>(std::stoull(fields[5]));
        route->inode_max = static_cast<uint64_t>(std::stoull(fields[6]));
        route->inode_count = static_cast<uint64_t>(std::stoull(fields[7]));
        route->dentry_count = static_cast<uint64_t>(std::stoull(fields[8]));
    } catch (...) {
        return false;
    }
    return !route->namespace_id.empty() && !route->path_prefix.empty();
}

std::string EncodeCurrentRoute(const MasstreeNamespaceRoute& route) {
    return route.generation_id + "\t" + route.manifest_path + "\t" +
           std::to_string(route.root_inode_id) + "\t" +
           std::to_string(route.inode_min) + "\t" +
           std::to_string(route.inode_max) + "\t" +
           std::to_string(route.inode_count) + "\t" +
           std::to_string(route.dentry_count);
}

bool DecodeCurrentRoute(const std::string& normalized_prefix,
                        const std::string& payload,
                        MasstreeNamespaceRoute* route) {
    if (!route) {
        return false;
    }
    std::istringstream stream(payload);
    std::string part;
    std::vector<std::string> fields;
    while (std::getline(stream, part, '\t')) {
        fields.push_back(part);
    }
    if (fields.size() != 7 || fields[0].empty()) {
        return false;
    }
    route->path_prefix = normalized_prefix;
    route->generation_id = fields[0];
    route->manifest_path = fields[1];
    try {
        route->root_inode_id = static_cast<uint64_t>(std::stoull(fields[2]));
        route->inode_min = static_cast<uint64_t>(std::stoull(fields[3]));
        route->inode_max = static_cast<uint64_t>(std::stoull(fields[4]));
        route->inode_count = static_cast<uint64_t>(std::stoull(fields[5]));
        route->dentry_count = static_cast<uint64_t>(std::stoull(fields[6]));
    } catch (...) {
        return false;
    }
    return true;
}

bool ApplyCurrentRoute(RocksMetaStore* store,
                       MasstreeNamespaceRoute* route,
                       std::string* error) {
    if (!store || !route) {
        if (error) {
            *error = "masstree namespace current route args are invalid";
        }
        return false;
    }
    std::string payload;
    std::string local_error;
    if (!store->Get(MasstreeNamespaceCurrentKey(route->path_prefix), &payload, &local_error)) {
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

    MasstreeNamespaceRoute current;
    if (!DecodeCurrentRoute(route->path_prefix, payload, &current)) {
        if (error) {
            *error = "invalid masstree namespace current payload";
        }
        return false;
    }
    route->generation_id = current.generation_id;
    route->manifest_path = current.manifest_path;
    route->root_inode_id = current.root_inode_id;
    route->inode_min = current.inode_min;
    route->inode_max = current.inode_max;
    route->inode_count = current.inode_count;
    route->dentry_count = current.dentry_count;
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
            *error = "masstree namespace secondary index delete args are invalid";
        }
        return false;
    }
    std::unique_ptr<rocksdb::Iterator> it(store->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string reverse_prefix = MasstreeRouteBucketPrefix(normalized_prefix);
    for (it->Seek(reverse_prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(reverse_prefix, 0) != 0) {
            break;
        }
        const std::string bucket_suffix = key.substr(reverse_prefix.size());
        batch->Delete(key);
        batch->Delete("MTIB/" + bucket_suffix + normalized_prefix);
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

MasstreeNamespaceCatalog::MasstreeNamespaceCatalog(RocksMetaStore* store)
    : store_(store) {
}

bool MasstreeNamespaceCatalog::PutRoute(const MasstreeNamespaceRoute& route, std::string* error) {
    return PutRoute(route, std::vector<uint64_t>(), error);
}

bool MasstreeNamespaceCatalog::PutRoute(const MasstreeNamespaceRoute& route,
                                        const std::vector<uint64_t>& inode_bucket_ids,
                                        std::string* error) {
    if (!store_) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(route.path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid masstree namespace path prefix";
        }
        return false;
    }
    MasstreeNamespaceRoute normalized = route;
    normalized.path_prefix = normalized_prefix;

    rocksdb::WriteBatch batch;
    if (!DeleteRouteSecondaryIndexes(store_, normalized_prefix, &batch, error)) {
        return false;
    }
    const std::string payload = EncodeRoute(normalized);
    const std::string current_payload = EncodeCurrentRoute(normalized);
    batch.Put(MasstreeNamespaceRouteKey(normalized_prefix), payload);
    batch.Put(MasstreeNamespaceCurrentKey(normalized_prefix), current_payload);
    for (uint64_t bucket_id : inode_bucket_ids) {
        batch.Put(MasstreeInodeBucketKey(bucket_id, normalized_prefix), payload);
        batch.Put(MasstreeRouteBucketKey(normalized_prefix, bucket_id), "");
    }
    return store_->WriteBatch(&batch, error);
}

bool MasstreeNamespaceCatalog::LookupByPath(const std::string& path,
                                            MasstreeNamespaceRoute* route,
                                            std::string* error) const {
    if (route) {
        *route = MasstreeNamespaceRoute();
    }
    if (!store_) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_path;
    if (!NormalizePathPrefix(path, &normalized_path)) {
        if (error) {
            *error = "invalid masstree namespace lookup path";
        }
        return false;
    }
    for (const auto& prefix : BuildPrefixCandidates(normalized_path)) {
        std::string payload;
        std::string local_error;
        if (!store_->Get(MasstreeNamespaceRouteKey(prefix), &payload, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }
        MasstreeNamespaceRoute loaded;
        if (!DecodeRoute(payload, &loaded)) {
            if (error) {
                *error = "invalid masstree namespace route payload";
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

bool MasstreeNamespaceCatalog::LookupByInode(uint64_t inode_id,
                                             std::vector<MasstreeNamespaceRoute>* routes,
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
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }

    const uint64_t bucket_id = MasstreeInodeBucketId(inode_id);
    const std::string prefix = MasstreeInodeBucketPrefix(bucket_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        MasstreeNamespaceRoute route;
        if (!DecodeRoute(it->value().ToString(), &route)) {
            if (error) {
                *error = "invalid masstree namespace bucket payload";
            }
            return false;
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

bool MasstreeNamespaceCatalog::ListRoutes(std::vector<MasstreeNamespaceRoute>* routes, std::string* error) const {
    if (!routes) {
        if (error) {
            *error = "routes output is null";
        }
        return false;
    }
    routes->clear();
    if (!store_ || !store_->db()) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }

    const std::string prefix = MasstreeNamespaceRoutePrefix();
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        MasstreeNamespaceRoute route;
        if (!DecodeRoute(it->value().ToString(), &route)) {
            if (error) {
                *error = "invalid masstree namespace route payload";
            }
            return false;
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

bool MasstreeNamespaceCatalog::SetCurrentRoute(const MasstreeNamespaceRoute& route, std::string* error) {
    if (!store_) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(route.path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid masstree namespace current path prefix";
        }
        return false;
    }
    return store_->Put(MasstreeNamespaceCurrentKey(normalized_prefix), EncodeCurrentRoute(route), error);
}

bool MasstreeNamespaceCatalog::LookupCurrentRoute(const std::string& path_prefix,
                                                  MasstreeNamespaceRoute* route,
                                                  std::string* error) const {
    if (route) {
        *route = MasstreeNamespaceRoute();
    }
    if (!store_) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid masstree namespace current path prefix";
        }
        return false;
    }
    std::string payload;
    if (!store_->Get(MasstreeNamespaceCurrentKey(normalized_prefix), &payload, error)) {
        return false;
    }
    MasstreeNamespaceRoute current;
    if (!DecodeCurrentRoute(normalized_prefix, payload, &current)) {
        if (error) {
            *error = "invalid masstree namespace current payload";
        }
        return false;
    }
    std::string route_payload;
    std::string route_error;
    if (store_->Get(MasstreeNamespaceRouteKey(normalized_prefix), &route_payload, &route_error)) {
        MasstreeNamespaceRoute metadata;
        if (!DecodeRoute(route_payload, &metadata)) {
            if (error) {
                *error = "invalid masstree namespace route payload";
            }
            return false;
        }
        current.namespace_id = metadata.namespace_id;
    } else if (!route_error.empty()) {
        if (error) {
            *error = route_error;
        }
        return false;
    }
    if (route) {
        *route = std::move(current);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeNamespaceCatalog::DeleteRoute(const std::string& path_prefix, std::string* error) {
    if (!store_) {
        if (error) {
            *error = "masstree namespace catalog store is unavailable";
        }
        return false;
    }
    std::string normalized_prefix;
    if (!NormalizePathPrefix(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid masstree namespace delete path prefix";
        }
        return false;
    }

    rocksdb::WriteBatch batch;
    if (!DeleteRouteSecondaryIndexes(store_, normalized_prefix, &batch, error)) {
        return false;
    }
    batch.Delete(MasstreeNamespaceRouteKey(normalized_prefix));
    batch.Delete(MasstreeNamespaceCurrentKey(normalized_prefix));
    return store_->WriteBatch(&batch, error);
}

} // namespace zb::mds
