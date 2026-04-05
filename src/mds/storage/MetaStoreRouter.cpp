#include "MetaStoreRouter.h"

#include <memory>
#include <sstream>
#include <vector>

#include <rocksdb/iterator.h>

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

} // namespace

MetaStoreRouter::MetaStoreRouter(RocksMetaStore* hot_store,
                                 ArchiveNamespaceCatalog* archive_catalog,
                                 ArchiveMetaStore* archive_store,
                                 MasstreeNamespaceCatalog* masstree_catalog,
                                 MasstreeMetaStore* masstree_store)
    : hot_store_(hot_store),
      archive_catalog_(archive_catalog),
      archive_store_(archive_store),
      masstree_catalog_(masstree_catalog),
      masstree_store_(masstree_store) {
}

bool MetaStoreRouter::GetInode(uint64_t inode_id,
                               zb::rpc::InodeAttr* attr,
                               std::string* error) const {
    if (!hot_store_ || !attr || inode_id == 0) {
        if (error) {
            *error = "invalid inode lookup args";
        }
        return false;
    }
    std::string data;
    if (!hot_store_->Get(InodeKey(inode_id), &data, error)) {
        if (error && !error->empty()) {
            return false;
        }
        if (GetArchiveInode(inode_id, attr, error)) {
            return true;
        }
        if (error && !error->empty()) {
            return false;
        }
        return GetMasstreeInode(inode_id, attr, error);
    }
    std::string decode_error;
    if (!MetaCodec::DecodeInodeAttrCompat(data, attr, nullptr, &decode_error)) {
        if (error) {
            *error = decode_error.empty() ? "invalid inode data" : decode_error;
        }
        return false;
    }
    return true;
}

bool MetaStoreRouter::GetArchiveInode(uint64_t inode_id,
                                      zb::rpc::InodeAttr* attr,
                                      std::string* error) const {
    if (!archive_catalog_ || !archive_store_) {
        if (error) {
            error->clear();
        }
        return false;
    }
    std::vector<ArchiveNamespaceRoute> routes;
    if (!archive_catalog_->LookupByInode(inode_id, &routes, error)) {
        return false;
    }
    if (routes.empty() && !archive_catalog_->ListRoutes(&routes, error)) {
        return false;
    }
    for (const auto& route : routes) {
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        zb::rpc::InodeAttr candidate;
        std::string local_error;
        if (archive_store_->GetInode(route, inode_id, &candidate, &local_error)) {
            *attr = std::move(candidate);
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MetaStoreRouter::GetMasstreeInode(uint64_t inode_id,
                                       zb::rpc::InodeAttr* attr,
                                       std::string* error) const {
    if (!masstree_catalog_ || !masstree_store_) {
        if (error) {
            error->clear();
        }
        return false;
    }
    std::vector<MasstreeNamespaceRoute> routes;
    if (!masstree_catalog_->LookupByInode(inode_id, &routes, error)) {
        return false;
    }
    if (routes.empty() && !masstree_catalog_->ListRoutes(&routes, error)) {
        return false;
    }
    for (const auto& route : routes) {
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        zb::rpc::InodeAttr candidate;
        std::string local_error;
        if (masstree_store_->GetInode(route, inode_id, &candidate, &local_error)) {
            *attr = std::move(candidate);
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MetaStoreRouter::GetDentry(uint64_t parent_inode,
                                const std::string& name,
                                uint64_t* inode_id,
                                std::string* error) const {
    if (!hot_store_ || !inode_id || parent_inode == 0 || name.empty()) {
        if (error) {
            *error = "invalid dentry lookup args";
        }
        return false;
    }
    std::string data;
    if (!hot_store_->Get(DentryKey(parent_inode, name), &data, error)) {
        return false;
    }
    if (!MetaCodec::DecodeUInt64(data, inode_id)) {
        if (error) {
            *error = "invalid dentry";
        }
        return false;
    }
    return true;
}

bool MetaStoreRouter::ResolvePath(const std::string& path,
                                  uint64_t* inode_id,
                                  zb::rpc::InodeAttr* attr,
                                  std::string* error) const {
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }

    ArchiveNamespaceRoute route;
    std::string route_error;
    if (archive_catalog_ && archive_store_ &&
        archive_catalog_->LookupByPath(path, &route, &route_error)) {
        return archive_store_->ResolvePath(route, path, inode_id, attr, error);
    }
    if (!route_error.empty() && error) {
        *error = route_error;
        return false;
    }

    MasstreeNamespaceRoute masstree_route;
    route_error.clear();
    if (masstree_catalog_ && masstree_store_ &&
        masstree_catalog_->LookupByPath(path, &masstree_route, &route_error)) {
        return masstree_store_->ResolvePath(masstree_route, path, inode_id, attr, error);
    }
    if (!route_error.empty() && error) {
        *error = route_error;
        return false;
    }

    std::vector<std::string> parts = SplitPath(path);
    uint64_t current = kRootInodeId;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return GetInode(current, attr, error);
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    for (const auto& name : parts) {
        uint64_t next = 0;
        if (!GetDentry(current, name, &next, error)) {
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return GetInode(current, attr, error);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MetaStoreRouter::Readdir(const std::string& path,
                              const std::string& start_after,
                              uint32_t limit,
                              std::vector<zb::rpc::Dentry>* entries,
                              bool* has_more,
                              std::string* next_token,
                              std::string* error) const {
    if (!entries) {
        if (error) {
            *error = "entries output is null";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    ArchiveNamespaceRoute route;
    std::string route_error;
    if (archive_catalog_ && archive_store_ &&
        archive_catalog_->LookupByPath(path, &route, &route_error)) {
        return archive_store_->Readdir(route, path, start_after, limit, entries, has_more, next_token, error);
    }
    if (!route_error.empty()) {
        if (error) {
            *error = route_error;
        }
        return false;
    }

    MasstreeNamespaceRoute masstree_route;
    route_error.clear();
    if (masstree_catalog_ && masstree_store_ &&
        masstree_catalog_->LookupByPath(path, &masstree_route, &route_error)) {
        return masstree_store_->Readdir(
            masstree_route, path, start_after, limit, entries, has_more, next_token, error);
    }
    if (!route_error.empty()) {
        if (error) {
            *error = route_error;
        }
        return false;
    }

    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!ResolvePath(path, &inode_id, &attr, error)) {
        return false;
    }
    if (attr.type() != zb::rpc::INODE_DIR) {
        if (error) {
            *error = "not a directory";
        }
        return false;
    }
    return ReaddirHot(inode_id, start_after, limit, entries, has_more, next_token, error);
}

bool MetaStoreRouter::ReaddirHot(uint64_t parent_inode,
                                 const std::string& start_after,
                                 uint32_t limit,
                                 std::vector<zb::rpc::Dentry>* entries,
                                 bool* has_more,
                                 std::string* next_token,
                                 std::string* error) const {
    if (!hot_store_ || !entries || !hot_store_->db() || parent_inode == 0) {
        if (error) {
            *error = "invalid readdir args";
        }
        return false;
    }
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    const std::string prefix = DentryPrefix(parent_inode);
    const std::string seek_key = prefix + start_after;
    std::unique_ptr<rocksdb::Iterator> it(hot_store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(start_after.empty() ? prefix : seek_key); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        const std::string name = it->key().ToString().substr(prefix.size());
        if (!start_after.empty() && name <= start_after) {
            continue;
        }
        uint64_t child_inode = 0;
        if (!MetaCodec::DecodeUInt64(it->value().ToString(), &child_inode)) {
            continue;
        }
        zb::rpc::InodeAttr child_attr;
        std::string inode_error;
        if (!GetInode(child_inode, &child_attr, &inode_error)) {
            continue;
        }
        zb::rpc::Dentry entry;
        entry.set_name(name);
        entry.set_inode_id(child_inode);
        entry.set_type(child_attr.type());
        entries->push_back(std::move(entry));
        if (next_token) {
            *next_token = name;
        }
        if (limit > 0 && entries->size() >= limit) {
            for (it->Next(); it->Valid(); it->Next()) {
                if (!it->key().starts_with(prefix)) {
                    break;
                }
                const std::string next_name = it->key().ToString().substr(prefix.size());
                if (next_name <= name) {
                    continue;
                }
                if (has_more) {
                    *has_more = true;
                }
                break;
            }
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
