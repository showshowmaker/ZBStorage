#include "MasstreeIndexRuntime.h"

#include <algorithm>
#include <functional>
#include <thread>
#include <vector>

#include "../storage/MetaSchema.h"

#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
#include "masstree_wrapper.h"
#endif

namespace zb::mds {

namespace {

bool ParseDentryBoundaryKey(const std::string& key,
                           const std::string& namespace_id,
                           uint64_t* max_parent_inode,
                           std::string* max_name) {
    if (!max_parent_inode || !max_name) {
        return false;
    }
    const std::string namespace_prefix = "MTD/" + namespace_id + "/";
    if (key.rfind(namespace_prefix, 0) != 0) {
        return false;
    }
    const size_t parent_begin = namespace_prefix.size();
    const size_t parent_end = key.find('/', parent_begin);
    if (parent_end == std::string::npos || parent_end == parent_begin) {
        return false;
    }
    try {
        *max_parent_inode = std::stoull(key.substr(parent_begin, parent_end - parent_begin));
    } catch (...) {
        return false;
    }
    *max_name = key.substr(parent_end + 1);
    return true;
}

} // namespace

MasstreeIndexRuntime::MasstreeIndexRuntime() = default;

MasstreeIndexRuntime::~MasstreeIndexRuntime() = default;

bool MasstreeIndexRuntime::Init(std::string* error) {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    inode_tree_ = std::make_unique<MasstreeWrapper>();
    dentry_tree_ = std::make_unique<MasstreeWrapper>();
    if (error) {
        error->clear();
    }
    return true;
#else
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::PutInodePageBoundary(const std::string& namespace_id,
                                                uint64_t max_inode_id,
                                                uint64_t page_offset,
                                                std::string* error) {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!inode_tree_) {
        if (error) {
            *error = "masstree inode index runtime is not initialized";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    inode_tree_->insert(MasstreeInodeIndexKey(namespace_id, max_inode_id), page_offset);
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)max_inode_id;
    (void)page_offset;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::FindInodePageBoundary(const std::string& namespace_id,
                                                 uint64_t inode_id,
                                                 MasstreeInodeSparseEntry* entry,
                                                 std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!inode_tree_ || !entry) {
        if (error) {
            *error = "masstree inode lookup args are invalid";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    const std::string seek_key = MasstreeInodeIndexKey(namespace_id, inode_id);
    std::vector<std::pair<std::string, uint64_t>> hits;
    inode_tree_->scan(seek_key, 1, hits);
    if (hits.empty()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const std::string prefix = MasstreeInodeIndexPrefix(namespace_id);
    if (hits.front().first.rfind(prefix, 0) != 0) {
        if (error) {
            error->clear();
        }
        return false;
    }
    entry->page_offset = hits.front().second;
    entry->max_inode_id = std::stoull(hits.front().first.substr(prefix.size()));
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)inode_id;
    (void)entry;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::PutDentryPageBoundary(const std::string& namespace_id,
                                                 uint64_t parent_inode,
                                                 const std::string& max_name,
                                                 uint64_t page_offset,
                                                 std::string* error) {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!dentry_tree_) {
        if (error) {
            *error = "masstree dentry sparse runtime is not initialized";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    dentry_tree_->insert(MasstreeDentryIndexKey(namespace_id, parent_inode, max_name), page_offset);
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)parent_inode;
    (void)max_name;
    (void)page_offset;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::FindDentryPageBoundary(const std::string& namespace_id,
                                                  uint64_t parent_inode,
                                                  const std::string& name,
                                                  MasstreeDentrySparseEntry* entry,
                                                  std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!dentry_tree_ || !entry) {
        if (error) {
            *error = "masstree dentry sparse lookup args are invalid";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    const std::string seek_key = MasstreeDentryIndexKey(namespace_id, parent_inode, name);
    std::vector<std::pair<std::string, uint64_t>> hits;
    dentry_tree_->scan(seek_key, 1, hits);
    if (hits.empty()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const auto& hit = hits.front();
    if (!ParseDentryBoundaryKey(hit.first,
                                namespace_id,
                                &entry->max_parent_inode,
                                &entry->max_name)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    entry->page_offset = hit.second;
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)parent_inode;
    (void)name;
    (void)entry;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::EnsureThreadInitialized(std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (MasstreeWrapper::ti == nullptr) {
        const uint64_t tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
        MasstreeWrapper::thread_init(static_cast<int>(tid & 0x7fffffff));
    }
    if (error) {
        error->clear();
    }
    return true;
#else
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

} // namespace zb::mds
