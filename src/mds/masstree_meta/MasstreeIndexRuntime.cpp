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

bool MasstreeIndexRuntime::PutInodeOffset(const std::string& namespace_id,
                                          uint64_t inode_id,
                                          uint64_t offset,
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
    inode_tree_->insert(MasstreeInodeIndexKey(namespace_id, inode_id), offset);
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)inode_id;
    (void)offset;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::GetInodeOffset(const std::string& namespace_id,
                                          uint64_t inode_id,
                                          uint64_t* offset,
                                          std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!inode_tree_ || !offset) {
        if (error) {
            *error = "masstree inode lookup args are invalid";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    if (!inode_tree_->search(MasstreeInodeIndexKey(namespace_id, inode_id), *offset)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)inode_id;
    (void)offset;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::PutDentryValue(const std::string& namespace_id,
                                          uint64_t parent_inode,
                                          const std::string& name,
                                          uint64_t child_inode,
                                          zb::rpc::InodeType type,
                                          std::string* error) {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!dentry_tree_) {
        if (error) {
            *error = "masstree dentry index runtime is not initialized";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    dentry_tree_->insert(MasstreeDentryIndexKey(namespace_id, parent_inode, name),
                         PackMasstreeDentryValue(child_inode, type));
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)parent_inode;
    (void)name;
    (void)child_inode;
    (void)type;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::GetDentryValue(const std::string& namespace_id,
                                          uint64_t parent_inode,
                                          const std::string& name,
                                          MasstreePackedDentryValue* value,
                                          std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!dentry_tree_ || !value) {
        if (error) {
            *error = "masstree dentry lookup args are invalid";
        }
        return false;
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }
    uint64_t packed = 0;
    if (!dentry_tree_->search(MasstreeDentryIndexKey(namespace_id, parent_inode, name), packed)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (!UnpackMasstreeDentryValue(packed, value)) {
        if (error) {
            *error = "invalid packed masstree dentry value";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)parent_inode;
    (void)name;
    (void)value;
    if (error) {
        *error = "masstree index support is unavailable in this build";
    }
    return false;
#endif
}

bool MasstreeIndexRuntime::ScanDentryValues(const std::string& namespace_id,
                                            uint64_t parent_inode,
                                            const std::string& start_after,
                                            uint32_t limit,
                                            std::vector<DentryScanEntry>* entries,
                                            bool* has_more,
                                            std::string* next_name,
                                            std::string* error) const {
#ifdef ZBSTORAGE_ENABLE_MASSTREE_INDEX
    if (!dentry_tree_ || !entries) {
        if (error) {
            *error = "masstree dentry scan args are invalid";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_name) {
        next_name->clear();
    }
    if (!EnsureThreadInitialized(error)) {
        return false;
    }

    constexpr size_t kScanBatchSize = 256;
    const std::string prefix = MasstreeDentryIndexPrefix(namespace_id, parent_inode);
    std::string seek_key = start_after.empty()
                               ? prefix
                               : MasstreeDentryIndexKey(namespace_id, parent_inode, start_after);
    std::string skip_until_key = start_after.empty() ? std::string() : seek_key;
    const size_t target = limit == 0 ? 0 : static_cast<size_t>(limit);

    std::vector<std::pair<std::string, uint64_t>> raw_hits;
    while (true) {
        raw_hits.clear();
        const size_t remaining = target == 0
                                     ? kScanBatchSize
                                     : std::max<size_t>(1, std::min(kScanBatchSize, target + 1 - entries->size()));
        dentry_tree_->scan(seek_key, static_cast<int>(remaining), raw_hits);
        if (raw_hits.empty()) {
            break;
        }

        bool saw_prefix_entry = false;
        std::string last_prefix_key;
        for (const auto& hit : raw_hits) {
            const std::string& full_key = hit.first;
            if (!skip_until_key.empty() && full_key <= skip_until_key) {
                continue;
            }
            if (full_key.rfind(prefix, 0) != 0) {
                if (error) {
                    error->clear();
                }
                return true;
            }
            MasstreePackedDentryValue decoded;
            if (!UnpackMasstreeDentryValue(hit.second, &decoded)) {
                if (error) {
                    *error = "invalid packed masstree dentry value";
                }
                return false;
            }

            saw_prefix_entry = true;
            last_prefix_key = full_key;
            const std::string name = full_key.substr(prefix.size());
            if (target != 0 && entries->size() >= target) {
                if (has_more) {
                    *has_more = true;
                }
                if (error) {
                    error->clear();
                }
                return true;
            }

            DentryScanEntry entry;
            entry.name = name;
            entry.value = decoded;
            entries->push_back(std::move(entry));
            if (next_name) {
                *next_name = name;
            }
        }

        if (!saw_prefix_entry) {
            break;
        }
        if (target != 0 && entries->size() > target) {
            entries->resize(target);
            if (has_more) {
                *has_more = true;
            }
            break;
        }
        seek_key = last_prefix_key;
        skip_until_key = last_prefix_key;
        if (raw_hits.size() < remaining) {
            break;
        }
    }

    if (error) {
        error->clear();
    }
    return true;
#else
    (void)namespace_id;
    (void)parent_inode;
    (void)start_after;
    (void)limit;
    (void)entries;
    (void)has_more;
    (void)next_name;
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
