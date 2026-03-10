#include "GcManager.h"

#include <chrono>
#include <deque>
#include <memory>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

std::string LayoutObjectIdFromKey(const std::string& key) {
    const std::string prefix = LayoutObjectPrefix();
    if (key.rfind(prefix, 0) != 0 || key.size() <= prefix.size()) {
        return "";
    }
    return key.substr(prefix.size());
}

} // namespace

GcManager::GcManager(RocksMetaStore* store, Options options) : store_(store), options_(std::move(options)) {
    if (options_.orphan_grace_ms == 0) {
        options_.orphan_grace_ms = 1;
    }
    if (options_.max_delete_per_round == 0) {
        options_.max_delete_per_round = 1;
    }
}

bool GcManager::RunOnce(std::string* error) {
    if (!store_ || !store_->db()) {
        if (error) {
            *error = "gc manager store is not initialized";
        }
        return false;
    }

    std::unordered_set<std::string> reachable;
    if (!CollectReachableLayoutObjects(&reachable, error)) {
        return false;
    }

    uint32_t deleted_count = 0;
    if (!SweepUnreachableLayoutObjects(reachable, &deleted_count, error)) {
        return false;
    }
    return true;
}

bool GcManager::CollectReachableLayoutObjects(std::unordered_set<std::string>* reachable, std::string* error) {
    if (!reachable) {
        if (error) {
            *error = "reachable set is null";
        }
        return false;
    }
    reachable->clear();

    std::deque<std::string> queue;
    const std::string root_prefix = LayoutRootPrefix();
    std::unique_ptr<rocksdb::Iterator> root_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (root_it->Seek(root_prefix); root_it->Valid(); root_it->Next()) {
        const std::string key = root_it->key().ToString();
        if (key.rfind(root_prefix, 0) != 0) {
            break;
        }
        LayoutRootRecord root;
        if (!MetaCodec::DecodeLayoutRoot(root_it->value().ToString(), &root)) {
            continue;
        }
        if (root.layout_root_id.empty()) {
            continue;
        }
        if (root.layout_root_id.rfind("legacy:", 0) == 0) {
            continue;
        }
        if (reachable->insert(root.layout_root_id).second) {
            queue.push_back(root.layout_root_id);
        }
    }

    while (!queue.empty()) {
        const std::string layout_obj_id = queue.front();
        queue.pop_front();
        std::string data;
        std::string local_error;
        if (!store_->Get(LayoutObjectKey(layout_obj_id), &data, &local_error)) {
            continue;
        }
        LayoutNodeRecord node;
        if (!MetaCodec::DecodeLayoutNode(data, &node)) {
            continue;
        }
        for (const auto& child : node.child_layout_ids) {
            if (child.empty()) {
                continue;
            }
            if (reachable->insert(child).second) {
                queue.push_back(child);
            }
        }
    }

    return true;
}

bool GcManager::SweepUnreachableLayoutObjects(const std::unordered_set<std::string>& reachable,
                                              uint32_t* deleted_count,
                                              std::string* error) {
    if (!store_ || !store_->db()) {
        if (error) {
            *error = "gc store not initialized";
        }
        return false;
    }

    rocksdb::WriteBatch batch;
    const uint64_t now_ms = NowMilliseconds();
    uint32_t deleted = 0;

    const std::string prefix = LayoutObjectPrefix();
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        const std::string layout_obj_id = LayoutObjectIdFromKey(key);
        if (layout_obj_id.empty()) {
            continue;
        }

        const std::string seen_key = LayoutGcSeenKey(layout_obj_id);
        if (reachable.find(layout_obj_id) != reachable.end()) {
            batch.Delete(seen_key);
            continue;
        }

        std::string seen_data;
        std::string local_error;
        if (!store_->Get(seen_key, &seen_data, &local_error)) {
            if (local_error.empty()) {
                batch.Put(seen_key, MetaCodec::EncodeUInt64(now_ms));
            }
            continue;
        }
        uint64_t first_seen_ms = 0;
        if (!MetaCodec::DecodeUInt64(seen_data, &first_seen_ms)) {
            batch.Put(seen_key, MetaCodec::EncodeUInt64(now_ms));
            continue;
        }
        if (now_ms < first_seen_ms || now_ms - first_seen_ms < options_.orphan_grace_ms) {
            continue;
        }
        batch.Delete(key);
        const std::string replica_prefix = LayoutObjectReplicaPrefix(layout_obj_id);
        std::unique_ptr<rocksdb::Iterator> replica_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
        for (replica_it->Seek(replica_prefix); replica_it->Valid(); replica_it->Next()) {
            const std::string replica_key = replica_it->key().ToString();
            if (replica_key.rfind(replica_prefix, 0) != 0) {
                break;
            }
            batch.Delete(replica_key);
        }
        batch.Delete(seen_key);
        ++deleted;
        if (deleted >= options_.max_delete_per_round) {
            break;
        }
    }

    if (deleted < options_.max_delete_per_round) {
        const std::string replica_prefix = LayoutObjectReplicaGlobalPrefix();
        std::unique_ptr<rocksdb::Iterator> replica_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
        for (replica_it->Seek(replica_prefix); replica_it->Valid(); replica_it->Next()) {
            if (deleted >= options_.max_delete_per_round) {
                break;
            }
            const std::string replica_key = replica_it->key().ToString();
            if (replica_key.rfind(replica_prefix, 0) != 0) {
                break;
            }

            std::string layout_obj_id;
            uint32_t replica_index = 0;
            if (!ParseLayoutObjectReplicaKey(replica_key, &layout_obj_id, &replica_index)) {
                continue;
            }
            (void)replica_index;
            if (layout_obj_id.empty() || reachable.find(layout_obj_id) != reachable.end()) {
                continue;
            }
            std::string exists_error;
            if (store_->Exists(LayoutObjectKey(layout_obj_id), &exists_error)) {
                continue;
            }

            const std::string seen_key = LayoutGcSeenKey(layout_obj_id);
            std::string seen_data;
            std::string local_error;
            if (!store_->Get(seen_key, &seen_data, &local_error)) {
                if (local_error.empty()) {
                    batch.Put(seen_key, MetaCodec::EncodeUInt64(now_ms));
                }
                continue;
            }
            uint64_t first_seen_ms = 0;
            if (!MetaCodec::DecodeUInt64(seen_data, &first_seen_ms)) {
                batch.Put(seen_key, MetaCodec::EncodeUInt64(now_ms));
                continue;
            }
            if (now_ms < first_seen_ms || now_ms - first_seen_ms < options_.orphan_grace_ms) {
                continue;
            }

            batch.Delete(replica_key);
            batch.Delete(seen_key);
            ++deleted;
        }
    }

    if (batch.Count() > 0) {
        std::string write_error;
        if (!store_->WriteBatch(&batch, &write_error)) {
            if (error) {
                *error = write_error.empty() ? "gc write batch failed" : write_error;
            }
            return false;
        }
    }
    if (deleted_count) {
        *deleted_count = deleted;
    }
    return true;
}

uint64_t GcManager::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::mds
