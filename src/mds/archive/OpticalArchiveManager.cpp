#include "OpticalArchiveManager.h"

#include <brpc/controller.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "mds.pb.h"
#include "real_node.pb.h"

namespace zb::mds {

namespace {

uint64_t NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

bool IsDiskReplica(const zb::rpc::ReplicaLocation& replica) {
    return replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK;
}

bool IsReadyOpticalReplica(const zb::rpc::ReplicaLocation& replica) {
    return replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
           replica.replica_state() == zb::rpc::REPLICA_READY;
}

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

std::string ArchiveObjectId(const zb::rpc::ArchiveLeaseRecord& record) {
    return record.object_id();
}

void EnsureReplicaObjectId(zb::rpc::ReplicaLocation* replica, const std::string& object_id) {
    if (!replica || object_id.empty()) {
        return;
    }
    replica->set_object_id(object_id);
}

void SetArchiveObjectId(zb::rpc::ClaimArchiveTaskRequest* request, const std::string& object_id) {
    if (!request || object_id.empty()) {
        return;
    }
    request->set_object_id(object_id);
}

void SetArchiveObjectId(zb::rpc::RenewArchiveLeaseRequest* request, const std::string& object_id) {
    if (!request || object_id.empty()) {
        return;
    }
    request->set_object_id(object_id);
}

void SetArchiveObjectId(zb::rpc::CommitArchiveTaskRequest* request, const std::string& object_id) {
    if (!request || object_id.empty()) {
        return;
    }
    request->set_object_id(object_id);
}

bool MatchesCandidateNodeId(const std::string& candidate_node_id, const std::string& replica_node_id) {
    if (candidate_node_id.empty()) {
        return true;
    }
    if (replica_node_id == candidate_node_id) {
        return true;
    }
    const std::string prefix = candidate_node_id + "-v";
    return replica_node_id.size() > prefix.size() && replica_node_id.rfind(prefix, 0) == 0;
}

bool ParseStableObjectId(const std::string& object_id, uint64_t* inode_id, uint32_t* object_index) {
    if (!inode_id || !object_index) {
        return false;
    }
    // Format: obj-<inode_id>-<object_index>
    constexpr char kPrefix[] = "obj-";
    if (object_id.rfind(kPrefix, 0) != 0) {
        return false;
    }
    const size_t second_dash = object_id.find('-', sizeof(kPrefix) - 1);
    if (second_dash == std::string::npos || second_dash + 1 >= object_id.size()) {
        return false;
    }
    const std::string inode_part = object_id.substr(sizeof(kPrefix) - 1, second_dash - (sizeof(kPrefix) - 1));
    const std::string index_part = object_id.substr(second_dash + 1);
    if (inode_part.empty() || index_part.empty()) {
        return false;
    }
    char* inode_end = nullptr;
    char* index_end = nullptr;
    const unsigned long long inode_value = std::strtoull(inode_part.c_str(), &inode_end, 10);
    const unsigned long long index_value = std::strtoull(index_part.c_str(), &index_end, 10);
    if (!inode_end || *inode_end != '\0' || !index_end || *index_end != '\0') {
        return false;
    }
    if (inode_value == 0 || index_value > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *inode_id = static_cast<uint64_t>(inode_value);
    *object_index = static_cast<uint32_t>(index_value);
    return true;
}

} // namespace

OpticalArchiveManager::OpticalArchiveManager(RocksMetaStore* store,
                                             NodeStateCache* cache,
                                             ArchiveCandidateQueue* candidate_queue,
                                             ArchiveBatchStager* batch_stager,
                                             ArchiveLeaseManager* lease_manager,
                                             Options options)
    : store_(store),
      cache_(cache),
      candidate_queue_(candidate_queue),
      batch_stager_(batch_stager),
      lease_manager_(lease_manager),
      options_(options) {
    if (options_.archive_target_bytes > options_.archive_trigger_bytes) {
        options_.archive_target_bytes = options_.archive_trigger_bytes;
    }
    if (options_.max_objects_per_round == 0) {
        options_.max_objects_per_round = 1;
    }
}

bool OpticalArchiveManager::RunOnce(std::string* error) {
    if (!store_ || !cache_) {
        if (error) {
            *error = "archive manager not initialized";
        }
        return false;
    }

    std::vector<NodeInfo> nodes = cache_->Snapshot();
    bool do_archive = ShouldArchiveNow(nodes);
    std::vector<NodeSelection> optical_nodes = cache_->PickNodesByType(1, NodeType::kOptical);

    if (!do_archive && options_.cold_file_ttl_sec == 0) {
        return true;
    }

    uint32_t archived_count = 0;
    uint64_t now = NowSeconds();

    rocksdb::WriteBatch batch;
    std::unordered_set<std::string> touched_object_keys;
    if (do_archive && !optical_nodes.empty() && candidate_queue_) {
        const NodeSelection& optical = optical_nodes.front();
        if (batch_stager_) {
            struct FileObjectTask {
                ArchiveCandidateEntry candidate;
                std::string object_key;
                zb::rpc::ReplicaLocation source_disk;
                uint64_t object_size{0};
                uint32_t object_index{0};
            };
            auto collect_file_object_tasks =
                [&](const ArchiveCandidateEntry& seed,
                    uint64_t* inode_id,
                    std::vector<FileObjectTask>* out,
                    uint64_t* total_size,
                    std::string* local_error) -> bool {
                if (!inode_id || !out || !total_size) {
                    if (local_error) {
                        *local_error = "invalid file task output buffers";
                    }
                    return false;
                }
                out->clear();
                *inode_id = 0;
                *total_size = 0;

                uint32_t seed_index = 0;
                if (!ResolveObjectOwner(seed.ArchiveObjectId(), inode_id, &seed_index, local_error)) {
                    return false;
                }
                (void)seed_index;

                const std::string prefix = ObjectPrefix(*inode_id);
                std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
                for (it->Seek(prefix); it->Valid(); it->Next()) {
                    const std::string object_key = it->key().ToString();
                    if (object_key.rfind(prefix, 0) != 0) {
                        break;
                    }

                    zb::rpc::ObjectMeta meta;
                    if (!MetaCodec::DecodeObjectMeta(it->value().ToString(), &meta)) {
                        continue;
                    }
                    uint32_t object_index = meta.index();
                    if (object_index == 0) {
                        uint64_t parsed_inode = 0;
                        uint32_t parsed_index = 0;
                        if (ParseObjectKey(object_key, &parsed_inode, &parsed_index) && parsed_inode == *inode_id) {
                            object_index = parsed_index;
                        }
                    }
                    bool has_optical_ready = false;
                    const zb::rpc::ReplicaLocation* selected_disk = nullptr;
                    const zb::rpc::ReplicaLocation* preferred_disk = nullptr;
                    for (const auto& replica : meta.replicas()) {
                        if (IsReadyOpticalReplica(replica)) {
                            has_optical_ready = true;
                        }
                        if (!IsDiskReplica(replica) || ReplicaObjectId(replica).empty()) {
                            continue;
                        }
                        if (!selected_disk) {
                            selected_disk = &replica;
                        }
                        if ((!seed.node_id.empty() && MatchesCandidateNodeId(seed.node_id, replica.node_id())) ||
                            (!seed.disk_id.empty() && replica.disk_id() == seed.disk_id)) {
                            preferred_disk = &replica;
                        }
                    }
                    if (has_optical_ready) {
                        continue;
                    }
                    if (preferred_disk) {
                        selected_disk = preferred_disk;
                    }
                    if (!selected_disk) {
                        continue;
                    }

                    FileObjectTask task;
                    task.object_key = object_key;
                    task.object_index = object_index;
                    task.source_disk = *selected_disk;
                    task.object_size = selected_disk->size() > 0
                                          ? selected_disk->size()
                                          : (seed.size_bytes > 0 ? seed.size_bytes : options_.default_object_unit_size);
                    task.candidate = seed;
                    task.candidate.node_id = selected_disk->node_id();
                    task.candidate.node_address = selected_disk->node_address();
                    task.candidate.disk_id = selected_disk->disk_id();
                    task.candidate.SetArchiveObjectId(ReplicaObjectId(*selected_disk));
                    task.candidate.size_bytes = task.object_size;
                    out->push_back(std::move(task));
                    *total_size += task.object_size;
                }

                std::sort(out->begin(), out->end(), [](const FileObjectTask& a, const FileObjectTask& b) {
                    return a.object_index < b.object_index;
                });
                return true;
            };
            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_object_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }

            uint32_t staged_count = 0;
            std::unordered_set<uint64_t> seen_inodes;
            while (staged_count < options_.max_objects_per_round && !batch_stager_->HasSealedBatch()) {
                ArchiveCandidateEntry seed_candidate;
                if (!candidate_queue_->PopTop(&seed_candidate)) {
                    break;
                }

                uint64_t inode_id = 0;
                uint64_t file_total_size = 0;
                std::vector<FileObjectTask> file_tasks;
                std::string local_error;
                if (!collect_file_object_tasks(seed_candidate, &inode_id, &file_tasks, &file_total_size, &local_error)) {
                    if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }
                if (inode_id != 0 && !seen_inodes.insert(inode_id).second) {
                    continue;
                }
                if (file_tasks.empty()) {
                    continue;
                }

                const uint32_t remaining_budget = options_.max_objects_per_round - staged_count;
                if (file_tasks.size() > static_cast<size_t>(remaining_budget)) {
                    std::vector<ArchiveCandidateEntry> deferred_batch;
                    deferred_batch.push_back(seed_candidate);
                    (void)candidate_queue_->PushBatch(deferred_batch);
                    continue;
                }

                const uint64_t disc_size = batch_stager_->DiscSizeBytes();
                const uint64_t current_bytes = batch_stager_->CurrentBytes();
                if (file_total_size > disc_size) {
                    if (error && error->empty()) {
                        *error = "file size exceeds configured archive disc size";
                    }
                    continue;
                }
                if (current_bytes > 0 && current_bytes + file_total_size > disc_size) {
                    std::vector<ArchiveCandidateEntry> deferred_batch;
                    deferred_batch.push_back(seed_candidate);
                    (void)candidate_queue_->PushBatch(deferred_batch);
                    continue;
                }

                struct LeasedObject {
                    ArchiveCandidateEntry candidate;
                    std::string lease_id;
                    std::string op_id;
                };
                std::vector<LeasedObject> leased_objects;
                std::vector<std::pair<ArchiveCandidateEntry, uint64_t>> archiving_updates;
                std::vector<std::string> staged_object_ids;
                bool file_failed = false;
                bool file_deferred = false;

                for (const auto& task : file_tasks) {
                    if (batch_stager_->ContainsObject(task.candidate.ArchiveObjectId())) {
                        continue;
                    }

                    bool granted = true;
                    std::string lease_id;
                    std::string op_id;
                    uint64_t stage_version = task.candidate.version > 0 ? task.candidate.version : 1;
                    zb::rpc::ClaimArchiveTaskReply claim_reply;
                    if (lease_manager_) {
                        zb::rpc::ClaimArchiveTaskRequest claim_req;
                        claim_req.set_node_id(task.candidate.node_id);
                        claim_req.set_node_address(task.candidate.node_address);
                        claim_req.set_disk_id(task.candidate.disk_id);
                        SetArchiveObjectId(&claim_req, task.candidate.ArchiveObjectId());
                        claim_req.set_requested_lease_ms(0);
                        claim_req.set_request_id(task.candidate.ArchiveObjectId() + ":" + batch_stager_->CurrentBatchId());

                        std::string claim_error;
                        if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                            granted = false;
                        } else {
                            lease_id = claim_reply.record().lease_id();
                            op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id()
                                                                              : task.candidate.ArchiveObjectId();
                            stage_version =
                                claim_reply.record().version() > 0 ? claim_reply.record().version() : stage_version;
                        }
                    }
                    if (!granted) {
                        bool has_optical_ready = false;
                        std::string ready_error;
                        if (HasReadyOpticalReplica(task.candidate.ArchiveObjectId(), &has_optical_ready, &ready_error) &&
                            has_optical_ready) {
                            std::string update_error;
                            (void)UpdateSourceArchiveState(task.candidate,
                                                           "archived",
                                                           claim_reply.record().version(),
                                                           &update_error);
                            continue;
                        }
                        file_failed = true;
                        break;
                    }

                    std::string data;
                    if (!ReadObjectFromReplica(task.source_disk, task.object_size, &data, &local_error)) {
                        if (lease_manager_ && !lease_id.empty()) {
                            zb::rpc::CommitArchiveTaskRequest commit_req;
                            commit_req.set_node_id(task.candidate.node_id);
                            SetArchiveObjectId(&commit_req, task.candidate.ArchiveObjectId());
                            commit_req.set_lease_id(lease_id);
                            commit_req.set_op_id(op_id);
                            commit_req.set_success(false);
                            commit_req.set_error_message(local_error.empty() ? "failed to read source object" : local_error);
                            zb::rpc::CommitArchiveTaskReply commit_reply;
                            std::string commit_error;
                            (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                        }
                        file_failed = true;
                        break;
                    }

                    bool inserted = false;
                    bool deferred = false;
                    if (!batch_stager_->StageObject(task.candidate,
                                                   lease_id,
                                                   op_id,
                                                   task.object_key,
                                                   stage_version,
                                                   data,
                                                   &inserted,
                                                   &deferred,
                                                   &local_error)) {
                        if (lease_manager_ && !lease_id.empty()) {
                            zb::rpc::CommitArchiveTaskRequest commit_req;
                            commit_req.set_node_id(task.candidate.node_id);
                            SetArchiveObjectId(&commit_req, task.candidate.ArchiveObjectId());
                            commit_req.set_lease_id(lease_id);
                            commit_req.set_op_id(op_id);
                            commit_req.set_success(false);
                            commit_req.set_error_message(local_error.empty() ? "failed to stage object" : local_error);
                            zb::rpc::CommitArchiveTaskReply commit_reply;
                            std::string commit_error;
                            (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                        }
                        file_failed = true;
                        break;
                    }

                    LeasedObject leased;
                    leased.candidate = task.candidate;
                    leased.lease_id = lease_id;
                    leased.op_id = op_id;
                    leased_objects.push_back(std::move(leased));

                    if (inserted) {
                        staged_object_ids.push_back(task.candidate.ArchiveObjectId());
                        archiving_updates.emplace_back(task.candidate, stage_version);
                        continue;
                    }
                    if (deferred) {
                        file_deferred = true;
                        file_failed = true;
                        break;
                    }
                    if (lease_manager_ && !lease_id.empty()) {
                        (void)batch_stager_->UpdateObjectLease(task.candidate.ArchiveObjectId(),
                                                               lease_id,
                                                               op_id,
                                                               stage_version,
                                                               nullptr);
                    }
                }

                if (file_failed) {
                    for (const auto& object_id : staged_object_ids) {
                        (void)batch_stager_->RemoveObject(object_id, nullptr);
                    }
                    for (const auto& leased : leased_objects) {
                        if (!lease_manager_ || leased.lease_id.empty()) {
                            continue;
                        }
                        zb::rpc::CommitArchiveTaskRequest commit_req;
                        commit_req.set_node_id(leased.candidate.node_id);
                        SetArchiveObjectId(&commit_req, leased.candidate.ArchiveObjectId());
                        commit_req.set_lease_id(leased.lease_id);
                        commit_req.set_op_id(leased.op_id);
                        commit_req.set_success(false);
                        commit_req.set_error_message(file_deferred ? "deferred to next archive batch"
                                                                   : "file-level archive staging failed");
                        zb::rpc::CommitArchiveTaskReply commit_reply;
                        std::string commit_error;
                        (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    }
                    if (file_deferred) {
                        std::vector<ArchiveCandidateEntry> deferred_batch;
                        deferred_batch.push_back(seed_candidate);
                        (void)candidate_queue_->PushBatch(deferred_batch);
                    } else if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }

                for (const auto& update : archiving_updates) {
                    std::string update_error;
                    (void)UpdateSourceArchiveState(update.first, "archiving", update.second, &update_error);
                }
                staged_count += static_cast<uint32_t>(staged_object_ids.size());
                std::string seal_error;
                (void)batch_stager_->SealIfReady(&seal_error);
                if (error && error->empty() && !seal_error.empty()) {
                    *error = seal_error;
                }
            }

            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_object_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }
        } else {
            while (archived_count < options_.max_objects_per_round) {
                ArchiveCandidateEntry candidate;
                if (!candidate_queue_->PopTop(&candidate)) {
                    break;
                }

                bool granted = true;
                std::string lease_id;
                std::string op_id;
                zb::rpc::ClaimArchiveTaskReply claim_reply;
                if (lease_manager_) {
                    zb::rpc::ClaimArchiveTaskRequest claim_req;
                    claim_req.set_node_id(candidate.node_id);
                    claim_req.set_node_address(candidate.node_address);
                    claim_req.set_disk_id(candidate.disk_id);
                    SetArchiveObjectId(&claim_req, candidate.ArchiveObjectId());
                    claim_req.set_requested_lease_ms(0);
                    claim_req.set_request_id(candidate.ArchiveObjectId());

                    std::string claim_error;
                    if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                        granted = false;
                    } else {
                        lease_id = claim_reply.record().lease_id();
                        op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id() : candidate.ArchiveObjectId();
                    }
                }
                if (!granted) {
                    bool has_optical_ready = false;
                    std::string ready_error;
                    if (HasReadyOpticalReplica(candidate.ArchiveObjectId(), &has_optical_ready, &ready_error) &&
                        has_optical_ready) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(candidate, "archived", claim_reply.record().version(), &update_error);
                    }
                    continue;
                }

                std::string local_error;
                const ArchiveByCandidateResult result =
                    ArchiveByCandidate(candidate, optical, lease_id, op_id, &touched_object_keys, &local_error);
                if (result == ArchiveByCandidateResult::kArchived) {
                    ++archived_count;
                }

                if (lease_manager_ && !lease_id.empty()) {
                    zb::rpc::CommitArchiveTaskRequest commit_req;
                    commit_req.set_node_id(candidate.node_id);
                    SetArchiveObjectId(&commit_req, candidate.ArchiveObjectId());
                    commit_req.set_lease_id(lease_id);
                    commit_req.set_op_id(op_id);
                    commit_req.set_success(result == ArchiveByCandidateResult::kArchived);
                    if (result != ArchiveByCandidateResult::kArchived) {
                        commit_req.set_error_message(local_error.empty() ? "archive candidate skipped/failed" : local_error);
                    }

                    zb::rpc::CommitArchiveTaskReply commit_reply;
                    std::string commit_error;
                    (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    if (result == ArchiveByCandidateResult::kArchived) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(candidate, "archived", commit_reply.record().version(), &update_error);
                    }
                }
            }
        }
    }

    if (options_.cold_file_ttl_sec == 0) {
        const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_objects_per_round * 4);
        std::string reconcile_error;
        (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
        if (error && error->empty() && !reconcile_error.empty()) {
            *error = reconcile_error;
        }
        return true;
    }

    const std::string object_prefix = ObjectGlobalPrefix();
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(object_prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(object_prefix, 0) != 0) {
            break;
        }
        if (touched_object_keys.find(key) != touched_object_keys.end()) {
            continue;
        }

        zb::rpc::ObjectMeta meta;
        if (!MetaCodec::DecodeObjectMeta(it->value().ToString(), &meta)) {
            continue;
        }

        std::string local_error;
        uint64_t inode_id = 0;
        uint32_t object_index = 0;
        bool owner_resolved = false;
        for (const auto& replica : meta.replicas()) {
            const std::string object_id = ReplicaObjectId(replica);
            if (object_id.empty()) {
                continue;
            }
            if (ResolveObjectOwner(object_id, &inode_id, &object_index, &local_error)) {
                owner_resolved = true;
            }
            break;
        }
        if (!owner_resolved && !ParseObjectKey(key, &inode_id, &object_index)) {
            continue;
        }
        (void)object_index;

        zb::rpc::InodeAttr inode;
        if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
            continue;
        }

        bool has_disk = false;
        bool has_optical_ready = false;
        const zb::rpc::ReplicaLocation* source_disk = nullptr;
        for (const auto& replica : meta.replicas()) {
            if (IsDiskReplica(replica)) {
                has_disk = true;
                if (!source_disk) {
                    source_disk = &replica;
                }
            }
            if (IsReadyOpticalReplica(replica)) {
                has_optical_ready = true;
            }
        }

        bool changed = false;

        // Stage-B policy: disable online full-table archive fallback.
        // Archiving must be driven by candidate queue + reverse index.
        const bool allow_scan_archive = false;
        if (allow_scan_archive && has_disk && !has_optical_ready && source_disk && !optical_nodes.empty() &&
            archived_count < options_.max_objects_per_round) {
            std::string data;
            uint64_t object_unit_size = inode.object_unit_size() ? inode.object_unit_size() : options_.default_object_unit_size;
            if (ReadObjectFromReplica(*source_disk, object_unit_size, &data, &local_error)) {
                const NodeSelection& optical = optical_nodes.front();
                zb::rpc::ReplicaLocation optical_location;
                const std::string source_object_id = ReplicaObjectId(*source_disk);
                if (WriteObjectToOptical(optical,
                                        source_object_id,
                                        source_object_id,
                                        data,
                                        inode_id,
                                        object_index,
                                        &inode,
                                        &optical_location,
                                        &local_error)) {
                    zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
                    new_replica->set_node_id(optical.node_id);
                    new_replica->set_node_address(optical.address);
                    new_replica->set_disk_id(optical.disk_id);
                    const std::string object_id = ReplicaObjectId(*source_disk);
                    EnsureReplicaObjectId(new_replica, object_id);
                    new_replica->set_size(static_cast<uint64_t>(data.size()));
                    new_replica->set_group_id(optical.group_id);
                    new_replica->set_epoch(optical.epoch);
                    new_replica->set_primary_node_id(optical.node_id);
                    new_replica->set_primary_address(optical.address);
                    new_replica->set_secondary_node_id(optical.secondary_node_id);
                    new_replica->set_secondary_address(optical.secondary_address);
                    new_replica->set_sync_ready(optical.sync_ready);
                    new_replica->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
                    new_replica->set_replica_state(zb::rpc::REPLICA_READY);
                    if (!optical_location.image_id().empty()) {
                        new_replica->set_image_id(optical_location.image_id());
                        new_replica->set_image_offset(optical_location.image_offset());
                        new_replica->set_image_length(optical_location.image_length());
                    }
                    changed = true;
                    has_optical_ready = true;
                    ++archived_count;
                }
            }
        }

        bool cold = options_.cold_file_ttl_sec > 0 &&
                    inode.atime() > 0 &&
                    inode.atime() + options_.cold_file_ttl_sec <= now;
        if (cold && has_optical_ready && has_disk) {
            std::vector<zb::rpc::ReplicaLocation> kept;
            kept.reserve(static_cast<size_t>(meta.replicas_size()));
            for (const auto& replica : meta.replicas()) {
                if (!IsDiskReplica(replica)) {
                    kept.push_back(replica);
                    continue;
                }
                if (!DeleteDiskReplica(replica, &local_error)) {
                    kept.push_back(replica);
                } else {
                    changed = true;
                }
            }
            if (changed) {
                meta.clear_replicas();
                for (const auto& item : kept) {
                    *meta.add_replicas() = item;
                }
            }
        }

        if (changed) {
            batch.Put(key, MetaCodec::EncodeObjectMeta(meta));
        }

        if (archived_count >= options_.max_objects_per_round && !options_.cold_file_ttl_sec) {
            break;
        }
    }

    if (batch.Count() > 0) {
        std::string write_error;
        if (!store_->WriteBatch(&batch, &write_error)) {
            if (error) {
                *error = write_error;
            }
            return false;
        }
    }

    const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_objects_per_round * 4);
    std::string reconcile_error;
    (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
    if (error && error->empty() && !reconcile_error.empty()) {
        *error = reconcile_error;
    }

    return true;
}

bool OpticalArchiveManager::ResolveCandidateSource(const ArchiveCandidateEntry& candidate,
                                                   std::string* object_key,
                                                   zb::rpc::ReplicaLocation* source_disk,
                                                   uint64_t* inode_id,
                                                   uint32_t* object_index,
                                                   uint64_t* object_size,
                                                   std::string* error) {
    if (!object_key || !source_disk || !inode_id || !object_index || !object_size) {
        if (error) {
            *error = "invalid source output pointers";
        }
        return false;
    }
    *inode_id = 0;
    *object_index = 0;
    *object_size = options_.default_object_unit_size;
    std::string local_error;
    bool has_object_key = FindObjectKeyByObjectId(candidate.ArchiveObjectId(), object_key, &local_error);
    if (!has_object_key) {
        object_key->clear();
    }

    if (!ResolveObjectOwner(candidate.ArchiveObjectId(), inode_id, object_index, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to resolve object owner for " + candidate.ArchiveObjectId()
                                         : local_error;
        }
        return false;
    }

    if (has_object_key) {
        std::string object_meta_data;
        if (!store_->Get(*object_key, &object_meta_data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to load object meta " + *object_key : local_error;
            }
            return false;
        }
        zb::rpc::ObjectMeta meta;
        if (!MetaCodec::DecodeObjectMeta(object_meta_data, &meta)) {
            if (error) {
                *error = "invalid object meta " + *object_key;
            }
            return false;
        }

        const zb::rpc::ReplicaLocation* preferred = nullptr;
        const zb::rpc::ReplicaLocation* fallback = nullptr;
        for (const auto& replica : meta.replicas()) {
            if (!IsDiskReplica(replica)) {
                continue;
            }
            if (!fallback) {
                fallback = &replica;
            }
            if (ReplicaObjectId(replica) == candidate.ArchiveObjectId() &&
                MatchesCandidateNodeId(candidate.node_id, replica.node_id()) &&
                (candidate.disk_id.empty() || replica.disk_id() == candidate.disk_id)) {
                preferred = &replica;
            }
        }
        const zb::rpc::ReplicaLocation* chosen = preferred ? preferred : fallback;
        if (!chosen) {
            if (error) {
                *error = "no disk replica available for " + candidate.ArchiveObjectId();
            }
            return false;
        }
        *source_disk = *chosen;
    } else {
        if (candidate.node_address.empty() || candidate.disk_id.empty()) {
            if (error) {
                *error = local_error.empty() ? "object key missing and candidate source is incomplete"
                                             : local_error;
            }
            return false;
        }
        source_disk->set_node_id(candidate.node_id);
        source_disk->set_node_address(candidate.node_address);
        source_disk->set_disk_id(candidate.disk_id);
        EnsureReplicaObjectId(source_disk, candidate.ArchiveObjectId());
        source_disk->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        source_disk->set_replica_state(zb::rpc::REPLICA_READY);
        if (candidate.size_bytes > 0) {
            source_disk->set_size(candidate.size_bytes);
        }
    }

    zb::rpc::InodeAttr inode;
    if (LoadInodeAttr(*inode_id, &inode, &local_error)) {
        *object_size = inode.object_unit_size() ? inode.object_unit_size() : options_.default_object_unit_size;
    } else {
        if (error) {
            *error = local_error.empty() ? "failed to load inode for " + candidate.ArchiveObjectId() : local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::PersistOpticalReplica(const std::string& object_key,
                                                  const NodeSelection& optical,
                                                  const std::string& object_id,
                                                  uint64_t data_size,
                                                  const zb::rpc::ReplicaLocation* optical_location,
                                                  rocksdb::WriteBatch* update_batch,
                                                  std::string* error) {
    if (object_id.empty()) {
        if (error) {
            *error = "object id is empty";
        }
        return false;
    }

    std::string local_error;
    std::string effective_object_key = object_key;
    uint32_t object_index = 0;
    if (effective_object_key.empty()) {
        uint64_t parsed_inode_id = 0;
        if (!ParseStableObjectId(object_id, &parsed_inode_id, &object_index)) {
            if (error) {
                *error = "object key missing and object id is not stable: " + object_id;
            }
            return false;
        }
        effective_object_key = ObjectKey(parsed_inode_id, object_index);
    }

    zb::rpc::ObjectMeta meta;
    std::string object_data;
    bool exists = store_->Get(effective_object_key, &object_data, &local_error);
    if (exists) {
        if (!MetaCodec::DecodeObjectMeta(object_data, &meta)) {
            if (error) {
                *error = "invalid object meta " + effective_object_key;
            }
            return false;
        }
    } else if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    if (meta.index() == 0) {
        if (object_index == 0) {
            uint64_t parsed_inode = 0;
            uint32_t parsed_index = 0;
            if (ParseObjectKey(effective_object_key, &parsed_inode, &parsed_index)) {
                object_index = parsed_index;
            }
        }
        if (object_index > 0) {
            meta.set_index(object_index);
        }
    }

    bool exists_optical_ready = false;
    for (const auto& replica : meta.replicas()) {
        if (IsReadyOpticalReplica(replica) && ReplicaObjectId(replica) == object_id) {
            exists_optical_ready = true;
            break;
        }
    }
    if (!exists_optical_ready) {
        zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
        new_replica->set_node_id(optical.node_id);
        new_replica->set_node_address(optical.address);
        new_replica->set_disk_id(optical.disk_id);
        EnsureReplicaObjectId(new_replica, object_id);
        new_replica->set_size(data_size);
        new_replica->set_group_id(optical.group_id);
        new_replica->set_epoch(optical.epoch);
        new_replica->set_primary_node_id(optical.node_id);
        new_replica->set_primary_address(optical.address);
        new_replica->set_secondary_node_id(optical.secondary_node_id);
        new_replica->set_secondary_address(optical.secondary_address);
        new_replica->set_sync_ready(optical.sync_ready);
        new_replica->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        new_replica->set_replica_state(zb::rpc::REPLICA_READY);
        if (optical_location && !optical_location->image_id().empty()) {
            new_replica->set_image_id(optical_location->image_id());
            new_replica->set_image_offset(optical_location->image_offset());
            new_replica->set_image_length(optical_location->image_length());
        }
    }

    rocksdb::WriteBatch local_update;
    rocksdb::WriteBatch* target_batch = update_batch ? update_batch : &local_update;
    target_batch->Put(effective_object_key, MetaCodec::EncodeObjectMeta(meta));
    if (!update_batch && !store_->WriteBatch(target_batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist object meta update" : local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::BurnSealedBatch(const NodeSelection& optical,
                                            uint32_t* archived_count,
                                            std::unordered_set<std::string>* touched_object_keys,
                                            std::string* error) {
    if (!batch_stager_) {
        return true;
    }
    struct PreparedBurnTask {
        StagedArchiveObject staged;
        std::string lease_id;
        std::string op_id;
        uint64_t version{0};
        std::string object_key;
        uint64_t data_size{0};
        zb::rpc::ReplicaLocation optical_location;
    };

    std::vector<StagedArchiveObject> tasks = batch_stager_->SnapshotSealedBatch();
    std::vector<PreparedBurnTask> prepared;
    prepared.reserve(tasks.size());
    bool batch_failed = false;

    for (const auto& staged : tasks) {
        std::string lease_id = staged.lease_id;
        std::string op_id = staged.op_id;
        uint64_t version = staged.version;
        std::string local_error;
        if (lease_manager_ && !lease_id.empty()) {
            zb::rpc::RenewArchiveLeaseRequest renew_req;
            renew_req.set_node_id(staged.candidate.node_id);
            SetArchiveObjectId(&renew_req, staged.candidate.ArchiveObjectId());
            renew_req.set_lease_id(lease_id);
            renew_req.set_requested_lease_ms(0);
            zb::rpc::RenewArchiveLeaseReply renew_reply;
            if (!lease_manager_->Renew(renew_req, &renew_reply, &local_error) || !renew_reply.renewed()) {
                zb::rpc::ClaimArchiveTaskRequest claim_req;
                claim_req.set_node_id(staged.candidate.node_id);
                claim_req.set_node_address(staged.candidate.node_address);
                claim_req.set_disk_id(staged.candidate.disk_id);
                SetArchiveObjectId(&claim_req, staged.candidate.ArchiveObjectId());
                claim_req.set_requested_lease_ms(0);
                claim_req.set_request_id(staged.candidate.ArchiveObjectId() + ":" + batch_stager_->CurrentBatchId());
                zb::rpc::ClaimArchiveTaskReply claim_reply;
                std::string claim_error;
                if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                    bool has_optical_ready = false;
                    std::string ready_error;
                    if (HasReadyOpticalReplica(staged.candidate.ArchiveObjectId(), &has_optical_ready, &ready_error) &&
                        has_optical_ready) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(staged.candidate, "archived", claim_reply.record().version(), &update_error);
                        (void)batch_stager_->MarkObjectDone(staged.candidate.ArchiveObjectId(), &claim_error);
                        continue;
                    }
                    if (error && error->empty()) {
                        *error = local_error.empty() ? claim_error : local_error;
                    }
                    batch_failed = true;
                    break;
                }
                lease_id = claim_reply.record().lease_id();
                op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id() : staged.candidate.ArchiveObjectId();
                version = claim_reply.record().version();
                (void)batch_stager_->UpdateObjectLease(staged.candidate.ArchiveObjectId(),
                                                       lease_id,
                                                       op_id,
                                                       version,
                                                       nullptr);
            } else {
                version = renew_reply.record().version();
                (void)batch_stager_->UpdateObjectLease(staged.candidate.ArchiveObjectId(),
                                                       lease_id,
                                                       op_id,
                                                       version,
                                                       nullptr);
            }
        }

        std::string data;
        if (!batch_stager_->ReadObjectData(staged, &data, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }

        std::string object_key = staged.object_key;
        uint64_t inode_id = 0;
        uint32_t object_index = 0;
        if (object_key.empty()) {
            zb::rpc::ReplicaLocation source_disk;
            uint64_t object_size = options_.default_object_unit_size;
            if (!ResolveCandidateSource(staged.candidate,
                                        &object_key,
                                        &source_disk,
                                        &inode_id,
                                        &object_index,
                                        &object_size,
                                        &local_error)) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                batch_failed = true;
                break;
            }
        } else if (!ResolveObjectOwner(staged.candidate.ArchiveObjectId(),
                                       &inode_id,
                                       &object_index,
                                       &local_error)) {
            if (error && error->empty()) {
                *error = local_error.empty()
                             ? "failed to resolve object owner for " + staged.candidate.ArchiveObjectId()
                             : local_error;
            }
            batch_failed = true;
            break;
        }
        zb::rpc::InodeAttr inode;
        if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
            if (error && error->empty()) {
                *error = local_error.empty() ? "failed to load inode for staged object" : local_error;
            }
            batch_failed = true;
            break;
        }

        zb::rpc::ReplicaLocation optical_location;
        if (!WriteObjectToOptical(optical,
                                 staged.candidate.ArchiveObjectId(),
                                 op_id,
                                 data,
                                 inode_id,
                                 object_index,
                                 &inode,
                                 &optical_location,
                                 &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }
        PreparedBurnTask item;
        item.staged = staged;
        item.lease_id = lease_id;
        item.op_id = op_id;
        item.version = version;
        item.object_key = object_key;
        item.data_size = static_cast<uint64_t>(data.size());
        item.optical_location = optical_location;
        prepared.push_back(std::move(item));
    }

    if (batch_failed) {
        return false;
    }

    if (!prepared.empty()) {
        rocksdb::WriteBatch metadata_batch;
        std::string local_error;
        for (const auto& item : prepared) {
            if (!PersistOpticalReplica(item.object_key,
                                       optical,
                                       item.staged.candidate.ArchiveObjectId(),
                                       item.data_size,
                                       &item.optical_location,
                                       &metadata_batch,
                                       &local_error)) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                return false;
            }
        }
        if (!store_->WriteBatch(&metadata_batch, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            return false;
        }

        std::vector<uint64_t> archived_versions(prepared.size(), 0);
        bool all_committed = true;
        for (size_t i = 0; i < prepared.size(); ++i) {
            archived_versions[i] = prepared[i].version;
            const auto& item = prepared[i];
            if (!lease_manager_ || item.lease_id.empty()) {
                continue;
            }
            zb::rpc::CommitArchiveTaskRequest commit_req;
            commit_req.set_node_id(item.staged.candidate.node_id);
            SetArchiveObjectId(&commit_req, item.staged.candidate.ArchiveObjectId());
            commit_req.set_lease_id(item.lease_id);
            commit_req.set_op_id(item.op_id);
            commit_req.set_success(true);
            zb::rpc::CommitArchiveTaskReply commit_reply;
            std::string commit_error;
            if (!lease_manager_->Commit(commit_req, &commit_reply, &commit_error) || !commit_reply.committed()) {
                all_committed = false;
                if (error && error->empty()) {
                    *error = commit_error.empty() ? "failed to commit archive task" : commit_error;
                }
                continue;
            }
            archived_versions[i] = commit_reply.record().version();
        }
        if (!all_committed) {
            return false;
        }

        if (touched_object_keys) {
            for (const auto& item : prepared) {
                touched_object_keys->insert(item.object_key);
            }
        }
        for (size_t i = 0; i < prepared.size(); ++i) {
            const auto& item = prepared[i];
            std::string update_error;
            (void)UpdateSourceArchiveState(item.staged.candidate, "archived", archived_versions[i], &update_error);
            (void)batch_stager_->MarkObjectDone(item.staged.candidate.ArchiveObjectId(), nullptr);
            if (archived_count) {
                ++(*archived_count);
            }
        }
    }

    std::string reset_error;
    if (!batch_stager_->ResetIfDrained(&reset_error) && error && error->empty()) {
        *error = reset_error;
    }
    return true;
}

OpticalArchiveManager::ArchiveByCandidateResult OpticalArchiveManager::ArchiveByCandidate(
    const ArchiveCandidateEntry& candidate,
    const NodeSelection& optical,
    const std::string& lease_id,
    const std::string& op_id,
    std::unordered_set<std::string>* touched_object_keys,
    std::string* error) {
    if (!touched_object_keys) {
        if (error) {
            *error = "touched object set is null";
        }
        return ArchiveByCandidateResult::kFailed;
    }
    if (candidate.ArchiveObjectId().empty()) {
        if (error) {
            *error = "empty candidate object id";
        }
        return ArchiveByCandidateResult::kFailed;
    }

    std::string local_error;
    std::string object_key;
    bool has_object_key = FindObjectKeyByObjectId(candidate.ArchiveObjectId(), &object_key, &local_error);
    if (!has_object_key) {
        object_key.clear();
    }

    uint64_t inode_id = 0;
    uint32_t object_index = 0;
    if (!ResolveObjectOwner(candidate.ArchiveObjectId(), &inode_id, &object_index, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to resolve object owner for " + candidate.ArchiveObjectId()
                                         : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    zb::rpc::ObjectMeta meta;
    bool has_disk = false;
    bool has_optical_ready = false;
    const zb::rpc::ReplicaLocation* source_disk = nullptr;
    const zb::rpc::ReplicaLocation* preferred_source = nullptr;
    zb::rpc::ReplicaLocation source_disk_fallback;
    if (has_object_key) {
        std::string object_data;
        if (!store_->Get(object_key, &object_data, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to load object meta " + object_key : local_error;
            }
            return ArchiveByCandidateResult::kFailed;
        }
        if (!MetaCodec::DecodeObjectMeta(object_data, &meta)) {
            if (error) {
                *error = "invalid object meta " + object_key;
            }
            return ArchiveByCandidateResult::kFailed;
        }
        for (const auto& replica : meta.replicas()) {
            if (IsDiskReplica(replica)) {
                has_disk = true;
                if (!source_disk) {
                    source_disk = &replica;
                }
                if (ReplicaObjectId(replica) == candidate.ArchiveObjectId() &&
                    MatchesCandidateNodeId(candidate.node_id, replica.node_id()) &&
                    (candidate.disk_id.empty() || replica.disk_id() == candidate.disk_id)) {
                    preferred_source = &replica;
                }
            }
            if (IsReadyOpticalReplica(replica)) {
                has_optical_ready = true;
            }
        }
        source_disk = preferred_source ? preferred_source : source_disk;
    } else {
        if (!HasReadyOpticalReplica(candidate.ArchiveObjectId(), &has_optical_ready, &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to query optical replica state for " + candidate.ArchiveObjectId()
                                             : local_error;
            }
            return ArchiveByCandidateResult::kFailed;
        }
        if (!has_optical_ready &&
            (!candidate.node_address.empty() && !candidate.disk_id.empty())) {
            source_disk_fallback.set_node_id(candidate.node_id);
            source_disk_fallback.set_node_address(candidate.node_address);
            source_disk_fallback.set_disk_id(candidate.disk_id);
            EnsureReplicaObjectId(&source_disk_fallback, candidate.ArchiveObjectId());
            source_disk_fallback.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
            source_disk_fallback.set_replica_state(zb::rpc::REPLICA_READY);
            if (candidate.size_bytes > 0) {
                source_disk_fallback.set_size(candidate.size_bytes);
            }
            source_disk = &source_disk_fallback;
            has_disk = true;
        }
    }
    if (has_optical_ready) {
        if (!object_key.empty()) {
            touched_object_keys->insert(object_key);
        }
        return ArchiveByCandidateResult::kArchived;
    }
    if (!has_disk || !source_disk) {
        if (error) {
            *error = "no disk replica available for " + candidate.ArchiveObjectId();
        }
        return ArchiveByCandidateResult::kFailed;
    }

    zb::rpc::InodeAttr inode;
    if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load inode for " + candidate.ArchiveObjectId() : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    std::string data;
    const uint64_t object_size = inode.object_unit_size() ? inode.object_unit_size() : options_.default_object_unit_size;
    if (!ReadObjectFromReplica(*source_disk, object_size, &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to read object from source replica" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }
    if (lease_manager_ && !lease_id.empty()) {
        zb::rpc::RenewArchiveLeaseRequest renew_req;
        renew_req.set_node_id(candidate.node_id);
        SetArchiveObjectId(&renew_req, candidate.ArchiveObjectId());
        renew_req.set_lease_id(lease_id);
        renew_req.set_requested_lease_ms(0);
        zb::rpc::RenewArchiveLeaseReply renew_reply;
        if (!lease_manager_->Renew(renew_req, &renew_reply, &local_error) || !renew_reply.renewed()) {
            if (error) {
                *error = local_error.empty() ? "failed to renew archive lease" : local_error;
            }
            return ArchiveByCandidateResult::kFailed;
        }
    }
    zb::rpc::ReplicaLocation optical_location;
    const std::string source_object_id = ReplicaObjectId(*source_disk);
    if (!WriteObjectToOptical(optical,
                             source_object_id,
                             op_id,
                             data,
                             inode_id,
                             object_index,
                             &inode,
                             &optical_location,
                             &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to write object to optical node" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    rocksdb::WriteBatch batch;
    if (has_object_key) {
        zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
        new_replica->set_node_id(optical.node_id);
        new_replica->set_node_address(optical.address);
        new_replica->set_disk_id(optical.disk_id);
        {
            const std::string object_id = ReplicaObjectId(*source_disk);
            EnsureReplicaObjectId(new_replica, object_id);
        }
        new_replica->set_size(static_cast<uint64_t>(data.size()));
        new_replica->set_group_id(optical.group_id);
        new_replica->set_epoch(optical.epoch);
        new_replica->set_primary_node_id(optical.node_id);
        new_replica->set_primary_address(optical.address);
        new_replica->set_secondary_node_id(optical.secondary_node_id);
        new_replica->set_secondary_address(optical.secondary_address);
        new_replica->set_sync_ready(optical.sync_ready);
        new_replica->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        new_replica->set_replica_state(zb::rpc::REPLICA_READY);
        if (!optical_location.image_id().empty()) {
            new_replica->set_image_id(optical_location.image_id());
            new_replica->set_image_offset(optical_location.image_offset());
            new_replica->set_image_length(optical_location.image_length());
        }

        batch.Put(object_key, MetaCodec::EncodeObjectMeta(meta));
    } else {
        if (!PersistOpticalReplica(object_key,
                                   optical,
                                   candidate.ArchiveObjectId(),
                                   static_cast<uint64_t>(data.size()),
                                   &optical_location,
                                   &batch,
                                   &local_error)) {
            if (error) {
                *error = local_error.empty() ? "failed to persist optical location index" : local_error;
            }
            return ArchiveByCandidateResult::kFailed;
        }
    }
    if (!store_->WriteBatch(&batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist object meta update" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }
    if (!object_key.empty()) {
        touched_object_keys->insert(object_key);
    }
    return ArchiveByCandidateResult::kArchived;
}

bool OpticalArchiveManager::UpdateSourceArchiveState(const ArchiveCandidateEntry& candidate,
                                                     const std::string& archive_state,
                                                     uint64_t version,
                                                     std::string* error) {
    if (candidate.node_address.empty() || candidate.disk_id.empty() || candidate.ArchiveObjectId().empty() || archive_state.empty()) {
        return false;
    }
    brpc::Channel* channel = GetChannel(candidate.node_address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::UpdateArchiveStateRequest req;
    req.set_disk_id(candidate.disk_id);
    req.set_object_id(candidate.ArchiveObjectId());
    req.set_archive_state(archive_state);
    req.set_version(version);

    zb::rpc::UpdateArchiveStateReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    stub.UpdateArchiveState(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::ReconcileArchiveStates(uint32_t max_records, std::string* error) {
    if (!store_ || max_records == 0) {
        return true;
    }

    const uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
    rocksdb::WriteBatch batch;
    std::vector<std::pair<ArchiveCandidateEntry, std::string>> source_updates;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    uint32_t visited = 0;
    for (it->Seek(ObjectArchiveStatePrefix()); it->Valid() && visited < max_records; it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(ObjectArchiveStatePrefix(), 0) != 0) {
            break;
        }
        ++visited;

        zb::rpc::ArchiveLeaseRecord record;
        if (!record.ParseFromString(it->value().ToString())) {
            continue;
        }
        bool changed = false;
        bool delete_record = false;
        std::string state_to_push;
        const std::string object_id = ArchiveObjectId(record);

        bool has_optical = false;
        std::string local_error;
        if (!HasReadyOpticalReplica(object_id, &has_optical, &local_error)) {
            continue;
        }

        if (has_optical) {
            delete_record = true;
            changed = true;
            state_to_push = "archived";
        } else if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVING &&
                   record.lease_expire_ts_ms() > 0 &&
                   record.lease_expire_ts_ms() <= now_ms) {
            record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
            record.clear_lease_id();
            record.set_lease_expire_ts_ms(0);
            record.set_update_ts_ms(now_ms);
            record.set_version(record.version() + 1);
            changed = true;
            state_to_push = "pending";
        } else if (record.state() != zb::rpc::ARCHIVE_STATE_PENDING &&
                   record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING) {
            // Legacy/unknown state cleanup: normalize back to pending.
            record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
            record.clear_lease_id();
            record.set_lease_expire_ts_ms(0);
            record.set_update_ts_ms(now_ms);
            record.set_version(record.version() + 1);
            changed = true;
            state_to_push = "pending";
        }

        if (!changed) {
            continue;
        }

        if (delete_record) {
            batch.Delete(ObjectArchiveStateKey(object_id));
        } else {
            std::string value;
            if (!record.SerializeToString(&value)) {
                continue;
            }
            batch.Put(ObjectArchiveStateKey(object_id), value);
        }

        ArchiveCandidateEntry source;
        source.node_id = record.owner_node_id();
        if (cache_) {
            (void)cache_->ResolveNodeAddress(source.node_id, &source.node_address);
        }
        source.disk_id = record.owner_disk_id();
        source.SetArchiveObjectId(object_id);
        source.version = record.version() + (delete_record ? 1 : 0);
        if (!source.node_address.empty() && !source.disk_id.empty() && !state_to_push.empty()) {
            source_updates.push_back(std::make_pair(source, state_to_push));
        }
    }

    if (batch.Count() > 0) {
        std::string write_error;
        if (!store_->WriteBatch(&batch, &write_error)) {
            if (error) {
                *error = write_error;
            }
            return false;
        }
    }
    for (const auto& item : source_updates) {
        std::string update_error;
        (void)UpdateSourceArchiveState(item.first, item.second, item.first.version, &update_error);
        if (error && error->empty() && !update_error.empty()) {
            *error = update_error;
        }
    }

    return true;
}

bool OpticalArchiveManager::ResolveObjectOwner(const std::string& object_id,
                                               uint64_t* inode_id,
                                               uint32_t* object_index,
                                               std::string* error) {
    if (object_id.empty() || !inode_id || !object_index) {
        if (error) {
            *error = "invalid object owner resolve args";
        }
        return false;
    }

    if (!ParseStableObjectId(object_id, inode_id, object_index)) {
        if (error) {
            *error = "object id is not stable: " + object_id;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::FindObjectKeyByObjectId(const std::string& object_id,
                                                    std::string* object_key,
                                                    std::string* error) {
    if (!object_key || object_id.empty()) {
        if (error) {
            *error = "invalid object lookup args";
        }
        return false;
    }
    object_key->clear();
    uint64_t inode_id = 0;
    uint32_t object_index = 0;
    if (ParseStableObjectId(object_id, &inode_id, &object_index)) {
        const std::string direct_key = ObjectKey(inode_id, object_index);
        std::string exists_error;
        if (store_->Exists(direct_key, &exists_error)) {
            *object_key = direct_key;
            return true;
        }
        if (!exists_error.empty()) {
            if (error) {
                *error = exists_error;
            }
            return false;
        }
    }

    if (error) {
        error->clear();
    }
    return false;
}

bool OpticalArchiveManager::HasReadyOpticalReplica(const std::string& object_id,
                                                   bool* has_ready,
                                                   std::string* error) {
    if (!has_ready) {
        if (error) {
            *error = "has_ready output is null";
        }
        return false;
    }
    *has_ready = false;
    if (object_id.empty()) {
        return true;
    }
    std::string object_key;
    std::string local_error;
    if (!FindObjectKeyByObjectId(object_id, &object_key, &local_error)) {
        if (local_error.empty()) {
            return true;
        }
        if (error) {
            *error = local_error;
        }
        return false;
    }

    std::string object_data;
    if (!store_->Get(object_key, &object_data, &local_error)) {
        if (local_error.empty()) {
            return true;
        }
        if (error) {
            *error = local_error;
        }
        return false;
    }
    zb::rpc::ObjectMeta meta;
    if (!MetaCodec::DecodeObjectMeta(object_data, &meta)) {
        if (error) {
            *error = "invalid object meta " + object_key;
        }
        return false;
    }
    for (const auto& replica : meta.replicas()) {
        if (!IsReadyOpticalReplica(replica)) {
            continue;
        }
        if (!ReplicaObjectId(replica).empty() && ReplicaObjectId(replica) != object_id) {
            continue;
        }
        if (!replica.node_id().empty() && !replica.disk_id().empty()) {
            *has_ready = true;
            break;
        }
    }
    return true;
}

bool OpticalArchiveManager::ShouldArchiveNow(const std::vector<NodeInfo>& nodes) {
    uint64_t max_used = 0;
    bool found = false;
    for (const auto& node : nodes) {
        if (!node.allocatable || !node.is_primary || node.type == NodeType::kOptical) {
            continue;
        }
        for (const auto& disk : node.disks) {
            if (!disk.is_healthy || disk.capacity_bytes == 0) {
                continue;
            }
            found = true;
            uint64_t used = disk.capacity_bytes > disk.free_bytes ? (disk.capacity_bytes - disk.free_bytes) : 0;
            max_used = std::max<uint64_t>(max_used, used);
        }
    }
    if (!found) {
        return false;
    }
    if (max_used >= options_.archive_trigger_bytes) {
        archive_mode_ = true;
    } else if (max_used <= options_.archive_target_bytes) {
        archive_mode_ = false;
    }
    return archive_mode_;
}

bool OpticalArchiveManager::LoadInodeAttr(uint64_t inode_id,
                                          zb::rpc::InodeAttr* attr,
                                          std::string* error) const {
    if (!attr) {
        if (error) {
            *error = "inode attr pointer is null";
        }
        return false;
    }

    std::string data;
    if (!store_->Get(InodeKey(inode_id), &data, error)) {
        return false;
    }
    if (!MetaCodec::DecodeInodeAttr(data, attr)) {
        if (error) {
            *error = "invalid inode attr";
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::ReadObjectFromReplica(const zb::rpc::ReplicaLocation& source,
                                                 uint64_t read_size,
                                                 std::string* data,
                                                 std::string* error) {
    if (!data) {
        if (error) {
            *error = "output buffer is null";
        }
        return false;
    }
    brpc::Channel* channel = GetChannel(source.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::ReadObjectRequest req;
    req.set_disk_id(source.disk_id());
    req.set_object_id(ReplicaObjectId(source));
    req.set_offset(0);
    req.set_size(read_size);
    if (!source.image_id().empty()) {
        req.set_image_id(source.image_id());
        req.set_image_offset(source.image_offset());
        req.set_image_length(source.image_length());
    }

    zb::rpc::ReadObjectReply resp;
    stub.ReadObject(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    *data = resp.data();
    return true;
}

bool OpticalArchiveManager::WriteObjectToOptical(const NodeSelection& optical,
                                                 const std::string& object_id,
                                                 const std::string& op_id,
                                                 const std::string& data,
                                                 uint64_t inode_id,
                                                 uint32_t object_index,
                                                 const zb::rpc::InodeAttr* inode_attr,
                                                 zb::rpc::ReplicaLocation* optical_location,
                                                 std::string* error) {
    const std::string effective_op_id = !op_id.empty() ? op_id : object_id;

    brpc::Channel* channel = GetChannel(optical.address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteObjectRequest req;
    req.set_disk_id(optical.disk_id);
    req.set_object_id(object_id);
    req.set_offset(0);
    req.set_data(data);
    req.set_epoch(optical.epoch);
    req.set_archive_op_id(effective_op_id);
    req.set_inode_id(inode_id);
    req.set_file_object_index(object_index);
    if (inode_attr) {
        req.set_file_size(inode_attr->size());
        req.set_file_mode(inode_attr->mode());
        req.set_file_uid(inode_attr->uid());
        req.set_file_gid(inode_attr->gid());
        req.set_file_mtime(inode_attr->mtime());
        const uint64_t object_unit_size =
            inode_attr->object_unit_size() > 0 ? inode_attr->object_unit_size() : options_.default_object_unit_size;
        req.set_file_offset(static_cast<uint64_t>(object_index) * object_unit_size);
    }
    if (inode_id != 0) {
        req.set_file_id("inode-" + std::to_string(inode_id));
        req.set_file_path("/inode/" + std::to_string(inode_id));
    }

    zb::rpc::WriteObjectReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    stub.WriteObject(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    if (optical_location) {
        optical_location->set_node_id(optical.node_id);
        optical_location->set_node_address(optical.address);
        optical_location->set_disk_id(optical.disk_id);
        EnsureReplicaObjectId(optical_location, object_id);
        optical_location->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        optical_location->set_replica_state(zb::rpc::REPLICA_READY);
        optical_location->set_image_id(resp.image_id());
        optical_location->set_image_offset(resp.image_offset());
        optical_location->set_image_length(resp.image_length());
    }
    return true;
}

bool OpticalArchiveManager::DeleteDiskReplica(const zb::rpc::ReplicaLocation& replica, std::string* error) {
    brpc::Channel* channel = GetChannel(replica.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::DeleteObjectRequest req;
    req.set_disk_id(replica.disk_id());
    req.set_object_id(ReplicaObjectId(replica));
    zb::rpc::DeleteObjectReply resp;
    stub.DeleteObject(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

brpc::Channel* OpticalArchiveManager::GetChannel(const std::string& address, std::string* error) {
    if (address.empty()) {
        if (error) {
            *error = "empty address";
        }
        return nullptr;
    }

    auto it = channels_.find(address);
    if (it != channels_.end()) {
        return it->second.get();
    }

    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    options.max_retry = 0;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) {
            *error = "failed to init channel to " + address;
        }
        return nullptr;
    }

    brpc::Channel* raw = channel.get();
    channels_[address] = std::move(channel);
    return raw;
}

} // namespace zb::mds
