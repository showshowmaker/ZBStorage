#include "OpticalArchiveManager.h"

#include <brpc/controller.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <chrono>
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
    if (options_.max_chunks_per_round == 0) {
        options_.max_chunks_per_round = 1;
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
    std::unordered_set<std::string> touched_chunk_keys;
    if (do_archive && !optical_nodes.empty() && candidate_queue_) {
        const NodeSelection& optical = optical_nodes.front();
        if (batch_stager_) {
            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_chunk_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }

            uint32_t staged_count = 0;
            while (staged_count < options_.max_chunks_per_round && !batch_stager_->HasSealedBatch()) {
                ArchiveCandidateEntry candidate;
                if (!candidate_queue_->PopTop(&candidate)) {
                    break;
                }
                if (batch_stager_->ContainsChunk(candidate.chunk_id)) {
                    continue;
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
                    claim_req.set_chunk_id(candidate.chunk_id);
                    claim_req.set_requested_lease_ms(0);
                    claim_req.set_request_id(candidate.chunk_id + ":" + batch_stager_->CurrentBatchId());

                    std::string claim_error;
                    if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                        granted = false;
                    } else {
                        lease_id = claim_reply.record().lease_id();
                        op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id() : candidate.chunk_id;
                    }
                }
                if (!granted) {
                    if (claim_reply.record().state() == zb::rpc::ARCHIVE_STATE_ARCHIVED) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(candidate, "archived", claim_reply.record().version(), &update_error);
                    }
                    continue;
                }

                std::string chunk_key;
                zb::rpc::ReplicaLocation source_disk;
                uint64_t chunk_size = 0;
                std::string local_error;
                if (!ResolveCandidateSource(candidate, &chunk_key, &source_disk, &chunk_size, &local_error)) {
                    if (lease_manager_ && !lease_id.empty()) {
                        zb::rpc::CommitArchiveTaskRequest commit_req;
                        commit_req.set_node_id(candidate.node_id);
                        commit_req.set_chunk_id(candidate.chunk_id);
                        commit_req.set_lease_id(lease_id);
                        commit_req.set_op_id(op_id);
                        commit_req.set_success(false);
                        commit_req.set_error_message(local_error.empty() ? "failed to resolve source chunk" : local_error);
                        zb::rpc::CommitArchiveTaskReply commit_reply;
                        std::string commit_error;
                        (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    }
                    continue;
                }

                std::string data;
                if (!ReadChunkFromReplica(source_disk, chunk_size, &data, &local_error)) {
                    if (lease_manager_ && !lease_id.empty()) {
                        zb::rpc::CommitArchiveTaskRequest commit_req;
                        commit_req.set_node_id(candidate.node_id);
                        commit_req.set_chunk_id(candidate.chunk_id);
                        commit_req.set_lease_id(lease_id);
                        commit_req.set_op_id(op_id);
                        commit_req.set_success(false);
                        commit_req.set_error_message(local_error.empty() ? "failed to read source chunk" : local_error);
                        zb::rpc::CommitArchiveTaskReply commit_reply;
                        std::string commit_error;
                        (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    }
                    continue;
                }

                bool inserted = false;
                bool deferred = false;
                const uint64_t stage_version = claim_reply.record().version() > 0
                                                   ? claim_reply.record().version()
                                                   : (candidate.version > 0 ? candidate.version : 1);
                if (!batch_stager_->StageChunk(candidate,
                                               lease_id,
                                               op_id,
                                               chunk_key,
                                               stage_version,
                                               data,
                                               &inserted,
                                               &deferred,
                                               &local_error)) {
                    if (lease_manager_ && !lease_id.empty()) {
                        zb::rpc::CommitArchiveTaskRequest commit_req;
                        commit_req.set_node_id(candidate.node_id);
                        commit_req.set_chunk_id(candidate.chunk_id);
                        commit_req.set_lease_id(lease_id);
                        commit_req.set_op_id(op_id);
                        commit_req.set_success(false);
                        commit_req.set_error_message(local_error.empty() ? "failed to stage chunk" : local_error);
                        zb::rpc::CommitArchiveTaskReply commit_reply;
                        std::string commit_error;
                        (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    }
                    continue;
                }
                if (inserted) {
                    std::string update_error;
                    (void)UpdateSourceArchiveState(candidate, "archiving", stage_version, &update_error);
                    ++staged_count;
                } else if (deferred) {
                    if (lease_manager_ && !lease_id.empty()) {
                        zb::rpc::CommitArchiveTaskRequest commit_req;
                        commit_req.set_node_id(candidate.node_id);
                        commit_req.set_chunk_id(candidate.chunk_id);
                        commit_req.set_lease_id(lease_id);
                        commit_req.set_op_id(op_id);
                        commit_req.set_success(false);
                        commit_req.set_error_message("deferred to next archive batch");
                        zb::rpc::CommitArchiveTaskReply commit_reply;
                        std::string commit_error;
                        (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    }
                    std::vector<ArchiveCandidateEntry> deferred_batch;
                    deferred_batch.push_back(candidate);
                    (void)candidate_queue_->PushBatch(deferred_batch);
                } else if (lease_manager_ && !lease_id.empty()) {
                    (void)batch_stager_->UpdateLease(candidate.chunk_id, lease_id, op_id, stage_version, nullptr);
                }

                std::string seal_error;
                (void)batch_stager_->SealIfReady(&seal_error);
                if (error && error->empty() && !seal_error.empty()) {
                    *error = seal_error;
                }
            }

            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_chunk_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }
        } else {
            while (archived_count < options_.max_chunks_per_round) {
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
                    claim_req.set_chunk_id(candidate.chunk_id);
                    claim_req.set_requested_lease_ms(0);
                    claim_req.set_request_id(candidate.chunk_id);

                    std::string claim_error;
                    if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                        granted = false;
                    } else {
                        lease_id = claim_reply.record().lease_id();
                        op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id() : candidate.chunk_id;
                    }
                }
                if (!granted) {
                    if (claim_reply.record().state() == zb::rpc::ARCHIVE_STATE_ARCHIVED) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(candidate, "archived", claim_reply.record().version(), &update_error);
                    }
                    continue;
                }

                std::string local_error;
                const ArchiveByCandidateResult result =
                    ArchiveByCandidate(candidate, optical, lease_id, op_id, &touched_chunk_keys, &local_error);
                if (result == ArchiveByCandidateResult::kArchived) {
                    ++archived_count;
                }

                if (lease_manager_ && !lease_id.empty()) {
                    zb::rpc::CommitArchiveTaskRequest commit_req;
                    commit_req.set_node_id(candidate.node_id);
                    commit_req.set_chunk_id(candidate.chunk_id);
                    commit_req.set_lease_id(lease_id);
                    commit_req.set_op_id(op_id);
                    commit_req.set_success(result == ArchiveByCandidateResult::kArchived);
                    if (result != ArchiveByCandidateResult::kArchived) {
                        commit_req.set_error_message(local_error.empty() ? "archive candidate skipped/failed" : local_error);
                    }

                    zb::rpc::CommitArchiveTaskReply commit_reply;
                    std::string commit_error;
                    (void)lease_manager_->Commit(commit_req, &commit_reply, &commit_error);
                    if (commit_reply.record().state() == zb::rpc::ARCHIVE_STATE_ARCHIVED &&
                        result == ArchiveByCandidateResult::kArchived) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(candidate, "archived", commit_reply.record().version(), &update_error);
                    }
                }
            }
        }
    }

    if (options_.cold_file_ttl_sec == 0) {
        const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_chunks_per_round * 4);
        std::string reconcile_error;
        (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
        if (error && error->empty() && !reconcile_error.empty()) {
            *error = reconcile_error;
        }
        return true;
    }

    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek("C/"); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.size() < 2 || key[0] != 'C' || key[1] != '/') {
            break;
        }
        if (touched_chunk_keys.find(key) != touched_chunk_keys.end()) {
            continue;
        }

        uint64_t inode_id = 0;
        uint32_t chunk_index = 0;
        if (!ParseChunkKey(key, &inode_id, &chunk_index)) {
            continue;
        }
        (void)chunk_index;

        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(it->value().ToString(), &meta)) {
            continue;
        }

        zb::rpc::InodeAttr inode;
        std::string local_error;
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

        const bool allow_scan_archive = do_archive && !candidate_queue_;
        if (allow_scan_archive && has_disk && !has_optical_ready && source_disk && !optical_nodes.empty() &&
            archived_count < options_.max_chunks_per_round) {
            std::string data;
            uint64_t chunk_size = inode.chunk_size() ? inode.chunk_size() : options_.default_chunk_size;
            if (ReadChunkFromReplica(*source_disk, chunk_size, &data, &local_error)) {
                const NodeSelection& optical = optical_nodes.front();
                if (WriteChunkToOptical(optical,
                                        source_disk->chunk_id(),
                                        source_disk->chunk_id(),
                                        data,
                                        &local_error)) {
                    zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
                    new_replica->set_node_id(optical.node_id);
                    new_replica->set_node_address(optical.address);
                    new_replica->set_disk_id(optical.disk_id);
                    new_replica->set_chunk_id(source_disk->chunk_id());
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
            batch.Put(key, MetaCodec::EncodeChunkMeta(meta));
        }

        if (archived_count >= options_.max_chunks_per_round && !options_.cold_file_ttl_sec) {
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

    const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_chunks_per_round * 4);
    std::string reconcile_error;
    (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
    if (error && error->empty() && !reconcile_error.empty()) {
        *error = reconcile_error;
    }

    return true;
}

bool OpticalArchiveManager::ResolveCandidateSource(const ArchiveCandidateEntry& candidate,
                                                   std::string* chunk_key,
                                                   zb::rpc::ReplicaLocation* source_disk,
                                                   uint64_t* chunk_size,
                                                   std::string* error) {
    if (!chunk_key || !source_disk || !chunk_size) {
        if (error) {
            *error = "invalid source output pointers";
        }
        return false;
    }
    *chunk_size = options_.default_chunk_size;
    std::string local_error;
    if (!FindChunkKeyByChunkId(candidate.chunk_id, chunk_key, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "chunk key not found for candidate " + candidate.chunk_id : local_error;
        }
        return false;
    }

    uint64_t inode_id = 0;
    uint32_t chunk_index = 0;
    if (!ParseChunkKey(*chunk_key, &inode_id, &chunk_index)) {
        if (error) {
            *error = "invalid chunk key " + *chunk_key;
        }
        return false;
    }

    std::string chunk_meta_data;
    if (!store_->Get(*chunk_key, &chunk_meta_data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load chunk meta " + *chunk_key : local_error;
        }
        return false;
    }
    zb::rpc::ChunkMeta meta;
    if (!MetaCodec::DecodeChunkMeta(chunk_meta_data, &meta)) {
        if (error) {
            *error = "invalid chunk meta " + *chunk_key;
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
        if (replica.chunk_id() == candidate.chunk_id &&
            (candidate.node_id.empty() || replica.node_id() == candidate.node_id) &&
            (candidate.disk_id.empty() || replica.disk_id() == candidate.disk_id)) {
            preferred = &replica;
        }
    }
    const zb::rpc::ReplicaLocation* chosen = preferred ? preferred : fallback;
    if (!chosen) {
        if (error) {
            *error = "no disk replica available for " + candidate.chunk_id;
        }
        return false;
    }
    *source_disk = *chosen;

    zb::rpc::InodeAttr inode;
    if (LoadInodeAttr(inode_id, &inode, &local_error)) {
        *chunk_size = inode.chunk_size() ? inode.chunk_size() : options_.default_chunk_size;
    } else {
        if (error) {
            *error = local_error.empty() ? "failed to load inode for " + candidate.chunk_id : local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::PersistOpticalReplica(const std::string& chunk_key,
                                                  const NodeSelection& optical,
                                                  const std::string& chunk_id,
                                                  uint64_t data_size,
                                                  rocksdb::WriteBatch* update_batch,
                                                  std::string* error) {
    if (chunk_key.empty() || chunk_id.empty()) {
        if (error) {
            *error = "chunk key or chunk id is empty";
        }
        return false;
    }

    std::string local_error;
    std::string chunk_data;
    if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to reload chunk meta " + chunk_key : local_error;
        }
        return false;
    }
    zb::rpc::ChunkMeta meta;
    if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
        if (error) {
            *error = "invalid chunk meta " + chunk_key;
        }
        return false;
    }
    bool exists_optical_ready = false;
    for (const auto& replica : meta.replicas()) {
        if (replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
            replica.replica_state() == zb::rpc::REPLICA_READY) {
            exists_optical_ready = true;
            break;
        }
    }
    if (!exists_optical_ready) {
        zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
        new_replica->set_node_id(optical.node_id);
        new_replica->set_node_address(optical.address);
        new_replica->set_disk_id(optical.disk_id);
        new_replica->set_chunk_id(chunk_id);
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
    }

    rocksdb::WriteBatch local_update;
    rocksdb::WriteBatch* target_batch = update_batch ? update_batch : &local_update;
    target_batch->Put(chunk_key, MetaCodec::EncodeChunkMeta(meta));
    target_batch->Put(ReverseChunkKey(chunk_id), chunk_key);
    if (!update_batch && !store_->WriteBatch(target_batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist chunk meta update" : local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::BurnSealedBatch(const NodeSelection& optical,
                                            uint32_t* archived_count,
                                            std::unordered_set<std::string>* touched_chunk_keys,
                                            std::string* error) {
    if (!batch_stager_) {
        return true;
    }
    struct PreparedBurnTask {
        StagedArchiveChunk staged;
        std::string lease_id;
        std::string op_id;
        uint64_t version{0};
        std::string chunk_key;
        uint64_t data_size{0};
    };

    std::vector<StagedArchiveChunk> tasks = batch_stager_->SnapshotSealedBatch();
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
            renew_req.set_chunk_id(staged.candidate.chunk_id);
            renew_req.set_lease_id(lease_id);
            renew_req.set_requested_lease_ms(0);
            zb::rpc::RenewArchiveLeaseReply renew_reply;
            if (!lease_manager_->Renew(renew_req, &renew_reply, &local_error) || !renew_reply.renewed()) {
                zb::rpc::ClaimArchiveTaskRequest claim_req;
                claim_req.set_node_id(staged.candidate.node_id);
                claim_req.set_node_address(staged.candidate.node_address);
                claim_req.set_disk_id(staged.candidate.disk_id);
                claim_req.set_chunk_id(staged.candidate.chunk_id);
                claim_req.set_requested_lease_ms(0);
                claim_req.set_request_id(staged.candidate.chunk_id + ":" + batch_stager_->CurrentBatchId());
                zb::rpc::ClaimArchiveTaskReply claim_reply;
                std::string claim_error;
                if (!lease_manager_->Claim(claim_req, &claim_reply, &claim_error) || !claim_reply.granted()) {
                    if (claim_reply.record().state() == zb::rpc::ARCHIVE_STATE_ARCHIVED) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(staged.candidate, "archived", claim_reply.record().version(), &update_error);
                        (void)batch_stager_->MarkChunkDone(staged.candidate.chunk_id, &claim_error);
                        continue;
                    }
                    if (error && error->empty()) {
                        *error = local_error.empty() ? claim_error : local_error;
                    }
                    batch_failed = true;
                    break;
                }
                lease_id = claim_reply.record().lease_id();
                op_id = !claim_reply.record().lease_id().empty() ? claim_reply.record().lease_id() : staged.candidate.chunk_id;
                version = claim_reply.record().version();
                (void)batch_stager_->UpdateLease(staged.candidate.chunk_id, lease_id, op_id, version, nullptr);
            } else {
                version = renew_reply.record().version();
                (void)batch_stager_->UpdateLease(staged.candidate.chunk_id, lease_id, op_id, version, nullptr);
            }
        }

        std::string data;
        if (!batch_stager_->ReadChunkData(staged, &data, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }

        if (!WriteChunkToOptical(optical, staged.candidate.chunk_id, op_id, data, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }
        std::string chunk_key = staged.chunk_key;
        if (chunk_key.empty()) {
            zb::rpc::ReplicaLocation source_disk;
            uint64_t chunk_size = options_.default_chunk_size;
            if (!ResolveCandidateSource(staged.candidate, &chunk_key, &source_disk, &chunk_size, &local_error)) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                batch_failed = true;
                break;
            }
        }
        PreparedBurnTask item;
        item.staged = staged;
        item.lease_id = lease_id;
        item.op_id = op_id;
        item.version = version;
        item.chunk_key = chunk_key;
        item.data_size = static_cast<uint64_t>(data.size());
        prepared.push_back(std::move(item));
    }

    if (batch_failed) {
        return false;
    }

    if (!prepared.empty()) {
        rocksdb::WriteBatch metadata_batch;
        std::string local_error;
        for (const auto& item : prepared) {
            if (!PersistOpticalReplica(item.chunk_key,
                                       optical,
                                       item.staged.candidate.chunk_id,
                                       item.data_size,
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
            commit_req.set_chunk_id(item.staged.candidate.chunk_id);
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

        if (touched_chunk_keys) {
            for (const auto& item : prepared) {
                touched_chunk_keys->insert(item.chunk_key);
            }
        }
        for (size_t i = 0; i < prepared.size(); ++i) {
            const auto& item = prepared[i];
            std::string update_error;
            (void)UpdateSourceArchiveState(item.staged.candidate, "archived", archived_versions[i], &update_error);
            (void)batch_stager_->MarkChunkDone(item.staged.candidate.chunk_id, nullptr);
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
    std::unordered_set<std::string>* touched_chunk_keys,
    std::string* error) {
    if (!touched_chunk_keys) {
        if (error) {
            *error = "touched chunk set is null";
        }
        return ArchiveByCandidateResult::kFailed;
    }
    if (candidate.chunk_id.empty()) {
        if (error) {
            *error = "empty candidate chunk id";
        }
        return ArchiveByCandidateResult::kFailed;
    }

    std::string local_error;
    std::string chunk_key;
    if (!FindChunkKeyByChunkId(candidate.chunk_id, &chunk_key, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "chunk key not found for candidate " + candidate.chunk_id : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    uint64_t inode_id = 0;
    uint32_t chunk_index = 0;
    if (!ParseChunkKey(chunk_key, &inode_id, &chunk_index)) {
        if (error) {
            *error = "invalid chunk key " + chunk_key;
        }
        return ArchiveByCandidateResult::kFailed;
    }
    (void)chunk_index;

    std::string chunk_data;
    if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load chunk meta " + chunk_key : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    zb::rpc::ChunkMeta meta;
    if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
        if (error) {
            *error = "invalid chunk meta " + chunk_key;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    bool has_disk = false;
    bool has_optical_ready = false;
    const zb::rpc::ReplicaLocation* source_disk = nullptr;
    const zb::rpc::ReplicaLocation* preferred_source = nullptr;
    for (const auto& replica : meta.replicas()) {
        if (IsDiskReplica(replica)) {
            has_disk = true;
            if (!source_disk) {
                source_disk = &replica;
            }
            if (replica.chunk_id() == candidate.chunk_id &&
                (candidate.node_id.empty() || replica.node_id() == candidate.node_id) &&
                (candidate.disk_id.empty() || replica.disk_id() == candidate.disk_id)) {
                preferred_source = &replica;
            }
        }
        if (IsReadyOpticalReplica(replica)) {
            has_optical_ready = true;
        }
    }

    source_disk = preferred_source ? preferred_source : source_disk;
    if (has_optical_ready) {
        touched_chunk_keys->insert(chunk_key);
        return ArchiveByCandidateResult::kArchived;
    }
    if (!has_disk || !source_disk) {
        if (error) {
            *error = "no disk replica available for " + candidate.chunk_id;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    zb::rpc::InodeAttr inode;
    if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load inode for " + candidate.chunk_id : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    std::string data;
    const uint64_t chunk_size = inode.chunk_size() ? inode.chunk_size() : options_.default_chunk_size;
    if (!ReadChunkFromReplica(*source_disk, chunk_size, &data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to read chunk from source replica" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }
    if (lease_manager_ && !lease_id.empty()) {
        zb::rpc::RenewArchiveLeaseRequest renew_req;
        renew_req.set_node_id(candidate.node_id);
        renew_req.set_chunk_id(candidate.chunk_id);
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
    if (!WriteChunkToOptical(optical, source_disk->chunk_id(), op_id, data, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to write chunk to optical node" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }

    zb::rpc::ReplicaLocation* new_replica = meta.add_replicas();
    new_replica->set_node_id(optical.node_id);
    new_replica->set_node_address(optical.address);
    new_replica->set_disk_id(optical.disk_id);
    new_replica->set_chunk_id(source_disk->chunk_id());
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

    rocksdb::WriteBatch batch;
    batch.Put(chunk_key, MetaCodec::EncodeChunkMeta(meta));
    batch.Put(ReverseChunkKey(candidate.chunk_id), chunk_key);
    if (!store_->WriteBatch(&batch, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist chunk meta update" : local_error;
        }
        return ArchiveByCandidateResult::kFailed;
    }
    touched_chunk_keys->insert(chunk_key);
    return ArchiveByCandidateResult::kArchived;
}

bool OpticalArchiveManager::UpdateSourceArchiveState(const ArchiveCandidateEntry& candidate,
                                                     const std::string& archive_state,
                                                     uint64_t version,
                                                     std::string* error) {
    if (candidate.node_address.empty() || candidate.disk_id.empty() || candidate.chunk_id.empty() || archive_state.empty()) {
        return false;
    }
    brpc::Channel* channel = GetChannel(candidate.node_address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::UpdateArchiveStateRequest req;
    req.set_disk_id(candidate.disk_id);
    req.set_chunk_id(candidate.chunk_id);
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
    for (it->Seek(ArchiveStatePrefix()); it->Valid() && visited < max_records; it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(ArchiveStatePrefix(), 0) != 0) {
            break;
        }
        ++visited;

        zb::rpc::ArchiveLeaseRecord record;
        if (!record.ParseFromString(it->value().ToString())) {
            continue;
        }
        bool changed = false;
        std::string state_to_push;
        if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVING &&
            record.lease_expire_ts_ms() > 0 &&
            record.lease_expire_ts_ms() <= now_ms) {
            record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
            record.clear_lease_id();
            record.set_lease_expire_ts_ms(0);
            record.set_update_ts_ms(now_ms);
            record.set_version(record.version() + 1);
            changed = true;
            state_to_push = "pending";
        }

        bool has_optical = false;
        std::string chunk_key;
        std::string local_error;
        if (FindChunkKeyByChunkId(record.chunk_id(), &chunk_key, &local_error) &&
            HasReadyOpticalReplica(chunk_key, record.chunk_id(), &has_optical, &local_error)) {
            if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVED && !has_optical) {
                record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
                record.clear_lease_id();
                record.set_lease_expire_ts_ms(0);
                record.set_update_ts_ms(now_ms);
                record.set_version(record.version() + 1);
                changed = true;
                state_to_push = "pending";
            } else if (record.state() == zb::rpc::ARCHIVE_STATE_PENDING && has_optical) {
                record.set_state(zb::rpc::ARCHIVE_STATE_ARCHIVED);
                record.set_archive_ts_ms(now_ms);
                record.set_update_ts_ms(now_ms);
                record.set_version(record.version() + 1);
                changed = true;
                state_to_push = "archived";
            }
        }

        if (!changed) {
            continue;
        }

        std::string value;
        if (!record.SerializeToString(&value)) {
            continue;
        }
        batch.Put(ArchiveStateKey(record.chunk_id()), value);

        ArchiveCandidateEntry source;
        source.node_id = record.owner_node_id();
        source.node_address = record.owner_node_address();
        source.disk_id = record.owner_disk_id();
        source.chunk_id = record.chunk_id();
        source.version = record.version();
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

    std::string repair_error;
    (void)ProcessReverseChunkRepairTasks(max_records, &repair_error);
    if (error && error->empty() && !repair_error.empty()) {
        *error = repair_error;
    }
    return true;
}

bool OpticalArchiveManager::ProcessReverseChunkRepairTasks(uint32_t max_records, std::string* error) {
    if (!store_ || max_records == 0) {
        return true;
    }

    std::vector<std::string> pending_chunk_ids;
    pending_chunk_ids.reserve(max_records);
    std::unique_ptr<rocksdb::Iterator> repair_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    const std::string prefix = ArchiveReverseRepairPrefix();
    for (repair_it->Seek(prefix); repair_it->Valid() && pending_chunk_ids.size() < max_records; repair_it->Next()) {
        const std::string key = repair_it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        const std::string chunk_id = key.substr(prefix.size());
        if (!chunk_id.empty()) {
            pending_chunk_ids.push_back(chunk_id);
        }
    }
    if (pending_chunk_ids.empty()) {
        return true;
    }

    std::unordered_set<std::string> pending_set(pending_chunk_ids.begin(), pending_chunk_ids.end());
    std::unordered_map<std::string, std::string> resolved;
    std::unique_ptr<rocksdb::Iterator> chunk_it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (chunk_it->Seek("C/"); chunk_it->Valid() && !pending_set.empty(); chunk_it->Next()) {
        const std::string chunk_key = chunk_it->key().ToString();
        if (chunk_key.size() < 2 || chunk_key[0] != 'C' || chunk_key[1] != '/') {
            break;
        }
        zb::rpc::ChunkMeta meta;
        if (!MetaCodec::DecodeChunkMeta(chunk_it->value().ToString(), &meta)) {
            continue;
        }
        for (const auto& replica : meta.replicas()) {
            if (replica.chunk_id().empty()) {
                continue;
            }
            auto pending_it = pending_set.find(replica.chunk_id());
            if (pending_it == pending_set.end()) {
                continue;
            }
            resolved[*pending_it] = chunk_key;
            pending_set.erase(pending_it);
            if (pending_set.empty()) {
                break;
            }
        }
    }

    rocksdb::WriteBatch batch;
    for (const auto& chunk_id : pending_chunk_ids) {
        auto resolved_it = resolved.find(chunk_id);
        if (resolved_it == resolved.end()) {
            continue;
        }
        batch.Put(ReverseChunkKey(chunk_id), resolved_it->second);
        batch.Delete(ArchiveReverseRepairKey(chunk_id));
    }
    if (batch.Count() == 0) {
        return true;
    }

    std::string write_error;
    if (!store_->WriteBatch(&batch, &write_error)) {
        if (error) {
            *error = write_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::EnqueueReverseChunkRepair(const std::string& chunk_id, std::string* error) {
    if (chunk_id.empty()) {
        return false;
    }
    return store_->Put(ArchiveReverseRepairKey(chunk_id), "1", error);
}

bool OpticalArchiveManager::FindChunkKeyByChunkId(const std::string& chunk_id,
                                                  std::string* chunk_key,
                                                  std::string* error) {
    if (!chunk_key || chunk_id.empty()) {
        if (error) {
            *error = "invalid chunk lookup args";
        }
        return false;
    }
    chunk_key->clear();
    std::string local_error;
    if (store_->Get(ReverseChunkKey(chunk_id), chunk_key, &local_error)) {
        return true;
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    std::string enqueue_error;
    (void)EnqueueReverseChunkRepair(chunk_id, &enqueue_error);
    if (error) {
        *error = enqueue_error.empty()
                     ? "reverse chunk index missing for " + chunk_id + ", queued for repair"
                     : enqueue_error;
    }
    return false;
}

bool OpticalArchiveManager::HasReadyOpticalReplica(const std::string& chunk_key,
                                                   const std::string& chunk_id,
                                                   bool* has_ready,
                                                   std::string* error) {
    if (!has_ready) {
        if (error) {
            *error = "has_ready output is null";
        }
        return false;
    }
    *has_ready = false;
    if (chunk_key.empty()) {
        return true;
    }
    std::string chunk_data;
    std::string local_error;
    if (!store_->Get(chunk_key, &chunk_data, &local_error)) {
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
        return true;
    }
    zb::rpc::ChunkMeta meta;
    if (!MetaCodec::DecodeChunkMeta(chunk_data, &meta)) {
        if (error) {
            *error = "invalid chunk meta " + chunk_key;
        }
        return false;
    }
    for (const auto& replica : meta.replicas()) {
        if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL ||
            replica.replica_state() != zb::rpc::REPLICA_READY) {
            continue;
        }
        if (chunk_id.empty() || replica.chunk_id() == chunk_id) {
            *has_ready = true;
            break;
        }
    }
    return true;
}

bool OpticalArchiveManager::IsOpticalWriteCommitted(const std::string& chunk_id,
                                                    const std::string& op_id,
                                                    bool* committed,
                                                    std::string* error) {
    if (!committed) {
        if (error) {
            *error = "committed output is null";
        }
        return false;
    }
    *committed = false;
    if (chunk_id.empty() || op_id.empty()) {
        return true;
    }
    std::string local_error;
    *committed = store_->Exists(ArchiveOpticalWriteKey(chunk_id, op_id), &local_error);
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::MarkOpticalWriteCommitted(const std::string& chunk_id,
                                                      const std::string& op_id,
                                                      std::string* error) {
    if (chunk_id.empty() || op_id.empty()) {
        return true;
    }
    return store_->Put(ArchiveOpticalWriteKey(chunk_id, op_id), "1", error);
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

bool OpticalArchiveManager::ReadChunkFromReplica(const zb::rpc::ReplicaLocation& source,
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
    zb::rpc::ReadChunkRequest req;
    req.set_disk_id(source.disk_id());
    req.set_chunk_id(source.chunk_id());
    req.set_offset(0);
    req.set_size(read_size);

    zb::rpc::ReadChunkReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    stub.ReadChunk(&cntl, &req, &resp, nullptr);
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

bool OpticalArchiveManager::WriteChunkToOptical(const NodeSelection& optical,
                                                const std::string& chunk_id,
                                                const std::string& op_id,
                                                const std::string& data,
                                                std::string* error) {
    const std::string effective_op_id = !op_id.empty() ? op_id : chunk_id;
    bool already_committed = false;
    if (!IsOpticalWriteCommitted(chunk_id, effective_op_id, &already_committed, error)) {
        return false;
    }
    if (already_committed) {
        return true;
    }

    brpc::Channel* channel = GetChannel(optical.address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteChunkRequest req;
    req.set_disk_id(optical.disk_id);
    req.set_chunk_id(chunk_id);
    req.set_offset(0);
    req.set_data(data);
    req.set_epoch(optical.epoch);
    req.set_archive_op_id(effective_op_id);

    zb::rpc::WriteChunkReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    stub.WriteChunk(&cntl, &req, &resp, nullptr);
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
    return MarkOpticalWriteCommitted(chunk_id, effective_op_id, error);
}

bool OpticalArchiveManager::DeleteDiskReplica(const zb::rpc::ReplicaLocation& replica, std::string* error) {
    brpc::Channel* channel = GetChannel(replica.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::DeleteChunkRequest req;
    req.set_disk_id(replica.disk_id());
    req.set_chunk_id(replica.chunk_id());

    zb::rpc::DeleteChunkReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    stub.DeleteChunk(&cntl, &req, &resp, nullptr);
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
