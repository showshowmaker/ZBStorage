#include "OpticalArchiveManager.h"

#include <brpc/controller.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <chrono>
#include <memory>

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
                                             Options options)
    : store_(store), cache_(cache), options_(options) {
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
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek("C/"); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.size() < 2 || key[0] != 'C' || key[1] != '/') {
            break;
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

        if (do_archive && has_disk && !has_optical_ready && source_disk && !optical_nodes.empty() &&
            archived_count < options_.max_chunks_per_round) {
            std::string data;
            uint64_t chunk_size = inode.chunk_size() ? inode.chunk_size() : options_.default_chunk_size;
            if (ReadChunkFromReplica(*source_disk, chunk_size, &data, &local_error)) {
                const NodeSelection& optical = optical_nodes.front();
                if (WriteChunkToOptical(optical, source_disk->chunk_id(), data, &local_error)) {
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
                                                const std::string& data,
                                                std::string* error) {
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
    return true;
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
