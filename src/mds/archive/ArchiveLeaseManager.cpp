#include "ArchiveLeaseManager.h"

#include <algorithm>
#include <chrono>
#include <random>

#include "../storage/MetaSchema.h"

namespace zb::mds {

ArchiveLeaseManager::ArchiveLeaseManager(RocksMetaStore* store, Options options)
    : store_(store), options_(options) {
    if (options_.min_lease_ms == 0) {
        options_.min_lease_ms = 1;
    }
    if (options_.default_lease_ms < options_.min_lease_ms) {
        options_.default_lease_ms = options_.min_lease_ms;
    }
    if (options_.max_lease_ms < options_.default_lease_ms) {
        options_.max_lease_ms = options_.default_lease_ms;
    }
}

bool ArchiveLeaseManager::Claim(const zb::rpc::ClaimArchiveTaskRequest& request,
                                zb::rpc::ClaimArchiveTaskReply* reply,
                                std::string* error) {
    if (!reply) {
        if (error) {
            *error = "claim reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    if (request.chunk_id().empty()) {
        if (error) {
            *error = "chunk_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(request.chunk_id(), &record, error)) {
        return false;
    }

    const uint64_t now_ms = NowMilliseconds();
    reply->set_granted(false);
    if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVED) {
        *reply->mutable_record() = record;
        return true;
    }
    if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVING && record.lease_expire_ts_ms() > now_ms) {
        if (record.owner_node_id() == request.node_id() &&
            record.owner_disk_id() == request.disk_id()) {
            reply->set_granted(true);
        }
        *reply->mutable_record() = record;
        return true;
    }

    const uint64_t lease_ms = NormalizeLeaseMs(request.requested_lease_ms());
    record.set_state(zb::rpc::ARCHIVE_STATE_ARCHIVING);
    record.set_owner_node_id(request.node_id());
    record.set_owner_node_address(request.node_address());
    record.set_owner_disk_id(request.disk_id());
    record.set_lease_id(GenerateLeaseId());
    record.set_lease_expire_ts_ms(now_ms + lease_ms);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);

    if (!PersistRecordLocked(record, error)) {
        return false;
    }
    reply->set_granted(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::Renew(const zb::rpc::RenewArchiveLeaseRequest& request,
                                zb::rpc::RenewArchiveLeaseReply* reply,
                                std::string* error) {
    if (!reply) {
        if (error) {
            *error = "renew reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    if (request.chunk_id().empty() || request.lease_id().empty()) {
        if (error) {
            *error = "chunk_id or lease_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(request.chunk_id(), &record, error)) {
        return false;
    }

    reply->set_renewed(false);
    const uint64_t now_ms = NowMilliseconds();
    if (record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.lease_id() != request.lease_id() ||
        (!request.node_id().empty() && record.owner_node_id() != request.node_id()) ||
        record.lease_expire_ts_ms() <= now_ms) {
        *reply->mutable_record() = record;
        return true;
    }

    const uint64_t lease_ms = NormalizeLeaseMs(request.requested_lease_ms());
    record.set_lease_expire_ts_ms(now_ms + lease_ms);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);
    if (!PersistRecordLocked(record, error)) {
        return false;
    }

    reply->set_renewed(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::Commit(const zb::rpc::CommitArchiveTaskRequest& request,
                                 zb::rpc::CommitArchiveTaskReply* reply,
                                 std::string* error) {
    if (!reply) {
        if (error) {
            *error = "commit reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    if (request.chunk_id().empty()) {
        if (error) {
            *error = "chunk_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(request.chunk_id(), &record, error)) {
        return false;
    }

    reply->set_committed(false);
    reply->set_idempotent(false);
    if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVED) {
        if (!request.op_id().empty() && record.last_op_id() == request.op_id()) {
            reply->set_committed(true);
            reply->set_idempotent(true);
        }
        *reply->mutable_record() = record;
        return true;
    }

    const uint64_t now_ms = NowMilliseconds();
    if (record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.lease_id() != request.lease_id() ||
        (!request.node_id().empty() && record.owner_node_id() != request.node_id())) {
        *reply->mutable_record() = record;
        return true;
    }

    if (!request.success() && record.lease_expire_ts_ms() <= now_ms) {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    } else if (request.success()) {
        record.set_state(zb::rpc::ARCHIVE_STATE_ARCHIVED);
        record.set_archive_ts_ms(now_ms);
    } else {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    }

    record.set_last_op_id(!request.op_id().empty() ? request.op_id() : record.lease_id());
    record.clear_lease_id();
    record.set_lease_expire_ts_ms(0);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);

    if (!PersistRecordLocked(record, error)) {
        return false;
    }

    reply->set_committed(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::LoadOrInitRecordLocked(const std::string& chunk_id,
                                                 zb::rpc::ArchiveLeaseRecord* record,
                                                 std::string* error) const {
    if (!record) {
        if (error) {
            *error = "record pointer is null";
        }
        return false;
    }
    std::string data;
    std::string get_error;
    if (store_->Get(ArchiveStateKey(chunk_id), &data, &get_error)) {
        if (!record->ParseFromString(data)) {
            if (error) {
                *error = "invalid archive lease record for chunk " + chunk_id;
            }
            return false;
        }
        return true;
    }
    if (!get_error.empty()) {
        if (error) {
            *error = get_error;
        }
        return false;
    }

    record->Clear();
    record->set_chunk_id(chunk_id);
    record->set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    record->set_version(1);
    record->set_update_ts_ms(NowMilliseconds());
    return true;
}

bool ArchiveLeaseManager::PersistRecordLocked(const zb::rpc::ArchiveLeaseRecord& record, std::string* error) const {
    std::string value;
    if (!record.SerializeToString(&value)) {
        if (error) {
            *error = "failed to serialize archive lease record";
        }
        return false;
    }
    return store_->Put(ArchiveStateKey(record.chunk_id()), value, error);
}

uint64_t ArchiveLeaseManager::NormalizeLeaseMs(uint64_t requested_lease_ms) const {
    uint64_t lease_ms = requested_lease_ms > 0 ? requested_lease_ms : options_.default_lease_ms;
    lease_ms = std::max<uint64_t>(lease_ms, options_.min_lease_ms);
    lease_ms = std::min<uint64_t>(lease_ms, options_.max_lease_ms);
    return lease_ms;
}

std::string ArchiveLeaseManager::GenerateLeaseId() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static const char kHex[] = "0123456789abcdef";
    std::string out(32, '0');
    for (size_t i = 0; i < out.size(); i += 16) {
        uint64_t value = rng();
        for (size_t j = 0; j < 16; ++j) {
            out[i + j] = kHex[(value >> ((15 - j) * 4)) & 0xF];
        }
    }
    return out;
}

uint64_t ArchiveLeaseManager::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::mds
