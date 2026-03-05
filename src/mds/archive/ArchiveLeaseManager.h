#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "../storage/RocksMetaStore.h"
#include "mds.pb.h"

namespace zb::mds {

class ArchiveLeaseManager {
public:
    struct Options {
        uint64_t default_lease_ms{30000};
        uint64_t min_lease_ms{1000};
        uint64_t max_lease_ms{300000};
    };

    explicit ArchiveLeaseManager(RocksMetaStore* store, Options options = {});

    bool Claim(const zb::rpc::ClaimArchiveTaskRequest& request,
               zb::rpc::ClaimArchiveTaskReply* reply,
               std::string* error);
    bool Renew(const zb::rpc::RenewArchiveLeaseRequest& request,
               zb::rpc::RenewArchiveLeaseReply* reply,
               std::string* error);
    bool Commit(const zb::rpc::CommitArchiveTaskRequest& request,
                zb::rpc::CommitArchiveTaskReply* reply,
                std::string* error);

private:
    bool LoadOrInitRecordLocked(const std::string& chunk_id,
                                zb::rpc::ArchiveLeaseRecord* record,
                                std::string* error) const;
    bool PersistRecordLocked(const zb::rpc::ArchiveLeaseRecord& record, std::string* error) const;
    uint64_t NormalizeLeaseMs(uint64_t requested_lease_ms) const;
    static std::string GenerateLeaseId();
    static uint64_t NowMilliseconds();

    RocksMetaStore* store_{};
    Options options_;
    mutable std::mutex mu_;
};

} // namespace zb::mds
