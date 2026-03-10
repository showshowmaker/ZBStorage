#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

#include "../storage/RocksMetaStore.h"

namespace zb::mds {

class GcManager {
public:
    struct Options {
        uint64_t orphan_grace_ms{10ULL * 60ULL * 1000ULL};
        uint32_t max_delete_per_round{1024};
    };

    explicit GcManager(RocksMetaStore* store);
    explicit GcManager(RocksMetaStore* store, Options options);

    bool RunOnce(std::string* error);

private:
    bool CollectReachableLayoutObjects(std::unordered_set<std::string>* reachable, std::string* error);
    bool SweepUnreachableLayoutObjects(const std::unordered_set<std::string>& reachable,
                                       uint32_t* deleted_count,
                                       std::string* error);
    static uint64_t NowMilliseconds();

    RocksMetaStore* store_{};
    Options options_;
};

} // namespace zb::mds
