#pragma once

#include <cstdint>

namespace zb::mds {

struct SystemMemorySnapshot {
    uint64_t total_bytes{0};
    uint64_t available_bytes{0};
};

bool ReadSystemMemorySnapshot(SystemMemorySnapshot* snapshot);

} // namespace zb::mds
